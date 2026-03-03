"""
Europe PMC collector — OA text mining of chemical entities.

Strategy
--------
1. Search Europe PMC for articles matching mental-health + metabolomics queries.
   Restrict to Open Access when possible (OPEN_ACCESS:y).
2. For each article, call the Annotations API to extract ChEBI/chemical
   entity annotations from abstract + OA full text.
3. Insert extracted chemical names as metabolite candidates, flagged
   from_text_mining=true (expect noise; no aggressive filtering).

Europe PMC Search API:
  GET /webservices/rest/search?query=...&format=json&resultType=core&pageSize=N

Europe PMC Annotations API:
  GET /annotations_api/annotationsByArticleIds?articleIds=PMC:PMCID,MED:PMID
  &type=Chemicals&format=JSON
"""

from __future__ import annotations
import logging
from typing import Iterator, Optional

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source,
)
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

SCHIZ_TERMS = {"schizophrenia", "psychosis", "schizoaffective"}

_NOISE_NAMES = frozenset({
    "glucose", "water", "ethanol", "oxygen", "carbon dioxide",
    "sodium", "potassium", "calcium", "chloride", "phosphate",
    "methanol", "acetonitrile", "formic acid", "acetic acid",
    "urea", "creatinine",          # not noise per se but ubiquitous — keep them, they may matter
})


def _is_schizophrenia(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in SCHIZ_TERMS)


def _build_search_query(keywords: list[str]) -> str:
    disease_part = " OR ".join(f'"{kw}"' for kw in keywords)
    return (
        f"({disease_part}) AND metabolomics AND (biomarker OR markers) "
        "AND OPEN_ACCESS:y"
    )


def _iter_search_results(
    http: HTTPClient,
    search_base: str,
    query: str,
    page_size: int,
    max_results: int,
) -> Iterator[dict]:
    """Paginate through Europe PMC search results."""
    fetched = 0
    cursor  = "*"
    while True:
        params = {
            "query":      query,
            "format":     "json",
            "resultType": "core",
            "pageSize":   str(page_size),
            "cursorMark": cursor,
        }
        try:
            data = http.get_json(search_base, params=params)
        except Exception as exc:
            logger.warning("EuropePMC search error (cursor=%s): %s", cursor, exc)
            break

        results   = data.get("resultList", {}).get("result", [])
        next_cursor = data.get("nextCursorMark", "")

        for item in results:
            yield item
            fetched += 1
            if max_results and fetched >= max_results:
                return

        if not results or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor


def _get_annotations(
    http: HTTPClient,
    ann_base: str,
    pmcid: Optional[str],
    pmid: Optional[str],
) -> list[dict]:
    """Fetch chemical annotations for one article."""
    if not pmcid and not pmid:
        return []

    article_ids_parts = []
    if pmcid:
        article_ids_parts.append(f"PMC:{pmcid}")
    if pmid:
        article_ids_parts.append(f"MED:{pmid}")
    article_ids = ",".join(article_ids_parts)

    params = {
        "articleIds": article_ids,
        "type":       "Chemicals",
        "format":     "JSON",
    }
    try:
        data = http.get_json(ann_base, params=params)
        if isinstance(data, list) and data:
            return data[0].get("annotations", [])
        return []
    except Exception as exc:
        logger.debug("EuropePMC annotations failed for %s: %s", article_ids, exc)
        return []


def collect(cfg: dict, db_path: str) -> int:
    epm_cfg     = cfg.get("europe_pmc", {})
    search_base = epm_cfg.get("search_base", "https://www.ebi.ac.uk/europepmc/webservices/rest/search")
    ann_base    = epm_cfg.get("annotations_base", "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds")
    page_size   = epm_cfg.get("page_size", 100)
    max_results = (cfg.get("max_records_per_source") or {}).get("europe_pmc", 500)
    keywords    = cfg.get("mental_health_keywords", ["schizophrenia"])

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    query = _build_search_query(keywords)
    logger.info("EuropePMC: query = %s", query)

    n_links = 0
    with get_conn(db_path) as conn:
        for article in _iter_search_results(http, search_base, query, page_size, max_results):
            pmid  = article.get("pmid",  "")
            pmcid = article.get("pmcid", "")
            title = article.get("title",  "")
            year_raw = article.get("pubYear") or article.get("firstPublicationDate", "")
            year = int(str(year_raw)[:4]) if str(year_raw)[:4].isdigit() else None
            journal = article.get("journalTitle", "")

            ref_label = pmcid or pmid or title[:60]
            schiz_hit = _is_schizophrenia(f"{title} {article.get('abstractText','')}")

            source_id = upsert_source(
                conn,
                source_type="EuropePMC",
                source_ref=ref_label,
                title=title,
                year=year,
            )

            annotations = _get_annotations(http, ann_base, pmcid, pmid)
            if not annotations:
                logger.debug("EuropePMC %s: no chemical annotations", ref_label)
                continue

            for ann in annotations:
                name = (ann.get("exact") or ann.get("name") or "").strip()
                if not name or len(name) < 3:
                    continue

                # Extract ChEBI ID if present
                chebi_id = None
                tags_ann = ann.get("tags", [])
                for tag in tags_ann:
                    uri = tag.get("uri", "")
                    if "CHEBI" in uri.upper():
                        chebi_id = uri.split("/")[-1]  # e.g. "CHEBI:15422"

                canon = normalize(name)
                nkey  = make_key(name)
                tags  = {
                    "mental_health":    True,
                    "schizophrenia":    schiz_hit,
                    "from_text_mining": True,
                    "fecal_hint":       False,
                }
                mid = upsert_metabolite(conn, canon, nkey, tags=tags)
                add_synonym(conn, mid, name, nkey)
                if chebi_id:
                    add_synonym(conn, mid, chebi_id, make_key(chebi_id))

                link_metabolite_source(
                    conn, mid, source_id, evidence_tag="text_mining"
                )
                n_links += 1

    logger.info("EuropePMC: inserted %d metabolite-source links", n_links)
    return n_links
