"""
PubTator3 (NCBI) text-mining collector.

Uses the NCBI PubTator3 REST API to:
  1. Search for PMIDs relevant to mental-health + metabolomics.
  2. Fetch chemical entity annotations for each PMID (abstracts + OA fulltext).
  3. Insert chemicals as metabolite candidates, flagged from_text_mining=True.

API docs: https://www.ncbi.nlm.nih.gov/research/pubtator3/api

Key endpoints:
  GET /research/pubtator3-api/search/?text=...&page=1&size=100
  GET /research/pubtator3-api/publications/export/biocjson?pmids=P1,P2,...

Rate limit: ~3 req/s without API key.
"""

from __future__ import annotations
import json
import logging
from typing import Iterator

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source,
)
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

_BASE = "https://www.ncbi.nlm.nih.gov/research/pubtator3-api"

SCHIZ_TERMS = {"schizophrenia", "psychosis", "schizoaffective", "first episode psychosis"}


def _is_schizophrenia(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in SCHIZ_TERMS)


def _build_queries(keywords: list[str]) -> list[str]:
    """
    Build search queries. PubTator3 search is keyword-based.
    We combine mental-health terms with metabolomics.
    """
    # Group schizophrenia terms together, then broader mental health
    schiz_q = "(schizophrenia OR psychosis OR schizoaffective) AND (metabolomics OR metabolite OR biomarker)"
    broader_q = "(bipolar OR depression OR anxiety OR autism OR ADHD) AND (metabolomics OR metabolite OR biomarker)"
    gut_q = "(schizophrenia OR psychosis OR depression) AND (gut microbiota OR fecal metabolome OR stool metabolomics)"
    return [schiz_q, broader_q, gut_q]


def _iter_pmids(
    http: HTTPClient,
    query: str,
    max_results: int,
) -> Iterator[str]:
    """Paginate PubTator3 search and yield PMIDs."""
    page = 1
    fetched = 0
    while True:
        try:
            data = http.get_json(
                f"{_BASE}/search/",
                params={"text": query, "page": page, "size": 100},
            )
        except Exception as exc:
            logger.warning("PubTator3 search error (q=%s, page=%d): %s", query[:40], page, exc)
            break

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            pmid = str(item.get("pmid", "")).strip()
            if pmid:
                yield pmid
                fetched += 1
                if max_results and fetched >= max_results:
                    return

        # Check if there are more pages
        total = data.get("total", 0)
        if fetched >= total or page * 100 >= total:
            break
        page += 1


def _fetch_annotations(
    http: HTTPClient,
    pmids: list[str],
) -> dict[str, dict]:
    """
    Fetch BioC JSON annotations for a batch of PMIDs.
    Returns dict: pmid -> {title, abstract, chemicals: [name, mesh_id, ...]}
    """
    if not pmids:
        return {}

    batch = ",".join(pmids[:100])   # max 100 per request
    try:
        data = http.get_json(
            f"{_BASE}/publications/export/biocjson",
            params={"pmids": batch},
        )
    except Exception as exc:
        logger.debug("PubTator3 annotations fetch failed for batch: %s", exc)
        return {}

    results: dict[str, dict] = {}
    # BioC JSON format: data is a list of documents
    docs = data if isinstance(data, list) else data.get("PubTator3", [])

    for doc in docs:
        pmid = str(doc.get("id", "")).strip()
        if not pmid:
            continue

        # Extract title + abstract text
        title = ""
        abstract = ""
        chemicals: list[dict] = []

        for passage in doc.get("passages", []):
            infons = passage.get("infons", {})
            ptype  = infons.get("type", "").lower()
            text   = passage.get("text", "")

            if "title" in ptype:
                title = text
            elif "abstract" in ptype:
                abstract = text

            # Annotations in this passage
            for ann in passage.get("annotations", []):
                ann_infons = ann.get("infons", {})
                ann_type   = ann_infons.get("type", "").lower()
                if ann_type not in ("chemical", "chebi", "mesh"):
                    continue
                name    = ann.get("text", "").strip()
                mesh_id = ann_infons.get("identifier", "") or ann_infons.get("MESH", "")
                if name:
                    chemicals.append({"name": name, "mesh_id": mesh_id or None})

        results[pmid] = {
            "title":     title,
            "abstract":  abstract,
            "chemicals": chemicals,
        }

    return results


def collect(cfg: dict, db_path: str) -> int:
    """Run PubTator3 collector. Returns n_links inserted."""
    pt_cfg   = cfg.get("pubtator", {})
    max_pmids = (cfg.get("max_records_per_source") or {}).get("pubtator", 300)
    keywords  = cfg.get("mental_health_keywords", ["schizophrenia"])

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    queries = _build_queries(keywords)
    all_pmids: set[str] = set()
    for q in queries:
        for pmid in _iter_pmids(http, q, max_pmids):
            all_pmids.add(pmid)
        logger.info("PubTator3: %d PMIDs collected so far (query: %s…)", len(all_pmids), q[:40])

    logger.info("PubTator3: %d unique PMIDs to annotate", len(all_pmids))

    # Fetch annotations in batches of 100
    pmid_list = list(all_pmids)
    all_annots: dict[str, dict] = {}
    for i in range(0, len(pmid_list), 100):
        batch = pmid_list[i : i + 100]
        annots = _fetch_annotations(http, batch)
        all_annots.update(annots)
        logger.info("PubTator3: annotated %d / %d PMIDs", len(all_annots), len(pmid_list))

    n_links = 0
    with get_conn(db_path) as conn:
        for pmid, doc in all_annots.items():
            if not doc["chemicals"]:
                continue

            schiz_hit = _is_schizophrenia(f"{doc['title']} {doc['abstract']}")
            source_id = upsert_source(
                conn,
                source_type="PubTator",
                source_ref=f"PMID:{pmid}",
                title=doc["title"] or None,
            )

            seen_names: set[str] = set()
            for chem in doc["chemicals"]:
                name = chem["name"].strip()
                if not name or len(name) < 3:
                    continue
                nkey = make_key(name)
                if nkey in seen_names:
                    continue
                seen_names.add(nkey)

                canon = normalize(name)
                tags  = {
                    "mental_health":    True,
                    "schizophrenia":    schiz_hit,
                    "from_text_mining": True,
                    "fecal_hint":       False,
                }
                mid = upsert_metabolite(conn, canon, nkey, tags=tags)
                add_synonym(conn, mid, name, nkey)
                if chem.get("mesh_id"):
                    add_synonym(conn, mid, chem["mesh_id"], make_key(chem["mesh_id"]))

                link_metabolite_source(conn, mid, source_id, evidence_tag="text_mining_abstract")
                n_links += 1

    logger.info("PubTator3: inserted %d metabolite-source links", n_links)
    return n_links
