"""
Fecal + mental-health text-mining collector.

Sources
-------
1. PubTator3 (NCBI):   search PMIDs → fetch chemical annotations from BioC JSON.
2. EuropePMC:          search articles (OA) → fetch Chemicals annotations.

Query strategy
--------------
  (schizophrenia OR psychosis OR depression OR "major depressive disorder"
   OR bipolar OR anxiety OR PTSD OR autism OR ADHD OR stress)
  AND
  (fecal OR faecal OR stool OR "gut microbiota" OR intestinal OR cecal)
  AND
  (metabolomics OR metabolome OR "LC-MS" OR "GC-MS" OR metabolite)

Dataset ID extraction
---------------------
Each abstract is scanned for:
  - MTBLS[0-9]+  → MetaboLights study IDs
  - ST[0-9]{4,6} → MWB study IDs
Found IDs are saved to outputs/discovered_datasets.json for the orchestrator
to potentially re-queue into the dataset collectors.

Returns (n_links, health_dict).
"""

from __future__ import annotations
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterator

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source, migrate_db,
)
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

_PUBTATOR_BASE = "https://www.ncbi.nlm.nih.gov/research/pubtator3-api"
_EPMC_SEARCH   = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
_EPMC_ANN      = "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"

# ── Query builders ────────────────────────────────────────────────────────────

_FECAL_MENTAL_QUERIES = [
    # PubTator / broad search
    ("(schizophrenia OR psychosis OR schizoaffective) AND "
     "(fecal OR faecal OR stool OR cecal OR gut microbiota) AND "
     "(metabolomics OR metabolome OR metabolite OR LC-MS OR GC-MS)"),
    ("(depression OR \"major depressive disorder\" OR bipolar OR anxiety OR PTSD) AND "
     "(fecal OR faecal OR stool OR gut OR intestinal) AND "
     "(metabolomics OR metabolome OR metabolite)"),
    ("(autism OR ASD OR ADHD OR stress OR CUMS OR \"social defeat\") AND "
     "(fecal OR faecal OR stool OR gut OR cecal) AND "
     "(metabolomics OR metabolome OR metabolite)"),
]

_EPMC_QUERIES = [
    ("(schizophrenia OR psychosis OR depression OR bipolar) AND "
     "(fecal OR faecal OR stool OR cecal) AND metabolomics AND OPEN_ACCESS:y"),
    ("(autism OR anxiety OR PTSD OR ADHD OR stress) AND "
     "(fecal OR faecal OR stool OR gut microbiota) AND metabolomics AND OPEN_ACCESS:y"),
]

_CONDITION_MAP: dict[str, str] = {
    "schizophrenia": "schizophrenia",
    "psychosis":     "schizophrenia",
    "schizoaffective": "schizophrenia",
    "depression":    "depression",
    "major depressive": "depression",
    "mdd":           "depression",
    "bipolar":       "bipolar",
    "anxiety":       "anxiety",
    "ptsd":          "ptsd",
    "autism":        "autism",
    "asd":           "autism",
    "adhd":          "adhd",
    "stress":        "stress",
    "social defeat": "stress",
    "cums":          "stress",
}

# Patterns to detect dataset IDs in free text
_MTBLS_RE = re.compile(r"\bMTBLS\d{3,6}\b")
_MWB_RE   = re.compile(r"\bST\d{4,6}\b")


def _condition_hits(text: str) -> list[str]:
    t = text.lower()
    return sorted({v for k, v in _CONDITION_MAP.items() if k in t})


def _extract_dataset_ids(text: str) -> dict[str, list[str]]:
    """Scan text for dataset accession numbers."""
    mtbls = list(set(_MTBLS_RE.findall(text)))
    mwb   = list(set(_MWB_RE.findall(text)))
    return {"MTBLS": mtbls, "MWB": mwb}


# ── PubTator helpers ──────────────────────────────────────────────────────────

def _pubtator_pmids(http: HTTPClient, query: str, max_results: int) -> Iterator[str]:
    page, fetched = 1, 0
    while True:
        try:
            data = http.get_json(
                f"{_PUBTATOR_BASE}/search/",
                params={"text": query, "page": page, "size": 100},
            )
        except Exception as exc:
            logger.debug("PubTator search p%d: %s", page, exc)
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

        total = data.get("total", 0)
        if fetched >= total or page * 100 >= total:
            break
        page += 1


def _pubtator_annotations(http: HTTPClient, pmids: list[str]) -> dict[str, dict]:
    if not pmids:
        return {}
    batch = ",".join(pmids[:100])
    try:
        data = http.get_json(
            f"{_PUBTATOR_BASE}/publications/export/biocjson",
            params={"pmids": batch},
        )
    except Exception as exc:
        logger.debug("PubTator annotations batch: %s", exc)
        return {}

    results: dict[str, dict] = {}
    docs = data if isinstance(data, list) else data.get("PubTator3", [])

    for doc in docs:
        pmid = str(doc.get("id", "")).strip()
        if not pmid:
            continue
        title, abstract, chemicals = "", "", []
        for passage in doc.get("passages", []):
            ptype = passage.get("infons", {}).get("type", "").lower()
            text  = passage.get("text", "")
            if "title" in ptype:
                title = text
            elif "abstract" in ptype:
                abstract = text
            for ann in passage.get("annotations", []):
                atype = ann.get("infons", {}).get("type", "").lower()
                if atype not in ("chemical", "chebi", "mesh"):
                    continue
                name = ann.get("text", "").strip()
                mesh_id = ann.get("infons", {}).get("identifier") or ann.get("infons", {}).get("MESH")
                if name:
                    chemicals.append({"name": name, "mesh_id": mesh_id or None})
        results[pmid] = {
            "title": title, "abstract": abstract, "chemicals": chemicals,
        }
    return results


# ── EuropePMC helpers ─────────────────────────────────────────────────────────

def _epmc_search(
    http: HTTPClient, query: str, page_size: int, max_results: int
) -> list[dict]:
    articles: list[dict] = []
    cursor = "*"
    while True:
        try:
            data = http.get_json(
                _EPMC_SEARCH,
                params={
                    "query":      query,
                    "format":     "json",
                    "resultType": "core",
                    "pageSize":   str(page_size),
                    "cursorMark": cursor,
                },
            )
        except Exception as exc:
            logger.debug("EuropePMC search: %s", exc)
            break
        results     = data.get("resultList", {}).get("result", [])
        next_cursor = data.get("nextCursorMark", "")
        articles.extend(results)
        if len(articles) >= max_results or not results or next_cursor == cursor:
            break
        cursor = next_cursor
    return articles[:max_results]


def _epmc_annotations(http: HTTPClient, pmcid: str, pmid: str) -> list[dict]:
    parts = []
    if pmcid:
        parts.append(f"PMC:{pmcid}")
    if pmid:
        parts.append(f"MED:{pmid}")
    if not parts:
        return []
    try:
        data = http.get_json(
            _EPMC_ANN,
            params={"articleIds": ",".join(parts), "type": "Chemicals", "format": "JSON"},
        )
        if isinstance(data, list) and data:
            return data[0].get("annotations", [])
        return []
    except Exception:
        return []


# ── Main collector ─────────────────────────────────────────────────────────────

def collect(cfg: dict, db_path: str) -> tuple[int, dict]:
    """Run fecal+MH text-mining collector. Returns (n_links, health_dict)."""
    migrate_db(db_path)

    fm_cfg      = cfg.get("fecal_mental", {})
    epm_cfg     = cfg.get("europe_pmc", {})
    max_papers  = fm_cfg.get("max_papers_text_mining", 500)
    max_pt_pmids = max_papers // 2   # split budget between PubTator and EuropePMC
    max_epmc    = max_papers - max_pt_pmids

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    health: dict[str, Any] = {
        "papers_found":          0,
        "chemicals_extracted":   0,
        "dataset_ids_detected":  [],
        "last_error":            "",
    }
    discovered: dict[str, list[str]] = {"MTBLS": [], "MWB": []}

    n_links = 0

    # ════════════════════════════════════════════════════════
    # A) PubTator3
    # ════════════════════════════════════════════════════════
    all_pmids: set[str] = set()
    for q in _FECAL_MENTAL_QUERIES:
        for pmid in _pubtator_pmids(http, q, max_pt_pmids):
            all_pmids.add(pmid)
        logger.info("PubTator fecal-MH: %d PMIDs so far (q: %s...)", len(all_pmids), q[:40])
        if len(all_pmids) >= max_pt_pmids:
            break

    pmid_list = list(all_pmids)
    all_annots: dict[str, dict] = {}
    for i in range(0, len(pmid_list), 100):
        batch_annots = _pubtator_annotations(http, pmid_list[i:i+100])
        all_annots.update(batch_annots)

    logger.info("PubTator fecal-MH: %d PMIDs annotated", len(all_annots))

    with get_conn(db_path) as conn:
        for pmid, doc in all_annots.items():
            if not doc["chemicals"]:
                continue
            health["papers_found"] += 1
            combined_text = f"{doc['title']} {doc['abstract']}"
            cond = _condition_hits(combined_text)
            if not cond:
                cond = ["mental_health"]

            # Dataset ID detection
            ds = _extract_dataset_ids(combined_text)
            for ids in ds["MTBLS"]:
                if ids not in discovered["MTBLS"]:
                    discovered["MTBLS"].append(ids)
            for ids in ds["MWB"]:
                if ids not in discovered["MWB"]:
                    discovered["MWB"].append(ids)

            source_id = upsert_source(
                conn,
                source_type="PubTator",
                source_ref=f"PMID:{pmid}",
                title=doc["title"] or None,
            )

            seen: set[str] = set()
            for chem in doc["chemicals"]:
                name = chem["name"].strip()
                if not name or len(name) < 3:
                    continue
                nkey = make_key(name)
                if nkey in seen:
                    continue
                seen.add(nkey)

                canon = normalize(name)
                tags  = {
                    "mental_health":       True,
                    "schizophrenia":       "schizophrenia" in cond,
                    "fecal_hint":          True,
                    "fecal_evidence_type": "text_mining_claim",
                    "from_text_mining":    True,
                    "condition_hits":      cond,
                }
                mid = upsert_metabolite(conn, canon, nkey, tags=tags)
                add_synonym(conn, mid, name, nkey)
                if chem.get("mesh_id"):
                    add_synonym(conn, mid, chem["mesh_id"], make_key(chem["mesh_id"]))

                link_metabolite_source(
                    conn, mid, source_id,
                    evidence_tag="text_mining_fecal_abstract",
                )
                n_links += 1
                health["chemicals_extracted"] += 1

    # ════════════════════════════════════════════════════════
    # B) EuropePMC
    # ════════════════════════════════════════════════════════
    for epmc_query in _EPMC_QUERIES:
        if health["papers_found"] >= max_papers:
            break
        articles = _epmc_search(http, epmc_query, page_size=100, max_results=max_epmc // len(_EPMC_QUERIES))
        logger.info("EuropePMC fecal-MH: %d articles (q: %s...)", len(articles), epmc_query[:50])

        with get_conn(db_path) as conn:
            for art in articles:
                pmid  = art.get("pmid",  "")
                pmcid = art.get("pmcid", "")
                title = art.get("title",  "")
                abstract = art.get("abstractText", "")
                combined_text = f"{title} {abstract}"
                cond = _condition_hits(combined_text)
                if not cond:
                    cond = ["mental_health"]

                ds = _extract_dataset_ids(combined_text)
                for ids in ds["MTBLS"]:
                    if ids not in discovered["MTBLS"]:
                        discovered["MTBLS"].append(ids)
                for ids in ds["MWB"]:
                    if ids not in discovered["MWB"]:
                        discovered["MWB"].append(ids)

                ref_label = pmcid or pmid or title[:60]
                source_id = upsert_source(
                    conn,
                    source_type="EuropePMC",
                    source_ref=ref_label,
                    title=title or None,
                    year=int(str(art.get("pubYear",""))[:4]) if str(art.get("pubYear",""))[:4].isdigit() else None,
                )

                anns = _epmc_annotations(http, pmcid, pmid)
                if not anns:
                    continue
                health["papers_found"] += 1

                seen: set[str] = set()
                for ann in anns:
                    name = (ann.get("exact") or ann.get("name") or "").strip()
                    if not name or len(name) < 3:
                        continue
                    nkey = make_key(name)
                    if nkey in seen:
                        continue
                    seen.add(nkey)

                    # Extract ChEBI ID if available
                    chebi_id = None
                    for tag in ann.get("tags", []):
                        uri = tag.get("uri", "")
                        if "CHEBI" in uri.upper():
                            chebi_id = uri.split("/")[-1]

                    canon = normalize(name)
                    tags  = {
                        "mental_health":       True,
                        "schizophrenia":       "schizophrenia" in cond,
                        "fecal_hint":          True,
                        "fecal_evidence_type": "text_mining_claim",
                        "from_text_mining":    True,
                        "condition_hits":      cond,
                    }
                    mid = upsert_metabolite(conn, canon, nkey, tags=tags)
                    add_synonym(conn, mid, name, nkey)
                    if chebi_id:
                        add_synonym(conn, mid, chebi_id, make_key(chebi_id))

                    link_metabolite_source(
                        conn, mid, source_id,
                        evidence_tag="text_mining_fecal_abstract",
                    )
                    n_links += 1
                    health["chemicals_extracted"] += 1

    # ── Save discovered dataset IDs ───────────────────────────────────────────
    health["dataset_ids_detected"] = (
        [f"MTBLS:{i}" for i in discovered["MTBLS"]] +
        [f"MWB:{i}"   for i in discovered["MWB"]]
    )
    disc_path = Path(cfg["paths"]["outputs"]) / "discovered_datasets.json"
    try:
        existing: dict = {}
        if disc_path.exists():
            existing = json.loads(disc_path.read_text(encoding="utf-8"))
        for k, v in discovered.items():
            existing[k] = sorted(set(existing.get(k, []) + v))
        disc_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Discovered dataset IDs saved to %s: %s", disc_path, health["dataset_ids_detected"][:10])
    except Exception as exc:
        logger.warning("Could not write discovered_datasets.json: %s", exc)

    logger.info("Fecal text-mining: %d links | health=%s", n_links, health)
    return n_links, health
