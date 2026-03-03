"""
MetaboLights (EBI) — fecal + mental-health focused collector.

Search strategy
---------------
1. EBI Search service (domain=metabolights) — proper keyword search returning
   MTBLS accession IDs: https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights
   Search separately by mental-health terms and by fecal terms, then intersect
   or keep all and filter by metadata.
2. For each MTBLS candidate:
   a. GET /metabolights/ws/studies/{id}
      Metadata lives in response['isaInvestigation']['studies'][0]
      (fields: title, description, studyDesignDescriptors, etc.)
   b. Confirm fecal matrix + mental-health relevance.
   c. GET /metabolights/ws/studies/{id}/files — find annotation TSVs
   d. Parse TSVs for metabolite names.

Returns (n_links, health_dict).
"""

from __future__ import annotations
import csv
import io
import logging
import re
from typing import Any, Optional

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source, migrate_db,
)
from src.matrix_parser import detect_matrix, matrix_result_to_hint_str
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

_EBI_SEARCH  = "https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights"
_ML_BASE     = "https://www.ebi.ac.uk/metabolights/ws"

_MH_TERMS = [
    "schizophrenia", "psychosis", "schizoaffective",
    "depression", "bipolar", "anxiety", "ptsd",
    "autism", "adhd", "stress", "psychiatric",
]

_FECAL_TERMS_SEARCH = ["fecal", "faecal", "stool", "cecal", "gut microbiota"]

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
    "cums":          "stress",
    "social defeat": "stress",
}

_FECAL_DETECT = (
    "fec", "stool", "faec", "cecal", "caecal",
    "cecum", "intestinal content", "gut content", "copro",
)

_MET_NAME_COL = re.compile(
    r"(metabolite.?name|compound.?name|feature.?name|chemical.?name"
    r"|annotation|^name$|identification)",
    re.IGNORECASE,
)
_INCHIKEY_COL = re.compile(r"inchi.?key", re.IGNORECASE)
_DB_ID_COL    = re.compile(r"(database.?identifier|hmdb|chebi|kegg|pubchem)", re.IGNORECASE)


def _condition_hits(text: str) -> list[str]:
    t = text.lower()
    return sorted({v for k, v in _CONDITION_MAP.items() if k in t})


def _is_fecal(text: str) -> bool:
    t = text.lower()
    return any(term in t for term in _FECAL_DETECT)


def _ebi_search(http: HTTPClient, query: str, max_hits: int = 200) -> list[str]:
    """Search EBI metabolights domain and return MTBLS accession IDs."""
    ids: list[str] = []
    try:
        data = http.get_json(
            _EBI_SEARCH,
            params={"query": query, "format": "json", "size": min(max_hits, 200)},
        )
        for entry in data.get("entries", []):
            acc = entry.get("id", "").strip()
            if acc.startswith("MTBLS"):
                ids.append(acc)
    except Exception as exc:
        logger.debug("EBI Search '%s': %s", query[:40], exc)
    return ids


def _ml_metadata(http: HTTPClient, sid: str) -> Optional[dict]:
    """
    Fetch MetaboLights study metadata.
    Title + description live in isaInvestigation.studies[0].
    """
    try:
        data = http.get_json(f"{_ML_BASE}/studies/{sid}")
        isa  = data.get("isaInvestigation", {})
        stud = isa.get("studies", [])
        if stud and isinstance(stud, list):
            s = stud[0]
            return {
                "title":       s.get("title", ""),
                "description": s.get("description", ""),
                "design":      " ".join(
                    d.get("annotationValue", "")
                    for d in s.get("studyDesignDescriptors", [])
                    if isinstance(d, dict)
                ),
            }
    except Exception as exc:
        logger.debug("ML metadata %s: %s", sid, exc)
    return None


def _parse_tsv(content: str) -> list[dict]:
    lines = [l for l in content.splitlines() if not l.startswith("#")]
    if not lines:
        return []
    try:
        return list(csv.DictReader(io.StringIO("\n".join(lines)), delimiter="\t"))
    except Exception:
        return []


def _find_col(headers: list[str], pattern: re.Pattern) -> Optional[str]:
    for h in headers:
        if pattern.search(h):
            return h
    return None


def collect(cfg: dict, db_path: str) -> tuple[int, dict]:
    """Run fecal-MH MetaboLights collector. Returns (n_links, health_dict)."""
    migrate_db(db_path)

    fm_cfg    = cfg.get("fecal_mental", {})
    max_total = fm_cfg.get("max_total_studies", 400)

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    health: dict[str, Any] = {
        "studies_found":       0,
        "studies_fecal":       0,
        "files_parsed":        0,
        "metabolites_extracted": 0,
        "last_error":          "",
    }

    # ── 1. Discover MTBLS IDs via EBI Search ────────────────────────────────
    candidate_ids: set[str] = set()

    # a) Mental-health terms
    for term in _MH_TERMS:
        ids = _ebi_search(http, term, max_hits=100)
        candidate_ids.update(ids)
        if ids:
            logger.debug("EBI Search '%s': %d MTBLS", term, len(ids))

    # b) Fecal terms
    for term in _FECAL_TERMS_SEARCH:
        ids = _ebi_search(http, term, max_hits=100)
        candidate_ids.update(ids)

    # c) Combination queries
    for combo in ["fecal schizophrenia", "stool depression", "gut microbiota mental health",
                  "fecal metabolomics psychiatric", "cecal metabolomics"]:
        ids = _ebi_search(http, combo, max_hits=50)
        candidate_ids.update(ids)

    health["studies_found"] = len(candidate_ids)
    logger.info("MetaboLights fecal: %d candidate studies from EBI Search", len(candidate_ids))

    if not candidate_ids:
        logger.warning("MetaboLights fecal: EBI Search returned 0 MTBLS IDs")
        return 0, health

    # ── 2. Filter by metadata + extract metabolites ───────────────────────────
    n_links = 0
    processed = 0
    with get_conn(db_path) as conn:
        for sid in sorted(candidate_ids):
            if processed >= max_total:
                break
            processed += 1

            meta = _ml_metadata(http, sid)
            if not meta:
                continue

            combined = f"{meta['title']} {meta['description']} {meta['design']}"

            # Confirm fecal
            mres = detect_matrix(meta["title"], meta["description"], meta["design"])
            if not (mres.fecal_hint or _is_fecal(combined)):
                continue

            cond = _condition_hits(combined)
            if not cond:
                cond = ["mental_health"]

            matrix_hint = matrix_result_to_hint_str(mres) or "fecal"
            method_hint = mres.method_hint
            health["studies_fecal"] += 1
            logger.info("MetaboLights fecal: %s '%s' matrix=%s cond=%s",
                        sid, meta["title"][:55], matrix_hint, cond)

            source_id = upsert_source(
                conn,
                source_type="MetaboLights",
                source_ref=sid,
                title=meta["title"] or None,
                matrix_hint=matrix_hint,
                method_hint=method_hint,
            )

            # ── 3. Get annotation TSVs ────────────────────────────────────────
            try:
                files_data = http.get_json(f"{_ML_BASE}/studies/{sid}/files")
                if isinstance(files_data, dict):
                    file_list = files_data.get("study", files_data.get("content", []))
                elif isinstance(files_data, list):
                    file_list = files_data
                else:
                    file_list = []
            except Exception as exc:
                health["last_error"] = str(exc)[:200]
                logger.debug("ML files %s: %s", sid, exc)
                continue

            ann_files = [
                f for f in file_list
                if isinstance(f, dict)
                and f.get("file", "").endswith(".tsv")
                and any(
                    tag in f.get("type", "").lower() + f.get("file", "").lower()
                    for tag in ("annotation", "assay", "metabolite", "a_", "m_")
                )
            ]

            if not ann_files:
                logger.debug("ML %s: no annotation TSVs", sid)
                continue

            for finfo in ann_files[:3]:   # max 3 files per study
                fname    = finfo.get("file", "")
                file_url = f"{_ML_BASE}/studies/{sid}/download?file={fname}"
                try:
                    content_tsv = http.get_text(file_url)
                except Exception as exc:
                    health["last_error"] = str(exc)[:200]
                    continue

                rows_data = _parse_tsv(content_tsv)
                if not rows_data:
                    continue

                headers   = list(rows_data[0].keys())
                name_col  = _find_col(headers, _MET_NAME_COL)
                ikey_col  = _find_col(headers, _INCHIKEY_COL)
                health["files_parsed"] += 1

                if not name_col:
                    continue

                for row in rows_data:
                    name = (row.get(name_col) or "").strip()
                    if not name or name.lower() in ("", "unknown", "n/a", "na", "-"):
                        continue

                    inchikey = None
                    if ikey_col:
                        ik = (row.get(ikey_col) or "").strip()
                        if len(ik) == 27 and ik[14] == "-":
                            inchikey = ik

                    canon = normalize(name)
                    nkey  = make_key(name)
                    tags  = {
                        "mental_health":       True,
                        "schizophrenia":       "schizophrenia" in cond,
                        "fecal_hint":          True,
                        "fecal_evidence_type": "dataset_metadata",
                        "condition_hits":      cond,
                    }
                    mid = upsert_metabolite(conn, canon, nkey, inchikey=inchikey, tags=tags)
                    add_synonym(conn, mid, name, nkey)

                    for col in headers:
                        if _DB_ID_COL.search(col):
                            val = (row.get(col) or "").strip()
                            if val and val.lower() not in ("", "n/a", "na", "-"):
                                add_synonym(conn, mid, val, make_key(val))

                    link_metabolite_source(
                        conn, mid, source_id,
                        evidence_tag="fecal_study_metabolite_list",
                    )
                    n_links += 1
                    health["metabolites_extracted"] += 1

    logger.info("MetaboLights fecal: %d links | health=%s", n_links, health)
    return n_links, health
