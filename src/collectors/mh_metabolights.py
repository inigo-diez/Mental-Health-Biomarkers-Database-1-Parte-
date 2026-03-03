"""
MetaboLights — Mental-Health Biomarkers collector.

Strategy
--------
1. EBI Search (metabolights domain) with ALL whitelist terms + metabolomics
   combinations.  No fecal requirement — any biological matrix accepted.
2. For each MTBLS study:
   a. Fetch metadata (title, description, study design descriptors).
   b. SKIP if detect_conditions() returns [] — strict whitelist, no fallback.
   c. Download annotation TSVs, parse metabolite names.
3. Tag metabolites with specific conditions (schizophrenia, depression, …).

Returns (n_links, health_dict).
"""

from __future__ import annotations
import csv
import io
import logging
import re
from typing import Any, Optional

from src.conditions import detect_conditions, tags_from_conditions
from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source, migrate_db,
)
from src.matrix_parser import detect_matrix, matrix_result_to_hint_str
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

_EBI_SEARCH = "https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights"
_ML_BASE    = "https://www.ebi.ac.uk/metabolights/ws"

# ── Search terms — cover every whitelist condition + metabolomics queries ─────
_MH_SEARCH_TERMS: list[str] = [
    # Direct condition terms
    "schizophrenia", "psychosis", "schizoaffective", "first episode psychosis",
    "depression", "major depressive disorder",
    "bipolar disorder", "bipolar",
    "anxiety disorder", "ptsd", "post-traumatic stress disorder",
    "autism spectrum disorder", "autism",
    "adhd", "attention deficit hyperactivity",
    # Condition + metabolomics (higher precision)
    "schizophrenia metabolomics", "psychosis metabolomics",
    "depression metabolomics", "major depressive metabolomics",
    "bipolar metabolomics", "bipolar disorder metabolome",
    "anxiety metabolomics", "ptsd metabolomics",
    "autism metabolomics", "adhd metabolomics",
    # Biomarker focused
    "schizophrenia biomarker", "depression biomarker",
    "psychiatric biomarker", "mental health metabolomics",
    "psychiatric metabolomics", "mental disorder metabolome",
    # Animal models (CUMS, social defeat, MIA, etc.)
    "CUMS metabolomics", "social defeat metabolomics",
    "maternal immune activation metabolomics",
    "chronic stress metabolomics", "chronic mild stress",
    "antipsychotic metabolomics", "antidepressant metabolomics",
]

# ── Column patterns ────────────────────────────────────────────────────────────
_MET_NAME_COL = re.compile(
    r"(metabolite.?name|compound.?name|feature.?name|chemical.?name"
    r"|annotation|^name$|identification)",
    re.IGNORECASE,
)
_INCHIKEY_COL = re.compile(r"inchi.?key", re.IGNORECASE)
_DB_ID_COL    = re.compile(r"(database.?identifier|hmdb|chebi|kegg|pubchem)", re.IGNORECASE)


def _ebi_search(http: HTTPClient, query: str, max_hits: int = 200) -> list[str]:
    """Return MTBLS accession IDs from EBI Search."""
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
        logger.debug("EBI Search '%s': %s", query[:50], exc)
    return ids


def _ml_metadata(http: HTTPClient, sid: str) -> Optional[dict]:
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
    """Run MH biomarkers MetaboLights collector. Returns (n_links, health_dict)."""
    migrate_db(db_path)

    mh_cfg    = cfg.get("mh_biomarkers", cfg.get("fecal_mental", {}))
    max_total = mh_cfg.get("max_total_studies", 600)

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    health: dict[str, Any] = {
        "studies_found":         0,
        "studies_mh":            0,
        "files_parsed":          0,
        "metabolites_extracted": 0,
        "last_error":            "",
    }

    # ── 1. Discover MTBLS IDs via EBI Search ─────────────────────────────────
    candidate_ids: set[str] = set()
    for term in _MH_SEARCH_TERMS:
        ids = _ebi_search(http, term, max_hits=100)
        candidate_ids.update(ids)
        if ids:
            logger.debug("EBI Search '%s': %d MTBLS", term, len(ids))

    health["studies_found"] = len(candidate_ids)
    logger.info("MH MetaboLights: %d candidate studies from EBI Search", len(candidate_ids))

    if not candidate_ids:
        logger.warning("MH MetaboLights: EBI Search returned 0 MTBLS IDs")
        return 0, health

    # ── 2. Filter by strict whitelist + extract metabolites ───────────────────
    n_links   = 0
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

            # Strict whitelist — SKIP if no specific MH condition found
            cond = detect_conditions(combined)
            if not cond:
                continue

            mres        = detect_matrix(meta["title"], meta["description"], meta["design"])
            matrix_hint = matrix_result_to_hint_str(mres) or "unknown"
            method_hint = mres.method_hint
            health["studies_mh"] += 1
            logger.info("MH MetaboLights: %s '%s' matrix=%s cond=%s",
                        sid, meta["title"][:55], matrix_hint, cond)

            source_id = upsert_source(
                conn,
                source_type="MetaboLights_MH",
                source_ref=sid,
                title=meta["title"] or None,
                matrix_hint=matrix_hint,
                method_hint=method_hint,
            )

            # ── 3. Get annotation TSVs ─────────────────────────────────────────
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
                logger.debug("ML MH %s: no annotation TSVs", sid)
                continue

            tags = tags_from_conditions(cond)
            tags["mh_evidence_type"] = "dataset_metabolomics"

            for finfo in ann_files[:5]:   # up to 5 files per study
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

                headers  = list(rows_data[0].keys())
                name_col = _find_col(headers, _MET_NAME_COL)
                ikey_col = _find_col(headers, _INCHIKEY_COL)
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
                    mid   = upsert_metabolite(conn, canon, nkey, inchikey=inchikey, tags=tags)
                    add_synonym(conn, mid, name, nkey)

                    for col in headers:
                        if _DB_ID_COL.search(col):
                            val = (row.get(col) or "").strip()
                            if val and val.lower() not in ("", "n/a", "na", "-"):
                                add_synonym(conn, mid, val, make_key(val))

                    link_metabolite_source(
                        conn, mid, source_id,
                        evidence_tag="mh_study_metabolite_list",
                    )
                    n_links += 1
                    health["metabolites_extracted"] += 1

    logger.info("MH MetaboLights: %d links | health=%s", n_links, health)
    return n_links, health
