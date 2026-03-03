"""
MetaboLights (EBI) collector.

API base: https://www.ebi.ac.uk/metabolights/ws

Workflow
--------
1. Fetch list of all public study IDs.
2. Filter by keyword match in study title/description.
3. For each MTBLS study:
   a. GET /studies/{id} for metadata (title, description).
   b. GET /studies/{id}/files for file list.
   c. Download assay / annotation TSV files that likely contain metabolite names.
   d. Parse column headers / data to extract metabolite names.
4. Insert metabolites and link to source.

MetaboLights TSV annotations typically have columns like:
  "Metabolite name", "database_identifier", "inchi", "smiles", "retention_time", …
"""

from __future__ import annotations
import csv
import io
import logging
import re
from pathlib import Path
from typing import Iterator, Optional

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source,
)
from src.matrix_parser import detect_matrix, matrix_result_to_hint_str
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

MENTAL_HEALTH_TERMS = {
    "schizophrenia", "psychosis", "bipolar", "depression", "mdd",
    "anxiety", "ptsd", "autism", "adhd", "attention deficit",
    "mental disorder", "psychiatric", "mood disorder", "schizoaffective",
}
SCHIZ_TERMS = {"schizophrenia", "psychosis", "schizoaffective"}

MATRIX_PATTERNS = {
    "fecal":  ["fec", "stool", "feces"],
    "urine":  ["urin"],
    "plasma": ["plasma"],
    "serum":  ["serum"],
    "csf":    ["csf", "cerebrospinal"],
    "saliva": ["saliva"],
    "breath": ["breath", "exhaled"],
}

# Column name patterns that indicate a metabolite name column
_MET_NAME_COLS = re.compile(
    r"(metabolite.?name|compound.?name|feature.?name|annotation.?name|"
    r"chemical.?name|name|identification)",
    re.IGNORECASE,
)
_INCHIKEY_COL = re.compile(r"inchi.?key", re.IGNORECASE)
_INCHI_COL    = re.compile(r"^inchi$", re.IGNORECASE)
_DB_ID_COL    = re.compile(r"(database.?identifier|hmdb|chebi|kegg)", re.IGNORECASE)


def _is_mental_health(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in MENTAL_HEALTH_TERMS)


def _is_schizophrenia(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in SCHIZ_TERMS)


def _guess_matrix(text: str) -> Optional[str]:
    t = text.lower()
    for label, pats in MATRIX_PATTERNS.items():
        if any(p in t for p in pats):
            return label
    return None


def _find_metabolite_col(headers: list[str]) -> Optional[str]:
    for h in headers:
        if _MET_NAME_COLS.search(h):
            return h
    return None


def _find_inchikey_col(headers: list[str]) -> Optional[str]:
    for h in headers:
        if _INCHIKEY_COL.search(h):
            return h
    return None


def _parse_tsv(content: str) -> Iterator[dict]:
    """Yield rows from a TSV, skipping comment lines."""
    lines = [l for l in content.splitlines() if not l.startswith("#")]
    if not lines:
        return
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="\t")
    yield from reader


def collect(cfg: dict, db_path: str) -> int:
    ml_cfg   = cfg.get("metabolights", {})
    base     = ml_cfg.get("rest_base", "https://www.ebi.ac.uk/metabolights/ws")
    list_url = ml_cfg.get("public_studies_url", f"{base}/studies/list")
    max_stud = (cfg.get("max_records_per_source") or {}).get("metabolights", 100)
    keywords = cfg.get("mental_health_keywords", ["schizophrenia"])

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    # ── 1. Fetch list of all public study IDs ─────────────────────────────────
    logger.info("MetaboLights: fetching public study list from %s", list_url)
    try:
        data = http.get_json(list_url)
        if isinstance(data, list):
            all_ids = data
        elif isinstance(data, dict):
            all_ids = data.get("content", data.get("studies", list(data.keys())))
        else:
            all_ids = []
    except Exception as exc:
        logger.error("MetaboLights: failed to fetch study list: %s", exc)
        return 0

    logger.info("MetaboLights: %d public studies found", len(all_ids))

    # ── 2. Filter studies by keyword ──────────────────────────────────────────
    relevant_ids: list[str] = []
    checked = 0
    for sid in all_ids:
        if not isinstance(sid, str) or not sid.startswith("MTBLS"):
            continue
        meta_url = f"{base}/studies/{sid}"
        try:
            meta = http.get_json(meta_url)
            if isinstance(meta, dict):
                content = meta.get("content", meta)
                title   = content.get("title", "") if isinstance(content, dict) else ""
                desc    = content.get("description", "") if isinstance(content, dict) else ""
            else:
                title, desc = "", ""
        except Exception:
            title, desc = "", sid  # still include if we can't check

        combined = f"{sid} {title} {desc}"
        if _is_mental_health(combined) or any(kw.lower() in combined.lower() for kw in keywords):
            relevant_ids.append(sid)
        checked += 1
        if max_stud and len(relevant_ids) >= max_stud:
            break
        if checked % 100 == 0:
            logger.info("MetaboLights: checked %d / %d studies, %d relevant so far",
                        checked, len(all_ids), len(relevant_ids))

    logger.info("MetaboLights: %d relevant studies to process", len(relevant_ids))

    n_links = 0
    with get_conn(db_path) as conn:
        for sid in relevant_ids:
            # ── 3. Fetch study metadata ───────────────────────────────────────
            title, matrix_hint, method_hint, schiz_hit = sid, None, None, _is_schizophrenia(sid)
            mres_fecal = False
            try:
                meta = http.get_json(f"{base}/studies/{sid}")
                if isinstance(meta, dict):
                    content = meta.get("content", meta)
                    if isinstance(content, dict):
                        title      = content.get("title", sid)
                        desc       = content.get("description", "")
                        study_type = content.get("studyType", "")
                        organism   = content.get("organism", "")
                        mres       = detect_matrix(title, desc, study_type, organism)
                        matrix_hint  = matrix_result_to_hint_str(mres)
                        method_hint  = mres.method_hint
                        mres_fecal   = mres.fecal_hint
                        schiz_hit    = _is_schizophrenia(f"{title} {desc}")
            except Exception as exc:
                logger.debug("MetaboLights metadata failed for %s: %s", sid, exc)

            source_id = upsert_source(
                conn,
                source_type="MetaboLights",
                source_ref=sid,
                title=title,
                matrix_hint=matrix_hint,
                method_hint=method_hint,
            )

            # ── 4. Fetch file list and find annotation TSVs ───────────────────
            try:
                files_data = http.get_json(f"{base}/studies/{sid}/files")
                if isinstance(files_data, dict):
                    file_list = files_data.get("study", files_data.get("content", []))
                elif isinstance(files_data, list):
                    file_list = files_data
                else:
                    file_list = []
            except Exception as exc:
                logger.debug("MetaboLights file list failed for %s: %s", sid, exc)
                continue

            # Filter to annotation/assay TSV files
            annotation_files = [
                f for f in file_list
                if isinstance(f, dict) and
                f.get("file", "").endswith(".tsv") and
                any(
                    tag in f.get("type", "").lower() + f.get("file", "").lower()
                    for tag in ("annotation", "assay", "metabolite", "a_", "m_")
                )
            ]

            if not annotation_files:
                logger.debug("MetaboLights %s: no annotation TSV files found", sid)
                continue

            for finfo in annotation_files:
                fname = finfo.get("file", "")
                file_url = f"{base}/studies/{sid}/download?file={fname}"
                try:
                    content = http.get_text(file_url)
                except Exception as exc:
                    logger.debug("MetaboLights %s/%s download failed: %s", sid, fname, exc)
                    continue

                rows = list(_parse_tsv(content))
                if not rows:
                    continue

                headers = list(rows[0].keys())
                name_col    = _find_metabolite_col(headers)
                inchikey_col = _find_inchikey_col(headers)

                if not name_col:
                    logger.debug("MetaboLights %s/%s: no metabolite name column found (headers: %s)",
                                 sid, fname, headers[:8])
                    continue

                for row in rows:
                    name = (row.get(name_col) or "").strip()
                    if not name or name.lower() in ("", "unknown", "n/a", "na", "-"):
                        continue

                    inchikey = None
                    if inchikey_col:
                        ik = (row.get(inchikey_col) or "").strip()
                        if len(ik) == 27 and ik[14] == "-":  # valid InChIKey format
                            inchikey = ik

                    canon = normalize(name)
                    nkey  = make_key(name)
                    tags  = {
                        "mental_health": True,
                        "schizophrenia": schiz_hit,
                        "fecal_hint":   mres_fecal,
                    }
                    mid = upsert_metabolite(conn, canon, nkey, inchikey=inchikey, tags=tags)
                    add_synonym(conn, mid, name, nkey)

                    # Store database IDs as synonyms
                    for col in headers:
                        if _DB_ID_COL.search(col):
                            val = (row.get(col) or "").strip()
                            if val and val.lower() not in ("", "n/a", "na", "-"):
                                add_synonym(conn, mid, val, make_key(val))

                    link_metabolite_source(
                        conn, mid, source_id,
                        evidence_tag="study_metabolite_list",
                    )
                    n_links += 1

    logger.info("MetaboLights: inserted %d metabolite-source links", n_links)
    return n_links
