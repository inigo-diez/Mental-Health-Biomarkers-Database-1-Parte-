"""
Metabolomics Workbench REST collector.

Endpoints used
--------------
  /rest/study/study_id/*/summary
  /rest/study/study_title/{keyword}/summary
  /rest/study/{study_id}/metabolites

Workflow
--------
1. Search studies by each mental-health keyword (study_title).
2. For each study_id, fetch full metadata → extract matrix_hint.
3. Fetch metabolite list → insert each metabolite.
4. Link metabolite ↔ source (study).
"""

from __future__ import annotations
import logging
from typing import Any

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source,
)
from src.matrix_parser import detect_matrix, matrix_result_to_hint_str
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

SCHIZ_KEYWORDS = {"schizophrenia", "psychosis", "schizoaffective", "first episode"}


def _is_schizophrenia(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in SCHIZ_KEYWORDS)


def collect(cfg: dict, db_path: str) -> int:
    mwb_cfg = cfg.get("metabolomics_workbench", {})
    base     = mwb_cfg.get("rest_base", "https://www.metabolomicsworkbench.org/rest")
    max_studies = (cfg.get("max_records_per_source") or {}).get("metabolomics_workbench", 200)
    keywords = cfg.get("mental_health_keywords", ["schizophrenia", "depression", "psychosis"])

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    # ── 1. Collect study IDs ──────────────────────────────────────────────────
    study_ids: set[str] = set()
    for kw in keywords:
        kw_safe = kw.replace(" ", "%20")
        url = f"{base}/study/study_title/{kw_safe}/summary"
        try:
            data = http.get_json(url)
            if isinstance(data, dict):
                # MWB wraps results in a dict keyed by study_id
                for sid in data:
                    study_ids.add(sid)
            elif isinstance(data, list):
                for item in data:
                    sid = item.get("study_id") or item.get("StudyID")
                    if sid:
                        study_ids.add(sid)
            logger.info("MWB keyword '%s': %d studies found (total so far: %d)", kw, len(data) if data else 0, len(study_ids))
        except Exception as exc:
            logger.warning("MWB search failed for keyword '%s': %s", kw, exc)

    if not study_ids:
        logger.warning("MWB: no studies found")
        return 0

    if max_studies:
        study_ids = set(list(study_ids)[:max_studies])
    logger.info("MWB: processing %d unique studies", len(study_ids))

    n_links = 0
    with get_conn(db_path) as conn:
        for study_id in sorted(study_ids):
            # ── 2. Fetch study metadata ───────────────────────────────────────
            meta_url = f"{base}/study/study_id/{study_id}/summary"
            title, matrix_hint, method_hint, schiz_hit = "", None, None, False
            try:
                meta = http.get_json(meta_url)
                if isinstance(meta, dict):
                    inner = meta.get(study_id, meta)
                    title        = inner.get("study_title", inner.get("title", ""))
                    subject_type = inner.get("subject_type", "")
                    sample_type  = inner.get("sample_type", "")
                    collection   = inner.get("collection", "")
                    ms_type      = inner.get("ms_type", "")
                    instrument   = inner.get("instrument_type", inner.get("instrument", ""))
                    # Use all metadata fields for robust detection
                    mres = detect_matrix(title, subject_type, sample_type,
                                         collection, ms_type, instrument)
                    matrix_hint  = matrix_result_to_hint_str(mres)
                    method_hint  = mres.method_hint
                    schiz_hit    = _is_schizophrenia(
                        f"{title} {subject_type} {sample_type}"
                    )
            except Exception as exc:
                logger.debug("MWB metadata failed for %s: %s", study_id, exc)

            source_id = upsert_source(
                conn,
                source_type="MWB",
                source_ref=study_id,
                title=title or None,
                matrix_hint=matrix_hint,
                method_hint=method_hint,
            )

            # ── 3. Fetch metabolites ──────────────────────────────────────────
            met_url = f"{base}/study/{study_id}/metabolites"
            try:
                met_data = http.get_json(met_url)
            except Exception as exc:
                logger.debug("MWB metabolites failed for %s: %s", study_id, exc)
                continue

            if not met_data:
                continue

            # MWB returns a dict keyed by an integer index
            rows: list[dict] = []
            if isinstance(met_data, dict):
                rows = list(met_data.values())
            elif isinstance(met_data, list):
                rows = met_data

            for row in rows:
                if not isinstance(row, dict):
                    continue
                name = (
                    row.get("metabolite_name") or row.get("name") or
                    row.get("Metabolite") or ""
                ).strip()
                if not name:
                    continue

                inchikey = (row.get("inchi_key") or row.get("inchikey") or "").strip() or None
                pubchem  = row.get("pubchem_id") or row.get("PubChem_ID")
                try:
                    pubchem = int(pubchem) if pubchem else None
                except (ValueError, TypeError):
                    pubchem = None

                canon = normalize(name)
                nkey  = make_key(name)
                # fecal_hint comes from the robust matrix detector (mres)
                tags  = {
                    "mental_health": True,
                    "schizophrenia": schiz_hit,
                    "fecal_hint":    bool(matrix_hint and any(
                        kw in matrix_hint for kw in ("fecal", "stool", "faec")
                    )),
                }
                mid = upsert_metabolite(conn, canon, nkey, inchikey=inchikey, pubchem_cid=pubchem, tags=tags)
                add_synonym(conn, mid, name, nkey)

                link_metabolite_source(conn, mid, source_id, evidence_tag="study_metabolite_list")
                n_links += 1

    logger.info("MWB: inserted %d metabolite-source links", n_links)
    return n_links
