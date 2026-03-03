"""
Metabolomics Workbench — Mental-Health Biomarkers collector.

Strategy
--------
1. Search MWB studies by whitelist condition terms (any matrix).
2. For each study fetch full metadata.
3. SKIP if detect_conditions() returns [] — strict whitelist, NO fallback.
4. Download metabolite list via /study/study_id/{id}/metabolites.
5. Tag with specific MH conditions.

Returns (n_links, health_dict).
"""

from __future__ import annotations
import logging
from typing import Any

from src.conditions import detect_conditions, tags_from_conditions
from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source, migrate_db,
)
from src.matrix_parser import detect_matrix, matrix_result_to_hint_str
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

# ── Search terms — whitelist conditions + metabolomics (no fecal) ─────────────
_MH_SEARCH_TERMS: list[str] = [
    # Direct condition searches
    "schizophrenia", "psychosis", "schizoaffective",
    "depression", "major depressive disorder",
    "bipolar disorder", "bipolar",
    "anxiety disorder", "PTSD", "post-traumatic stress",
    "autism spectrum disorder", "autism",
    "ADHD", "attention deficit hyperactivity",
    # Condition + metabolomics
    "schizophrenia metabolomics", "psychosis metabolomics",
    "depression metabolomics", "bipolar metabolomics",
    "anxiety metabolomics", "ptsd metabolomics",
    "autism metabolomics", "adhd metabolomics",
    "psychiatric metabolomics", "mental health metabolomics",
    # Biomarker focused
    "schizophrenia biomarker", "depression biomarker",
    "psychiatric biomarker metabolite",
    # Animal models
    "CUMS metabolomics", "social defeat stress metabolomics",
    "chronic unpredictable stress", "maternal immune activation",
    "antipsychotic metabolomics", "antidepressant metabolomics",
    "haloperidol metabolomics", "clozapine metabolomics",
    # Specific matrices used in psychiatry
    "schizophrenia plasma", "schizophrenia serum",
    "schizophrenia urine", "schizophrenia cerebrospinal",
    "depression plasma", "depression serum",
    "bipolar plasma", "autism plasma",
]


def collect(cfg: dict, db_path: str) -> tuple[int, dict]:
    """Run MH biomarkers MWB collector. Returns (n_links, health_dict)."""
    migrate_db(db_path)

    mwb_cfg   = cfg.get("metabolomics_workbench", {})
    mh_cfg    = cfg.get("mh_biomarkers", cfg.get("fecal_mental", {}))
    base      = mwb_cfg.get("rest_base", "https://www.metabolomicsworkbench.org/rest")
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
        "metabolites_extracted": 0,
        "last_error":            "",
    }

    # ── 1. Collect candidate study IDs ────────────────────────────────────────
    study_ids: set[str] = set()
    for term in _MH_SEARCH_TERMS:
        if len(study_ids) >= max_total:
            break
        kw_safe = term.replace(" ", "%20")
        url = f"{base}/study/study_title/{kw_safe}/summary"
        try:
            data = http.get_json(url)
            if isinstance(data, dict):
                for v in data.values():
                    sid = (v.get("study_id") or "").strip() if isinstance(v, dict) else ""
                    if sid:
                        study_ids.add(sid)
            elif isinstance(data, list):
                for item in data:
                    sid = (item.get("study_id") or item.get("StudyID") or "").strip()
                    if sid:
                        study_ids.add(sid)
        except Exception as exc:
            health["last_error"] = str(exc)[:200]
            logger.debug("MWB MH search '%s': %s", term, exc)

    health["studies_found"] = len(study_ids)
    logger.info("MH MWB: %d candidate studies", len(study_ids))

    if not study_ids:
        logger.warning("MH MWB: no studies found — check network/API")
        return 0, health

    # ── 2. Filter by strict whitelist + extract metabolites ───────────────────
    n_links = 0
    with get_conn(db_path) as conn:
        for study_id in sorted(study_ids):
            meta_url = f"{base}/study/study_id/{study_id}/summary"
            try:
                meta = http.get_json(meta_url)
                if not isinstance(meta, dict):
                    continue
                inner        = meta.get(study_id, meta)
                title        = inner.get("study_title", inner.get("title", ""))
                subject_type = inner.get("subject_type", "")
                sample_type  = inner.get("sample_type", "")
                collection   = inner.get("collection", "")
                ms_type      = inner.get("ms_type", "")
                instrument   = inner.get("instrument_type", inner.get("instrument", ""))
                description  = inner.get("summary", inner.get("description", ""))
            except Exception as exc:
                health["last_error"] = str(exc)[:200]
                logger.debug("MWB MH metadata %s: %s", study_id, exc)
                continue

            combined = f"{title} {subject_type} {sample_type} {collection} {description}"

            # Strict whitelist — SKIP if no specific MH condition
            cond = detect_conditions(combined)
            if not cond:
                continue

            mres        = detect_matrix(title, subject_type, sample_type,
                                        collection, ms_type, instrument, description)
            matrix_hint = matrix_result_to_hint_str(mres) or "unknown"
            method_hint = mres.method_hint
            health["studies_mh"] += 1
            logger.info("MH MWB: %s '%s' matrix=%s cond=%s",
                        study_id, title[:55], matrix_hint, cond)

            source_id = upsert_source(
                conn,
                source_type="MWB_MH",
                source_ref=study_id,
                title=title or None,
                matrix_hint=matrix_hint,
                method_hint=method_hint,
            )

            met_url = f"{base}/study/study_id/{study_id}/metabolites"
            try:
                met_data = http.get_json(met_url)
            except Exception as exc:
                health["last_error"] = str(exc)[:200]
                logger.debug("MWB MH metabolites %s: %s", study_id, exc)
                continue

            if not met_data:
                continue

            rows_data: list[dict] = (
                list(met_data.values()) if isinstance(met_data, dict) else
                met_data if isinstance(met_data, list) else []
            )

            tags = tags_from_conditions(cond)
            tags["mh_evidence_type"] = "dataset_metabolomics"

            for row in rows_data:
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
                mid   = upsert_metabolite(
                    conn, canon, nkey,
                    inchikey=inchikey, pubchem_cid=pubchem, tags=tags,
                )
                add_synonym(conn, mid, name, nkey)

                for id_key in ("refmet_name", "kegg_id", "hmdb_id", "chebi_id"):
                    val = (row.get(id_key) or "").strip()
                    if val and val.lower() not in ("", "n/a", "na", "-"):
                        add_synonym(conn, mid, val, make_key(val))

                link_metabolite_source(
                    conn, mid, source_id,
                    evidence_tag="mh_study_metabolite_list",
                )
                n_links += 1
                health["metabolites_extracted"] += 1

    logger.info("MH MWB: %d links | health=%s", n_links, health)
    return n_links, health
