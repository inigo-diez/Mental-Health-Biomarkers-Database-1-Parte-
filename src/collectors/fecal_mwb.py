"""
Metabolomics Workbench — fecal + mental-health focused collector.

Strategy
--------
1. Search MWB studies by combinations of:
   - Mental-health keywords (schizophrenia, depression, bipolar, PTSD, autism,
     ADHD, stress, CUMS, social defeat, maternal separation …)
   - Fecal keywords directly (stool, fecal, gut metabolomics …)
2. For each candidate study fetch full metadata.
3. Keep only studies where the matrix is confirmed fecal/stool/cecal.
4. Download metabolite list via /study/{id}/metabolites.
5. Insert metabolites into the shared DB with tags:
     mental_health=True, fecal_hint=True,
     fecal_evidence_type=dataset_metadata,
     condition_hits=[…]

Returns (n_links, health_dict).
"""

from __future__ import annotations
import json
import logging
from typing import Any

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source, migrate_db,
)
from src.matrix_parser import detect_matrix, matrix_result_to_hint_str
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

# ── Keyword lists ─────────────────────────────────────────────────────────────

_MH_SEARCH_TERMS: list[str] = [
    # Specific + fecal combined (best precision)
    "schizophrenia fecal", "schizophrenia stool",
    "psychosis fecal", "psychosis stool",
    "depression fecal", "depression stool",
    "bipolar fecal", "bipolar stool",
    "anxiety fecal", "anxiety stool",
    "autism fecal", "autism stool",
    "PTSD fecal", "stress fecal",
    "CUMS fecal", "social defeat fecal",
    "maternal separation fecal",
    # Gut/microbiome + psychiatric (broad)
    "gut microbiota schizophrenia",
    "gut microbiota depression",
    "gut microbiota bipolar",
    "gut microbiota anxiety",
    "gut microbiota autism",
    "gut brain schizophrenia",
    "fecal metabolomics psychiatric",
    "stool metabolomics mental",
    "intestinal metabolomics schizophrenia",
    # Animal models
    "chronic unpredictable mild stress fecal",
    "social defeat stress metabolomics",
    # Also search by pure mental health terms (filter by matrix after)
    "schizophrenia", "psychosis", "schizoaffective",
    "depression metabolomics", "major depressive disorder metabolomics",
    "bipolar metabolomics", "anxiety metabolomics",
    "autism metabolomics", "ADHD metabolomics",
    "PTSD metabolomics",
    # Pure fecal metabolomics (filter by condition after)
    "fecal metabolomics", "stool metabolomics",
    "gut metabolome", "cecal metabolomics",
]

_CONDITION_MAP: dict[str, str] = {
    "schizophrenia": "schizophrenia",
    "psychosis":     "schizophrenia",
    "schizoaffective": "schizophrenia",
    "first episode": "schizophrenia",
    "depression":    "depression",
    "major depressive": "depression",
    "mdd":           "depression",
    "bipolar":       "bipolar",
    "anxiety":       "anxiety",
    "ptsd":          "ptsd",
    "post-traumatic": "ptsd",
    "autism":        "autism",
    "asd":           "autism",
    "adhd":          "adhd",
    "attention deficit": "adhd",
    "stress":        "stress",
    "cums":          "stress",
    "social defeat": "stress",
    "maternal separation": "stress",
    "chronic unpredictable": "stress",
}

_FECAL_TERMS = (
    "fec", "stool", "faec", "cecal", "caecal",
    "cecum", "caecum", "intestinal content",
    "gut content", "colon content", "rectal",
    "copro", "bowel content",
)


def _condition_hits(text: str) -> list[str]:
    t = text.lower()
    return sorted({v for k, v in _CONDITION_MAP.items() if k in t})


def _is_fecal(text: str) -> bool:
    t = text.lower()
    return any(term in t for term in _FECAL_TERMS)


def _is_mental_health(text: str) -> bool:
    return bool(_condition_hits(text))


def collect(cfg: dict, db_path: str) -> tuple[int, dict]:
    """Run fecal-MH MWB collector. Returns (n_links, health_dict)."""
    migrate_db(db_path)

    mwb_cfg  = cfg.get("metabolomics_workbench", {})
    fm_cfg   = cfg.get("fecal_mental", {})
    base     = mwb_cfg.get("rest_base", "https://www.metabolomicsworkbench.org/rest")
    max_total = fm_cfg.get("max_total_studies", 400)

    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )

    health: dict[str, Any] = {
        "studies_found": 0,
        "studies_fecal": 0,
        "metabolites_extracted": 0,
        "last_error": "",
    }

    # ── 1. Collect candidate study IDs ───────────────────────────────────────
    study_ids: set[str] = set()
    for term in _MH_SEARCH_TERMS:
        if len(study_ids) >= max_total:
            break
        kw_safe = term.replace(" ", "%20")
        url = f"{base}/study/study_title/{kw_safe}/summary"
        try:
            data = http.get_json(url)
            # MWB returns a dict keyed by '1','2',… where each value
            # is the study record containing 'study_id'.
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
            logger.debug("MWB fecal search '%s': %s", term, exc)

    health["studies_found"] = len(study_ids)
    logger.info("MWB fecal: %d candidate studies", len(study_ids))

    if not study_ids:
        logger.warning("MWB fecal: no studies found — check network/API")
        return 0, health

    # ── 2. Filter by fecal matrix + extract metabolites ──────────────────────
    n_links = 0
    with get_conn(db_path) as conn:
        for study_id in sorted(study_ids):
            # Fetch metadata
            meta_url = f"{base}/study/study_id/{study_id}/summary"
            try:
                meta = http.get_json(meta_url)
                if not isinstance(meta, dict):
                    continue
                inner = meta.get(study_id, meta)
                title        = inner.get("study_title", inner.get("title", ""))
                subject_type = inner.get("subject_type", "")
                sample_type  = inner.get("sample_type", "")
                collection   = inner.get("collection", "")
                ms_type      = inner.get("ms_type", "")
                instrument   = inner.get("instrument_type", inner.get("instrument", ""))
                description  = inner.get("summary", inner.get("description", ""))
            except Exception as exc:
                health["last_error"] = str(exc)[:200]
                logger.debug("MWB fecal metadata %s: %s", study_id, exc)
                continue

            combined = f"{title} {subject_type} {sample_type} {collection} {description}"

            # Confirm fecal matrix
            mres = detect_matrix(title, subject_type, sample_type,
                                 collection, ms_type, instrument, description)
            if not (mres.fecal_hint or _is_fecal(combined)):
                continue

            # Confirm mental health relevance (animal models included)
            cond = _condition_hits(combined)
            if not cond:
                # Could still be useful as fecal psychiatric study
                cond = ["mental_health"]

            matrix_hint = matrix_result_to_hint_str(mres) or "fecal"
            method_hint = mres.method_hint
            health["studies_fecal"] += 1
            logger.info("MWB fecal: %s '%s' matrix=%s cond=%s",
                        study_id, title[:55], matrix_hint, cond)

            source_id = upsert_source(
                conn,
                source_type="MWB",
                source_ref=study_id,
                title=title or None,
                matrix_hint=matrix_hint,
                method_hint=method_hint,
            )

            # Fetch metabolite list
            met_url = f"{base}/study/study_id/{study_id}/metabolites"
            try:
                met_data = http.get_json(met_url)
            except Exception as exc:
                health["last_error"] = str(exc)[:200]
                logger.debug("MWB fecal metabolites %s: %s", study_id, exc)
                continue

            if not met_data:
                continue

            rows_data: list[dict] = (
                list(met_data.values()) if isinstance(met_data, dict) else
                met_data if isinstance(met_data, list) else []
            )

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
                tags  = {
                    "mental_health":       True,
                    "schizophrenia":       "schizophrenia" in cond,
                    "fecal_hint":          True,
                    "fecal_evidence_type": "dataset_metadata",
                    "condition_hits":      cond,
                }
                mid = upsert_metabolite(
                    conn, canon, nkey,
                    inchikey=inchikey, pubchem_cid=pubchem, tags=tags,
                )
                add_synonym(conn, mid, name, nkey)

                # Store extra identifiers as synonyms
                for id_key in ("refmet_name", "kegg_id", "hmdb_id", "chebi_id"):
                    val = (row.get(id_key) or "").strip()
                    if val and val.lower() not in ("", "n/a", "na", "-"):
                        add_synonym(conn, mid, val, make_key(val))

                link_metabolite_source(
                    conn, mid, source_id,
                    evidence_tag="fecal_study_metabolite_list",
                )
                n_links += 1
                health["metabolites_extracted"] += 1

    logger.info("MWB fecal: %d links | health=%s", n_links, health)
    return n_links, health
