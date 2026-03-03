"""
MarkerDB 2.0 collector.

Strategy (tries in order):
  1. Download the public TSV bulk export (no API key needed).
  2. If TSV fails, fall back to XML bulk export.
  3. If both fail and an API key is configured, use the paginated REST API.

Fields extracted per record:
  - biomarker name
  - condition / disease
  - biofluid / matrix
  - InChIKey (if present)
  - HMDB ID (if present)
  - SMILES (if present)
"""

from __future__ import annotations
import csv
import io
import logging
import os
import xml.etree.ElementTree as ET
from typing import Generator, Optional

from src.cache import DiskCache
from src.db import (
    get_conn, upsert_metabolite, upsert_source, add_synonym,
    link_metabolite_source,
)
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

MENTAL_HEALTH_CONDITIONS = {
    "schizophrenia", "psychosis", "bipolar", "depression",
    "major depressive disorder", "mdd", "anxiety", "ptsd",
    "autism", "asd", "adhd", "attention deficit",
    "mental disorder", "psychiatric disorder", "mental health",
    "mood disorder", "schizoaffective", "first episode psychosis",
}


def _matches_mental_health(condition: str) -> bool:
    cond_lower = condition.lower()
    return any(kw in cond_lower for kw in MENTAL_HEALTH_CONDITIONS)


def _schizophrenia_hit(condition: str) -> bool:
    cond_lower = condition.lower()
    return any(kw in cond_lower for kw in ("schizophrenia", "psychosis", "schizoaffective"))


# ── TSV parser ───────────────────────────────────────────────────────────────

def _iter_tsv(content: str) -> Generator[dict, None, None]:
    """Yield rows from MarkerDB TSV bulk export."""
    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    for row in reader:
        yield row


def _extract_from_row(row: dict) -> Optional[dict]:
    """
    Normalise a MarkerDB TSV row.
    Actual columns (confirmed from /downloads page):
      id, name, hmdb_id, conditions, indication_types,
      concentration, age, sex, biofluid, citation
    """
    # Primary column names (actual MarkerDB schema)
    name = (row.get("name") or row.get("biomarker_name") or "").strip()
    if not name:
        return None

    # 'conditions' is the real column name (not 'condition')
    condition = (row.get("conditions") or row.get("condition") or "").strip()
    biofluid  = (row.get("biofluid")   or row.get("matrix")    or "").strip()
    hmdb_id   = (row.get("hmdb_id")    or row.get("HMDB")      or "").strip() or None
    # MarkerDB chemical TSV has no InChIKey column; HMDB ID is the main identifier
    inchikey  = (row.get("inchikey")   or row.get("InChIKey")  or "").strip() or None

    return {
        "name":      name,
        "condition": condition,
        "biofluid":  biofluid,
        "inchikey":  inchikey,
        "hmdb_id":   hmdb_id,
    }


# ── XML parser ───────────────────────────────────────────────────────────────

def _iter_xml(content: str) -> Generator[dict, None, None]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        logger.error("MarkerDB XML parse error: %s", exc)
        return

    for biomarker in root.iter("biomarker"):
        name_el = biomarker.find("name") or biomarker.find("biomarker_name")
        name = (name_el.text or "").strip() if name_el is not None else ""
        if not name:
            continue

        cond_el = biomarker.find("condition") or biomarker.find("associated_condition")
        condition = (cond_el.text or "").strip() if cond_el is not None else ""

        bf_el = biomarker.find("biofluid") or biomarker.find("matrix")
        biofluid = (bf_el.text or "").strip() if bf_el is not None else ""

        ik_el = biomarker.find("inchikey") or biomarker.find("InChIKey")
        inchikey = (ik_el.text or "").strip() if ik_el is not None else None

        hmdb_el = biomarker.find("hmdb_id") or biomarker.find("HMDB")
        hmdb_id = (hmdb_el.text or "").strip() if hmdb_el is not None else None

        yield {
            "name": name,
            "condition": condition,
            "biofluid": biofluid,
            "inchikey": inchikey or None,
            "hmdb_id": hmdb_id or None,
        }


# ── Main collector ───────────────────────────────────────────────────────────

def collect(cfg: dict, db_path: str) -> int:
    """
    Run the MarkerDB collector.
    Returns number of metabolite-source links inserted.
    """
    mcfg = cfg.get("markerdb", {})
    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )
    api_key = mcfg.get("markerdb_api_key") or os.environ.get("MARKERDB_API_KEY", "")
    records: list[dict] = []

    # ── Download from all three chemical endpoints ────────────────────────────
    # Confirmed URLs from https://markerdb.ca/downloads (updated 2024-09-12):
    #   /pages/download_all_chemicals?format=tsv          (general chemical markers)
    #   /pages/download_all_diagnostic_chemicals?format=tsv
    #   /pages/download_all_exposure_chemicals?format=tsv
    tsv_urls = mcfg.get("download_urls", [
        "https://markerdb.ca/pages/download_all_chemicals?format=tsv",
        "https://markerdb.ca/pages/download_all_diagnostic_chemicals?format=tsv",
        "https://markerdb.ca/pages/download_all_exposure_chemicals?format=tsv",
    ])
    for tsv_url in tsv_urls:
        logger.info("MarkerDB: downloading %s", tsv_url)
        try:
            text = http.get_text(tsv_url, use_cache=True)
            rows = list(_iter_tsv(text))
            if rows:
                logger.info("MarkerDB TSV (%s): %d raw rows", tsv_url.split("?")[0].split("/")[-1], len(rows))
                new_recs = [r for r in (_extract_from_row(row) for row in rows) if r]
                records.extend(new_recs)
        except Exception as exc:
            logger.warning("MarkerDB TSV failed (%s): %s", tsv_url, exc)

    # ── Attempt 2: XML bulk download (fallback) ───────────────────────────────
    if not records:
        xml_url = mcfg.get("download_url_xml", "https://markerdb.ca/pages/download_all_chemicals?format=xml")
        logger.info("MarkerDB: trying XML bulk download from %s", xml_url)
        try:
            text = http.get_text(xml_url, use_cache=True)
            records = list(_iter_xml(text))
            logger.info("MarkerDB XML: %d raw records", len(records))
        except Exception as exc:
            logger.warning("MarkerDB XML failed: %s", exc)

    # ── Attempt 3: Paginated REST API ─────────────────────────────────────────
    if not records and api_key:
        logger.info("MarkerDB: falling back to paginated REST API")
        api_base = mcfg.get("api_base", "https://markerdb.ca/api/v2")
        page, per_page = 1, 200
        while True:
            try:
                data = http.get_json(
                    f"{api_base}/biomarkers",
                    params={"page": page, "per_page": per_page, "api_key": api_key},
                )
            except Exception as exc:
                logger.error("MarkerDB REST API page %d failed: %s", page, exc)
                break
            items = data if isinstance(data, list) else data.get("biomarkers", data.get("data", []))
            if not items:
                break
            for item in items:
                r = _extract_from_row(item)
                if r:
                    records.append(r)
            if len(items) < per_page:
                break
            page += 1
        logger.info("MarkerDB REST: %d records fetched", len(records))

    if not records:
        logger.warning("MarkerDB: no records fetched from any source")
        return 0

    # ── Filter to mental-health conditions ───────────────────────────────────
    mh_records = [r for r in records if _matches_mental_health(r["condition"])]
    logger.info(
        "MarkerDB: %d / %d records match mental-health conditions",
        len(mh_records), len(records),
    )

    if not mh_records:
        logger.warning("MarkerDB: zero mental-health records — inserting ALL chemical records as fallback")
        mh_records = records  # broad insert

    # ── Persist to DB ─────────────────────────────────────────────────────────
    n_links = 0
    with get_conn(db_path) as conn:
        # Group by condition to create one source per condition
        from collections import defaultdict
        by_condition: dict[str, list[dict]] = defaultdict(list)
        for rec in mh_records:
            by_condition[rec["condition"] or "Unknown"].append(rec)

        for condition, recs in by_condition.items():
            matrix_hint = None
            biofluids = {r["biofluid"] for r in recs if r["biofluid"]}
            if biofluids:
                matrix_hint = "; ".join(sorted(biofluids))

            source_id = upsert_source(
                conn,
                source_type="MarkerDB",
                source_ref=condition,
                matrix_hint=matrix_hint,
            )

            for rec in recs:
                canon = normalize(rec["name"])
                nkey  = make_key(rec["name"])
                tags = {
                    "mental_health": _matches_mental_health(condition),
                    "schizophrenia": _schizophrenia_hit(condition),
                    "fecal_hint": any(
                        kw in (rec["biofluid"] or "").lower()
                        for kw in ("fec", "stool", "feces")
                    ),
                }
                mid = upsert_metabolite(
                    conn, canon, nkey,
                    inchikey=rec["inchikey"],
                    tags=tags,
                )
                add_synonym(conn, mid, rec["name"], nkey)
                if rec.get("hmdb_id"):
                    add_synonym(conn, mid, rec["hmdb_id"], make_key(rec["hmdb_id"]))

                link_metabolite_source(conn, mid, source_id, evidence_tag="biomarker_db")
                n_links += 1

    logger.info("MarkerDB: inserted %d metabolite-source links", n_links)
    return n_links
