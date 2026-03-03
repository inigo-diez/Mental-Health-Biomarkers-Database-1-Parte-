"""
CTD (Comparative Toxicogenomics Database) collector.

Downloads CTD_chemicals_diseases.tsv.gz (public, no auth needed) and
filters chemical–disease associations where the disease matches the
mental-health keyword set.

CTD file columns (tab-separated, lines starting with '#' are comments):
  ChemicalName, ChemicalID (MeSH), CasRN, DiseaseName, DiseaseID (MeSH),
  DirectEvidence, InferenceGeneSymbol, InferenceScore, OmimIDs, PubMedIDs
"""

from __future__ import annotations
import csv
import gzip
import io
import logging
import os
from pathlib import Path
from typing import Iterator

from src.db import (
    get_conn, upsert_metabolite, upsert_source,
    add_synonym, link_metabolite_source,
)
from src.normalize import normalize, make_key
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

MENTAL_HEALTH_TERMS = {
    "schizophrenia", "psychosis", "bipolar", "depression",
    "major depressive disorder", "mdd", "anxiety", "ptsd",
    "autism", "autistic", "adhd", "attention deficit",
    "mental disorder", "psychiatric disorder", "mental health",
    "mood disorder", "schizoaffective",
}

SCHIZOPHRENIA_TERMS = {"schizophrenia", "psychosis", "schizoaffective"}


def _is_mental_health(disease_name: str) -> bool:
    dl = disease_name.lower()
    return any(t in dl for t in MENTAL_HEALTH_TERMS)


def _is_schizophrenia(disease_name: str) -> bool:
    dl = disease_name.lower()
    return any(t in dl for t in SCHIZOPHRENIA_TERMS)


def _iter_ctd_gz(content: bytes) -> Iterator[dict]:
    """
    Yield rows from the CTD gzipped TSV.

    Actual CTD format (confirmed from file inspection):
      - Lines starting with '#' are comments.
      - The comment block contains:
          # Fields:
          # ChemicalName\\tChemicalID\\t...   <- column names here, tab-separated
          #
      - Immediately after the comments, DATA rows start (no separate header row).
    """
    with gzip.open(io.BytesIO(content), "rt", encoding="utf-8", errors="replace") as f:
        header: list[str] | None = None
        after_fields = False  # True once we've seen '# Fields:'

        for line in f:
            if line.startswith("#"):
                if line.strip() == "# Fields:":
                    after_fields = True
                    continue
                if after_fields and header is None:
                    # Next comment line after '# Fields:' contains tab-separated column names
                    raw = line.lstrip("#").strip()
                    if "\t" in raw:
                        header = [c.strip() for c in raw.split("\t")]
                continue

            # Data line
            if header is None:
                # Absolute fallback: treat first non-comment line as header
                header = [c.strip() for c in line.rstrip("\n").split("\t")]
                continue

            values = line.rstrip("\n").split("\t")
            if len(values) < len(header):
                values += [""] * (len(header) - len(values))
            yield dict(zip(header, values))


def _download_or_load(cfg: dict, http: HTTPClient) -> bytes:
    """Return raw bytes of the CTD gzipped TSV (from cache or download)."""
    ctd_cfg = cfg.get("ctd", {})
    local_path = Path(ctd_cfg.get("local_cache", "cache/CTD_chemicals_diseases.tsv.gz"))

    if local_path.exists():
        logger.info("CTD: loading from local cache %s", local_path)
        return local_path.read_bytes()

    url = ctd_cfg.get("url", "https://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz")
    logger.info("CTD: downloading from %s (this may take several minutes)…", url)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    resp = http.get(url, use_cache=False, stream=True)
    data = resp.content if hasattr(resp, "content") else b""
    if not data:
        # fallback: read streamed
        chunks = []
        for chunk in resp.iter_content(chunk_size=1 << 20):
            chunks.append(chunk)
        data = b"".join(chunks)

    local_path.write_bytes(data)
    logger.info("CTD: saved %d MB to %s", len(data) // (1 << 20), local_path)
    return data


def collect(cfg: dict, db_path: str) -> int:
    """Run the CTD collector. Returns number of metabolite-source links inserted."""
    http = HTTPClient(
        rate=cfg.get("rate_limit", 3),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        timeout=cfg.get("request_timeout", 30),
        cache_dir=str(cfg["paths"]["cache"]) + "/http",
    )
    max_rec = (cfg.get("max_records_per_source") or {}).get("ctd")

    try:
        raw = _download_or_load(cfg, http)
    except Exception as exc:
        logger.error("CTD: download/load failed: %s", exc)
        return 0

    logger.info("CTD: parsing TSV…")

    # Group rows by disease for source creation
    from collections import defaultdict
    by_disease: dict[str, list[dict]] = defaultdict(list)
    total_rows = 0

    for row in _iter_ctd_gz(raw):
        disease = row.get("DiseaseName", "").strip()
        if not _is_mental_health(disease):
            continue
        chem_name = (
            row.get("ChemicalName") or row.get("Chemical Name") or ""
        ).strip()
        if not chem_name:
            continue
        by_disease[disease].append(row)
        total_rows += 1
        if max_rec and total_rows >= max_rec:
            break

    logger.info("CTD: %d chemical-disease associations matching mental health", total_rows)

    if not by_disease:
        logger.warning("CTD: no mental-health associations found — check file format")
        return 0

    n_links = 0
    with get_conn(db_path) as conn:
        for disease, rows in by_disease.items():
            source_id = upsert_source(
                conn,
                source_type="CTD",
                source_ref=disease,
            )
            for row in rows:
                chem_name = (
                    row.get("ChemicalName") or row.get("Chemical Name") or ""
                ).strip()
                mesh_id  = (row.get("ChemicalID") or "").strip() or None
                cas_rn   = (row.get("CasRN") or "").strip() or None

                canon = normalize(chem_name)
                nkey  = make_key(chem_name)
                tags = {
                    "mental_health": True,
                    "schizophrenia": _is_schizophrenia(disease),
                    "fecal_hint": False,
                }
                mid = upsert_metabolite(conn, canon, nkey, tags=tags)
                add_synonym(conn, mid, chem_name, nkey)
                if mesh_id:
                    add_synonym(conn, mid, f"MeSH:{mesh_id}", make_key(f"MeSH:{mesh_id}"))
                if cas_rn:
                    add_synonym(conn, mid, f"CAS:{cas_rn}", make_key(f"CAS:{cas_rn}"))

                link_metabolite_source(conn, mid, source_id, evidence_tag="chemical-disease")
                n_links += 1

    logger.info("CTD: inserted %d metabolite-source links", n_links)
    return n_links
