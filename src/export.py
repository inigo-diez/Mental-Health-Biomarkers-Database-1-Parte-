"""
Export CLI — generates the master CSV and source summary CSV.

Usage:
    python -m src.export [--config config.yaml] [--db outputs/metabolites.db]

Columns in candidates_master.csv
---------------------------------
  metabolite_id, canonical_name, normalized_key, inchikey, pubchem_cid, status,
  n_sources_distinct   -- COUNT(DISTINCT source_type)  [correct multi-source metric]
  n_records_total      -- COUNT(*) links (may be high for CTD due to many conditions)
  source_types         -- comma-separated distinct source types
  mental_health_terms_hit, schizophrenia_hit,
  fecal_hint           -- 1 if any source/tag flags fecal matrix
  known_fecal_metabolite -- 1 if in HMDB feces catalog
  from_text_mining     -- 1 if added via text mining (PubTator / EuropePMC)
  matrix_hints         -- free-text matrix hints from sources
  volatility           -- GC-compatible / LC-compatible / Unknown (from classify_compound)
  gc_compatible, lc_compatible, is_inorganic, is_drug, is_environmental, is_category_like,
  tags_json
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import sys
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# ── Master CSV columns ────────────────────────────────────────────────────────

_MASTER_COLS = [
    "metabolite_id",
    "canonical_name",
    "normalized_key",
    "inchikey",
    "pubchem_cid",
    "status",
    "n_sources_distinct",      # correct: COUNT(DISTINCT source_type)
    "n_records_total",         # total links (may be inflated by CTD conditions)
    "source_types",            # comma-separated distinct source types
    "mental_health_terms_hit",
    "schizophrenia_hit",
    "fecal_hint",              # 1 if any evidence of fecal matrix
    "known_fecal_metabolite",  # 1 if in HMDB feces catalog
    "from_text_mining",        # 1 if added by text mining
    "matrix_hints",
    "volatility",              # GC-compatible / LC-compatible / Unknown
    "gc_compatible",           # true / false / Unknown
    "lc_compatible",
    "is_inorganic",
    "is_drug",
    "is_environmental",
    "is_category_like",
    "tags_json",
]

_SOURCE_COLS = [
    "source_id",
    "source_type",
    "source_ref",
    "title",
    "year",
    "method_hint",
    "matrix_hint",
    "n_metabolites",
]


def export_all(cfg: dict, db_path: str | None = None) -> None:
    from src.db import get_conn

    if db_path is None:
        db_path = cfg["paths"]["db"]
    out_dir = Path(cfg["paths"]["outputs"])
    out_dir.mkdir(parents=True, exist_ok=True)

    master_path = out_dir / "candidates_master.csv"
    source_path = out_dir / "source_summary.csv"

    with get_conn(db_path) as conn:
        _write_master(conn, master_path)
        _write_source_summary(conn, source_path)

    logger.info("Exported: %s", master_path)
    logger.info("Exported: %s", source_path)


def _write_master(conn, path: Path) -> None:
    # Use pre-computed enrichment columns directly; fall back gracefully
    # if enrichment hasn't been run yet (columns may be NULL / 0).
    query = """
    SELECT
        m.metabolite_id,
        m.canonical_name,
        m.normalized_key,
        m.inchikey,
        m.pubchem_cid,
        m.status,
        m.tags_json,
        COALESCE(m.n_sources_distinct, 0)    AS n_sources_distinct,
        COALESCE(m.n_records_total,    0)    AS n_records_total,
        m.source_types_distinct,
        COALESCE(m.fecal_hint,         0)    AS fecal_hint,
        COALESCE(m.from_text_mining,   0)    AS from_text_mining,
        m.matrix_hints_col,
        COALESCE(m.volatility,    'Unknown') AS volatility,
        COALESCE(m.gc_compatible, 'Unknown') AS gc_compatible,
        COALESCE(m.lc_compatible, 'Unknown') AS lc_compatible,
        COALESCE(m.is_inorganic,  'unknown') AS is_inorganic,
        COALESCE(m.is_drug,       'unknown') AS is_drug,
        COALESCE(m.is_environmental, 'unknown') AS is_environmental,
        COALESCE(m.is_category_like, 'false')   AS is_category_like
    FROM metabolites m
    ORDER BY n_sources_distinct DESC, m.canonical_name
    """
    rows = conn.execute(query).fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_MASTER_COLS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            tags = json.loads(row["tags_json"] or "{}")

            # Parse pre-computed source types list
            src_types_raw = row["source_types_distinct"] or "[]"
            try:
                src_list = json.loads(src_types_raw)
            except (ValueError, TypeError):
                src_list = [s.strip() for s in src_types_raw.split(",") if s.strip()]

            # Mental-health terms hit (from tags)
            mh_terms = []
            if tags.get("mental_health"):
                mh_terms.append("mental_health")
            if tags.get("schizophrenia"):
                mh_terms.append("schizophrenia")

            writer.writerow({
                "metabolite_id":          row["metabolite_id"],
                "canonical_name":         row["canonical_name"],
                "normalized_key":         row["normalized_key"],
                "inchikey":               row["inchikey"] or "",
                "pubchem_cid":            row["pubchem_cid"] or "",
                "status":                 row["status"],
                "n_sources_distinct":     row["n_sources_distinct"],
                "n_records_total":        row["n_records_total"],
                "source_types":           ",".join(src_list),
                "mental_health_terms_hit": "|".join(mh_terms),
                "schizophrenia_hit":      "1" if tags.get("schizophrenia") else "0",
                "fecal_hint":             row["fecal_hint"],
                "known_fecal_metabolite": "1" if tags.get("known_fecal_metabolite") else "0",
                "from_text_mining":       row["from_text_mining"],
                "matrix_hints":           row["matrix_hints_col"] or "",
                "volatility":             row["volatility"],
                "gc_compatible":          row["gc_compatible"],
                "lc_compatible":          row["lc_compatible"],
                "is_inorganic":           row["is_inorganic"],
                "is_drug":                row["is_drug"],
                "is_environmental":       row["is_environmental"],
                "is_category_like":       row["is_category_like"],
                "tags_json":              row["tags_json"],
            })

    logger.info("Master CSV: %d rows -> %s", len(rows), path)


def _write_source_summary(conn, path: Path) -> None:
    query = """
    SELECT
        s.source_id,
        s.source_type,
        s.source_ref,
        s.title,
        s.year,
        s.method_hint,
        s.matrix_hint,
        COUNT(DISTINCT ms.metabolite_id) AS n_metabolites
    FROM sources s
    LEFT JOIN metabolite_sources ms ON s.source_id = ms.source_id
    GROUP BY s.source_id
    ORDER BY n_metabolites DESC, s.source_type
    """
    rows = conn.execute(query).fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_SOURCE_COLS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "source_id":    row["source_id"],
                "source_type":  row["source_type"],
                "source_ref":   row["source_ref"] or "",
                "title":        row["title"] or "",
                "year":         row["year"] or "",
                "method_hint":  row["method_hint"] or "",
                "matrix_hint":  row["matrix_hint"] or "",
                "n_metabolites": row["n_metabolites"],
            })

    logger.info("Source summary: %d rows -> %s", len(rows), path)


# ── CLI entry point ──────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export metabolite database to CSV")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db",     default=None, help="Override DB path from config")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    db_path = args.db or cfg["paths"]["db"]
    export_all(cfg, db_path)


if __name__ == "__main__":
    main()
