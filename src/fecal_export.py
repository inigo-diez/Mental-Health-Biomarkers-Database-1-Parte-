"""
Fecal + mental-health candidate export.

Generates:
  outputs/fecal_mental_candidates.csv   — metabolites with fecal_hint=1
                                          AND mental_health tag or condition_hits
  outputs/fecal_health.json             — per-source health statistics

Columns in fecal_mental_candidates.csv
---------------------------------------
  canonical_name
  name_raw             — first synonym (raw name used by the source)
  condition_hits       — comma-separated (schizophrenia, depression, ...)
  fecal_evidence       — dataset_metadata | fecal_catalog | text_mining_claim
  source_types_distinct
  source_refs          — comma-separated study/paper IDs
  matrix_hint
  method_hint
  n_sources_distinct
  n_links_total
  inchikey
  pubchem_cid

Usage:
    python -m src.fecal_export [--config config.yaml] [--db outputs/metabolites.db]
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

_FECAL_COLS = [
    "metabolite_id",
    "canonical_name",
    "name_raw",
    "condition_hits",
    "fecal_evidence",
    "source_types_distinct",
    "source_refs",
    "matrix_hint",
    "method_hint",
    "n_sources_distinct",
    "n_links_total",
    "inchikey",
    "pubchem_cid",
]


def export_fecal_mental(cfg: dict, db_path: str | None = None) -> int:
    """
    Export fecal+MH candidates to CSV and write health JSON.
    Returns number of rows written.
    """
    from src.db import get_conn, migrate_db

    if db_path is None:
        db_path = cfg["paths"]["db"]

    # Ensure columns exist before querying
    migrate_db(db_path)

    out_dir = Path(cfg["paths"]["outputs"])
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "fecal_mental_candidates.csv"

    with get_conn(db_path) as conn:
        rows = _query_fecal_mental(conn)
        _write_csv(rows, csv_path)

    logger.info("Fecal+MH candidates: %d rows -> %s", len(rows), csv_path)
    return len(rows)


def _query_fecal_mental(conn) -> list[dict]:
    """
    Fetch all metabolites that have:
      - fecal_hint = 1  (set by any fecal collector or enrichment)
      - mental_health flag OR condition_hits is non-empty
    Aggregates source refs and matrix hints across all linked sources.
    """
    # Core metabolite data
    mets = conn.execute("""
        SELECT
            m.metabolite_id,
            m.canonical_name,
            m.inchikey,
            m.pubchem_cid,
            m.tags_json,
            m.source_types_distinct,
            COALESCE(m.n_sources_distinct, 0) AS n_sources_distinct,
            COALESCE(m.n_records_total,    0) AS n_links_total,
            m.fecal_evidence_type,
            m.condition_hits                  AS condition_hits_json
        FROM metabolites m
        WHERE m.fecal_hint = 1
    """).fetchall()

    # Build output rows
    result: list[dict] = []
    for met in mets:
        mid  = met["metabolite_id"]
        tags = json.loads(met["tags_json"] or "{}")

        # Only include if there is some mental-health signal
        cond_json = met["condition_hits_json"] or "[]"
        try:
            cond_list: list[str] = json.loads(cond_json)
        except (ValueError, TypeError):
            cond_list = []

        has_mh = (
            tags.get("mental_health") or
            tags.get("schizophrenia") or
            bool(cond_list)
        )
        if not has_mh:
            continue

        # Source refs + matrix/method hints
        sources = conn.execute("""
            SELECT s.source_type, s.source_ref, s.matrix_hint, s.method_hint
            FROM metabolite_sources ms
            JOIN sources s ON ms.source_id = s.source_id
            WHERE ms.metabolite_id = ?
        """, (mid,)).fetchall()

        src_types = sorted({s["source_type"] for s in sources if s["source_type"]})
        src_refs  = sorted({s["source_ref"]  for s in sources if s["source_ref"]})
        matrix_hints = sorted({
            s["matrix_hint"] for s in sources
            if s["matrix_hint"] and _is_fecal_hint(s["matrix_hint"])
        })
        method_hints = sorted({
            s["method_hint"] for s in sources
            if s["method_hint"]
        })

        # First synonym as name_raw
        syn_row = conn.execute(
            "SELECT synonym FROM synonyms WHERE metabolite_id=? LIMIT 1", (mid,)
        ).fetchone()
        name_raw = syn_row["synonym"] if syn_row else met["canonical_name"]

        # Fecal evidence type (aggregate)
        ev_type = met["fecal_evidence_type"] or ""
        if not ev_type:
            # Derive from source types and evidence tags
            evidence_tags = conn.execute("""
                SELECT DISTINCT evidence_tag
                FROM metabolite_sources WHERE metabolite_id=?
            """, (mid,)).fetchall()
            ev_set = {r["evidence_tag"] for r in evidence_tags if r["evidence_tag"]}
            parts = []
            if any("study_metabolite_list" in e or "dataset" in e for e in ev_set):
                parts.append("dataset_metadata")
            if any("catalog" in e or "fecal_catalog" in e for e in ev_set):
                parts.append("fecal_catalog")
            if any("text_mining" in e for e in ev_set):
                parts.append("text_mining_claim")
            ev_type = "|".join(parts) if parts else "unknown"

        result.append({
            "metabolite_id":        mid,
            "canonical_name":       met["canonical_name"],
            "name_raw":             name_raw,
            "condition_hits":       "|".join(cond_list) if cond_list else (
                "schizophrenia" if tags.get("schizophrenia") else
                "mental_health" if tags.get("mental_health") else ""
            ),
            "fecal_evidence":       ev_type,
            "source_types_distinct": ",".join(src_types),
            "source_refs":          ",".join(src_refs[:10]),   # cap at 10 for readability
            "matrix_hint":          "; ".join(matrix_hints) or "",
            "method_hint":          "; ".join(method_hints) or "",
            "n_sources_distinct":   met["n_sources_distinct"],
            "n_links_total":        met["n_links_total"],
            "inchikey":             met["inchikey"] or "",
            "pubchem_cid":          met["pubchem_cid"] or "",
        })

    # Sort: most evidence first
    result.sort(key=lambda r: (-r["n_sources_distinct"], -r["n_links_total"], r["canonical_name"]))
    return result


def _is_fecal_hint(text: str) -> bool:
    t = text.lower()
    return any(term in t for term in ("fec", "stool", "faec", "cecal", "intestinal", "copro"))


def _write_csv(rows: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FECAL_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Fecal CSV: %d rows -> %s", len(rows), path)


def write_health_report(health_data: dict, out_dir: Path) -> None:
    """Write per-source health stats to outputs/fecal_health.json."""
    path = out_dir / "fecal_health.json"
    try:
        path.write_text(json.dumps(health_data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Health report: %s", path)
    except Exception as exc:
        logger.warning("Could not write fecal_health.json: %s", exc)


# ── CLI entry ─────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export fecal+mental-health candidates")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db",     default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    db_path = args.db or cfg["paths"]["db"]
    n = export_fecal_mental(cfg, db_path)
    print(f"Exported {n} fecal+MH candidates")


if __name__ == "__main__":
    main()
