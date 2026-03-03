"""
Enrichment pass — runs AFTER all collectors.

1. Computes n_sources_distinct, n_records_total, source_types_distinct
   (correct multi-source metric: based on distinct source_type, not source_id)
2. Applies classify_compound (flags: is_inorganic, is_drug, is_environmental,
   is_category_like, volatility, gc_compatible, lc_compatible)
3. Propagates fecal_hint and from_text_mining from tags_json to dedicated columns
4. Derives volatility from source method_hints (GC/LC)
5. Syncs resolved_ids from inchikey/pubchem_cid

All operations are UPDATE-only — no rows deleted, no data lost.
"""

from __future__ import annotations
import json
import logging
import sqlite3
from pathlib import Path

from src.classify_compound import classify
from src.db import get_conn, migrate_db

logger = logging.getLogger(__name__)


def run_enrichment(db_path: str | Path) -> None:
    """Run the full enrichment pass. Safe to re-run (idempotent)."""
    logger.info("Enrichment: starting pass on %s", db_path)

    # Ensure new columns exist
    migrate_db(db_path)

    with get_conn(db_path) as conn:
        _update_metrics(conn)
        _propagate_flags(conn)
        _classify_all(conn)
        _sync_resolved_ids(conn)

    logger.info("Enrichment: done")


# ── Step 1: n_sources_distinct / n_records_total ─────────────────────────────

def _update_metrics(conn: sqlite3.Connection) -> None:
    """
    n_sources_distinct = COUNT(DISTINCT source_type)
    n_records_total    = COUNT(*) from metabolite_sources (all links, incl. duplicates within same type)
    source_types_distinct = JSON array of distinct source_type names
    """
    logger.info("Enrichment: computing source metrics...")

    rows = conn.execute("""
        SELECT
            ms.metabolite_id,
            COUNT(ms.source_id)               AS n_records_total,
            COUNT(DISTINCT s.source_type)     AS n_sources_distinct,
            GROUP_CONCAT(DISTINCT s.source_type) AS src_types
        FROM metabolite_sources ms
        JOIN sources s ON ms.source_id = s.source_id
        GROUP BY ms.metabolite_id
    """).fetchall()

    for row in rows:
        src_list = sorted(set((row["src_types"] or "").split(",")))
        conn.execute(
            """
            UPDATE metabolites
            SET n_records_total      = ?,
                n_sources_distinct   = ?,
                source_types_distinct = ?
            WHERE metabolite_id = ?
            """,
            (
                row["n_records_total"],
                row["n_sources_distinct"],
                json.dumps(src_list),
                row["metabolite_id"],
            ),
        )
    logger.info("Enrichment: updated metrics for %d metabolites", len(rows))


# ── Step 2: propagate fecal_hint, from_text_mining from tags_json ────────────

def _propagate_flags(conn: sqlite3.Connection) -> None:
    """
    Read the existing tags_json and write to dedicated columns.
    Also detect fecal_hint from:
      - sources.matrix_hint containing 'fecal'/'stool'/'faeces'
    """
    logger.info("Enrichment: propagating flags from tags_json and sources...")

    # Fecal hint from sources
    fecal_mids = conn.execute("""
        SELECT DISTINCT ms.metabolite_id
        FROM metabolite_sources ms
        JOIN sources s ON ms.source_id = s.source_id
        WHERE s.matrix_hint IS NOT NULL
          AND (
            lower(s.matrix_hint) LIKE '%fec%'
            OR lower(s.matrix_hint) LIKE '%stool%'
            OR lower(s.matrix_hint) LIKE '%faec%'
          )
    """).fetchall()
    fecal_mid_set = {r["metabolite_id"] for r in fecal_mids}

    # Text-mining from sources (all text-mining evidence tags)
    txt_mids = conn.execute("""
        SELECT DISTINCT ms.metabolite_id
        FROM metabolite_sources ms
        WHERE ms.evidence_tag LIKE 'text_mining%'
           OR ms.evidence_tag LIKE 'mh_text_mining%'
    """).fetchall()
    txt_mid_set = {r["metabolite_id"] for r in txt_mids}

    # Also get method hints per metabolite (for volatility)
    method_hints = conn.execute("""
        SELECT ms.metabolite_id, GROUP_CONCAT(DISTINCT s.method_hint) AS methods
        FROM metabolite_sources ms
        JOIN sources s ON ms.source_id = s.source_id
        WHERE s.method_hint IS NOT NULL
        GROUP BY ms.metabolite_id
    """).fetchall()
    method_map = {r["metabolite_id"]: r["methods"] for r in method_hints}

    # Update all metabolites
    all_mets = conn.execute(
        "SELECT metabolite_id, tags_json FROM metabolites"
    ).fetchall()

    for row in all_mets:
        mid   = row["metabolite_id"]
        tags  = json.loads(row["tags_json"] or "{}")

        fecal = (
            tags.get("fecal_hint") or
            tags.get("known_fecal_metabolite") or
            mid in fecal_mid_set
        )
        txt_mining = (
            tags.get("from_text_mining") or
            mid in txt_mid_set
        )

        # Propagate condition_hits from tags_json → DB column (union/merge)
        tag_cond = tags.get("condition_hits", [])
        if isinstance(tag_cond, list) and tag_cond:
            existing_cond_raw = conn.execute(
                "SELECT condition_hits FROM metabolites WHERE metabolite_id=?", (mid,)
            ).fetchone()
            existing_cond: list = []
            if existing_cond_raw and existing_cond_raw[0]:
                try:
                    existing_cond = json.loads(existing_cond_raw[0])
                except (ValueError, TypeError):
                    existing_cond = []
            merged = sorted(set(existing_cond + tag_cond))
            conn.execute(
                "UPDATE metabolites SET condition_hits=? WHERE metabolite_id=?",
                (json.dumps(merged), mid),
            )

        conn.execute(
            """
            UPDATE metabolites
            SET fecal_hint      = ?,
                from_text_mining = ?
            WHERE metabolite_id = ?
            """,
            (1 if fecal else 0, 1 if txt_mining else 0, mid),
        )

    logger.info("Enrichment: flags propagated for %d metabolites", len(all_mets))


# ── Step 3: classify_compound flags ──────────────────────────────────────────

def _classify_all(conn: sqlite3.Connection) -> None:
    """Run compound classifier on all metabolites, update flags."""
    logger.info("Enrichment: classifying compounds...")

    rows = conn.execute(
        """
        SELECT m.metabolite_id, m.canonical_name, m.source_types_distinct,
               GROUP_CONCAT(DISTINCT s.method_hint) AS methods
        FROM metabolites m
        LEFT JOIN metabolite_sources ms ON m.metabolite_id = ms.metabolite_id
        LEFT JOIN sources s ON ms.source_id = s.source_id
        GROUP BY m.metabolite_id
        """
    ).fetchall()

    updated = 0
    for row in rows:
        name        = row["canonical_name"] or ""
        src_types   = row["source_types_distinct"] or ""
        method_hint = row["methods"] or None

        flags = classify(name, method_hint=method_hint, source_types=src_types)

        conn.execute(
            """
            UPDATE metabolites SET
                is_inorganic    = ?,
                is_drug         = ?,
                is_environmental = ?,
                is_category_like = ?,
                volatility      = ?,
                gc_compatible   = ?,
                lc_compatible   = ?
            WHERE metabolite_id = ?
            """,
            (
                flags.is_inorganic,
                flags.is_drug,
                flags.is_environmental,
                flags.is_category_like,
                flags.volatility,
                flags.gc_compatible,
                flags.lc_compatible,
                row["metabolite_id"],
            ),
        )
        updated += 1

    logger.info("Enrichment: classified %d compounds", updated)


# ── Step 4: sync resolved_ids from known columns ─────────────────────────────

def _sync_resolved_ids(conn: sqlite3.Connection) -> None:
    """Build resolved_ids JSON from inchikey, pubchem_cid, synonym MeSH/HMDB."""
    logger.info("Enrichment: syncing resolved_ids...")

    rows = conn.execute(
        "SELECT metabolite_id, inchikey, pubchem_cid FROM metabolites"
    ).fetchall()

    for row in rows:
        ids: dict = {}
        if row["inchikey"]:
            ids["inchikey"] = row["inchikey"]
        if row["pubchem_cid"]:
            ids["pubchem_cid"] = row["pubchem_cid"]

        # Pull MeSH / HMDB from synonyms
        syns = conn.execute(
            "SELECT synonym FROM synonyms WHERE metabolite_id=?",
            (row["metabolite_id"],),
        ).fetchall()
        for s in syns:
            val = s["synonym"]
            if val.startswith("HMDB"):
                ids["hmdb_id"] = val
            elif val.startswith("MESH:") or val.startswith("D0") or val.startswith("C0"):
                ids.setdefault("mesh_ids", [])
                if val not in ids["mesh_ids"]:
                    ids["mesh_ids"].append(val)
            elif val.startswith("CHEBI:"):
                ids["chebi_id"] = val

        if ids:
            conn.execute(
                "UPDATE metabolites SET resolved_ids=? WHERE metabolite_id=?",
                (json.dumps(ids), row["metabolite_id"]),
            )

    logger.info("Enrichment: resolved_ids synced for %d metabolites", len(rows))


# ── CLI entry ─────────────────────────────────────────────────────────────────

def main(argv=None):
    import argparse, sys
    import yaml

    parser = argparse.ArgumentParser(description="Run enrichment pass on the metabolite DB")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db",     default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    db_path = args.db or cfg["paths"]["db"]
    run_enrichment(db_path)


if __name__ == "__main__":
    main()
