"""
Search CLI — fuzzy lookup of a metabolite in the SQLite database.

Usage:
    python -m src.search "2-Pentanone"
    python -m src.search "pentanone" --top 10 --config config.yaml
    python -m src.search "HMDB0000062" --by-synonym

Features
--------
- Exact normalized-key match (fastest)
- Substring match on canonical_name and synonyms
- Simple fuzzy scoring via difflib (no external dependency)
- Shows sources and tags for each hit
"""

from __future__ import annotations
import argparse
import difflib
import json
import logging
import sys
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def _search(conn, query: str, top_n: int = 10) -> list[dict]:
    """
    Returns list of result dicts sorted by relevance.
    Tries: 1) exact norm key, 2) substring in canonical/synonym, 3) fuzzy.
    """
    from src.normalize import make_key
    nkey = make_key(query)
    q_lower = query.lower()

    results: dict[int, dict] = {}  # metabolite_id → result

    def _add(row, score: float, match_via: str) -> None:
        mid = row["metabolite_id"]
        if mid not in results or results[mid]["score"] < score:
            results[mid] = {
                "metabolite_id": mid,
                "canonical_name": row["canonical_name"],
                "normalized_key": row["normalized_key"],
                "inchikey":       row["inchikey"],
                "pubchem_cid":    row["pubchem_cid"],
                "status":         row["status"],
                "tags":           json.loads(row["tags_json"] or "{}"),
                "score":          score,
                "match_via":      match_via,
            }

    # 1) Exact normalized key match
    row = conn.execute(
        "SELECT * FROM metabolites WHERE normalized_key=?", (nkey,)
    ).fetchone()
    if row:
        _add(row, 1.0, "exact_key")

    # 2) Substring in canonical_name
    for row in conn.execute(
        "SELECT * FROM metabolites WHERE lower(canonical_name) LIKE ?",
        (f"%{q_lower}%",),
    ):
        _add(row, 0.85, "name_substring")

    # 3) Substring in synonyms
    for syn_row in conn.execute(
        "SELECT s.metabolite_id, s.synonym FROM synonyms WHERE lower(synonym) LIKE ?",
        (f"%{q_lower}%",),
    ):
        m_row = conn.execute(
            "SELECT * FROM metabolites WHERE metabolite_id=?",
            (syn_row["metabolite_id"],),
        ).fetchone()
        if m_row:
            _add(m_row, 0.75, f"synonym:{syn_row['synonym']}")

    # 4) Fuzzy on normalized keys (if we still have few results)
    if len(results) < top_n:
        all_keys = conn.execute(
            "SELECT metabolite_id, normalized_key FROM metabolites LIMIT 20000"
        ).fetchall()
        for row2 in all_keys:
            ratio = difflib.SequenceMatcher(None, nkey, row2["normalized_key"]).ratio()
            if ratio >= 0.7 and row2["metabolite_id"] not in results:
                m_row = conn.execute(
                    "SELECT * FROM metabolites WHERE metabolite_id=?",
                    (row2["metabolite_id"],),
                ).fetchone()
                if m_row:
                    _add(m_row, ratio * 0.65, "fuzzy")

    sorted_results = sorted(results.values(), key=lambda r: -r["score"])
    return sorted_results[:top_n]


def _get_sources(conn, metabolite_id: int) -> list[dict]:
    return conn.execute(
        """
        SELECT s.source_type, s.source_ref, s.title, s.matrix_hint, ms.evidence_tag
        FROM metabolite_sources ms
        JOIN sources s ON ms.source_id = s.source_id
        WHERE ms.metabolite_id = ?
        ORDER BY s.source_type
        """,
        (metabolite_id,),
    ).fetchall()


def _print_result(r: dict, sources: list) -> None:
    print(f"\n  [{r['score']:.2f}] {r['canonical_name']}")
    print(f"         id={r['metabolite_id']}  key={r['normalized_key']}")
    print(f"         inchikey={r['inchikey'] or '—'}  cid={r['pubchem_cid'] or '—'}")
    print(f"         status={r['status']}  match_via={r['match_via']}")
    tag_items = [f"{k}={v}" for k, v in r["tags"].items() if v]
    if tag_items:
        print(f"         tags: {', '.join(tag_items)}")
    if sources:
        print(f"         sources ({len(sources)}):")
        for s in sources:
            mat = f" [{s['matrix_hint']}]" if s["matrix_hint"] else ""
            print(f"           • {s['source_type']}/{s['source_ref']}{mat}  ({s['evidence_tag']})")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Search the metabolite database")
    parser.add_argument("query", help="Metabolite name (or ID) to search")
    parser.add_argument("--top",    type=int, default=10)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db",     default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING)

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    db_path = args.db or cfg["paths"]["db"]

    if not Path(db_path).exists():
        print(f"DB not found: {db_path}. Run `python -m src.collect` first.")
        sys.exit(1)

    from src.db import get_conn
    with get_conn(db_path) as conn:
        hits = _search(conn, args.query, top_n=args.top)
        if not hits:
            print(f"No results for '{args.query}'.")
            return
        print(f"\nSearch results for '{args.query}' (top {len(hits)}):")
        for hit in hits:
            srcs = _get_sources(conn, hit["metabolite_id"])
            _print_result(hit, list(srcs))
    print()


if __name__ == "__main__":
    main()
