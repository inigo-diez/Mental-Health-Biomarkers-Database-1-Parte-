"""
SQLite schema + CRUD operations.

Tables
------
metabolites       – deduplicated canonical metabolites
synonyms          – all raw names mapped to a metabolite
sources           – studies / papers / databases used
metabolite_sources – M:N linking table with evidence tag
"""

from __future__ import annotations
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

# ── Schema DDL ──────────────────────────────────────────────────────────────

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metabolites (
    metabolite_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name  TEXT    NOT NULL,
    normalized_key  TEXT    NOT NULL UNIQUE,
    inchikey        TEXT,
    pubchem_cid     INTEGER,
    status          TEXT    NOT NULL DEFAULT 'candidate',
    tags_json       TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_met_inchikey  ON metabolites(inchikey);
CREATE INDEX IF NOT EXISTS idx_met_norm      ON metabolites(normalized_key);
CREATE INDEX IF NOT EXISTS idx_met_cid       ON metabolites(pubchem_cid);

CREATE TABLE IF NOT EXISTS synonyms (
    synonym_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    metabolite_id   INTEGER NOT NULL REFERENCES metabolites(metabolite_id),
    synonym         TEXT    NOT NULL,
    normalized_key  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_syn_norm ON synonyms(normalized_key);
CREATE INDEX IF NOT EXISTS idx_syn_mid  ON synonyms(metabolite_id);

CREATE TABLE IF NOT EXISTS sources (
    source_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type  TEXT NOT NULL,   -- MarkerDB|CTD|MWB|MetaboLights|EuropePMC|HMDB_Feces|NIST_SRM8048|Local
    source_ref   TEXT,            -- condition / study_id / MTBLS_id / PMCID
    title        TEXT,
    year         INTEGER,
    method_hint  TEXT,
    matrix_hint  TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_src_unique ON sources(source_type, source_ref);

CREATE TABLE IF NOT EXISTS metabolite_sources (
    metabolite_id  INTEGER NOT NULL REFERENCES metabolites(metabolite_id),
    source_id      INTEGER NOT NULL REFERENCES sources(source_id),
    evidence_tag   TEXT,
    PRIMARY KEY (metabolite_id, source_id)
);
"""


# ── Connection context ──────────────────────────────────────────────────────

@contextmanager
def get_conn(db_path: str | Path):
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | Path) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with get_conn(db_path) as conn:
        conn.executescript(_DDL)
    migrate_db(db_path)


# ── Additive migration (non-destructive) ─────────────────────────────────────

_NEW_COLUMNS: list[tuple[str, str]] = [
    # (column_name, column_def)
    ("n_sources_distinct",   "INTEGER DEFAULT 0"),
    ("n_records_total",      "INTEGER DEFAULT 0"),
    ("source_types_distinct","TEXT    DEFAULT '[]'"),
    ("is_inorganic",         "TEXT    DEFAULT 'unknown'"),
    ("is_drug",              "TEXT    DEFAULT 'unknown'"),
    ("is_environmental",     "TEXT    DEFAULT 'unknown'"),
    ("is_category_like",     "TEXT    DEFAULT 'false'"),
    ("volatility",           "TEXT    DEFAULT 'Unknown'"),
    ("gc_compatible",        "TEXT    DEFAULT 'Unknown'"),
    ("lc_compatible",        "TEXT    DEFAULT 'Unknown'"),
    ("fecal_hint",           "INTEGER DEFAULT 0"),
    ("from_text_mining",     "INTEGER DEFAULT 0"),
    ("matrix_hints_col",     "TEXT    DEFAULT ''"),    # separate from tags_json
    ("fecal_catalog_flags",  "TEXT    DEFAULT '{}'"),
    ("resolved_ids",         "TEXT    DEFAULT '{}'"),
    # fecal_mental mode additions
    ("fecal_evidence_type",  "TEXT    DEFAULT ''"),    # dataset_metadata|fecal_catalog|text_mining_claim
    ("condition_hits",       "TEXT    DEFAULT '[]'"),  # JSON list: ["schizophrenia","depression",...]
]

_NEW_SOURCE_COLUMNS: list[tuple[str, str]] = [
    ("oa_flag", "INTEGER DEFAULT 0"),
]


def migrate_db(db_path: str | Path) -> None:
    """Add new columns to existing tables without losing data."""
    with get_conn(db_path) as conn:
        existing_met = {
            row[1]
            for row in conn.execute("PRAGMA table_info(metabolites)").fetchall()
        }
        for col, defn in _NEW_COLUMNS:
            if col not in existing_met:
                try:
                    conn.execute(f"ALTER TABLE metabolites ADD COLUMN {col} {defn}")
                except Exception:
                    pass  # column already exists

        existing_src = {
            row[1]
            for row in conn.execute("PRAGMA table_info(sources)").fetchall()
        }
        for col, defn in _NEW_SOURCE_COLUMNS:
            if col not in existing_src:
                try:
                    conn.execute(f"ALTER TABLE sources ADD COLUMN {col} {defn}")
                except Exception:
                    pass


# ── Source helpers ──────────────────────────────────────────────────────────

def upsert_source(
    conn: sqlite3.Connection,
    source_type: str,
    source_ref: str,
    title: Optional[str] = None,
    year: Optional[int] = None,
    method_hint: Optional[str] = None,
    matrix_hint: Optional[str] = None,
) -> int:
    conn.execute(
        """
        INSERT INTO sources (source_type, source_ref, title, year, method_hint, matrix_hint)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_type, source_ref) DO UPDATE SET
            title       = COALESCE(excluded.title,       title),
            year        = COALESCE(excluded.year,        year),
            method_hint = COALESCE(excluded.method_hint, method_hint),
            matrix_hint = COALESCE(excluded.matrix_hint, matrix_hint)
        """,
        (source_type, source_ref, title, year, method_hint, matrix_hint),
    )
    row = conn.execute(
        "SELECT source_id FROM sources WHERE source_type=? AND source_ref=?",
        (source_type, source_ref),
    ).fetchone()
    return row["source_id"]


# ── Metabolite helpers ──────────────────────────────────────────────────────

def get_metabolite_by_norm(conn: sqlite3.Connection, normalized_key: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM metabolites WHERE normalized_key=?", (normalized_key,)
    ).fetchone()


def get_metabolite_by_inchikey(conn: sqlite3.Connection, inchikey: str) -> Optional[sqlite3.Row]:
    if not inchikey:
        return None
    return conn.execute(
        "SELECT * FROM metabolites WHERE inchikey=?", (inchikey,)
    ).fetchone()


def get_metabolite_by_cid(conn: sqlite3.Connection, cid: int) -> Optional[sqlite3.Row]:
    if not cid:
        return None
    return conn.execute(
        "SELECT * FROM metabolites WHERE pubchem_cid=?", (cid,)
    ).fetchone()


def upsert_metabolite(
    conn: sqlite3.Connection,
    canonical_name: str,
    normalized_key: str,
    inchikey: Optional[str] = None,
    pubchem_cid: Optional[int] = None,
    tags: Optional[dict] = None,
) -> int:
    """
    Insert or find a metabolite. Returns metabolite_id.
    Priority for merging: InChIKey > PubChem CID > normalized_key.
    """
    existing = None

    # 1) Match by InChIKey
    if inchikey:
        existing = get_metabolite_by_inchikey(conn, inchikey)

    # 2) Match by PubChem CID
    if existing is None and pubchem_cid:
        existing = get_metabolite_by_cid(conn, pubchem_cid)

    # 3) Match by normalized_key
    if existing is None:
        existing = get_metabolite_by_norm(conn, normalized_key)

    tags_json = json.dumps(tags or {})

    if existing is None:
        conn.execute(
            """
            INSERT INTO metabolites
                (canonical_name, normalized_key, inchikey, pubchem_cid, status, tags_json)
            VALUES (?, ?, ?, ?, 'candidate', ?)
            """,
            (canonical_name, normalized_key, inchikey, pubchem_cid, tags_json),
        )
        mid = conn.execute(
            "SELECT metabolite_id FROM metabolites WHERE normalized_key=?",
            (normalized_key,),
        ).fetchone()["metabolite_id"]
    else:
        mid = existing["metabolite_id"]
        # Merge tags — booleans use OR (True wins), lists use union
        old_tags = json.loads(existing["tags_json"] or "{}")
        for k, v in (tags or {}).items():
            if k not in old_tags:
                old_tags[k] = v
            elif isinstance(v, bool) and isinstance(old_tags[k], bool):
                old_tags[k] = old_tags[k] or v   # True is never overwritten by False
            elif isinstance(v, list) and isinstance(old_tags[k], list):
                old_tags[k] = sorted(set(old_tags[k] + v))
            else:
                old_tags[k] = v
        conn.execute(
            """
            UPDATE metabolites SET
                inchikey    = COALESCE(inchikey,    ?),
                pubchem_cid = COALESCE(pubchem_cid, ?),
                tags_json   = ?
            WHERE metabolite_id = ?
            """,
            (inchikey, pubchem_cid, json.dumps(old_tags), mid),
        )

    return mid


def add_synonym(conn: sqlite3.Connection, metabolite_id: int, synonym: str, norm_key: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO synonyms (metabolite_id, synonym, normalized_key)
        VALUES (?, ?, ?)
        """,
        (metabolite_id, synonym, norm_key),
    )


def link_metabolite_source(
    conn: sqlite3.Connection, metabolite_id: int, source_id: int, evidence_tag: str
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO metabolite_sources (metabolite_id, source_id, evidence_tag)
        VALUES (?, ?, ?)
        """,
        (metabolite_id, source_id, evidence_tag),
    )


# ── Stats helpers ───────────────────────────────────────────────────────────

def count_metabolites(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM metabolites").fetchone()[0]


def count_sources(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
