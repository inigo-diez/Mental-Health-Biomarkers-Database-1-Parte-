"""
Mental-Health Biomarkers export.

Generates:
  outputs/mh_biomarkers.csv   — metabolites with at least one specific
                                whitelist condition (schizophrenia, depression,
                                bipolar, anxiety, ptsd, autism, adhd).
                                Generic 'mental_health' fallback entries are
                                excluded.
  outputs/mh_health.json      — per-source health statistics.

Columns in mh_biomarkers.csv
------------------------------
  metabolite_id
  canonical_name
  conditions          — pipe-separated (schizophrenia|depression|…)
  schizophrenia_hit   — 1/0
  depression_hit
  bipolar_hit
  anxiety_hit
  ptsd_hit
  autism_hit
  adhd_hit
  n_conditions        — how many distinct conditions
  n_sources_distinct
  n_links_total
  source_types_distinct
  source_refs         — comma-separated (capped at 15)
  matrix_hints        — pipe-separated distinct matrix hints
  method_hints        — pipe-separated (LC-MS, GC-MS, NMR, …)
  fecal_matrix        — 1 if any source has fecal/stool matrix
  mh_evidence         — dataset_metabolomics|text_mining_claim|…
  inchikey
  pubchem_cid

Usage:
    python -m src.mh_export [--config config.yaml] [--db outputs/metabolites.db]
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import re
import sys
from pathlib import Path

import yaml

from src.conditions import CONDITIONS

logger = logging.getLogger(__name__)

_SPECIFIC_CONDITIONS = set(CONDITIONS)   # schizophrenia, depression, …

_COLS = [
    "metabolite_id", "canonical_name", "conditions",
    "schizophrenia_hit", "depression_hit", "bipolar_hit",
    "anxiety_hit", "ptsd_hit", "autism_hit", "adhd_hit",
    "n_conditions", "n_sources_distinct", "n_links_total",
    "source_types_distinct", "source_refs",
    "matrix_hints", "method_hints", "fecal_matrix",
    "mh_evidence", "inchikey", "pubchem_cid",
    # New annotation columns
    "volatilidad", "vinculo_microbiota", "tipo_vinculo_microbiota", "posible_origen",
]

_FECAL_TERMS = ("fec", "stool", "faec", "cecal", "caecal", "copro", "intestinal content")

# ── Volatility annotation (from method_hints) ─────────────────────────────────

_GC_RE = re.compile(r"\bgc\b|hs[-.]spme|voc|\bgc.gc\b", re.IGNORECASE)
_LC_RE = re.compile(r"\blc\b|\bulhplc\b|\buhplc\b|\bhilic\b|\bhplc\b|\blc-ms\b", re.IGNORECASE)


def _compute_volatilidad(method_hints: list[str]) -> str:
    methods = "|".join(method_hints)
    has_gc = bool(_GC_RE.search(methods))
    has_lc = bool(_LC_RE.search(methods))
    if has_gc and has_lc:
        return "Mixto"
    if has_gc:
        return "VOC"
    if has_lc:
        return "No-volatil"
    return "Desconocido"


# ── Microbiota link annotation (from canonical name) ─────────────────────────

_SCFA_KW = [
    "acetic acid", "acetate", "acido acetico", "ácido acético",
    "propionic acid", "propionate", "acido propionico", "ácido propiónico",
    "butyric acid", "butyrate", "acido butirico", "ácido butírico",
    "valeric acid", "valerate", "acido valerico", "ácido valérico",
    "isobutyric acid", "isobutyrate", "acido isobutirico", "ácido isobutírico",
    "isovaleric acid", "isovalerate", "acido isovalerico", "ácido isovalérico",
    "hexanoic acid", "caproic acid", "acido hexanoico", "ácido hexanoico",
    "isohexanoic", "isocaproic", "acido isohexanoico",
    "pentanoic acid",
]
_INDOLE_KW = [
    "indole", "indol", "skatole", "skatol", "3-methylindole", "3-metilindol",
    "4-methylindole", "7-methylindole", "indole-3-acetic", "indole-3-propionic",
    "indol-3", "methylindole",
]
_PHENOL_KW = [
    "p-cresol", "4-methylphenol", "4-metilfenol",
    "4-ethylphenol", "4-etilfenol", "4-ethyl-phenol",
]
_AMINE_KW   = ["putrescine", "cadaverine", "spermidine", "spermine", "tyramine"]
_SULFUR_KW  = [
    "methanethiol", "methane thiol",
    "dimethyl sulfide", "dimethyl disulfide", "dimethyl trisulfide",
    "hydrogen sulfide",
    "dmds", "dmts",
]


def _classify_microbiota(
    name: str,
    fecal_matrix: bool,
    is_drug: int,
    is_category_like: int,
) -> tuple[str, str]:
    """Returns (vinculo_microbiota, tipo_vinculo_microbiota)."""
    if is_drug:
        return "No", "Desconocido"
    if is_category_like:
        return "Desconocido", "Desconocido"

    nl = name.lower()

    for kw in _SCFA_KW:
        if kw in nl:
            return "Si", "Producto bacteriano"

    for kw in _INDOLE_KW:
        if kw in nl:
            return "Si", "Transformacion bacteriana"

    for kw in _PHENOL_KW:
        if kw in nl:
            return "Si", "Transformacion bacteriana"
    # standalone phenol / fenol
    if re.search(r"^phenol$|^fenol$|\bphenol\b|\bfenol\b", nl) and len(nl) < 15:
        return "Si", "Transformacion bacteriana"

    for kw in _AMINE_KW:
        if kw in nl:
            return "Si", "Producto bacteriano"

    for kw in _SULFUR_KW:
        if kw in nl:
            return "Si", "Producto bacteriano"

    if fecal_matrix:
        return "Posible", "Marcador indirecto"

    return "Desconocido", "Desconocido"


def _compute_posible_origen(
    vinculo: str,
    is_drug: int,
    is_category_like: int,
) -> str:
    if vinculo == "Si":
        return "Microbiano probable"
    if vinculo == "Posible":
        return "Microbiano posible"
    if is_drug:
        return "Dieta/exogeno posible"
    if is_category_like:
        return "Desconocido"
    return "Desconocido"


def _is_fecal_hint(text: str) -> bool:
    t = text.lower()
    return any(term in t for term in _FECAL_TERMS)


def export_mh_biomarkers(cfg: dict, db_path: str | None = None) -> int:
    """
    Export MH biomarker candidates to CSV.
    Returns number of rows written.
    """
    from src.db import get_conn, migrate_db

    if db_path is None:
        db_path = cfg["paths"]["db"]

    migrate_db(db_path)

    out_dir  = Path(cfg["paths"]["outputs"])
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "mh_biomarkers.csv"

    with get_conn(db_path) as conn:
        rows = _query_mh_biomarkers(conn)
        _write_csv(rows, csv_path)

    # ── Log new annotation counts ────────────────────────────────────────────
    def _count(col: str, val: str) -> int:
        return sum(1 for r in rows if r.get(col) == val)

    logger.info("MH biomarkers: %d rows -> %s", len(rows), csv_path)
    logger.info(
        "  Volatilidad: VOC=%d | No-volatil=%d | Mixto=%d | Desconocido=%d",
        _count("volatilidad", "VOC"),
        _count("volatilidad", "No-volatil"),
        _count("volatilidad", "Mixto"),
        _count("volatilidad", "Desconocido"),
    )
    logger.info(
        "  Vinculo microbiota: Si=%d | Posible=%d | No=%d | Desconocido=%d",
        _count("vinculo_microbiota", "Si"),
        _count("vinculo_microbiota", "Posible"),
        _count("vinculo_microbiota", "No"),
        _count("vinculo_microbiota", "Desconocido"),
    )
    return len(rows)


def _query_mh_biomarkers(conn) -> list[dict]:
    """
    Fetch metabolites with at least one specific whitelist condition.
    Excludes rows where only generic 'mental_health' tag is set.
    """
    all_mets = conn.execute("""
        SELECT
            m.metabolite_id,
            m.canonical_name,
            m.inchikey,
            m.pubchem_cid,
            m.tags_json,
            m.source_types_distinct,
            COALESCE(m.n_sources_distinct, 0) AS n_sources_distinct,
            COALESCE(m.n_records_total,    0) AS n_links_total,
            m.condition_hits,
            m.fecal_evidence_type,
            COALESCE(m.is_drug,          0) AS is_drug,
            COALESCE(m.is_category_like, 0) AS is_category_like
        FROM metabolites m
    """).fetchall()

    result: list[dict] = []
    for met in all_mets:
        mid  = met["metabolite_id"]
        tags = json.loads(met["tags_json"] or "{}")

        # Build condition list from tags (per-condition booleans + condition_hits list)
        cond_set: set[str] = set()
        for c in CONDITIONS:
            if tags.get(c):
                cond_set.add(c)

        # Also check condition_hits list in tags_json
        tag_cond = tags.get("condition_hits", [])
        if isinstance(tag_cond, list):
            cond_set.update(c for c in tag_cond if c in _SPECIFIC_CONDITIONS)
        elif isinstance(tag_cond, str):
            try:
                lst = json.loads(tag_cond)
                cond_set.update(c for c in lst if c in _SPECIFIC_CONDITIONS)
            except (ValueError, TypeError):
                pass

        # Also check the condition_hits DB column
        db_cond_raw = met["condition_hits"] or "[]"
        try:
            db_cond = json.loads(db_cond_raw)
            cond_set.update(c for c in db_cond if c in _SPECIFIC_CONDITIONS)
        except (ValueError, TypeError):
            pass

        # Skip if no specific whitelist condition found
        if not cond_set:
            continue

        cond_list = sorted(cond_set)

        # Source info
        sources = conn.execute("""
            SELECT s.source_type, s.source_ref, s.matrix_hint, s.method_hint
            FROM metabolite_sources ms
            JOIN sources s ON ms.source_id = s.source_id
            WHERE ms.metabolite_id = ?
        """, (mid,)).fetchall()

        src_types = sorted({s["source_type"] for s in sources if s["source_type"]})
        src_refs  = sorted({s["source_ref"]  for s in sources if s["source_ref"]})

        matrix_hints = sorted({
            s["matrix_hint"] for s in sources if s["matrix_hint"]
        })
        method_hints = sorted({
            s["method_hint"] for s in sources if s["method_hint"]
        })
        fecal_matrix = 1 if any(_is_fecal_hint(m) for m in matrix_hints) else 0

        # Evidence type
        evidence_tags = conn.execute("""
            SELECT DISTINCT evidence_tag FROM metabolite_sources WHERE metabolite_id=?
        """, (mid,)).fetchall()
        ev_set = {r["evidence_tag"] for r in evidence_tags if r["evidence_tag"]}
        ev_parts = []
        if any("study_metabolite" in e or "dataset" in e for e in ev_set):
            ev_parts.append("dataset_metabolomics")
        if any("text_mining" in e for e in ev_set):
            ev_parts.append("text_mining_claim")
        if any("catalog" in e for e in ev_set):
            ev_parts.append("fecal_catalog")
        mh_evidence = "|".join(ev_parts) if ev_parts else "unknown"

        # ── New annotation columns ────────────────────────────────────────────
        volatilidad = _compute_volatilidad(method_hints)

        def _to_int(v) -> int:
            try:
                return int(v or 0)
            except (ValueError, TypeError):
                return 0
        is_drug          = _to_int(met["is_drug"])
        is_category_like = _to_int(met["is_category_like"])
        vinculo, tipo_vinculo = _classify_microbiota(
            met["canonical_name"] or "",
            fecal_matrix == 1,
            is_drug,
            is_category_like,
        )
        posible_origen = _compute_posible_origen(vinculo, is_drug, is_category_like)

        result.append({
            "metabolite_id":        mid,
            "canonical_name":       met["canonical_name"],
            "conditions":           "|".join(cond_list),
            "schizophrenia_hit":    1 if "schizophrenia" in cond_set else 0,
            "depression_hit":       1 if "depression"    in cond_set else 0,
            "bipolar_hit":          1 if "bipolar"       in cond_set else 0,
            "anxiety_hit":          1 if "anxiety"       in cond_set else 0,
            "ptsd_hit":             1 if "ptsd"          in cond_set else 0,
            "autism_hit":           1 if "autism"        in cond_set else 0,
            "adhd_hit":             1 if "adhd"          in cond_set else 0,
            "n_conditions":         len(cond_set),
            "n_sources_distinct":   met["n_sources_distinct"],
            "n_links_total":        met["n_links_total"],
            "source_types_distinct": ",".join(src_types),
            "source_refs":          ",".join(src_refs[:15]),
            "matrix_hints":         "|".join(matrix_hints),
            "method_hints":         "|".join(method_hints),
            "fecal_matrix":         fecal_matrix,
            "mh_evidence":          mh_evidence,
            "inchikey":             met["inchikey"] or "",
            "pubchem_cid":          met["pubchem_cid"] or "",
            "volatilidad":          volatilidad,
            "vinculo_microbiota":   vinculo,
            "tipo_vinculo_microbiota": tipo_vinculo,
            "posible_origen":       posible_origen,
        })

    # Sort: multi-condition first, then multi-source, then alphabetical
    result.sort(key=lambda r: (-r["n_conditions"], -r["n_sources_distinct"],
                               -r["n_links_total"], r["canonical_name"]))
    return result


def _write_csv(rows: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("MH biomarkers CSV: %d rows -> %s", len(rows), path)


def write_health_report(health_data: dict, out_dir: Path) -> None:
    path = out_dir / "mh_health.json"
    try:
        path.write_text(json.dumps(health_data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("MH health report: %s", path)
    except Exception as exc:
        logger.warning("Could not write mh_health.json: %s", exc)


# ── CLI entry ──────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export MH biomarkers CSV")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db",     default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    db_path = args.db or cfg["paths"]["db"]
    n = export_mh_biomarkers(cfg, db_path)
    print(f"Exported {n} MH biomarker candidates -> outputs/mh_biomarkers.csv")


if __name__ == "__main__":
    main()
