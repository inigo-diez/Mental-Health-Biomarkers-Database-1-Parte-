"""
Main collector orchestrator.

Usage:
    python -m src.collect [--config config.yaml] [--sources markerdb,ctd,...]
                          [--skip-pubchem] [--dry-run]

Runs collectors in priority order:
  1. MarkerDB
  2. CTD
  3. Metabolomics Workbench
  4. MetaboLights
  5. Europe PMC
  6. HMDB Feces (tagging pass)
"""

from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import yaml

# ── Logging setup ────────────────────────────────────────────────────────────

def _setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


# ── Config loader ────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Resolve paths relative to config file location
    base = Path(path).parent
    for key in ("db", "outputs", "cache", "logs", "papers_local"):
        if key in cfg.get("paths", {}):
            p = Path(cfg["paths"][key])
            if not p.is_absolute():
                cfg["paths"][key] = str(base / p)
    # Ensure dirs exist (db and logs point to files, so create their parents)
    for key in ("outputs", "cache", "papers_local"):
        Path(cfg["paths"][key]).mkdir(parents=True, exist_ok=True)
    for key in ("db", "logs"):
        Path(cfg["paths"][key]).parent.mkdir(parents=True, exist_ok=True)
    return cfg


# ── PubChem enrichment pass ──────────────────────────────────────────────────

def _enrich_pubchem(cfg: dict, db_path: str) -> None:
    from src.db import get_conn, count_metabolites
    from src.pubchem import PubChemResolver
    from src.utils import HTTPClient
    from src.normalize import make_key

    http = HTTPClient(
        rate=cfg.get("pubchem_rate", 5),
        retry_max=cfg.get("retry_max", 5),
        retry_backoff=cfg.get("retry_backoff", 2.0),
        cache_dir=str(cfg["paths"]["cache"]) + "/pubchem",
        cache_ttl_days=60,
    )
    resolver = PubChemResolver(http, enabled=True)

    logger = logging.getLogger("pubchem_enrichment")
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT metabolite_id, canonical_name FROM metabolites "
            "WHERE pubchem_cid IS NULL AND inchikey IS NULL "
            "ORDER BY metabolite_id LIMIT 5000"
        ).fetchall()

    logger.info("PubChem: enriching %d metabolites without CID/InChIKey", len(rows))
    enriched = 0
    for row in rows:
        result = resolver.resolve(row["canonical_name"])
        if result.cid or result.inchikey:
            with get_conn(db_path) as conn:
                conn.execute(
                    """
                    UPDATE metabolites
                    SET pubchem_cid = COALESCE(pubchem_cid, ?),
                        inchikey    = COALESCE(inchikey,    ?),
                        canonical_name = CASE WHEN ? IS NOT NULL THEN ? ELSE canonical_name END
                    WHERE metabolite_id = ?
                    """,
                    (
                        result.cid, result.inchikey,
                        result.canonical_name, result.canonical_name,
                        row["metabolite_id"],
                    ),
                )
                # Add PubChem synonyms
                from src.db import add_synonym
                for syn in result.synonyms:
                    add_synonym(conn, row["metabolite_id"], syn, make_key(syn))
            enriched += 1
    logger.info("PubChem: enriched %d metabolites", enriched)


# ── Main ─────────────────────────────────────────────────────────────────────

def _run_standard_mode(
    cfg: dict, db_path: str, args, logger: logging.Logger
) -> None:
    """Standard collection mode: all configured sources."""
    from src.db import count_metabolites, count_sources, get_conn

    sources_cfg = cfg.get("sources", {})
    requested   = set(args.sources.split(",")) if args.sources else None

    def should_run(name: str, key: str) -> bool:
        if requested is not None:
            return name in requested
        return sources_cfg.get(key, True)

    results: dict[str, int] = {}
    t0 = time.perf_counter()

    # ── 1. MarkerDB ────────────────────────────────────────────────────────────
    if should_run("markerdb", "markerdb"):
        logger.info("--- MarkerDB ---")
        from src.collectors import markerdb
        results["markerdb"] = markerdb.collect(cfg, db_path)

    # ── 2. CTD ────────────────────────────────────────────────────────────────
    if should_run("ctd", "ctd"):
        logger.info("--- CTD ---")
        from src.collectors import ctd
        results["ctd"] = ctd.collect(cfg, db_path)

    # ── 3. Metabolomics Workbench ──────────────────────────────────────────────
    if should_run("mwb", "metabolomics_workbench"):
        logger.info("--- Metabolomics Workbench ---")
        from src.collectors import metabolomics_workbench as mwb
        results["mwb"] = mwb.collect(cfg, db_path)

    # ── 4. MetaboLights ────────────────────────────────────────────────────────
    if should_run("metabolights", "metabolights"):
        logger.info("--- MetaboLights ---")
        from src.collectors import metabolights
        results["metabolights"] = metabolights.collect(cfg, db_path)

    # ── 5. Europe PMC ─────────────────────────────────────────────────────────
    if should_run("europepmc", "europe_pmc"):
        logger.info("--- Europe PMC ---")
        from src.collectors import europe_pmc
        results["europepmc"] = europe_pmc.collect(cfg, db_path)

    # ── 6. HMDB Feces (tagging pass) ──────────────────────────────────────────
    if should_run("hmdb_feces", "hmdb_feces"):
        logger.info("--- HMDB Feces ---")
        from src.collectors import hmdb_feces
        results["hmdb_feces"] = hmdb_feces.collect(cfg, db_path)

    # ── 7. PubTator3 (text mining) ────────────────────────────────────────────
    if should_run("pubtator", "pubtator"):
        logger.info("--- PubTator3 text mining ---")
        from src.collectors import pubtator
        results["pubtator"] = pubtator.collect(cfg, db_path)

    # ── PubChem enrichment ────────────────────────────────────────────────────
    use_pubchem = cfg.get("use_pubchem", True) and not args.skip_pubchem
    if use_pubchem:
        logger.info("--- PubChem enrichment ---")
        _enrich_pubchem(cfg, db_path)

    # ── Enrichment pass ───────────────────────────────────────────────────────
    logger.info("--- Enrichment pass ---")
    from src.enrich import run_enrichment
    run_enrichment(db_path)

    elapsed = time.perf_counter() - t0
    with get_conn(db_path) as conn:
        n_met = count_metabolites(conn)
        n_src = count_sources(conn)

    logger.info("=== DONE in %.1f s ===", elapsed)
    logger.info("Total unique metabolites in DB : %d", n_met)
    logger.info("Total sources in DB            : %d", n_src)
    for src, n in results.items():
        logger.info("  %-20s -> %d links", src, n)

    # Export
    logger.info("--- Exporting CSVs ---")
    from src import export as exp
    exp.export_all(cfg, db_path)
    logger.info("CSVs written to %s/", cfg["paths"]["outputs"])


def _run_fecal_mental_mode(
    cfg: dict, db_path: str, args, logger: logging.Logger
) -> None:
    """
    Fecal + mental-health focused collection mode.

    Runs dedicated fecal collectors (MWB, MetaboLights, text-mining) that
    filter strictly for fecal matrix AND mental-health conditions.
    Existing data is NOT deleted. Results are added to the shared DB and
    exported as outputs/fecal_mental_candidates.csv.
    """
    from pathlib import Path
    from src.db import count_metabolites, count_sources, get_conn
    from src.fecal_export import export_fecal_mental, write_health_report

    out_dir = Path(cfg["paths"]["outputs"])
    health_all: dict = {}
    results: dict[str, int] = {}
    t0 = time.perf_counter()

    # ── 1. MWB fecal+MH ──────────────────────────────────────────────────────
    logger.info("--- MWB fecal+MH ---")
    from src.collectors import fecal_mwb
    n, h = fecal_mwb.collect(cfg, db_path)
    results["fecal_mwb"]   = n
    health_all["MWB"]      = h

    # ── 2. MetaboLights fecal+MH ─────────────────────────────────────────────
    logger.info("--- MetaboLights fecal+MH ---")
    from src.collectors import fecal_metabolights
    n, h = fecal_metabolights.collect(cfg, db_path)
    results["fecal_metabolights"] = n
    health_all["MetaboLights"]    = h

    # ── 3. Text-mining fecal+MH ──────────────────────────────────────────────
    logger.info("--- Text-mining fecal+MH (PubTator + EuropePMC) ---")
    from src.collectors import fecal_textmining
    n, h = fecal_textmining.collect(cfg, db_path)
    results["fecal_textmining"]     = n
    health_all["TextMining"]        = h

    # ── 4. HMDB Feces catalog (tagging only) ─────────────────────────────────
    if cfg.get("sources", {}).get("hmdb_feces", True):
        logger.info("--- HMDB Feces catalog tagging ---")
        from src.collectors import hmdb_feces
        results["hmdb_feces"] = hmdb_feces.collect(cfg, db_path)

    # ── 5. Enrichment pass ───────────────────────────────────────────────────
    # Note: skips classify_compound (user said no GC/LC/inorganic in this pass)
    logger.info("--- Enrichment pass (metrics + flags only) ---")
    from src.enrich import run_enrichment
    run_enrichment(db_path)

    # ── 6. Export fecal_mental_candidates.csv ────────────────────────────────
    logger.info("--- Exporting fecal+MH candidates ---")
    n_candidates = export_fecal_mental(cfg, db_path)
    write_health_report(health_all, out_dir)

    elapsed = time.perf_counter() - t0
    with get_conn(db_path) as conn:
        n_met = count_metabolites(conn)
        n_src = count_sources(conn)

    logger.info("=== FECAL+MH DONE in %.1f s ===", elapsed)
    logger.info("Total unique metabolites in DB   : %d", n_met)
    logger.info("Total sources in DB              : %d", n_src)
    logger.info("Fecal+MH candidates exported     : %d", n_candidates)
    for src, n in results.items():
        logger.info("  %-25s -> %d links", src, n)

    logger.info("Source health:")
    for src, h in health_all.items():
        logger.info("  %s: %s", src, h)


def _run_mh_biomarkers_mode(
    cfg: dict, db_path: str, args, logger: logging.Logger
) -> None:
    """
    Strict mental-health biomarkers mode.

    Runs dedicated MH collectors (MetaboLights, MWB, text-mining) that require
    an explicit whitelist condition match (schizophrenia, depression, bipolar,
    anxiety, PTSD, autism, ADHD).  Any biological matrix is accepted.
    Results are added to the shared DB and exported as mh_biomarkers.csv +
    report_mhb.html.  Existing data and report.html are NOT modified.
    """
    from pathlib import Path
    from src.db import count_metabolites, count_sources, get_conn
    from src.mh_export import export_mh_biomarkers, write_health_report
    from src.report_mhb import generate_report_mhb
    from src.enrich import run_enrichment

    out_dir = Path(cfg["paths"]["outputs"])
    health_all: dict = {}
    results: dict[str, int] = {}
    t0 = time.perf_counter()

    # ── 0. Manual Excel database ──────────────────────────────────────────────
    logger.info("--- Manual Excel database ---")
    from src.collectors import manual_excel
    n, h = manual_excel.collect(cfg, db_path)
    results["manual_excel"]    = n
    health_all["Manual_Excel"] = h

    # ── 1. MarkerDB (curated MH diseases) ────────────────────────────────────
    logger.info("--- MarkerDB ---")
    from src.collectors import markerdb
    results["markerdb"] = markerdb.collect(cfg, db_path)

    # ── 2. CTD (Comparative Toxicogenomics DB — disease-chemical links) ──────
    logger.info("--- CTD ---")
    from src.collectors import ctd
    results["ctd"] = ctd.collect(cfg, db_path)

    # ── 3. MetaboLights — strict MH whitelist, any matrix ────────────────────
    logger.info("--- MH MetaboLights (strict whitelist) ---")
    from src.collectors import mh_metabolights
    n, h = mh_metabolights.collect(cfg, db_path)
    results["mh_metabolights"]    = n
    health_all["MetaboLights_MH"] = h

    # ── 4. MWB — strict MH whitelist, any matrix ─────────────────────────────
    logger.info("--- MH MWB (strict whitelist) ---")
    from src.collectors import mh_mwb
    n, h = mh_mwb.collect(cfg, db_path)
    results["mh_mwb"]    = n
    health_all["MWB_MH"] = h

    # ── 5. Text-mining — PubTator3 + EuropePMC, strict whitelist ─────────────
    logger.info("--- MH Text-mining (PubTator + EuropePMC, strict whitelist) ---")
    from src.collectors import mh_textmining
    n, h = mh_textmining.collect(cfg, db_path)
    results["mh_textmining"]     = n
    health_all["TextMining_MH"]  = h

    # ── 6. Enrichment pass (propagates condition_hits, classifies compounds) ──
    logger.info("--- Enrichment pass ---")
    run_enrichment(db_path)

    # ── 7. Export mh_biomarkers.csv ──────────────────────────────────────────
    logger.info("--- Exporting MH biomarkers CSV ---")
    n_candidates = export_mh_biomarkers(cfg, db_path)
    write_health_report(health_all, out_dir)

    # ── 8. Generate report_mhb.html ──────────────────────────────────────────
    logger.info("--- Generating report_mhb.html ---")
    generate_report_mhb(cfg)

    elapsed = time.perf_counter() - t0
    with get_conn(db_path) as conn:
        n_met = count_metabolites(conn)
        n_src = count_sources(conn)

    logger.info("=== MH BIOMARKERS DONE in %.1f s ===", elapsed)
    logger.info("Total unique metabolites in DB   : %d", n_met)
    logger.info("Total sources in DB              : %d", n_src)
    logger.info("MH biomarker candidates exported : %d", n_candidates)
    for src, n in results.items():
        logger.info("  %-25s -> %d links", src, n)

    logger.info("Source health:")
    for src, h in health_all.items():
        logger.info("  %s: %s", src, h)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Metabolite collector — runs all configured sources"
    )
    parser.add_argument("--config",    default="config.yaml", help="Path to config.yaml")
    parser.add_argument(
        "--mode",
        default="standard",
        choices=["standard", "fecal_mental", "mh_biomarkers"],
        help=(
            "Collection mode: "
            "'standard' (all sources), "
            "'fecal_mental' (fecal+MH focused), "
            "'mh_biomarkers' (strict MH whitelist, any matrix)"
        ),
    )
    parser.add_argument(
        "--sources",
        default=None,
        help="(standard mode only) Comma-separated sources to run. "
             "Options: markerdb,ctd,mwb,metabolights,europepmc,hmdb_feces,pubtator",
    )
    parser.add_argument("--skip-pubchem", action="store_true", help="Skip PubChem enrichment pass")
    parser.add_argument("--dry-run",  action="store_true", help="Load config and exit without running")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    _setup_logging(cfg["paths"]["logs"])
    logger = logging.getLogger("collect")

    db_path = cfg["paths"]["db"]
    logger.info("=== Metabolite Collector starting (mode=%s) ===", args.mode)
    logger.info("DB: %s", db_path)

    # Init DB
    from src.db import init_db, count_metabolites, count_sources, get_conn
    init_db(db_path)

    if args.dry_run:
        logger.info("Dry run — exiting.")
        return

    if args.mode == "fecal_mental":
        _run_fecal_mental_mode(cfg, db_path, args, logger)
    elif args.mode == "mh_biomarkers":
        _run_mh_biomarkers_mode(cfg, db_path, args, logger)
    else:
        _run_standard_mode(cfg, db_path, args, logger)


if __name__ == "__main__":
    main()
