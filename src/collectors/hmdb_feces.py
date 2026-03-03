"""
HMDB Feces metabolite catalog collector.

Purpose: NOT a biomarker source. Used only to tag existing candidates with:
  - known_fecal_metabolite = true

Input: Local XML file from HMDB ("Stool Metabolites" category).
  Download manually from https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip
  and extract to cache/hmdb_metabolites_feces.xml  (or the full hmdb_metabolites.xml).

The XML format is:
  <hmdb>
    <metabolite>
      <accession>HMDB...</accession>
      <name>...</name>
      <synonyms><synonym>...</synonym></synonyms>
      <inchikey>...</inchikey>
      <biological_properties>
        <biospecimen_locations>
          <biospecimen>Feces</biospecimen>
        </biospecimen_locations>
      </biological_properties>
    </metabolite>
  </hmdb>

If the file is not found, the collector emits a warning and returns 0.
"""

from __future__ import annotations
import json
import logging
import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator, Optional

from src.db import get_conn, get_metabolite_by_norm, get_metabolite_by_inchikey, upsert_source
from src.normalize import normalize, make_key

logger = logging.getLogger(__name__)

_NS = {"hmdb": "http://www.hmdb.ca"}   # HMDB uses namespace in XML


def _iter_feces_metabolites(xml_path: Path) -> Iterator[dict]:
    """
    Stream HMDB XML and yield metabolites that have 'Feces' as biospecimen.
    Uses iterparse to avoid loading the whole file into memory.
    """
    context = ET.iterparse(str(xml_path), events=("end",))
    for event, elem in context:
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if tag != "metabolite":
            continue

        # Biospecimen locations
        locations = [
            (loc.text or "").strip().lower()
            for loc in elem.iter()
            if (loc.tag.split("}")[-1] if "}" in loc.tag else loc.tag) == "biospecimen"
        ]
        if not any("fec" in loc or "stool" in loc for loc in locations):
            elem.clear()
            continue

        # Name
        name_el = elem.find("name") or elem.find("{http://www.hmdb.ca}name")
        name = (name_el.text or "").strip() if name_el is not None else ""
        if not name:
            elem.clear()
            continue

        # Accession
        acc_el = elem.find("accession") or elem.find("{http://www.hmdb.ca}accession")
        accession = (acc_el.text or "").strip() if acc_el is not None else None

        # InChIKey
        ik_el = elem.find("inchikey") or elem.find("{http://www.hmdb.ca}inchikey")
        inchikey = (ik_el.text or "").strip() if ik_el is not None else None
        if inchikey == "Not Available" or not inchikey:
            inchikey = None

        # Synonyms
        synonyms = []
        for syn_el in elem.iter():
            syn_tag = syn_el.tag.split("}")[-1] if "}" in syn_el.tag else syn_el.tag
            if syn_tag == "synonym" and syn_el.text:
                synonyms.append(syn_el.text.strip())

        yield {
            "name":      name,
            "accession": accession,
            "inchikey":  inchikey,
            "synonyms":  synonyms,
        }
        elem.clear()


def _flag_existing(conn: sqlite3.Connection, inchikey: Optional[str], nkey: str) -> bool:
    """
    Find an existing metabolite by InChIKey or normalized key and add the
    known_fecal_metabolite tag. Returns True if found.
    """
    row = None
    if inchikey:
        row = get_metabolite_by_inchikey(conn, inchikey)
    if row is None:
        row = get_metabolite_by_norm(conn, nkey)
    if row is None:
        return False

    mid = row["metabolite_id"]
    import json as _json
    old_tags = _json.loads(row["tags_json"] or "{}")
    old_tags["known_fecal_metabolite"] = True
    conn.execute(
        "UPDATE metabolites SET tags_json=? WHERE metabolite_id=?",
        (_json.dumps(old_tags), mid),
    )
    return True


def collect(cfg: dict, db_path: str) -> int:
    """
    Tag existing candidate metabolites as known fecal metabolites.
    Returns number of metabolites tagged.
    """
    hmdb_cfg  = cfg.get("hmdb", {})
    xml_path  = Path(hmdb_cfg.get("feces_xml_path", "cache/hmdb_metabolites_feces.xml"))

    if not xml_path.exists():
        # Also try full HMDB dump with any name
        alt = xml_path.parent / "hmdb_metabolites.xml"
        if alt.exists():
            xml_path = alt
        else:
            logger.warning(
                "HMDB feces XML not found at %s. "
                "Download from https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip "
                "and place the extracted XML in cache/",
                xml_path,
            )
            return 0

    logger.info("HMDB feces: parsing %s", xml_path)

    # Insert one source record for the HMDB catalog
    n_tagged = 0
    with get_conn(db_path) as conn:
        source_id = upsert_source(
            conn,
            source_type="HMDB_Feces",
            source_ref="HMDB_stool_catalog",
            title="HMDB Feces/Stool Metabolites Catalog",
            matrix_hint="fecal",
        )

        for rec in _iter_feces_metabolites(xml_path):
            nkey = make_key(rec["name"])
            found = _flag_existing(conn, rec["inchikey"], nkey)
            if found:
                n_tagged += 1
                # Also link to source
                row = (
                    get_metabolite_by_inchikey(conn, rec["inchikey"])
                    if rec["inchikey"]
                    else get_metabolite_by_norm(conn, nkey)
                )
                if row:
                    conn.execute(
                        "INSERT OR IGNORE INTO metabolite_sources (metabolite_id, source_id, evidence_tag) "
                        "VALUES (?,?,?)",
                        (row["metabolite_id"], source_id, "fecal_catalog"),
                    )
            # Even if not in DB yet, insert it as fecal_only metabolite
            # so researchers can cross-reference later
            else:
                canon = normalize(rec["name"])
                import json as _json
                conn.execute(
                    """
                    INSERT OR IGNORE INTO metabolites
                        (canonical_name, normalized_key, inchikey, status, tags_json)
                    VALUES (?, ?, ?, 'fecal_only', ?)
                    """,
                    (
                        canon, nkey, rec["inchikey"],
                        _json.dumps({"known_fecal_metabolite": True, "mental_health": False}),
                    ),
                )
                mid_row = conn.execute(
                    "SELECT metabolite_id FROM metabolites WHERE normalized_key=?", (nkey,)
                ).fetchone()
                if mid_row:
                    conn.execute(
                        "INSERT OR IGNORE INTO metabolite_sources (metabolite_id, source_id, evidence_tag) "
                        "VALUES (?,?,?)",
                        (mid_row["metabolite_id"], source_id, "fecal_catalog"),
                    )

    logger.info("HMDB feces: tagged %d existing metabolites as known_fecal_metabolite", n_tagged)
    return n_tagged
