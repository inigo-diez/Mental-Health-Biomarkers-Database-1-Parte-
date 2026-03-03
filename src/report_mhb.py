"""
Generate report_mhb.html — focused Mental-Health Biomarkers interactive report.

Reads:
  - outputs/mh_biomarkers.csv               (primary data from DB pipeline)
  - outputs/MH_Biomarkers_Salud_Mental_CURADO.xlsx  (curated aux: Flag_contaminante,
      Origen_probable, Origen_alternativos, Confianza_origen, Motivo_origen)

Produces:
  - outputs/report_mhb.html

Retrocompatibilidad: si el Excel curado no existe, las columnas nuevas se rellenan
con defaults (Flag_contaminante="No", Origen_probable="Desconocido",
Origen_alternativos="", Confianza_origen="Baja", Motivo_origen="").

Usage:
    python -m src.report_mhb [--config config.yaml] [--out outputs/report_mhb.html]
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


def _load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _load_curated_excel(path: Path) -> dict[str, dict]:
    """
    Load MH_Biomarkers_Salud_Mental_CURADO.xlsx and return a dict keyed by str(ID)
    with the new annotation fields.  Falls back to CSV (same stem, .csv) if Excel
    cannot be read.
    Retrocompat defaults applied here; caller may still get {} for missing IDs.
    """
    flag_map: dict[str, dict] = {}

    # Try Excel first, then CSV sibling
    sources = [path]
    csv_sibling = path.with_suffix(".csv")
    if csv_sibling.exists():
        sources.append(csv_sibling)

    for src in sources:
        if not src.exists():
            continue
        try:
            if src.suffix.lower() in (".xlsx", ".xlsm", ".xls"):
                import openpyxl
                wb = openpyxl.load_workbook(str(src), read_only=True, data_only=True)
                ws = wb.active
                rows_iter = ws.iter_rows(values_only=True)
                headers = [str(h) if h is not None else "" for h in next(rows_iter)]
                for row_vals in rows_iter:
                    row_d = dict(zip(headers, row_vals))
                    _id = str(row_d.get("ID", "") or "").strip()
                    if not _id:
                        continue
                    flag_map[_id] = _row_to_aux(row_d)
                wb.close()
            else:
                # CSV with semicolon + BOM
                with open(src, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f, delimiter=";")
                    for row_d in reader:
                        _id = str(row_d.get("ID", "") or "").strip()
                        if not _id:
                            continue
                        flag_map[_id] = _row_to_aux(row_d)
            logger.info("Loaded %d curated entries from %s", len(flag_map), src)
            return flag_map
        except Exception as exc:
            logger.warning("Could not read curated file %s: %s", src, exc)

    logger.warning("No curated file found at %s — new columns will use defaults", path)
    return flag_map


def _row_to_aux(row_d: dict) -> dict:
    """Map curated-file row to aux dict with retrocompat defaults."""
    def _s(key: str, default: str = "") -> str:
        v = row_d.get(key)
        return str(v).strip() if v is not None else default

    return {
        "flag_cont":   _s("Flag_contaminante", "No") or "No",
        "origen_prob": _s("Origen_probable",  "Desconocido") or "Desconocido",
        "origen_alt":  _s("Origen_alternativos", ""),
        "confianza":   _s("Confianza_origen", "Baja") or "Baja",
        "motivo":      _s("Motivo_origen", ""),
    }


def _stats(rows: list[dict]) -> dict:
    def c(cond): return sum(1 for r in rows if r.get(f"{cond}_hit", "0") == "1")
    multi      = sum(1 for r in rows if int(r.get("n_sources_distinct") or 0) > 1)
    fecal      = sum(1 for r in rows if r.get("fecal_matrix", "0") == "1")
    multi_cond = sum(1 for r in rows if int(r.get("n_conditions") or 0) > 1)
    voc        = sum(1 for r in rows if r.get("volatilidad") == "VOC")
    mic_yes    = sum(1 for r in rows if r.get("vinculo_microbiota") == "Si")
    mic_poss   = sum(1 for r in rows if r.get("vinculo_microbiota") == "Posible")

    sources: dict[str, int] = {}
    for r in rows:
        for s in (r.get("source_types_distinct") or "").split(","):
            s = s.strip()
            if s:
                sources[s] = sources.get(s, 0) + 1

    return {
        "total":           len(rows),
        "schizophrenia":   c("schizophrenia"),
        "depression":      c("depression"),
        "bipolar":         c("bipolar"),
        "anxiety":         c("anxiety"),
        "ptsd":            c("ptsd"),
        "autism":          c("autism"),
        "adhd":            c("adhd"),
        "multi_source":    multi,
        "multi_condition": multi_cond,
        "fecal_matrix":    fecal,
        "voc_count":       voc,
        "microbiota_si":   mic_yes,
        "microbiota_pos":  mic_poss,
        "by_source":       dict(sorted(sources.items(), key=lambda x: -x[1])),
    }


def generate_report_mhb(cfg: dict, db_path: str | None = None) -> None:
    out_dir  = Path(cfg["paths"]["outputs"])
    csv_path = out_dir / "mh_biomarkers.csv"
    out_path = out_dir / "report_mhb.html"

    if not csv_path.exists():
        logger.error("mh_biomarkers.csv not found. Run mh_export first.")
        return

    rows  = _load_csv(csv_path)
    stats = _stats(rows)

    # ── Load curated Excel (replaces old aux CSV) ─────────────────────────────
    curated_path = out_dir / "MH_Biomarkers_Salud_Mental_CURADO.xlsx"
    flag_map = _load_curated_excel(curated_path)

    table_data = []
    for r in rows:
        n_dist  = int(r.get("n_sources_distinct") or 0)
        n_tot   = int(r.get("n_links_total") or 0)
        n_cond  = int(r.get("n_conditions") or 0)
        met_id  = str(r.get("metabolite_id", ""))
        aux     = flag_map.get(met_id, {})
        table_data.append({
            "id":         met_id,
            "name":       r.get("canonical_name", ""),
            "inchikey":   r.get("inchikey", "") or "",
            # CID kept in data but not shown in table
            "cid":        r.get("pubchem_cid", "") or "",
            "conds":      r.get("conditions", ""),
            "n_cond":     n_cond,
            "n_dist":     n_dist,
            "n_total":    n_tot,
            "sources":    r.get("source_types_distinct", ""),
            "matrix":     r.get("matrix_hints", "") or "",
            "method":     r.get("method_hints", "") or "",
            "fecal":      r.get("fecal_matrix", "0") == "1",
            "evidence":   r.get("mh_evidence", ""),
            "volatilidad":  r.get("volatilidad", "Desconocido"),
            "vinculo":      r.get("vinculo_microbiota", "Desconocido"),
            "tipo_vinculo": r.get("tipo_vinculo_microbiota", "Desconocido"),
            # Curated annotation columns (with retrocompat defaults)
            "flag_cont":   aux.get("flag_cont",   "No"),
            "origen_prob": aux.get("origen_prob", "Desconocido"),
            "origen_alt":  aux.get("origen_alt",  ""),
            "confianza":   aux.get("confianza",   "Baja"),
            "motivo":      aux.get("motivo",      ""),
        })

    # Stats derived from merged table_data
    stats["n_contaminante"]    = sum(1 for t in table_data if t["flag_cont"] == "Posible_contaminante")
    stats["n_confianza_alta"]  = sum(1 for t in table_data if t["confianza"] == "Alta")
    stats["n_confianza_media"] = sum(1 for t in table_data if t["confianza"] == "Media")
    stats["n_confianza_baja"]  = sum(1 for t in table_data if t["confianza"] == "Baja")

    data_json  = json.dumps(table_data, ensure_ascii=False)
    stats_json = json.dumps(stats,      ensure_ascii=False)

    # ── Dynamic source checkboxes ─────────────────────────────────────────────
    def _src_safe_id(s: str) -> str:
        return "chkSrc" + "".join(c for c in s if c.isalnum())

    src_checkbox_html = ""
    for s, n in stats["by_source"].items():
        sid = _src_safe_id(s)
        src_checkbox_html += (
            f'        <div class="form-check mb-1">\n'
            f'          <input class="form-check-input" type="checkbox" id="{sid}">\n'
            f'          <label class="form-check-label small" for="{sid}"'
            f' title="{n:,} registros">{s}</label>\n'
            f'        </div>\n'
        )

    src_filter_map_js = "".join(
        f'    ["{_src_safe_id(s)}", "{s}"],\n'
        for s in stats["by_source"]
    )
    src_filter_ids_js = ",\n  ".join(
        f'"{_src_safe_id(s)}"' for s in stats["by_source"]
    )
    src_reset_js = "\n  ".join(
        f'document.getElementById("{_src_safe_id(s)}").checked = false;'
        for s in stats["by_source"]
    )

    # All unique source names for header (sorted by name for readability)
    all_sources_header = " \u00b7 ".join(sorted(stats["by_source"].keys()))

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MH Biomarkers — Biomarcadores Salud Mental</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.bootstrap5.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/select/1.7.0/css/select.bootstrap5.min.css">
<style>
  body {{ font-size: 0.86rem; background: #f8f9fa; }}
  h1   {{ font-size: 1.25rem; font-weight: 700; }}
  .stat-card {{ border-radius: 8px; text-align: center; padding: 12px 8px; }}
  .stat-num  {{ font-size: 1.9rem; font-weight: 700; line-height: 1.1; }}
  .stat-lbl  {{ font-size: 0.70rem; color: #555; }}
  table.dataTable td {{ vertical-align: middle; }}
  .name-cell  {{ max-width: 210px; word-break: break-word; }}
  .cond-badge {{ font-size: 0.60rem; padding: 1px 3px; border-radius: 3px; margin: 1px; }}
  .voc-badge  {{ font-size: 0.60rem; padding: 1px 4px; border-radius: 3px; }}
  /* ── Tabla compacta: toda columna trunca con ellipsis ── */
  #mhTable {{ table-layout: fixed; width: 100% !important; }}
  #mhTable td {{ font-size: 0.72rem; white-space: nowrap; overflow: hidden;
                 text-overflow: ellipsis; padding: 3px 5px !important; }}
  #mhTable th {{ font-size: 0.72rem; white-space: nowrap; overflow: hidden;
                 text-overflow: ellipsis; padding: 4px 5px !important; }}
  /* ── Filtros: una sola fila con scroll horizontal propio ── */
  #filterBar  {{ gap: 6px; flex-wrap: nowrap; overflow-x: auto; align-items: flex-start;
                 padding-bottom: 4px; }}
  .filter-group {{ border: 1px solid #dee2e6; border-radius: 6px;
                   padding: 5px 8px; background: #fff; min-width: 90px; flex-shrink: 0; }}
  .filter-group label.group-title {{
    font-weight: 700; font-size: 0.73rem; display: block; margin-bottom: 4px;
    text-transform: uppercase; letter-spacing: 0.03em; color: #444; }}
  .info-panel {{
    border: 1px solid #b8daff; background: #e8f4fd; border-radius: 5px;
    padding: 5px 9px; font-size: 0.69rem; color: #1a4a7a; margin-top: 6px; }}
  .info-panel ul {{ margin: 3px 0 0 0; padding-left: 14px; }}
  .info-panel li {{ margin-bottom: 2px; }}
  .info-toggle {{ cursor: pointer; color: #0d6efd; font-size: 0.68rem;
                  text-decoration: none; user-select: none; }}
  .info-toggle:hover {{ text-decoration: underline; }}
  .flag-badge {{ font-size: 0.63rem; padding: 1px 5px; border-radius: 3px; cursor: help; }}
  .dt-buttons .btn {{ background-color: #fff !important; }}
  .legend-box {{
    border: 1px solid #b8daff; background: #e8f4fd; border-radius: 6px;
    padding: 7px 11px; font-size: 0.68rem; color: #1a4a7a;
    max-width: 310px; line-height: 1.45; }}
  .legend-box summary {{
    font-weight: 700; font-size: 0.71rem; cursor: pointer;
    color: #0d6efd; list-style: none; display: flex; align-items: center; gap: 4px; }}
  .legend-box summary::-webkit-details-marker {{ display: none; }}
  .legend-box summary::before {{ content: "\u24d8"; font-size: 0.85rem; }}
  .legend-box ul {{ margin: 5px 0 0 0; padding-left: 13px; }}
  .legend-box li {{ margin-bottom: 4px; }}
  .legend-box .leg-note {{
    margin-top: 5px; padding: 4px 7px; background: #fff3cd;
    border-left: 3px solid #ffc107; border-radius: 2px; color: #664d03; }}
  /* Row selection */
  table.dataTable tbody tr.selected td {{ background-color: #cfe2ff !important; }}
</style>
</head>
<body>
<div class="container-fluid py-3">

<!-- Header -->
<div class="mb-2">
  <h1 class="mb-0">Biomarcadores Metabolicos — Enfermedades Mentales</h1>
  <small class="text-muted">
    Condiciones: esquizofrenia · depresion · bipolar · ansiedad · PTSD · autismo · TDAH
    &nbsp;|&nbsp; Fuentes: {all_sources_header}
    &nbsp;|&nbsp; <strong>Por I&ntilde;igo Diez Osua</strong>
  </small>
</div>

<!-- Stats row -->
<div class="row g-2 mb-3" id="statsRow"></div>

<!-- Filter bar -->
<div class="card mb-3 shadow-sm">
  <div class="card-body py-2">
    <div class="d-flex" id="filterBar">

      <!-- Source -->
      <div class="filter-group" style="max-width:130px">
        <label class="group-title">Fuente</label>
{src_checkbox_html}      </div>

      <!-- Condicion especifica -->
      <div class="filter-group">
        <label class="group-title">Condicion</label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkSchiz">
          <label class="form-check-label small" for="chkSchiz">Esquizofrenia</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkDep">
          <label class="form-check-label small" for="chkDep">Depresion</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkBip">
          <label class="form-check-label small" for="chkBip">Bipolar</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkAnx">
          <label class="form-check-label small" for="chkAnx">Ansiedad</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkPtsd">
          <label class="form-check-label small" for="chkPtsd">PTSD</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkAut">
          <label class="form-check-label small" for="chkAut">Autismo</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkAdhd">
          <label class="form-check-label small" for="chkAdhd">TDAH</label>
        </div>
      </div>

      <!-- Evidencia y calidad -->
      <div class="filter-group">
        <label class="group-title">Calidad</label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkMulti">
          <label class="form-check-label small" for="chkMulti">&gt;1 fuente distinta</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkMultiCond">
          <label class="form-check-label small" for="chkMultiCond">&gt;1 condicion</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkDataset">
          <label class="form-check-label small" for="chkDataset">Solo datasets</label>
        </div>
      </div>

      <!-- Matriz fecal -->
      <div class="filter-group border-success">
        <label class="group-title text-success">Matriz Fecal</label>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkFecal" style="accent-color:#198754">
          <label class="form-check-label small fw-semibold text-success" for="chkFecal">
            Solo matriz fecal/heces
          </label>
        </div>
        <hr class="my-1">
        <label class="group-title mt-1">Otras matrices</label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkPlasma">
          <label class="form-check-label small" for="chkPlasma">Plasma/suero</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkUrine">
          <label class="form-check-label small" for="chkUrine">Orina</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkCSF">
          <label class="form-check-label small" for="chkCSF">LCR / CSF</label>
        </div>
      </div>

      <!-- Volatilidad -->
      <div class="filter-group">
        <label class="group-title">Volatilidad</label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkVolVOC">
          <label class="form-check-label small" for="chkVolVOC">VOC</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkVolNoVol">
          <label class="form-check-label small" for="chkVolNoVol">No-volatil</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkVolMixto">
          <label class="form-check-label small" for="chkVolMixto">Mixto</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkVolDesc">
          <label class="form-check-label small" for="chkVolDesc">Desconocido</label>
        </div>
      </div>

      <!-- Vinculo microbiota -->
      <div class="filter-group">
        <label class="group-title">Vinculo Microbiota
          <a class="info-toggle ms-1" onclick="toggleInfo('infoVinculo')" title="Ver leyenda">[i]</a>
        </label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkVinSi">
          <label class="form-check-label small" for="chkVinSi">Si</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkVinPosible">
          <label class="form-check-label small" for="chkVinPosible">Posible</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkVinNo">
          <label class="form-check-label small" for="chkVinNo">No</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkVinDesc">
          <label class="form-check-label small" for="chkVinDesc">Desconocido</label>
        </div>
        <div class="info-panel d-none" id="infoVinculo">
          <strong style="font-size:.68rem">Leyenda Vinculo Microbiota</strong>
          <ul>
            <li><strong>Si</strong>: evidencia solida de que el metabolito es producido por microbiota o su presencia depende directamente de actividad microbiana.</li>
            <li><strong>Posible</strong>: relacion plausible/indirecta (reportado en literatura como asociado a microbiota) pero sin demostracion directa o puede depender de dieta/host.</li>
            <li><strong>No</strong>: sin evidencia de relacion con microbiota (mas probable origen humano, dieta, farmaco, ambiente o artefacto).</li>
          </ul>
        </div>
        <hr class="my-1">
        <label class="group-title mt-1">Tipo vinculo
          <a class="info-toggle ms-1" onclick="toggleInfo('infoTipo')" title="Ver leyenda">[i]</a>
        </label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkTipoProd">
          <label class="form-check-label small" for="chkTipoProd">Producto bacteriano</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkTipoTrans">
          <label class="form-check-label small" for="chkTipoTrans">Transformacion bact.</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkTipoMarcador">
          <label class="form-check-label small" for="chkTipoMarcador">Marcador indirecto</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkTipoDesc">
          <label class="form-check-label small" for="chkTipoDesc">Desconocido</label>
        </div>
        <div class="info-panel d-none" id="infoTipo">
          <strong style="font-size:.68rem">Leyenda Tipo Vinculo</strong>
          <ul>
            <li><strong>Producto bacteriano</strong>: metabolito generado directamente por rutas metabolicas bacterianas (fermentacion, putrefaccion, etc.).</li>
            <li><strong>Transformacion bact.</strong>: compuesto que llega por dieta/huesped y la microbiota lo modifica (biotransformacion).</li>
            <li><strong>Marcador indirecto</strong>: no producido por bacterias, pero refleja cambios en microbiota/funcion intestinal (pH, inflamacion, permeabilidad, etc.).</li>
          </ul>
        </div>
      </div>

      <!-- Contaminacion -->
      <div class="filter-group border-danger">
        <label class="group-title text-danger">Contaminacion
          <a class="info-toggle ms-1" onclick="toggleInfo('infoCont')" title="Ver leyenda">[i]</a>
        </label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkContSi" style="accent-color:#dc3545">
          <label class="form-check-label small text-danger fw-semibold" for="chkContSi">
            Posible contaminante
          </label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkContNo">
          <label class="form-check-label small" for="chkContNo">No contaminante</label>
        </div>
        <div class="info-panel d-none" id="infoCont">
          <strong style="font-size:.68rem">Leyenda Flag_contaminante</strong>
          <ul>
            <li><strong>No</strong> &rarr; compuesto compatible con origen biologico o sin indicios de interferencia.</li>
            <li><strong>Posible_contaminante</strong> &rarr; senal potencialmente influida por factores no biologicos (exposicion exogena y/o interferencias tecnicas): dieta, farmacos/excipientes, higiene/cosmetica, ambiente/limpieza, consumibles o instrumental.</li>
          </ul>
          <div style="margin-top:4px;padding:3px 6px;background:#fff3cd;border-left:3px solid #ffc107;border-radius:2px;color:#664d03">
            <strong>Nota:</strong> &ldquo;Posible_contaminante&rdquo; no implica necesariamente contaminacion del laboratorio; en muchos casos refleja exposicion (p.&nbsp;ej., dieta o medicacion).
          </div>
        </div>
      </div>

      <!-- Origen probable -->
      <div class="filter-group">
        <label class="group-title">Origen probable
          <a class="info-toggle ms-1" onclick="toggleInfo('infoOrigen')" title="Ver leyenda">[i]</a>
        </label>
        <div class="info-panel d-none" id="infoOrigen">
          <strong style="font-size:.68rem">Leyenda Origen_probable</strong><br>
          <span style="font-size:.67rem;color:#333">Clasifica el origen mas plausible del compuesto para ayudar a interpretar si la senal es biologica, exogena o tecnica.</span>
          <ul style="margin-top:4px">
            <li><strong>Biologico humano</strong>: metabolitos endogenos del huesped.</li>
            <li><strong>Biologico microbiano</strong>: metabolitos atribuibles a actividad de la microbiota (p.&nbsp;ej., fermentacion/biotransformacion).</li>
            <li><strong>Dieta</strong>: compuestos derivados de alimentos o ingesta reciente.</li>
            <li><strong>Farmaco/excipiente</strong>: principios activos, metabolitos de farmacos o excipientes frecuentes.</li>
            <li><strong>Higiene/cosmetica</strong>: compuestos tipicos de fragancias y cuidado personal.</li>
            <li><strong>Ambiente/limpieza</strong>: VOCs ambientales o asociados a productos de limpieza/disolventes.</li>
            <li><strong>Pre-analitica (recogida/recipiente)</strong>: aportes del material de recogida/transporte (recipientes, tapones, etc.).</li>
            <li><strong>Laboratorio (consumibles/reactivos)</strong>: aportes de reactivos, viales, septa, guantes, etc.</li>
            <li><strong>Instrumental (GC-MS/columna)</strong>: artefactos del sistema (bleed de columna/septo, carryover).</li>
            <li><strong>Mis-ID / dudoso</strong>: anotacion poco fiable (coelusion, baja calidad de match).</li>
            <li><strong>Desconocido</strong>: origen no asignable con la informacion disponible.</li>
          </ul>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenInst">
          <label class="form-check-label small" for="chkOrigenInst">Instrumental (GC-MS)</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenLab">
          <label class="form-check-label small" for="chkOrigenLab">Laboratorio</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenPre">
          <label class="form-check-label small" for="chkOrigenPre">Pre-analitica</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenAmb">
          <label class="form-check-label small" for="chkOrigenAmb">Ambiente/limpieza</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenHig">
          <label class="form-check-label small" for="chkOrigenHig">Higiene/cosmetica</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenFar">
          <label class="form-check-label small" for="chkOrigenFar">Farmaco/excipiente</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenDieta">
          <label class="form-check-label small" for="chkOrigenDieta">Dieta</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenMicro">
          <label class="form-check-label small" for="chkOrigenMicro">Biologico microbiano</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenHum">
          <label class="form-check-label small" for="chkOrigenHum">Biologico humano</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkOrigenMisID">
          <label class="form-check-label small" for="chkOrigenMisID">Mis-ID / dudoso</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkOrigenDesc">
          <label class="form-check-label small" for="chkOrigenDesc">Desconocido</label>
        </div>
      </div>

      <!-- Confianza origen -->
      <div class="filter-group border-primary">
        <label class="group-title text-primary">Confianza Origen
          <a class="info-toggle ms-1" onclick="toggleInfo('infoConfianza')" title="Ver leyenda">[i]</a>
        </label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkConfAlta" style="accent-color:#198754">
          <label class="form-check-label small text-success fw-semibold" for="chkConfAlta">Alta</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkConfMedia">
          <label class="form-check-label small" for="chkConfMedia">Media</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkConfBaja">
          <label class="form-check-label small text-muted" for="chkConfBaja">Baja</label>
        </div>
        <div class="info-panel d-none" id="infoConfianza">
          <strong style="font-size:.68rem">Leyenda Confianza_origen</strong>
          <ul>
            <li><strong>Alta</strong>: asignacion de origen muy probable (evidencia bioquimica clara o compuesto bien caracterizado).</li>
            <li><strong>Media</strong>: asignacion plausible pero con cierta ambiguedad.</li>
            <li><strong>Baja</strong>: origen incierto; requiere validacion adicional.</li>
          </ul>
        </div>
      </div>

      <button class="btn btn-sm btn-outline-secondary ms-auto align-self-start mt-2"
              onclick="resetFilters()">Resetear</button>
    </div>
  </div>
</div>

<!-- Table -->
<div class="card shadow-sm">
  <div class="card-body p-2">
    <div class="mb-1 d-flex gap-3 flex-wrap" style="font-size:.71rem; color:#555; line-height:1.4">
      <span style="font-style:italic">
        <strong style="font-style:normal">Tooltip:</strong>
        situe el cursor sobre cualquier celda que muestre &ldquo;&hellip;&rdquo; para visualizar su contenido completo.
      </span>
      <span style="border-left:2px solid #dee2e6; padding-left:.6rem; font-style:italic">
        <strong style="font-style:normal">Exportar:</strong>
        clic en fila para seleccionar (Ctrl+clic para varias);
        si hay seleccion activa se exportaran unicamente dichos registros,
        en caso contrario se exportara el conjunto filtrado completo.
      </span>
    </div>
    <table id="mhTable" class="table table-sm table-striped table-hover w-100">
      <thead class="table-dark">
        <tr>
          <th>ID</th>
          <th>Nombre canonico</th>
          <th>InChIKey</th>
          <th title="Condiciones especificas detectadas">Condiciones</th>
          <th title="N condiciones distintas" class="text-center">#C</th>
          <th title="Fuentes de datos distintas" class="text-center">#F</th>
          <th title="Total links en BD" class="text-center">#L</th>
          <th>Fuentes</th>
          <th>Matriz</th>
          <th>Metodo</th>
          <th>Evidencia</th>
          <th title="Volatilidad: derivada del metodo analitico">Volatilidad</th>
          <th title="Vinculo con microbiota intestinal">Vinculo Microbio.</th>
          <th title="Tipo de vinculo con microbiota">Tipo Vinculo</th>
          <th title="Flag de posible contaminante analitico">Flag Contam.</th>
          <th title="Origen probable del metabolito o contaminante (curado)">Origen probable</th>
          <th title="Origenes alternativos posibles">Orig. Alternativos</th>
          <th title="Nivel de confianza en la asignacion de origen de Origen_probable">Confianza</th>
          <th title="Motivo de la asignacion de origen (columna oculta — visible en tooltip de Confianza)">Motivo</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

</div>

<!-- Footer -->
<footer class="text-center text-muted mt-4 mb-2" style="font-size:0.72rem; border-top:1px solid #dee2e6; padding-top:10px;">
  &copy; 2026 I&ntilde;igo Diez Osua &mdash; Base de Datos para Trabajo de Fin de Grado.<br>
  Uso acad&eacute;mico. Prohibida la reproducci&oacute;n sin autorizaci&oacute;n.
</footer>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.bootstrap5.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>
<script src="https://cdn.datatables.net/select/1.7.0/js/dataTables.select.min.js"></script>

<script>
const ALL_DATA = {data_json};
const STATS    = {stats_json};

// ── Stats cards ──────────────────────────────────────────────────────────────
const statsConfig = [
  {{ label: "Total MH biomarkers",   value: STATS.total,              color: "primary"   }},
  {{ label: "Esquizofrenia",         value: STATS.schizophrenia,      color: "danger"    }},
  {{ label: "Depresion",             value: STATS.depression,         color: "primary"   }},
  {{ label: "Bipolar",               value: STATS.bipolar,            color: "warning"   }},
  {{ label: "Ansiedad",              value: STATS.anxiety,            color: "info"      }},
  {{ label: "PTSD",                  value: STATS.ptsd,               color: "secondary" }},
  {{ label: "Autismo",               value: STATS.autism,             color: "success"   }},
  {{ label: "TDAH",                  value: STATS.adhd,               color: "warning"   }},
  {{ label: "Multi-fuente (>1)",     value: STATS.multi_source,       color: "dark"      }},
  {{ label: "Multi-condicion",       value: STATS.multi_condition,    color: "dark"      }},
  {{ label: "Matriz Fecal",          value: STATS.fecal_matrix,       color: "success"   }},
  {{ label: "VOC (GC)",              value: STATS.voc_count,          color: "warning"   }},
  {{ label: "Vinculo microbio. Si",  value: STATS.microbiota_si,      color: "success"   }},
  {{ label: "Vinculo posible",       value: STATS.microbiota_pos,     color: "info"      }},
  {{ label: "Posible contaminante",  value: STATS.n_contaminante,     color: "danger"    }},
  {{ label: "Confianza Alta",        value: STATS.n_confianza_alta,   color: "success"   }},
  {{ label: "Confianza Media",       value: STATS.n_confianza_media,  color: "warning"   }},
];
const statsRow = document.getElementById("statsRow");
statsConfig.forEach(s => {{
  const col = document.createElement("div");
  col.className = "col-6 col-sm-4 col-md-2 col-lg-1";
  col.style.minWidth = "120px";
  col.innerHTML = `<div class="stat-card bg-${{s.color}} bg-opacity-10 border border-${{s.color}} border-opacity-25">
    <div class="stat-num text-${{s.color}}">${{s.value.toLocaleString()}}</div>
    <div class="stat-lbl">${{s.label}}</div>
  </div>`;
  statsRow.appendChild(col);
}});

// Source breakdown — compact
const srcDiv = document.createElement("div");
srcDiv.className = "col-12 col-md-auto";
srcDiv.style.maxWidth = "220px";
let srcHtml = '<div class="stat-card bg-white border h-100" style="padding:6px 8px"><div class="stat-lbl mb-1 fw-semibold" style="font-size:.68rem">Por fuente</div><div class="d-flex flex-wrap gap-1">';
Object.entries(STATS.by_source).forEach(([s, n]) => {{
  srcHtml += `<span class="badge bg-dark" style="font-size:.58rem">${{s}}: ${{n.toLocaleString()}}</span>`;
}});
srcDiv.innerHTML = srcHtml + '</div></div>';
statsRow.appendChild(srcDiv);

// ── Helpers ───────────────────────────────────────────────────────────────────
const COND_STYLE = {{
  schizophrenia: "bg-danger",
  depression:    "bg-primary",
  bipolar:       "bg-warning text-dark",
  anxiety:       "bg-info text-dark",
  ptsd:          "bg-secondary",
  autism:        "bg-success",
  adhd:          "bg-warning text-dark",
}};
const COND_LABEL = {{
  schizophrenia: "Esquiz.",
  depression:    "Depres.",
  bipolar:       "Bipolar",
  anxiety:       "Ansied.",
  ptsd:          "PTSD",
  autism:        "Autism",
  adhd:          "TDAH",
}};
const VOL_STYLE = {{
  "VOC":          "bg-warning text-dark",
  "No-volatil":   "bg-info text-dark",
  "Mixto":        "bg-secondary",
  "Desconocido":  "bg-light text-muted border",
}};
const VIN_STYLE = {{
  "Si":           "bg-success",
  "Posible":      "bg-info text-dark",
  "No":           "bg-danger",
  "Desconocido":  "bg-light text-muted border",
}};
const CONF_STYLE = {{
  "Alta":         "bg-success",
  "Media":        "bg-warning text-dark",
  "Baja":         "bg-secondary",
}};

function condBadges(conds) {{
  if (!conds) return '<span class="text-muted">-</span>';
  return conds.split("|").map(c => {{
    const cls = COND_STYLE[c] || "bg-secondary";
    return `<span class="badge ${{cls}} cond-badge">${{COND_LABEL[c] || c}}</span>`;
  }}).join("");
}}

function srcBadges(sources) {{
  return (sources||"").split(",").filter(Boolean).map(s =>
    `<span class="badge bg-secondary" style="font-size:.6rem">${{s.trim()}}</span>`
  ).join(" ");
}}

function matrixBadges(matrix) {{
  if (!matrix) return '<span class="text-muted">-</span>';
  return matrix.split("|").filter(Boolean).map(m => {{
    const ml = m.toLowerCase();
    const cls = (ml.includes("fec") || ml.includes("stool"))
                ? "bg-success" : "bg-light text-dark border";
    return `<span class="badge ${{cls}}" style="font-size:.6rem">${{m}}</span>`;
  }}).join(" ");
}}

function evBadge(ev) {{
  if (!ev) return '<span class="text-muted">-</span>';
  return ev.split("|").map(e => {{
    const cls = e.includes("dataset") ? "bg-primary" :
                e.includes("text")    ? "bg-secondary" :
                e.includes("manual")  ? "bg-warning text-dark" : "bg-light text-dark border";
    return `<span class="badge ${{cls}}" style="font-size:.6rem">${{e}}</span>`;
  }}).join(" ");
}}

function volBadge(v) {{
  const cls = VOL_STYLE[v] || "bg-light text-muted border";
  return `<span class="badge ${{cls}} voc-badge">${{v || "Desc."}}</span>`;
}}

function vinBadge(v) {{
  const cls = VIN_STYLE[v] || "bg-light text-muted border";
  return `<span class="badge ${{cls}} voc-badge">${{v || "Desc."}}</span>`;
}}

function flagBadge(v) {{
  if (v === "Posible_contaminante")
    return `<span class="badge bg-danger flag-badge">Contaminante?</span>`;
  return `<span class="badge bg-success flag-badge">No</span>`;
}}

function confBadge(v, motivo) {{
  const cls = CONF_STYLE[v] || "bg-light text-muted border";
  const tip = motivo ? ` title="${{motivo}}"` : "";
  return `<span class="badge ${{cls}} flag-badge"${{tip}}>${{v || "?"}}</span>`;
}}

function toggleInfo(id) {{
  const el = document.getElementById(id);
  el.classList.toggle("d-none");
}}

// ── DataTable ─────────────────────────────────────────────────────────────────
// Helper: escape HTML attribute values (evita romper title="...")
function esc(s) {{
  return (s||"").replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}}

// Column index map (0-based):
//  0:id  1:name  2:inchikey  3:conds  4:n_cond  5:n_dist  6:n_total
//  7:sources  8:matrix  9:method  10:evidence  11:volatilidad
//  12:vinculo  13:tipo_vinculo  14:flag_cont  15:origen_prob
//  16:origen_alt  17:confianza  [18:motivo hidden]  [19:cid hidden]

const dt = $("#mhTable").DataTable({{
  data: ALL_DATA,
  autoWidth: false,
  columns: [
    {{ data: "id",          width: "32px" }},
    {{ data: "name",        width: "120px",
       render: (d, t) => t === "display"
         ? `<span title="${{esc(d)}}">${{d}}</span>` : (d||"") }},
    {{ data: "inchikey",    width: "108px",
       render: (d, t) => t === "display"
         ? (d ? `<code title="${{esc(d)}}" style="font-size:.65rem">${{d}}</code>`
              : '<span class="text-muted">-</span>') : (d||"") }},
    {{ data: "conds",       width: "108px",
       render: (d, t) => t === "display"
         ? `<span title="${{esc((d||"").replace(/[|]/g,", "))}}">${{condBadges(d)}}</span>`
         : (d||"") }},
    {{ data: "n_cond",      width: "26px", className: "text-center",
       render: (d, t) => t === "display" ? `<span class="badge bg-dark" style="font-size:.62rem">${{d}}</span>` : d }},
    {{ data: "n_dist",      width: "26px", className: "text-center" }},
    {{ data: "n_total",     width: "30px", className: "text-center" }},
    {{ data: "sources",     width: "110px",
       render: (d, t) => t === "display"
         ? `<span title="${{esc((d||"").replace(/,/g,", "))}}">${{srcBadges(d)}}</span>`
         : (d||"") }},
    {{ data: "matrix",      width: "68px",
       render: (d, t) => t === "display"
         ? `<span title="${{esc(d)}}">${{matrixBadges(d)}}</span>` : (d||"") }},
    {{ data: "method",      width: "44px",
       render: (d, t) => t === "display" ? `<small class="text-muted" title="${{esc(d)}}">${{d || "-"}}</small>` : (d||"") }},
    {{ data: "evidence",    width: "72px",
       render: (d, t) => t === "display"
         ? `<span title="${{esc((d||"").replace(/[|]/g,", "))}}">${{evBadge(d)}}</span>`
         : (d||"") }},
    {{ data: "volatilidad", width: "60px",
       render: (d, t) => t === "display" ? volBadge(d) : (d||"") }},
    {{ data: "vinculo",     width: "55px",
       render: (d, t) => t === "display" ? vinBadge(d) : (d||"") }},
    {{ data: "tipo_vinculo",width: "90px",
       render: (d, t) => t === "display"
         ? `<small title="${{esc(d)}}">${{d || "-"}}</small>` : (d||"") }},
    {{ data: "flag_cont",   width: "68px",
       render: (d, t) => t === "display" ? flagBadge(d) : (d||"") }},
    {{ data: "origen_prob", width: "100px",
       render: (d, t) => t === "display"
         ? `<small class="text-muted" title="${{esc(d)}}">${{d || "-"}}</small>` : (d||"") }},
    {{ data: "origen_alt",  width: "88px",
       render: (d, t) => t === "display"
         ? `<small class="text-muted" title="${{esc(d)}}">${{d || "-"}}</small>` : (d||"") }},
    {{ data: "confianza",   width: "60px",
       render: (d, t, r) => t === "display" ? confBadge(d, r.motivo||"") : (d||"") }},
    // Motivo — oculto (accesible via tooltip en Confianza y en export)
    {{ data: "motivo",      visible: false,
       render: (d, t) => t === "display" ? `<small class="text-muted">${{d || "-"}}</small>` : (d||"") }},
    // CID — oculto (se incluye en export si se activa)
    {{ data: "cid",         visible: false }},
  ],
  pageLength: 25,
  lengthMenu: [10, 25, 50, 100, 250],
  deferRender: true,
  order: [[4, "desc"], [5, "desc"]],
  select: true,
  language: {{ url: "//cdn.datatables.net/plug-ins/1.13.8/i18n/es-ES.json", search: "Buscar:" }},
  dom: '<"d-flex align-items-center flex-wrap mb-1"B<"ms-auto"f>>rtip',
  buttons: [
    // CSV con separador ';' y BOM UTF-8 (Excel Windows ES)
    {{
      extend: "csvHtml5",
      text: "CSV (;)",
      className: "btn-sm btn-outline-secondary",
      bom: true,
      fieldSeparator: ";",
      exportOptions: {{ columns: ":visible" }},
      action: function(e, dt, button, config) {{
        const selCount = dt.rows({{ selected: true }}).count();
        config.exportOptions.rows = selCount > 0
          ? {{ selected: true }}
          : {{ filter: "applied" }};
        $.fn.DataTable.ext.buttons.csvHtml5.action.call(this, e, dt, button, config);
      }}
    }},
    // Excel .xlsx
    {{
      extend: "excelHtml5",
      text: "Excel",
      className: "btn-sm btn-outline-success",
      exportOptions: {{ columns: ":visible" }},
      action: function(e, dt, button, config) {{
        const selCount = dt.rows({{ selected: true }}).count();
        config.exportOptions.rows = selCount > 0
          ? {{ selected: true }}
          : {{ filter: "applied" }};
        $.fn.DataTable.ext.buttons.excelHtml5.action.call(this, e, dt, button, config);
      }}
    }},
  ],
}});

// ── Custom filters ────────────────────────────────────────────────────────────
$.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {{
  const row = ALL_DATA[dataIndex];

  // Fuente — OR logic (checkbox-based, exact match por fuente)
  const srcMap = [
{src_filter_map_js}  ];
  const srcChk = srcMap.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (srcChk.length > 0) {{
    const rowSrcList = (row.sources || "").split(",").map(s => s.trim());
    if (!srcChk.some(s => rowSrcList.includes(s))) return false;
  }}

  const schiz   = document.getElementById("chkSchiz").checked;
  const dep     = document.getElementById("chkDep").checked;
  const bip     = document.getElementById("chkBip").checked;
  const anx     = document.getElementById("chkAnx").checked;
  const ptsd    = document.getElementById("chkPtsd").checked;
  const aut     = document.getElementById("chkAut").checked;
  const adhd    = document.getElementById("chkAdhd").checked;
  const multi   = document.getElementById("chkMulti").checked;
  const multiC  = document.getElementById("chkMultiCond").checked;
  const dataset = document.getElementById("chkDataset").checked;
  const fecal   = document.getElementById("chkFecal").checked;
  const plasma  = document.getElementById("chkPlasma").checked;
  const urine   = document.getElementById("chkUrine").checked;
  const csf     = document.getElementById("chkCSF").checked;

  const conds = row.conds || "";
  const mx    = (row.matrix || "").toLowerCase();

  if (schiz   && !conds.includes("schizophrenia"))                  return false;
  if (dep     && !conds.includes("depression"))                     return false;
  if (bip     && !conds.includes("bipolar"))                        return false;
  if (anx     && !conds.includes("anxiety"))                        return false;
  if (ptsd    && !conds.includes("ptsd"))                           return false;
  if (aut     && !conds.includes("autism"))                         return false;
  if (adhd    && !conds.includes("adhd"))                           return false;
  if (multi   && row.n_dist <= 1)                                   return false;
  if (multiC  && row.n_cond <= 1)                                   return false;
  if (dataset && (row.evidence||"").includes("text_mining") && !(row.evidence||"").includes("dataset"))
                                                                    return false;
  if (fecal   && !row.fecal)                                        return false;
  if (plasma  && !mx.match(/plasma|serum/))                         return false;
  if (urine   && !mx.match(/urine|urin|orina/))                     return false;
  if (csf     && !mx.match(/csf|cerebrospinal|liquor|lcr/))         return false;

  // Volatilidad — OR logic
  const volVals = [["chkVolVOC","VOC"],["chkVolNoVol","No-volatil"],
                   ["chkVolMixto","Mixto"],["chkVolDesc","Desconocido"]];
  const volChk  = volVals.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (volChk.length > 0 && !volChk.includes(row.volatilidad)) return false;

  // Vinculo microbiota — OR logic
  const vinVals = [["chkVinSi","Si"],["chkVinPosible","Posible"],
                   ["chkVinNo","No"],["chkVinDesc","Desconocido"]];
  const vinChk  = vinVals.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (vinChk.length > 0 && !vinChk.includes(row.vinculo)) return false;

  // Tipo vinculo — OR logic
  const tipoVals = [["chkTipoProd","Producto bacteriano"],["chkTipoTrans","Transformacion bacteriana"],
                    ["chkTipoMarcador","Marcador indirecto"],["chkTipoDesc","Desconocido"]];
  const tipoChk  = tipoVals.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (tipoChk.length > 0 && !tipoChk.includes(row.tipo_vinculo)) return false;

  // Contaminacion — OR logic (values: "Posible_contaminante" | "No")
  const contVals = [["chkContSi","Posible_contaminante"],["chkContNo","No"]];
  const contChk  = contVals.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (contChk.length > 0 && !contChk.includes(row.flag_cont)) return false;

  // Origen — OR logic sobre Origen_probable Y Origen_alternativos (substring match)
  const origenMap = [
    ["chkOrigenInst",  "Instrumental"],
    ["chkOrigenLab",   "Laboratorio"],
    ["chkOrigenPre",   "Pre-anal"],
    ["chkOrigenAmb",   "Ambiente"],
    ["chkOrigenHig",   "Higiene"],
    ["chkOrigenFar",   "Farm"],
    ["chkOrigenDieta", "Dieta"],
    ["chkOrigenMicro", "microbiano"],
    ["chkOrigenHum",   "humano"],
    ["chkOrigenMisID", "Mis-ID"],
    ["chkOrigenDesc",  "Desconocido"],
  ];
  const origenChk = origenMap.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (origenChk.length > 0) {{
    // Busca en origen_probable Y en origen_alternativos para no perder coincidencias
    const origenAll = (row.origen_prob || "") + " " + (row.origen_alt || "");
    if (!origenChk.some(kw => origenAll.includes(kw))) return false;
  }}

  // Confianza origen — OR logic (exact match)
  const confVals = [["chkConfAlta","Alta"],["chkConfMedia","Media"],["chkConfBaja","Baja"]];
  const confChk  = confVals.filter(([id]) => document.getElementById(id).checked).map(([,v]) => v);
  if (confChk.length > 0 && !confChk.includes(row.confianza)) return false;

  return true;
}});

const filterIds = [
  {src_filter_ids_js},
  "chkSchiz","chkDep","chkBip","chkAnx","chkPtsd","chkAut","chkAdhd",
  "chkMulti","chkMultiCond","chkDataset",
  "chkFecal","chkPlasma","chkUrine","chkCSF",
  "chkVolVOC","chkVolNoVol","chkVolMixto","chkVolDesc",
  "chkVinSi","chkVinPosible","chkVinNo","chkVinDesc",
  "chkTipoProd","chkTipoTrans","chkTipoMarcador","chkTipoDesc",
  "chkContSi","chkContNo",
  "chkOrigenInst","chkOrigenLab","chkOrigenPre","chkOrigenAmb","chkOrigenHig",
  "chkOrigenFar","chkOrigenDieta","chkOrigenMicro","chkOrigenHum","chkOrigenMisID","chkOrigenDesc",
  "chkConfAlta","chkConfMedia","chkConfBaja",
];
filterIds.forEach(id => {{
  document.getElementById(id).addEventListener("change", () => dt.draw());
}});

function resetFilters() {{
  {src_reset_js}
  filterIds.forEach(id => {{
    document.getElementById(id).checked = false;
  }});
  dt.rows().deselect();
  dt.search("").draw();
}}
</script>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    logger.info("report_mhb.html written: %d rows -> %s", len(rows), out_path)
    print(f"report_mhb.html: {out_path}  ({len(rows):,} biomarcadores MH)")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate MH biomarkers HTML report")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--out",    default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(levelname)s %(message)s")

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if args.out:
        cfg["paths"]["outputs"] = str(Path(args.out).parent)

    generate_report_mhb(cfg)


if __name__ == "__main__":
    main()
