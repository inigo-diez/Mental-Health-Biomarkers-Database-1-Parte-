"""
Generate an interactive HTML report from candidates_master.csv.

Usage:
    python -m src.report [--config config.yaml] [--out outputs/report.html]

Features:
  - Search / filter by name, source, condition flags
  - Sortable columns with DataTables
  - Color-coded badges (schizophrenia, fecal, text-mining, quality flags)
  - Anti-noise filters: exclude inorganic, drugs, category-like compounds
  - Volatility filter: GC-compatible, LC-compatible
  - Summary stats panel (multi-source uses n_sources_distinct)
  - Export to CSV/Excel
  - Works fully offline after initial CDN load
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


def _stats(rows: list[dict], fecal_mental_csv: Path | None = None) -> dict:
    total  = len(rows)
    schiz  = sum(1 for r in rows if r.get("schizophrenia_hit") == "1")
    fecal  = sum(1 for r in rows if str(r.get("fecal_hint", "0")) == "1")
    txt    = sum(1 for r in rows if str(r.get("from_text_mining", "0")) == "1")
    # Multi-source: based on n_sources_distinct (correct metric)
    multi  = sum(1 for r in rows if int(r.get("n_sources_distinct") or 0) > 1)
    gc_ok  = sum(1 for r in rows if r.get("gc_compatible", "").lower() == "true")
    lc_ok  = sum(1 for r in rows if r.get("lc_compatible", "").lower() == "true")

    sources: dict[str, int] = {}
    for r in rows:
        for s in (r.get("source_types") or "").split(","):
            s = s.strip()
            if s:
                sources[s] = sources.get(s, 0) + 1

    # Count fecal + mental-health candidates from dedicated CSV (if available)
    fecal_mental_count = 0
    if fecal_mental_csv and fecal_mental_csv.exists():
        try:
            with open(fecal_mental_csv, encoding="utf-8") as f:
                fecal_mental_count = sum(1 for _ in csv.DictReader(f))
        except Exception:
            pass

    return {
        "total":              total,
        "schizophrenia_hit":  schiz,
        "fecal_flagged":      fecal,
        "fecal_mental":       fecal_mental_count,
        "from_text_mining":   txt,
        "multi_source":       multi,
        "gc_compatible":      gc_ok,
        "lc_compatible":      lc_ok,
        "by_source":          dict(sorted(sources.items(), key=lambda x: -x[1])),
    }


def generate_html(csv_path: Path, out_path: Path) -> None:
    rows  = _load_csv(csv_path)
    fecal_mental_csv = csv_path.parent / "fecal_mental_candidates.csv"
    stats = _stats(rows, fecal_mental_csv=fecal_mental_csv)

    # Prepare data for DataTables
    table_data = []
    for r in rows:
        n_dist  = int(r.get("n_sources_distinct") or 0)
        n_total = int(r.get("n_records_total") or 0)
        table_data.append({
            "id":          r.get("metabolite_id", ""),
            "name":        r.get("canonical_name", ""),
            "inchikey":    r.get("inchikey", "") or "",
            "cid":         r.get("pubchem_cid", "") or "",
            "n_dist":      n_dist,
            "n_total":     n_total,
            "sources":     r.get("source_types", ""),
            "mh_terms":    r.get("mental_health_terms_hit", ""),
            "schiz":       r.get("schizophrenia_hit", "0") == "1",
            "matrix":      r.get("matrix_hints", "") or "",
            "txt_mining":  str(r.get("from_text_mining", "0")) == "1",
            "fecal":       str(r.get("fecal_hint", "0")) == "1",
            "fecal_cat":   r.get("known_fecal_metabolite", "0") == "1",
            "status":      r.get("status", "candidate"),
            "volatility":  r.get("volatility", "Unknown"),
            "gc_compat":   r.get("gc_compatible", "Unknown").lower() == "true",
            "lc_compat":   r.get("lc_compatible", "Unknown").lower() == "true",
            "is_inorganic":    r.get("is_inorganic", "unknown"),
            "is_drug":         r.get("is_drug", "unknown"),
            "is_environmental": r.get("is_environmental", "unknown"),
            "is_category":     r.get("is_category_like", "false"),
        })

    data_json  = json.dumps(table_data, ensure_ascii=False)
    stats_json = json.dumps(stats,      ensure_ascii=False)

    source_options = "".join(
        f'<option value="{s}">{s} ({n})</option>'
        for s, n in stats["by_source"].items()
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Metabolite Collector — Resultados</title>

<!-- Bootstrap 5 -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
<!-- DataTables Bootstrap5 -->
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.bootstrap5.min.css">

<style>
  body {{ font-size: 0.87rem; background: #f8f9fa; }}
  h1   {{ font-size: 1.35rem; }}
  .stat-card {{ border-radius: 10px; text-align: center; padding: 12px 8px; }}
  .stat-num  {{ font-size: 1.75rem; font-weight: 700; line-height: 1.1; }}
  .stat-lbl  {{ font-size: 0.72rem; color: #555; }}
  table.dataTable td {{ vertical-align: middle; }}
  .name-cell {{ max-width: 240px; word-break: break-word; }}
  .src-badge {{ font-size: 0.68rem; }}
  #filterBar {{ gap: 8px; flex-wrap: wrap; align-items: flex-start; }}
  .filter-group {{ border: 1px solid #dee2e6; border-radius: 6px; padding: 6px 10px; background: #fff; }}
  .filter-group label.group-title {{ font-weight: 600; font-size: 0.75rem; display: block; margin-bottom: 4px; }}
  .qual-badge {{ font-size: 0.62rem; padding: 1px 4px; border-radius: 3px; }}
</style>
</head>
<body>
<div class="container-fluid py-3">

<!-- ── Header ──────────────────────────────────────── -->
<div class="d-flex align-items-center mb-3">
  <div>
    <h1 class="mb-0">Metabolite Collector &mdash; Candidatos a Biomarcadores</h1>
    <small class="text-muted">Salud mental (esquizofrenia, depresión, bipolar, ansiedad, autism, ADHD&hellip;) &middot;
      Fuentes: MarkerDB &middot; CTD &middot; MWB &middot; MetaboLights &middot; EuropePMC &middot; PubTator3</small>
  </div>
</div>

<!-- ── Stats cards ──────────────────────────────────── -->
<div class="row g-2 mb-3" id="statsRow"></div>

<!-- ── Filter bar ───────────────────────────────────── -->
<div class="card mb-3 shadow-sm">
  <div class="card-body py-2">
    <div class="d-flex" id="filterBar">

      <!-- Source filter -->
      <div class="filter-group">
        <label class="group-title">Fuente</label>
        <select id="filterSource" class="form-select form-select-sm">
          <option value="">Todas</option>
          {source_options}
        </select>
      </div>

      <!-- Positive filters -->
      <div class="filter-group">
        <label class="group-title">Mostrar solo</label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkSchiz">
          <label class="form-check-label small" for="chkSchiz">Esquizofrenia</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkFecal">
          <label class="form-check-label small" for="chkFecal">Pista fecal (cualquier)</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkFecalMH" style="accent-color:#198754">
          <label class="form-check-label small fw-semibold text-success" for="chkFecalMH">Fecal + Salud Mental</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkMulti">
          <label class="form-check-label small" for="chkMulti">&gt;1 fuente distinta</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkGC">
          <label class="form-check-label small" for="chkGC">GC-compatible</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkLC">
          <label class="form-check-label small" for="chkLC">LC-compatible</label>
        </div>
      </div>

      <!-- Negative / exclusion filters -->
      <div class="filter-group">
        <label class="group-title">Excluir</label>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkNoTxt">
          <label class="form-check-label small" for="chkNoTxt">Solo text-mining</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkNoInorg">
          <label class="form-check-label small" for="chkNoInorg">Inorgánicos</label>
        </div>
        <div class="form-check mb-1">
          <input class="form-check-input" type="checkbox" id="chkNoDrug">
          <label class="form-check-label small" for="chkNoDrug">Fármacos</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="checkbox" id="chkNoCategory">
          <label class="form-check-label small" for="chkNoCategory">Categorías (ruido)</label>
        </div>
      </div>

      <button class="btn btn-sm btn-outline-secondary ms-auto align-self-start mt-2"
              onclick="resetFilters()">Resetear</button>
    </div>
  </div>
</div>

<!-- ── Table ─────────────────────────────────────────── -->
<div class="card shadow-sm">
  <div class="card-body p-2">
    <table id="metTable" class="table table-sm table-striped table-hover w-100">
      <thead class="table-dark">
        <tr>
          <th>ID</th>
          <th>Nombre canónico</th>
          <th>InChIKey</th>
          <th>CID</th>
          <th title="Fuentes distintas (tipos)">N dist.</th>
          <th title="Total de links en la BD">N links</th>
          <th>Fuentes</th>
          <th>Esquizofrenia</th>
          <th>Text-mining</th>
          <th>Fecal</th>
          <th title="GC-compatible / LC-compatible / Unknown">Volatil.</th>
          <th title="Flags de calidad: inorgánico, fármaco, categoría, ambiental">Calidad</th>
          <th>Matriz</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

</div><!-- /container -->

<!-- Scripts -->
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.bootstrap5.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>

<script>
const ALL_DATA = {data_json};
const STATS    = {stats_json};

// ── Stats cards ────────────────────────────────────────────────────────────
const statsConfig = [
  {{ label: "Total metabolitos",    value: STATS.total,              color: "primary",   icon: "" }},
  {{ label: "Hit esquizofrenia",    value: STATS.schizophrenia_hit,  color: "danger",    icon: "" }},
  {{ label: "Multi-fuente (>1)",   value: STATS.multi_source,        color: "info",      icon: "" }},
  {{ label: "Pista fecal",          value: STATS.fecal_flagged,      color: "success",   icon: "" }},
  {{ label: "Fecal+Sal. mental",   value: STATS.fecal_mental,        color: "success",   icon: "" }},
  {{ label: "Text-mining",          value: STATS.from_text_mining,   color: "secondary", icon: "" }},
  {{ label: "GC-compat.",           value: STATS.gc_compatible,      color: "warning",   icon: "" }},
  {{ label: "LC-compat.",           value: STATS.lc_compatible,      color: "warning",   icon: "" }},
];
const statsRow = document.getElementById("statsRow");
statsConfig.forEach(s => {{
  const col = document.createElement("div");
  col.className = "col-6 col-sm-4 col-md-2 col-lg-1";
  col.style.minWidth = "110px";
  col.innerHTML = `<div class="stat-card bg-${{s.color}} bg-opacity-10 border border-${{s.color}} border-opacity-25">
    <div class="stat-num text-${{s.color}}">${{s.icon}}<br>${{s.value.toLocaleString()}}</div>
    <div class="stat-lbl">${{s.label}}</div>
  </div>`;
  statsRow.appendChild(col);
}});

// Source breakdown card
const srcDiv = document.createElement("div");
srcDiv.className = "col-12 col-md";
let srcHtml = '<div class="stat-card bg-white border h-100"><div class="stat-lbl mb-1 fw-semibold">Por fuente</div><div class="d-flex flex-wrap gap-1">';
Object.entries(STATS.by_source).forEach(([s, n]) => {{
  srcHtml += `<span class="badge bg-dark src-badge">${{s}}: ${{n.toLocaleString()}}</span>`;
}});
srcDiv.innerHTML = srcHtml + '</div></div>';
statsRow.appendChild(srcDiv);

// ── Helpers ────────────────────────────────────────────────────────────────
function badge(text, cls, title) {{
  const t = title ? ` title="${{title}}"` : "";
  return `<span class="badge ${{cls}} src-badge"${{t}}>${{text}}</span>`;
}}
function yn(val, yesLabel, noLabel) {{
  yesLabel = yesLabel || "Si";
  noLabel  = noLabel  || "No";
  return val ? badge(yesLabel, "bg-danger") : badge(noLabel, "bg-light text-dark");
}}
function srcBadges(sources) {{
  return sources.split(",").filter(Boolean).map(s =>
    badge(s.trim(), "bg-secondary src-badge")
  ).join(" ");
}}

function volatilBadge(v, gc, lc) {{
  if (gc && lc)  return badge("GC+LC", "bg-warning text-dark");
  if (gc)        return badge("GC", "bg-warning text-dark");
  if (lc)        return badge("LC", "bg-info text-dark");
  if (v === "Unknown") return badge("?", "bg-light text-muted");
  return badge(v, "bg-secondary");
}}

function qualBadges(row) {{
  let badges = "";
  if (row.is_inorganic === "true")
    badges += `<span class="qual-badge bg-secondary text-white me-1">inorg</span>`;
  if (row.is_drug === "true")
    badges += `<span class="qual-badge bg-danger text-white me-1">farmaco</span>`;
  if (row.is_category === "true")
    badges += `<span class="qual-badge bg-warning text-dark me-1">categ</span>`;
  if (row.is_environmental === "true")
    badges += `<span class="qual-badge bg-info text-dark me-1">env</span>`;
  return badges || '<span class="text-muted small">—</span>';
}}

function fecalBadge(row) {{
  if (row.fecal_cat && row.fecal) return badge("cat+pista", "bg-success");
  if (row.fecal_cat) return badge("catalogo", "bg-success");
  if (row.fecal)     return badge("pista", "bg-success bg-opacity-75");
  return badge("No", "bg-light text-dark");
}}

// ── DataTable ──────────────────────────────────────────────────────────────
const tableData = ALL_DATA.map(r => [
  r.id,
  `<span class="name-cell d-inline-block">${{r.name}}</span>`,
  r.inchikey ? `<code class="small">${{r.inchikey}}</code>` : '<span class="text-muted">—</span>',
  r.cid ? `<a href="https://pubchem.ncbi.nlm.nih.gov/compound/${{r.cid}}" target="_blank" rel="noopener">${{r.cid}}</a>` : '—',
  r.n_dist,
  r.n_total,
  srcBadges(r.sources),
  yn(r.schiz),
  yn(r.txt_mining),
  fecalBadge(r),
  volatilBadge(r.volatility, r.gc_compat, r.lc_compat),
  qualBadges(r),
  `<small class="text-muted">${{r.matrix || "—"}}</small>`,
]);

const dt = $("#metTable").DataTable({{
  data: tableData,
  columns: [
    {{ width: "40px"  }},
    {{ width: "220px" }},
    {{ width: "140px" }},
    {{ width: "65px"  }},
    {{ width: "55px", className: "text-center" }},
    {{ width: "55px", className: "text-center" }},
    {{}},
    {{ width: "80px", className: "text-center" }},
    {{ width: "80px", className: "text-center" }},
    {{ width: "85px", className: "text-center" }},
    {{ width: "65px", className: "text-center" }},
    {{ width: "110px" }},
    {{ width: "100px" }},
  ],
  pageLength: 25,
  lengthMenu: [10, 25, 50, 100, 250],
  order: [[4, "desc"]],
  language: {{
    url: "//cdn.datatables.net/plug-ins/1.13.8/i18n/es-ES.json"
  }},
  dom: "Bfrtip",
  buttons: [
    {{ extend: "csvHtml5",   text: "CSV",   className: "btn-sm btn-outline-secondary",
       exportOptions: {{ columns: [0,1,2,3,4,5,6,7,8,9,10,11,12] }} }},
    {{ extend: "excelHtml5", text: "Excel", className: "btn-sm btn-outline-success",
       exportOptions: {{ columns: [0,1,2,3,4,5,6,7,8,9,10,11,12] }} }},
  ],
}});

// ── Custom filters ─────────────────────────────────────────────────────────
$.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {{
  const row = ALL_DATA[dataIndex];
  const src        = document.getElementById("filterSource").value;
  const schiz      = document.getElementById("chkSchiz").checked;
  const fecal      = document.getElementById("chkFecal").checked;
  const fecalMH    = document.getElementById("chkFecalMH").checked;
  const multi      = document.getElementById("chkMulti").checked;
  const gcOnly     = document.getElementById("chkGC").checked;
  const lcOnly     = document.getElementById("chkLC").checked;
  const noTxt      = document.getElementById("chkNoTxt").checked;
  const noInorg    = document.getElementById("chkNoInorg").checked;
  const noDrug     = document.getElementById("chkNoDrug").checked;
  const noCategory = document.getElementById("chkNoCategory").checked;

  if (src   && !row.sources.includes(src))               return false;
  if (schiz && !row.schiz)                               return false;
  if (fecal && !row.fecal && !row.fecal_cat)             return false;
  // Fecal + Mental-health: must have BOTH fecal signal AND MH tag
  if (fecalMH && !(row.fecal || row.fecal_cat) )         return false;
  if (fecalMH && !row.schiz && row.mh_terms === "")      return false;
  if (multi && row.n_dist <= 1)                          return false;
  if (gcOnly && !row.gc_compat)                          return false;
  if (lcOnly && !row.lc_compat)                          return false;
  // Exclusions
  if (noTxt      && row.txt_mining && row.n_dist <= 1)   return false;
  if (noInorg    && row.is_inorganic    === "true")       return false;
  if (noDrug     && row.is_drug         === "true")       return false;
  if (noCategory && row.is_category     === "true")       return false;
  return true;
}});

const filterIds = [
  "filterSource","chkSchiz","chkFecal","chkFecalMH","chkMulti","chkGC","chkLC",
  "chkNoTxt","chkNoInorg","chkNoDrug","chkNoCategory"
];
filterIds.forEach(id => {{
  document.getElementById(id).addEventListener("change", () => dt.draw());
}});

function resetFilters() {{
  document.getElementById("filterSource").value = "";
  filterIds.slice(1).forEach(id => {{
    document.getElementById(id).checked = false;
  }});
  dt.search("").draw();
}}
</script>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    logger.info("Report written to %s (%d rows)", out_path, len(rows))
    print(f"Report: {out_path}  ({len(rows)} metabolitos)")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate interactive HTML report")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--out",    default=None, help="Output HTML path")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(levelname)s %(message)s")

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    csv_path = Path(cfg["paths"]["outputs"]) / "candidates_master.csv"
    out_path = Path(args.out) if args.out else Path(cfg["paths"]["outputs"]) / "report.html"

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}. Run `python -m src.collect` first.")
        sys.exit(1)

    generate_html(csv_path, out_path)


if __name__ == "__main__":
    main()
