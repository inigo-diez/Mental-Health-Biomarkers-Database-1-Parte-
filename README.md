# Mental-Health Metabolite Collector

> **[Live Demo](https://inigo-diez.github.io/Mental-Health-Biomarkers-Database/report_mhb.html)** — Interactive dashboard with all ~39,000 entries. Filter by condition, source, origin and microbiota link. No installation required, open directly in browser.

Automated pipeline that aggregates **metabolite biomarker candidates** for psychiatric conditions
(schizophrenia, depression, bipolar disorder, anxiety, PTSD, autism, ADHD) from
**6 open scientific databases**, deduplicates them, and exports an interactive HTML dashboard.

Built as part of a Final Degree Project (TFG) in Biomedical Sciences.

---

## Highlights

- Integrates **6 open databases** automatically — MarkerDB, CTD, Metabolomics Workbench, MetaboLights, Europe PMC, HMDB Feces
- Chemical deduplication by InChIKey → PubChem CID → normalized name (priority cascade)
- Optional **PubChem enrichment** to resolve identifiers (CID + InChIKey)
- **Volatility annotation** — VOC vs non-volatile compounds, inferred from analytical method hints (GC vs LC)
- **Gut-microbiota link classification** — SCFAs, indoles, phenols, biogenic amines, sulfur compounds
- **Interactive HTML dashboard** (DataTables) with per-condition filters, source filters, origin confidence levels, **Motivo_origen filter** (18 categories, stacked below Confianza Origen), and one-click CSV/Excel export
- Included curated dataset: `outputs/MH_Biomarkers_Salud_Mental_CURADO.xlsx` — **38 952 entries**, manually reviewed and annotated

---

## Data sources

| Priority | Source | Access | Evidence type |
|---|---|---|---|
| 1 | **MarkerDB 2.0** | Public bulk download (TSV/XML) | `biomarker_db` |
| 2 | **CTD** | Public `.tsv.gz` (auto-downloaded) | `chemical-disease` |
| 3 | **Metabolomics Workbench** | REST API | `study_metabolite_list` |
| 4 | **MetaboLights** (EBI) | REST API + ISA-Tab TSV | `study_metabolite_list` |
| 5 | **Europe PMC** | Search + Annotations API | `text_mining` |
| 6 | **HMDB Feces** | Local XML (manual download) | `fecal_catalog` |

---

## Quick start

### Requirements

Python 3.10+ recommended.

```bash
pip install -r requirements.txt
```

Core dependencies: `requests`, `PyYAML`, `openpyxl`.
Everything else (`sqlite3`, `csv`, `gzip`, `xml.etree`, `difflib`) is Python stdlib — no heavy ML stack needed.

### 1. Run the full collection pipeline

```bash
python -m src.collect --config config.yaml
```

This will:
1. Download and parse all configured sources
2. Deduplicate and store entries in `outputs/metabolites.db` (SQLite)
3. Optionally enrich via PubChem REST API
4. Export `outputs/candidates_master.csv` and `outputs/source_summary.csv`

**Useful flags:**

```
--sources markerdb,ctd     # run only specific sources
--skip-pubchem             # skip PubChem enrichment (much faster)
--dry-run                  # validate config and exit
```

### 2. Export the MH biomarker CSV

```bash
python -m src.mh_export --config config.yaml
```

Generates `outputs/mh_biomarkers.csv` — metabolites with at least one specific psychiatric condition hit.

### 3. Generate the interactive dashboard

```bash
python -m src.report_mhb
```

Generates `outputs/report_mhb.html`. Open directly in any browser — no server required.

### 4. Search a metabolite

```bash
python -m src.search "skatole" --top 10
python -m src.search "indole-3-acetic"
python -m src.search "butyrate" --top 20
```

---

## Outputs

| File | Description | In repo |
|---|---|---|
| `outputs/metabolites.db` | SQLite master database (~390 MB) | No (generated) |
| `outputs/mh_biomarkers.csv` | Filtered MH biomarker candidates | No (generated) |
| `outputs/report_mhb.html` | Interactive dashboard (~19 MB) | No (generated) |
| `outputs/MH_Biomarkers_Salud_Mental_CURADO.xlsx` | **Curated annotated dataset** | **Yes** |
| `outputs/mh_health.json` | Per-source health statistics | Yes |
| `outputs/discovered_datasets.json` | Discovered study metadata | Yes |

### Curated dataset — column reference

`outputs/MH_Biomarkers_Salud_Mental_CURADO.xlsx` (38 952 entries):

| Column | Description |
|---|---|
| `ID` | Internal metabolite identifier |
| `Nombre canonico` | Normalized compound name |
| `InChIKey` / `CID` | Standard chemical identifiers |
| `Condiciones` | Psychiatric conditions (pipe-separated) |
| `#C` / `#F` / `#L` | Condition count / source count / link count |
| `Fuentes` | Data sources (comma-separated) |
| `Matriz` | Biological matrix hints (plasma, feces, CSF…) |
| `Metodo` | Analytical method hints (LC-MS, GC-MS, NMR…) |
| `Evidencia` | Evidence type (dataset / text mining / catalog) |
| `Volatilidad` | `VOC` / `No-volatil` / `Mixto` / `Desconocido` |
| `Vinculo Microbio.` | Gut microbiota link: `Si` / `Posible` / `No` / `Desconocido` |
| `Tipo Vinculo` | Link type: bacterial product, transformation, indirect marker |
| `Flag_contaminante` | Contaminant flag |
| `Origen_probable` | Most likely biological origin |
| `Origen_alternativos` | Alternative origin hypotheses |
| `Confianza_origen` | Origin confidence: `Alta` / `Media` / `Baja` |
| `Motivo_origen` | Rationale for origin assignment (18 categories: *heurística metabolito*, *firma microbiana*, *aminoácido*, etc.) — visible in dashboard, filterable via Motivo Origen panel |

---

## Project structure

```
├── src/
│   ├── collect.py              # CLI orchestrator
│   ├── db.py                   # SQLite schema + CRUD
│   ├── normalize.py            # Name normalization + deduplication
│   ├── mh_export.py            # MH biomarker CSV export
│   ├── report_mhb.py           # Interactive HTML dashboard generator
│   ├── report.py               # General report generator
│   ├── search.py               # Fuzzy metabolite search (CLI)
│   ├── pubchem.py              # PubChem REST enrichment
│   ├── cache.py                # HTTP cache (7-day TTL, disk-based)
│   ├── utils.py                # HTTP retries + token-bucket rate limiter
│   ├── enrich.py               # Post-collection enrichment steps
│   ├── conditions.py           # Psychiatric condition keyword registry
│   ├── classify_compound.py    # Compound classification helpers
│   └── matrix_parser.py        # Biological matrix parsing
├── outputs/
│   └── MH_Biomarkers_Salud_Mental_CURADO.xlsx   # Curated dataset (included)
├── tests/
│   └── test_normalize.py
├── config.yaml                 # Central configuration
├── requirements.txt
└── LICENSE
```

---

## Configuration (`config.yaml`)

Key parameters:

```yaml
use_pubchem: true          # false to skip enrichment (much faster)
rate_limit: 3              # global requests/second
max_records_per_source:
  metabolights: 100
  europe_pmc: 500
sources:
  europe_pmc: false        # disable any source
```

---

## Manual downloads

### HMDB Feces (optional but recommended)
Enables tagging of known fecal metabolites from the HMDB catalog:
- Go to <https://hmdb.ca/downloads>
- Download the full metabolites XML
- Extract `hmdb_metabolites.xml` to `cache/`

### CTD
Downloaded automatically on first run. Manual fallback: <https://ctdbase.org/downloads/>

---

## Technical notes

- **Deduplication priority**: InChIKey → PubChem CID → normalized name key
- **HTTP cache**: all API responses cached for 7 days in `cache/http/`. Delete to force refresh.
- **Rate limiting**: global token-bucket. Adjust `rate_limit` in `config.yaml`.
- **Text mining flag** (`from_text_mining=1`): higher noise, may include drugs. Use as a filter flag when analyzing.
- **MarkerDB API key**: optional — set `MARKERDB_API_KEY` env variable or in `config.yaml`.

---

## Running tests

```bash
pytest tests/ -v
```

---

## License

MIT — see [LICENSE](LICENSE).

