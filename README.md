# Mental-Health Metabolite Collector

> **[Click here]**[📊 Click here](https://inigo-diez.github.io/Mental-Health-Biomarkers-Database/report_mhb.html)** para ver el reporte interactivo** — Interactive dashboard with all ~39,000 entries. Filter by condition, source, origin and microbiota link. No installation required, open directly in browser.

Automated pipeline that aggregates **metabolite biomarker candidates** for psychiatric conditions
(schizophrenia, depression, bipolar disorder, anxiety, PTSD, autism, ADHD) from
**6 open scientific databases**, deduplicates them, and exports an interactive HTML dashboard.

Built as part of a Final Degree Project (TFG). 

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
---

## License

MIT — see [LICENSE](LICENSE).



