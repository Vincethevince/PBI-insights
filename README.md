# PBI-insights

Transform opaque Power BI files into a transparent, understandable, and queryable source of information — empowering both technical and non-technical users to fully comprehend the data and calculations driving their business intelligence.

## Overview

Power BI report files (`.pbix` / `.pbir`) can be unpacked into their underlying folder structure, exposing the report definition as human-readable JSON files. This project parses those files to extract **pages**, **visuals**, and **DAX measures**, then enriches the data with AI-generated descriptions and makes it searchable via a vector database.

### Supported file formats

| Format | Structure | Detection |
|--------|-----------|-----------|
| **Old `.pbix`** | Single `Report/Layout` file (UTF-16-LE JSON) | `Report/Layout` exists, no `Report/definition/` |
| **New `.pbix`** | `Report/definition/` directory tree | `Report/definition/` exists inside the unzipped folder |
| **`.pbir` folder** | `definition/` directory at the root level | Top-level `definition/` directory |

Both the new `.pbix` and `.pbir` formats share an identical `definition/` directory structure, so a single parsing path handles both. The correct format is **auto-detected** — no manual configuration required.

### What gets parsed

- **Measures** — name, DAX expression, table, usage state (`Directly Used`, `Indirectly Used`, `Unreferenced`, `Dangling`), dependency graph, optional author/description/last-change metadata from inline comments
- **Pages** — display name, visibility, size, page-level filters, ordinal order
- **Visuals** — type, position, title, used fields/measures per visual

### How it works

1. **Unzip** `.pbix` files from `data/pbi_files/` → `data/unzipped_pbi_folders/`
2. **Parse** all unzipped `.pbix` folders and `.pbir` folders from `data/pbir_folders/`
3. **Export** to timestamped Excel or CSV files in `output/`
4. *(Optional)* **AI Analysis** — generate natural-language descriptions for measures and pages using Vertex AI (Google Cloud)
5. *(Optional)* **Vector DB** — embed page descriptions into a ChromaDB vector store for semantic search across all reports

## Project structure

```
PBI-insights/
├── config/
│   └── base.py                  # Path constants
├── data/
│   ├── pbi_files/               # Place your source .pbix files here
│   ├── unzipped_pbi_folders/    # Auto-populated by the unzipper
│   └── pbir_folders/            # Place your extracted .pbir folders here
├── output/                      # Exported Excel/CSV files (timestamped)
├── pbi_insights/
│   ├── unzip.py                 # Unzips .pbix files
│   ├── report.py                # Report model + all three format parsers
│   ├── report_parser.py         # CLI entry point (run / parse-single / analyze-file)
│   ├── page.py                  # Page model
│   ├── visual.py                # Visual model
│   ├── measure.py               # Measure model + UsageState enum
│   ├── utils.py                 # Recursive field-extraction helpers
│   ├── exporter.py              # Excel/CSV export logic
│   ├── analyzer.py              # AI analysis (Vertex AI)
│   └── vector_db.py             # ChromaDB vector store
└── requirements.txt
```

## Installation

```bash
pip install -r requirements.txt
```

> **Note:** AI analysis and vector DB features require a Google Cloud project with Vertex AI enabled. Set the `GCP_PROJECT` environment variable (e.g. via a `.env` file) to enable them. Without it, the local `all-MiniLM-L6-v2` sentence-transformer model is used for embeddings instead.

## Usage

### 1. Prepare your report files

**For `.pbix` files** — place them in `data/pbi_files/`, then unzip:

```bash
python -m pbi_insights.unzip
```

This extracts all `.pbix` files to `data/unzipped_pbi_folders/`. New-format `.pbix` files (with a `Report/definition/` directory inside) are automatically detected and parsed correctly alongside old-format files.

**For `.pbir` folders** — place the extracted `.pbir` report folders directly in `data/pbir_folders/`. No unzipping step is needed.

### 2. Parse and export all reports

Parse all reports (from both `unzipped_pbi_folders/` and `pbir_folders/`) and export to Excel or CSV:

```bash
# Export to Excel (default)
python -m pbi_insights.report_parser run

# Export to CSV
python -m pbi_insights.report_parser run --file_type csv

# Export with AI-generated descriptions for measures and pages
python -m pbi_insights.report_parser run --analyze
```

Output files are saved to `output/` with timestamps, e.g.:
- `measures_2026-03-25_10-00-00.xlsx`
- `pages_2026-03-25_10-00-00.xlsx`

### 3. Parse a single report

Parse one report and append its data to the latest output files:

```bash
# From unzipped_pbi_folders/ (old or new .pbix format, auto-detected)
python -m pbi_insights.report_parser parse-single "My Report Name"

# From pbir_folders/ (searched automatically if not found in unzipped_pbi_folders/)
python -m pbi_insights.report_parser parse-single "My Report Name"
# or with the .Report suffix:
python -m pbi_insights.report_parser parse-single "My Report Name.Report"
```

### 4. Run AI analysis on an existing export file

Re-analyze a previously exported file without re-parsing all reports:

```bash
# Analyze measures
python -m pbi_insights.report_parser analyze-file output/measures_2026-03-25_10-00-00.xlsx measures

# Analyze pages
python -m pbi_insights.report_parser analyze-file output/pages_2026-03-25_10-00-00.xlsx pages
```

## Roadmap

- [x] `.pbix` Unzipper
- [x] Old-format `.pbix` parser (single `Report/Layout` file)
- [x] New-format `.pbix` parser (`Report/definition/` directory tree)
- [x] `.pbir` folder parser (`definition/` at root level)
- [x] Measure usage analysis (Directly Used / Indirectly Used / Unreferenced / Dangling)
- [x] Export to Excel and CSV
- [x] AI-generated descriptions for measures and pages (Vertex AI)
- [x] Vector DB for semantic page search (ChromaDB)
- [ ] Structured logging & error handling
- [ ] Change detection / caching for AI analysis
- [ ] DataModel parser (tables, columns, relationships)

