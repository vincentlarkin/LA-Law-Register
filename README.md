# LA Law Register

Offline Louisiana law downloader + search tools.

Source TOC: https://www.legis.la.gov/legis/LawsContents.aspx

## What This Repo Produces

- Local law text and metadata (`sections/*.txt`, `sections/*.json`)
- Per-bundle PDFs with real TOC page numbers
- Fast local full-text index (`out/index.sqlite`)
- Desktop GUI for near-live local search

Default output root: `out/`

## Setup

```powershell
python -m pip install -r requirements.txt
```

Notes:
- Uses Playwright with installed Edge (`msedge`).
- PDF page-number scan prefers `pymupdf` and falls back to `pypdf`.

## Scripts

- `scripts/download_louisiana_laws.py`
  - Scrapes TOC, downloads laws, writes local files, builds PDFs.
  - Default category: `revised-statutes`.
- `scripts/build_search_index.py`
  - Builds SQLite FTS5 index from `out/**/bundle.json` and `sections/*.json`.
  - Rebuild is atomic (temp DB swap), so interrupted runs do not overwrite a good index.
- `scripts/search_laws.py`
  - CLI query tool for the SQLite index.
- `scripts/search_laws_gui.py`
  - Desktop GUI search with:
    - category/bundle filtering
    - regex mode
    - local full-text preview with highlighting
    - citation sorting (ex: `RS 14:*` before `RS 34:*`)
- `scripts/dev/test_toc_postback.py`
  - Small dev test for TOC postback behavior.

## Project Layout

- `scripts/` - downloader, indexer, CLI search, GUI search, and dev helper scripts.
- `out/` - primary generated output (laws, metadata, PDFs, and optionally `index.sqlite`).
- `.toc-cache/` - cached TOC snapshots used to speed resume runs.
- `requirements.txt` - Python dependencies for scraping, PDF generation, and GUI search.
- `.gitignore` - ignores generated caches/test artifacts and Python cache files.

## Common Commands

Download default (Revised Statutes):

```powershell
python scripts\download_louisiana_laws.py
```

Download everything:

```powershell
python scripts\download_louisiana_laws.py --categories all
```

Re-run one bundle:

```powershell
python scripts\download_louisiana_laws.py --categories revised-statutes --bundle-regex "^TITLE 9 "
```

Build/rebuild search index:

```powershell
python scripts\build_search_index.py --rebuild
```

CLI search:

```powershell
python scripts\search_laws.py "\"capital punishment\""
```

GUI search:

```powershell
python scripts\search_laws_gui.py
```

Raw text search without SQLite:

```powershell
rg -n -S "capital punishment" out\
```

## Categories

`revised-statutes`, `louisiana-constitution`, `constitution-ancillaries`, `childrens-code`, `civil-code`, `code-of-civil-procedure`, `code-of-criminal-procedure`, `code-of-evidence`, `house-rules`, `senate-rules`, `joint-rules`, `all`

## Repo Hygiene

Generated caches/test artifacts are now ignored:

- `.toc-cache/`
- `out_test/`
- `out_cache_test/`
- `playwright-browsers/`
- Python cache files (`__pycache__`, `*.pyc`)

