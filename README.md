# LA Law Register

Offline Louisiana law downloader + search tools.

Source TOC: https://www.legis.la.gov/legis/LawsContents.aspx
Official Supreme Court opinions archive: https://www.lasc.org/CourtActions/2026

## What This Repo Produces

- Local law text and metadata (`sections/*.txt`, `sections/*.json`)
- Local Louisiana Supreme Court opinion PDFs plus extracted text/metadata
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
- `scripts/download_louisiana_case_law.py`
  - Downloads Louisiana Supreme Court opinions from the official Louisiana Supreme Court archive.
  - Saves yearly bundles, local opinion PDFs, and extracted full text for indexing.
- `scripts/search_laws.py`
  - CLI query tool for the SQLite index.
- `scripts/search_laws_gui.py`
  - Desktop GUI search with:
    - category/bundle filtering plus source preset buttons
    - regex mode
    - local full-text preview with highlighting
    - local opinion PDF opening when available
    - citation sorting (ex: `RS 14:*` before `RS 34:*`)
- `scripts/law_browser_gui.py`
  - Browser-style GUI for navigating the indexed library by category, bundle, and document.
  - Includes a case-law-focused "What Was Learned" summary view for Louisiana Supreme Court opinions.
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

Download Louisiana Supreme Court opinions (official archive, 2000-current by default):

```powershell
python scripts\download_louisiana_case_law.py
```

Download a narrower Supreme Court range:

```powershell
python scripts\download_louisiana_case_law.py --years 2020-2026
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

GUI browser:

```powershell
python scripts\law_browser_gui.py
```

Build Windows `.exe` for GUI:

```powershell
python -m pip install pyinstaller
python -m PyInstaller --noconfirm --clean --windowed --onefile --name LA-Law-Search scripts\search_laws_gui.py
```

Output: `dist\LA-Law-Search.exe`

Raw text search without SQLite:

```powershell
rg -n -S "capital punishment" out\
```

## Categories

`revised-statutes`, `louisiana-constitution`, `constitution-ancillaries`, `childrens-code`, `civil-code`, `code-of-civil-procedure`, `code-of-criminal-procedure`, `code-of-evidence`, `house-rules`, `senate-rules`, `joint-rules`, `all`

Additional indexed source after running the case-law downloader:

- `Louisiana Supreme Court Decisions`

Notes:

- The Louisiana Constitution is already supported by `scripts\download_louisiana_laws.py` via `--categories louisiana-constitution`.
- Supreme Court decisions are currently sourced from the official Louisiana Supreme Court archive rather than Justia.

## Repo Hygiene

Generated caches/test artifacts are now ignored:

- `.toc-cache/`
- `out_test/`
- `out_cache_test/`
- `playwright-browsers/`
- Python cache files (`__pycache__`, `*.pyc`)

