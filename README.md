# LA Law Register - Offline Louisiana Laws

Goal: download the Louisiana laws listed on the Legislature's "Laws Table of Contents" page and produce:

- local, text-based copies (per-section `.txt` + metadata)
- hard, static PDFs (per bundle, e.g. `RS 1 ... .pdf`)
- a folder layout that mirrors the table-of-contents structure

Source TOC: https://www.legis.la.gov/legis/LawsContents.aspx

## How It Works (Implementation Map)

1. **Discover the TOC structure**
   - The site is ASP.NET WebForms (`.aspx`) and expands via postbacks (no stable URLs for many TOC nodes).
   - We use Playwright (driving installed Microsoft Edge) to click TOC nodes the same way a user would.
   - From each leaf list we extract the real, stable document URLs: `Law.aspx?d=<id>`.

2. **Download documents**
   - For each `Law.aspx?d=<id>` page we fetch HTML with `requests` and extract:
     - citation (e.g. `RS 1:1`, `CC 1`, `HRULE 1.1`)
     - the actual law text block
   - Saved locally as:
     - `sections/*.txt` (UTF-8 searchable text)
     - `sections/*.json` (metadata + `doc_text` + `doc_html` + source URL)

3. **Build per-bundle PDFs**
   - We generate one PDF per bundle with a Table of Contents that includes **real page numbers**:
     - render a draft PDF with hidden per-section markers
     - scan the draft PDF to compute section start pages
     - re-render the final PDF with TOC page numbers filled in

## Output Layout (Default)

All output goes into `out/` (configurable via `--out`).

- `out/Revised Statutes/TITLE 1 - General Provisions/TITLE 1 - General Provisions.pdf`
- `out/Revised Statutes/TITLE 1 - General Provisions/sections/*.txt`
- `out/Louisiana Constitution/ARTICLE 1 - Declaration of Rights/ARTICLE 1 - Declaration of Rights.pdf`
- Categories that are already "flat" on the site (e.g. `Civil Code`) become a single bundle PDF under their category folder.

## Setup

```powershell
python -m pip install -r requirements.txt
```

Notes:
- The script uses Playwright with the installed Edge browser (`channel="msedge"`), so you **do not** need to run `playwright install` unless you want to switch browsers.
- Pass1 TOC page-number scanning now prefers `pymupdf` (much faster on large bundles) and falls back to `pypdf` when needed.

## Run

### Default Run (Recommended)

Downloads Revised Statutes, saves local text, and generates static PDFs with TOC page numbers:

```powershell
python scripts\download_louisiana_laws.py
```

Same as: `python scripts\download_louisiana_laws.py --categories revised-statutes`

Defaults (no flags needed):
- Resume is on (`--resume`).
- PDFs are on (with real TOC page numbers).
- TOC is cached to `.toc-cache/` for 7 days, so resume runs don’t re-scrape the TOC.
- Parallel downloads per bundle: `--workers 16`.
- Ctrl+C stops cleanly; re-run the same command to resume.

### Download Everything In The TOC

```powershell
python scripts\download_louisiana_laws.py --categories all
```

### Re-run Only One Bundle

```powershell
python scripts\download_louisiana_laws.py --categories revised-statutes --bundle-regex "^TITLE 9 "
```

### If You Need To Refresh The TOC Cache

```powershell
python scripts\download_louisiana_laws.py --refresh-toc
```

### Helpful limits while testing

```powershell
python scripts\download_louisiana_laws.py --categories revised-statutes --max-bundles 1 --max-sections 10
```

### Force the fast page-number scanner

```powershell
python scripts\download_louisiana_laws.py --categories revised-statutes --pdf-scan-backend pymupdf
```

The default is `--pdf-scan-backend auto` (try `pymupdf`, then fall back to `pypdf`).

## Search / Index

Quick (no database) search across everything downloaded:

```powershell
rg -n -S "capital punishment" out\
```

Build a fast full-text index (SQLite FTS5) and search it:

```powershell
python scripts\build_search_index.py --rebuild
python scripts\search_laws.py "\"capital punishment\""
```

## Categories

- `revised-statutes`
- `louisiana-constitution`
- `constitution-ancillaries`
- `childrens-code`
- `civil-code`
- `code-of-civil-procedure`
- `code-of-criminal-procedure`
- `code-of-evidence`
- `house-rules`
- `senate-rules`
- `joint-rules`
- `all`

