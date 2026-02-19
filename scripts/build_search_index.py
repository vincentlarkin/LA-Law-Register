"""
Build a local full-text search index (SQLite FTS5) from the downloaded laws.

Input:  out/**/bundle.json + out/**/sections/*.json
Output: out/index.sqlite (configurable)

Run:
  python scripts/build_search_index.py
  python scripts/build_search_index.py --out out --db out/index.sqlite --rebuild
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def _doc_id_from_url(url: str) -> str:
    qs = parse_qs(urlparse(url).query)
    return (qs.get("d") or [""])[0]


def _load_doc_text(meta_json_path: Path, fallback_txt_path: Path) -> str:
    if meta_json_path.exists():
        try:
            meta = json.loads(meta_json_path.read_text(encoding="utf-8"))
            txt = meta.get("doc_text")
            if isinstance(txt, str) and txt.strip():
                return txt
        except Exception:
            pass

    if fallback_txt_path.exists():
        raw = fallback_txt_path.read_text(encoding="utf-8", errors="replace")
        # Our .txt wrapper is:
        # citation\n
        # title?\n
        # url\n
        # \n
        # body...
        parts = raw.split("\n\n", 1)
        if len(parts) == 2:
            return parts[1].strip()
        return raw.strip()

    return ""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="out", help="Output directory used by downloader (default: out)")
    p.add_argument("--db", default=None, help="SQLite DB path (default: <out>/index.sqlite)")
    p.add_argument("--rebuild", action="store_true", help="Drop and rebuild the index")
    args = p.parse_args(argv)

    out_dir = Path(args.out)
    if not out_dir.exists():
        raise SystemExit(f"Out dir not found: {out_dir}")

    db_path = Path(args.db) if args.db else (out_dir / "index.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")

        if args.rebuild:
            con.execute("DROP TABLE IF EXISTS docs_fts;")

        # Store metadata as UNINDEXED; index only citation/title/text.
        con.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(
              doc_id UNINDEXED,
              category UNINDEXED,
              bundle UNINDEXED,
              citation,
              title,
              text,
              url UNINDEXED,
              tokenize = 'unicode61'
            );
            """
        )

        # Rebuild means clearing existing rows too.
        if args.rebuild:
            con.execute("INSERT INTO docs_fts(docs_fts) VALUES('rebuild');")
        else:
            # If not rebuilding, we'll still do a clean insert for now.
            con.execute("DELETE FROM docs_fts;")

        bundle_paths = sorted(out_dir.rglob("bundle.json"))
        if not bundle_paths:
            print("[warn] No bundle.json files found under out/. Run the downloader first.", file=sys.stderr)
            return 1

        total = 0
        with con:
            for bundle_path in bundle_paths:
                bundle_dir = bundle_path.parent
                bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
                category = bundle.get("category_name") or ""
                bundle_name = bundle.get("bundle_name") or bundle_dir.name
                entries = bundle.get("entries") or []

                for ent in entries:
                    url = ent.get("url") or ""
                    doc_id = _doc_id_from_url(url) if url else ""
                    if not doc_id:
                        continue
                    citation = ent.get("citation") or ""
                    title = ent.get("title") or ""
                    meta_path = bundle_dir / "sections" / f"{doc_id}.json"
                    txt_path = bundle_dir / "sections" / f"{doc_id}.txt"
                    text = _load_doc_text(meta_path, txt_path)
                    if not text:
                        continue

                    con.execute(
                        "INSERT INTO docs_fts(doc_id, category, bundle, citation, title, text, url) VALUES (?,?,?,?,?,?,?)",
                        (doc_id, category, bundle_name, citation, title, text, url),
                    )
                    total += 1

        print(f"[ok] indexed {total} docs -> {db_path}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

