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
import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def _doc_id_from_url(url: str) -> str:
    qs = parse_qs(urlparse(url).query)
    return (qs.get("d") or [""])[0]


def _doc_id_from_entry(entry: dict[str, object]) -> str:
    raw = entry.get("doc_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    url = entry.get("url")
    if isinstance(url, str) and url.strip():
        return _doc_id_from_url(url)
    return ""


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


def _cleanup_temp_db(tmp_db_path: Path) -> None:
    for p in (
        tmp_db_path,
        tmp_db_path.with_name(tmp_db_path.name + "-wal"),
        tmp_db_path.with_name(tmp_db_path.name + "-shm"),
    ):
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def _build_index_into_db(out_dir: Path, db_path: Path) -> int:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")

        # Store metadata as UNINDEXED; index only citation/title/text.
        con.execute(
            """
            CREATE VIRTUAL TABLE docs_fts USING fts5(
              doc_id UNINDEXED,
              category UNINDEXED,
              bundle UNINDEXED,
              citation,
              title,
              text,
              url UNINDEXED,
              local_file UNINDEXED,
              tokenize = 'unicode61',
              prefix = '2 3 4 5'
            );
            """
        )

        bundle_paths = sorted(out_dir.rglob("bundle.json"))
        if not bundle_paths:
            print("[warn] No bundle.json files found under out/. Run the downloader first.", file=sys.stderr)
            return 0

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
                    doc_id = _doc_id_from_entry(ent)
                    if not doc_id:
                        continue
                    citation = ent.get("citation") or ""
                    title = ent.get("title") or ""
                    meta_path = bundle_dir / "sections" / f"{doc_id}.json"
                    txt_path = bundle_dir / "sections" / f"{doc_id}.txt"
                    text = _load_doc_text(meta_path, txt_path)
                    if not text:
                        continue
                    local_file = ""
                    if meta_path.exists():
                        try:
                            meta = json.loads(meta_path.read_text(encoding="utf-8"))
                            raw_local_file = meta.get("local_file")
                            if isinstance(raw_local_file, str):
                                local_file = raw_local_file.strip()
                        except Exception:
                            local_file = ""

                    con.execute(
                        "INSERT INTO docs_fts(doc_id, category, bundle, citation, title, text, url, local_file) VALUES (?,?,?,?,?,?,?,?)",
                        (doc_id, category, bundle_name, citation, title, text, url, local_file),
                    )
                    total += 1

        con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        return total
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="out", help="Output directory used by downloader (default: out)")
    p.add_argument("--db", default=None, help="SQLite DB path (default: <out>/index.sqlite)")
    p.add_argument(
        "--rebuild",
        action="store_true",
        help="Compatibility flag; rebuild is now always full and atomic",
    )
    args = p.parse_args(argv)

    out_dir = Path(args.out)
    if not out_dir.exists():
        raise SystemExit(f"Out dir not found: {out_dir}")

    bundle_paths = sorted(out_dir.rglob("bundle.json"))
    if not bundle_paths:
        print("[warn] No bundle.json files found under out/. Run the downloader first.", file=sys.stderr)
        return 1

    db_path = Path(args.db) if args.db else (out_dir / "index.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_db_path = db_path.with_name(db_path.name + ".building")

    _cleanup_temp_db(tmp_db_path)
    try:
        total = _build_index_into_db(out_dir, tmp_db_path)
        if total <= 0:
            _cleanup_temp_db(tmp_db_path)
            print("[warn] Built 0 docs; refusing to replace the existing index.", file=sys.stderr)
            return 1

        os.replace(tmp_db_path, db_path)
        _cleanup_temp_db(tmp_db_path)
        print(f"[ok] indexed {total} docs -> {db_path}")
        return 0
    except KeyboardInterrupt:
        _cleanup_temp_db(tmp_db_path)
        print("[warn] interrupted; existing index file left unchanged", file=sys.stderr)
        return 130
    except PermissionError:
        _cleanup_temp_db(tmp_db_path)
        print(
            f"[error] Could not replace {db_path} (file may be open). Close apps using it and retry.",
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        _cleanup_temp_db(tmp_db_path)
        print(f"[error] index build failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

