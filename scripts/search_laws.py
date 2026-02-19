"""
Search the local SQLite FTS index built by scripts/build_search_index.py.

Run:
  python scripts/search_laws.py "capital punishment"
  python scripts/search_laws.py "\"capital punishment\""   # phrase search
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("query", help="FTS5 query (use quotes for phrase search)")
    p.add_argument("--db", default="out/index.sqlite", help="SQLite DB path (default: out/index.sqlite)")
    p.add_argument("--limit", type=int, default=25, help="Max results (default: 25)")
    args = p.parse_args(argv)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Index not found: {db_path}", file=sys.stderr)
        print("Build it first: python scripts/build_search_index.py", file=sys.stderr)
        return 2

    con = sqlite3.connect(str(db_path))
    try:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT
              doc_id,
              category,
              bundle,
              citation,
              title,
              url,
              snippet(docs_fts, 5, '[', ']', ' ... ', 12) AS snippet
            FROM docs_fts
            WHERE docs_fts MATCH ?
            LIMIT ?;
            """,
            (args.query, int(args.limit)),
        ).fetchall()

        if not rows:
            print("[no results]")
            return 0

        for r in rows:
            # One-line header + a short snippet.
            print(f"{r['citation']}  ({r['category']} / {r['bundle']})")
            if r["title"]:
                print(f"  {r['title']}")
            if r["url"]:
                print(f"  {r['url']}")
            if r["snippet"]:
                print(f"  {r['snippet']}".rstrip())
            print()

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

