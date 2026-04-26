"""
Build the canonical BayouLex content database from generated downloader output.

Input:  out/**/bundle.json + sections/*.json + sections/*.txt, plus existing out/index.sqlite when present
Output: data/bayoulex-content.sqlite

This is the bridge away from many tiny runtime files. It preserves the existing
downloaders as ingestion tools while producing one API/client-ready SQLite DB.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
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


def _read_json(path: Path) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _load_text(meta: dict[str, object], fallback_txt_path: Path) -> str:
    raw_text = meta.get("doc_text")
    if isinstance(raw_text, str) and raw_text.strip():
        return raw_text.strip()

    if not fallback_txt_path.exists():
        return ""

    raw = fallback_txt_path.read_text(encoding="utf-8", errors="replace")
    parts = raw.split("\n\n", 1)
    if len(parts) == 2:
        return parts[1].strip()
    return raw.strip()


def _find_bundle_paths(out_dir: Path) -> list[Path]:
    bundle_paths: list[Path] = []
    for root, dirs, files in os.walk(out_dir):
        dirs[:] = [
            d
            for d in dirs
            if d not in {"sections", "__pycache__"} and not d.startswith(".") and not d.endswith(".building")
        ]
        if "bundle.json" in files:
            bundle_paths.append(Path(root) / "bundle.json")
    return sorted(bundle_paths)


def _document_key(category: str, bundle: str, doc_id: str, url: str) -> str:
    raw = "\x1f".join([category, bundle, doc_id, url])
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:24]


def _schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -200000;

        CREATE TABLE dataset_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            document_key TEXT NOT NULL UNIQUE,
            doc_id TEXT NOT NULL,
            category TEXT NOT NULL,
            bundle TEXT NOT NULL,
            session_id TEXT NOT NULL DEFAULT '',
            chamber TEXT NOT NULL DEFAULT '',
            status_group TEXT NOT NULL DEFAULT '',
            status_label TEXT NOT NULL DEFAULT '',
            citation TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            text TEXT NOT NULL,
            url TEXT NOT NULL DEFAULT '',
            local_file TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            source_path TEXT NOT NULL DEFAULT ''
        );

        CREATE INDEX idx_documents_category_bundle ON documents(category, bundle);
        CREATE INDEX idx_documents_status ON documents(status_label);
        CREATE INDEX idx_documents_doc_id ON documents(doc_id);

        CREATE VIRTUAL TABLE documents_fts USING fts5(
            citation,
            title,
            text,
            content='documents',
            content_rowid='id',
            tokenize='unicode61',
            prefix='2 3 4 5'
        );
        """
    )


def _insert_metadata(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT OR REPLACE INTO dataset_metadata(key, value) VALUES (?, ?)",
        (key, value),
    )


def _insert_document(
    con: sqlite3.Connection,
    *,
    category: str,
    bundle_name: str,
    doc_id: str,
    url: str,
    citation: str,
    title: str,
    text: str,
    session_id: str = "",
    chamber: str = "",
    status_group: str = "",
    status_label: str = "",
    local_file: str = "",
    metadata: dict[str, object] | None = None,
    source_path: str = "",
) -> int:
    key = _document_key(category, bundle_name, doc_id, url)
    cursor = con.execute(
        """
        INSERT OR IGNORE INTO documents(
            document_key, doc_id, category, bundle, session_id, chamber,
            status_group, status_label, citation, title, text, url,
            local_file, metadata_json, source_path
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            doc_id,
            category,
            bundle_name,
            session_id,
            chamber,
            status_group,
            status_label,
            citation,
            title,
            text,
            url,
            local_file,
            json.dumps(metadata or {}, ensure_ascii=False, separators=(",", ":")),
            source_path,
        ),
    )
    return 1 if cursor.rowcount > 0 else 0


def _merge_existing_index(con: sqlite3.Connection, index_path: Path) -> int:
    if not index_path.exists():
        return 0

    src = sqlite3.connect(index_path)
    src.row_factory = sqlite3.Row
    inserted = 0
    try:
        try:
            rows = src.execute(
                """
                SELECT doc_id, category, bundle, session_id, chamber, status_group, status_label,
                       citation, title, text, url, local_file
                FROM docs_fts
                WHERE COALESCE(text, '') <> ''
                """
            )
        except sqlite3.Error:
            return 0

        for row in rows:
            category = str(row["category"] or "").strip()
            bundle_name = str(row["bundle"] or "").strip()
            doc_id = str(row["doc_id"] or "").strip()
            text = str(row["text"] or "").strip()
            if not category or not bundle_name or not doc_id or not text:
                continue
            inserted += _insert_document(
                con,
                category=category,
                bundle_name=bundle_name,
                doc_id=doc_id,
                url=str(row["url"] or "").strip(),
                citation=str(row["citation"] or "").strip(),
                title=str(row["title"] or "").strip(),
                text=text,
                session_id=str(row["session_id"] or "").strip(),
                chamber=str(row["chamber"] or "").strip(),
                status_group=str(row["status_group"] or "").strip(),
                status_label=str(row["status_label"] or "").strip(),
                local_file=str(row["local_file"] or "").strip(),
                source_path=str(index_path),
            )
    finally:
        src.close()

    return inserted


def build_dataset(out_dir: Path, db_path: Path, dataset_version: str, existing_index: Path | None) -> int:
    bundle_paths = _find_bundle_paths(out_dir)
    if not bundle_paths:
        raise RuntimeError(f"No bundle.json files found under {out_dir}")

    tmp_path = db_path.with_name(db_path.name + ".building")
    for path in [tmp_path, tmp_path.with_name(tmp_path.name + "-wal"), tmp_path.with_name(tmp_path.name + "-shm")]:
        if path.exists():
            path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(tmp_path)
    total = 0
    try:
        _schema(con)
        with con:
            _insert_metadata(con, "dataset_version", dataset_version)
            _insert_metadata(con, "built_at_epoch", str(int(time.time())))
            _insert_metadata(con, "source_out_dir", str(out_dir.resolve()))

            for bundle_path in bundle_paths:
                bundle_dir = bundle_path.parent
                bundle = _read_json(bundle_path)
                category = str(bundle.get("category_name") or "").strip()
                bundle_name = str(bundle.get("bundle_name") or bundle_dir.name).strip()
                entries = bundle.get("entries") or []
                if not category or not isinstance(entries, list):
                    continue

                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    doc_id = _doc_id_from_entry(entry)
                    if not doc_id:
                        continue
                    meta_path = bundle_dir / "sections" / f"{doc_id}.json"
                    txt_path = bundle_dir / "sections" / f"{doc_id}.txt"
                    meta = _read_json(meta_path)
                    text = _load_text(meta, txt_path)
                    if not text:
                        continue

                    url = str(meta.get("url") or entry.get("url") or "").strip()
                    citation = str(meta.get("citation") or entry.get("citation") or "").strip()
                    title = str(meta.get("title") or entry.get("title") or "").strip()
                    session_id = str(meta.get("session_name") or meta.get("session_id") or "").strip()
                    chamber = str(meta.get("chamber_label") or meta.get("chamber") or "").strip()
                    status_group = str(meta.get("bill_status_group") or "").strip()
                    status_label = str(meta.get("bill_status_label") or "").strip()
                    local_file = str(meta.get("local_file") or "").strip()
                    total += _insert_document(
                        con,
                        category=category,
                        bundle_name=bundle_name,
                        doc_id=doc_id,
                        url=url,
                        citation=citation,
                        title=title,
                        text=text,
                        session_id=session_id,
                        chamber=chamber,
                        status_group=status_group,
                        status_label=status_label,
                        local_file=local_file,
                        metadata=meta,
                        source_path=str(meta_path.relative_to(out_dir)) if meta_path.exists() else "",
                    )

            if existing_index is not None:
                merged = _merge_existing_index(con, existing_index)
                total += merged
                _insert_metadata(con, "merged_existing_index_rows", str(merged))

            con.execute("INSERT INTO documents_fts(documents_fts) VALUES ('rebuild')")
            _insert_metadata(con, "document_count", str(total))

        con.execute("PRAGMA optimize;")
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    finally:
        con.close()

    if total <= 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError("Built 0 documents; refusing to replace dataset")

    os.replace(tmp_path, db_path)
    for path in [tmp_path.with_name(tmp_path.name + "-wal"), tmp_path.with_name(tmp_path.name + "-shm")]:
        path.unlink(missing_ok=True)
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="out", help="Generated downloader output root")
    parser.add_argument("--db", default="data/bayoulex-content.sqlite", help="Output SQLite database")
    parser.add_argument("--dataset-version", default=time.strftime("%Y%m%d"), help="Immutable dataset version label")
    parser.add_argument(
        "--existing-index",
        default=None,
        help="Existing docs_fts SQLite index to merge for content not present in sidecar files (default: <out>/index.sqlite when present)",
    )
    args = parser.parse_args(argv)

    try:
        out_dir = Path(args.out)
        existing_index = Path(args.existing_index) if args.existing_index else out_dir / "index.sqlite"
        if not existing_index.exists():
            existing_index = None
        total = build_dataset(out_dir, Path(args.db), str(args.dataset_version), existing_index)
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    print(f"[ok] built {total:,} documents -> {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
