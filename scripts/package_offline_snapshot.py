"""
Compress and chunk a BayouLex SQLite dataset for throttled offline download.

Input:  data/bayoulex-content.sqlite
Output: data/offline/<version>/manifest.json + chunk-*.brpart
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _compress_brotli(src: Path, dst: Path) -> None:
    try:
        import brotli
    except ImportError as exc:
        raise RuntimeError("Install the 'brotli' Python package to create .br offline snapshots.") from exc

    compressor = brotli.Compressor(quality=6)
    with src.open("rb") as source, dst.open("wb") as target:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            target.write(compressor.process(block))
        target.write(compressor.finish())


def package_snapshot(db_path: Path, out_root: Path, version: str, chunk_size: int) -> Path:
    if not db_path.exists():
        raise RuntimeError(f"Dataset not found: {db_path}")

    version_dir = out_root / version
    if version_dir.exists():
        shutil.rmtree(version_dir)
    version_dir.mkdir(parents=True, exist_ok=True)

    compressed_name = f"bayoulex-content-{version}.sqlite.br"
    compressed_path = version_dir / compressed_name
    _compress_brotli(db_path, compressed_path)

    chunks: list[dict[str, object]] = []
    offset = 0
    number = 0
    with compressed_path.open("rb") as source:
        while True:
            data = source.read(chunk_size)
            if not data:
                break
            number += 1
            name = f"chunk-{number:06d}.brpart"
            chunk_path = version_dir / name
            chunk_path.write_bytes(data)
            chunks.append(
                {
                    "number": number,
                    "fileName": name,
                    "offset": offset,
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
            offset += len(data)

    manifest = {
        "datasetVersion": version,
        "fileName": compressed_name,
        "compressedBytes": compressed_path.stat().st_size,
        "uncompressedBytes": db_path.stat().st_size,
        "sha256": _sha256_file(compressed_path),
        "chunkSizeBytes": chunk_size,
        "chunks": chunks,
    }
    manifest_path = version_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    compressed_path.unlink()
    return manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/bayoulex-content.sqlite")
    parser.add_argument("--out", default="data/offline")
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--chunk-size", type=int, default=16 * 1024 * 1024)
    args = parser.parse_args(argv)

    try:
        manifest = package_snapshot(Path(args.db), Path(args.out), args.dataset_version, int(args.chunk_size))
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    print(f"[ok] wrote offline manifest -> {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
