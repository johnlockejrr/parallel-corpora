#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import requests


def main() -> int:
    p = argparse.ArgumentParser(description="Create/import a corpus by uploading CSV/TSV to the Web Dashboard server.")
    p.add_argument("--url", default="http://127.0.0.1:5001", help="Server base URL (default: http://127.0.0.1:5001)")
    p.add_argument("--file", required=True, help="Path to CSV/TSV (UTF-8; BOM ok)")
    p.add_argument("--name", default="", help="Corpus name (default: file stem)")
    p.add_argument("--description", default="", help="Optional description")
    p.add_argument("--delimiter", default="", help="Optional forced delimiter: ',' '\\t' ';' '|' or ' ' (space)")
    p.add_argument("--key-field", default="", help="Optional header field to use as external key (e.g. key,id)")
    p.add_argument(
        "--columns",
        default="",
        help="Comma-separated list of columns to import (default: server chooses all except key-field)",
    )
    args = p.parse_args()

    file_path = Path(args.file).expanduser().resolve()
    if not file_path.exists():
        raise SystemExit(f"File not found: {file_path}")

    base = args.url.rstrip("/")
    endpoint = f"{base}/api/corpora/import"

    data: dict[str, str] = {}
    if args.name.strip():
        data["name"] = args.name.strip()
    if args.description.strip():
        data["description"] = args.description.strip()
    if args.delimiter.strip():
        data["delimiter"] = args.delimiter.strip()
    if args.key_field.strip():
        data["key_field"] = args.key_field.strip()
    if args.columns.strip():
        data["columns"] = args.columns.strip()

    with file_path.open("rb") as f:
        files = {"file": (file_path.name, f, "text/plain")}
        r = requests.post(endpoint, data=data, files=files, timeout=300)

    try:
        payload = r.json()
    except Exception:
        raise SystemExit(f"Server returned non-JSON (status {r.status_code}):\n{r.text[:5000]}")

    if not r.ok or not payload.get("success"):
        raise SystemExit(f"Import failed (status {r.status_code}): {payload}")

    corpus = payload.get("corpus") or {}
    print(f"Created corpus: {corpus.get('name')} (id={corpus.get('id')})")
    print(f"Imported rows: {payload.get('imported')}")
    if "upserted" in payload:
        print(f"Upserted (keyed) rows: {payload.get('upserted')}")
    print(f"Open: {base}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

