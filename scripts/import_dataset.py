#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Any

from db import create_corpus, create_row_in_corpus_table, init_db, upsert_row_by_key_in_corpus_table


def sniff_dialect(sample: str, default_delimiter: str | None) -> csv.Dialect:
    if default_delimiter:
        class D(csv.Dialect):
            delimiter = default_delimiter
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return D()
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
    except Exception:
        return csv.excel


def read_rows(path: Path, delimiter: str | None) -> tuple[list[str], list[dict[str, str]]]:
    raw = path.read_text(encoding="utf-8-sig", errors="strict")
    sample = raw[:4096]
    dialect = sniff_dialect(sample, delimiter)
    reader = csv.DictReader(raw.splitlines(), dialect=dialect)
    if not reader.fieldnames:
        raise SystemExit("Input has no header row.")
    header = [h.strip() for h in reader.fieldnames if h is not None]
    data: list[dict[str, str]] = []
    for row in reader:
        clean: dict[str, str] = {}
        for k, v in row.items():
            if k is None:
                continue
            key = k.strip()
            clean[key] = "" if v is None else str(v)
        data.append(clean)
    return header, data


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Import UTF-8 CSV/TSV into the Web Dashboard SQLite DB.")
    p.add_argument("input", type=str, help="Path to CSV/TSV file (UTF-8; BOM ok).")
    p.add_argument("--db", type=str, default="", help="Path to sqlite DB (default: ./data/dashboard.sqlite3).")
    p.add_argument("--corpus-name", type=str, default="", help="Corpus name (default: file stem).")
    p.add_argument(
        "--delimiter",
        type=str,
        default="",
        help="Force delimiter (e.g. ',' or '\\t'). If omitted, auto-detect.",
    )
    p.add_argument(
        "--key-field",
        type=str,
        default="key",
        help="Field to use as unique row key (default: key). If missing/empty, generates one.",
    )
    p.add_argument(
        "--columns",
        type=str,
        default="",
        help="Comma-separated column keys to import (default: all header fields except key).",
    )
    args = p.parse_args(argv)

    in_path = Path(args.input).expanduser().resolve()
    if not in_path.exists():
        raise SystemExit(f"File not found: {in_path}")

    db_path = args.db.strip() or str((Path(__file__).resolve().parents[1] / "data" / "dashboard.sqlite3").resolve())
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    init_db(db_path)

    delimiter = args.delimiter
    if delimiter == "\\t":
        delimiter = "\t"
    delimiter = delimiter if delimiter else None

    header, data_rows = read_rows(in_path, delimiter=delimiter)

    key_field = args.key_field.strip() or "key"

    if args.columns.strip():
        columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    else:
        columns = [h for h in header if h not in {key_field}]
    if not columns:
        raise SystemExit("No columns selected to import.")

    corpus_name = args.corpus_name.strip() or in_path.stem
    corpus = create_corpus(db_path, name=corpus_name, columns=columns, description=f"Imported from {in_path.name}")
    corpus_id = int(corpus["id"])

    imported = 0
    skipped = 0
    for i, r in enumerate(data_rows, start=1):
        key = (r.get(key_field) or "").strip()

        cells: dict[str, Any] = {}
        for c in columns:
            cells[c] = (r.get(c) or "")

        try:
            if key:
                upsert_row_by_key_in_corpus_table(db_path, corpus=corpus, key=key, cells=cells)
            else:
                create_row_in_corpus_table(db_path, corpus=corpus, key=None, cells=cells)
            imported += 1
        except Exception:
            skipped += 1

    print(f"DB: {db_path}")
    print(f"Corpus: {corpus_name} (id={corpus_id})")
    print(f"Imported rows: {imported}")
    if skipped:
        print(f"Skipped rows (duplicate keys or errors): {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

