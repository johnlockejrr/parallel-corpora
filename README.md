# Web Dashboard (Parallel Corpora Editor)

A small **UTF‑8-safe** Flask + SQLite dashboard that works like a lightweight spreadsheet for **parallel corpora** (e.g. Syriac ↔ German, Arabic ↔ English): edit text in parallel columns, search across everything, and do full CRUD from the browser.

## Features

- **Fully UTF‑8 compatible**: stores and serves text like `ܫܠܡܐ`, `السلام عليكم`, `für äußere Übung`, etc.
- **Spreadsheet-like table**: one row per aligned unit, one column per language/field.
- **Real-time search**: filters by key and any cell content.
- **CRUD**: create rows, inline edit cells, delete rows.
- **Pagination**: browse large corpora with next/prev pages.
- **Export/backup**: download any corpus as UTF‑8 CSV (default pipe-delimited).
- **SQLite**: single local file at `./data/dashboard.sqlite3`.

## Project layout

- `app.py`: Flask server + JSON API
- `db.py`: SQLite schema + helpers
- `templates/index.html`: UI
- `static/`: CSS/JS
- `scripts/import_dataset.py`: import a UTF‑8 CSV/TSV into a new corpus
- `create-corpus.py`: upload CSV/TSV to server API (create + import)

## Setup (your existing `uv` venv)

You already have a virtualenv at `./.venv`.

Install dependencies:

```bash
./.venv/bin/python -m pip install -r requirements.txt
```

Run the server:

```bash
./.venv/bin/python app.py
```

Open:

- `http://localhost:5000`

If port `5000` is already in use (e.g. by your existing ContextKeep UI), run on a different port:

```bash
PORT=5001 ./.venv/bin/python app.py
```

## Import a CSV/TSV (preprocess)

Your file should have a header row. Recommended header fields:

- `key` (optional; if missing/empty the importer generates `r000001`, …)
- any number of **column keys** (e.g. `syc`, `de`, `ar`, `en`)

Examples:

```bash
# Auto-detect delimiter (, \\t, ;, |)
./.venv/bin/python scripts/import_dataset.py data/my_parallel.tsv

# Force TSV
./.venv/bin/python scripts/import_dataset.py data/my_parallel.tsv --delimiter \\t

# Choose which columns to import
./.venv/bin/python scripts/import_dataset.py data/my_parallel.csv --columns syc,de

# Custom key field
./.venv/bin/python scripts/import_dataset.py data/my_parallel.csv --key-field id
```

The importer creates a **new corpus** each time.

## Import via API (upload a file)

The server supports creating a corpus by uploading a CSV/TSV:

- `POST /api/corpora/import` (multipart/form-data)

Fields:

- `file` (required): UTF‑8 CSV/TSV (BOM ok)
- `name` (optional): corpus name (default: filename)
- `description` (optional)
- `delimiter` (optional): `,` `;` `|` or `\\t` (auto-detect if omitted)
- `key_field` (optional): header field to use as external key (default `key`)
- `columns` (optional): comma-separated list of columns to import (default: all headers except `key_field`)

Example:

```bash
curl -F "file=@data/my_parallel.tsv" -F "delimiter=\\t" -F "name=Syriac-German" http://127.0.0.1:5001/api/corpora/import
```

Notes:

- Rows are **upserted by key** when `key_field` is provided and non-empty.

## Create/import via CLI (calls the server API)

```bash
./.venv/bin/python create-corpus.py --url http://127.0.0.1:5001 --file data/my_parallel.tsv --delimiter "\\t" --name "Syriac-German" --key-field key --columns syc,de
```

## Export/backup a corpus

- UI: click **Export** in the toolbar.
- API: `GET /api/corpora/<corpus_id>/export`

Query params:

- `delimiter`: one of `|` (default), `,`, `;`, `\\t`
- `include_key`: `1` (default) or `0`

## Notes

- SQLite stores text as Unicode; this app also returns JSON with `ensure_ascii = False` so non‑Latin scripts remain readable in API responses.
- **`id` vs `key`**: `id` is the internal identity; `key` is optional and only needed if you want stable external references + upserts on re-import.
- Current search uses a simple `LIKE` over key/raw JSON cells. For large corpora, we can add SQLite FTS5 later.

---

## License

MIT © 2026 **johnlockejrr**

Project: [johnlockejrr/parallel-corpora](https://github.com/johnlockejrr/parallel-corpora)

