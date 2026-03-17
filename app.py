from __future__ import annotations

import csv
import io
import os
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, request

from db import (
    create_corpus,
    create_row_in_corpus_table,
    delete_corpus,
    delete_row_in_corpus_table,
    get_corpus,
    init_db,
    list_corpora,
    list_rows_in_corpus_table,
    upsert_row_by_key_in_corpus_table,
    update_corpus,
    update_row_in_corpus_table,
)


def create_app() -> Flask:
    app = Flask(__name__)

    # Ensure non-ASCII stays readable (Hebrew/Arabic/diacritics) in JSON responses.
    app.json.ensure_ascii = False

    data_dir = Path(os.environ.get("WEB_DASHBOARD_DATA_DIR", Path(__file__).parent / "data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "dashboard.sqlite3"
    app.config["DB_PATH"] = str(db_path)

    init_db(app.config["DB_PATH"])

    def _max_upload_bytes() -> int:
        # Default 10MB; override with WEB_DASHBOARD_MAX_UPLOAD_MB.
        try:
            mb = int(os.environ.get("WEB_DASHBOARD_MAX_UPLOAD_MB", "10"))
        except Exception:
            mb = 10
        return max(1, mb) * 1024 * 1024

    def _decode_upload_to_text(up) -> str:
        raw = up.stream.read(_max_upload_bytes() + 1)
        if len(raw) > _max_upload_bytes():
            raise ValueError("file too large")
        try:
            return raw.decode("utf-8-sig")
        except Exception as e:
            raise ValueError("file must be UTF-8 (BOM ok)") from e

    def _sniff_dialect(sample: str, forced_delimiter: str | None) -> csv.Dialect:
        if forced_delimiter:
            class Forced(csv.Dialect):
                delimiter = forced_delimiter
                quotechar = '"'
                doublequote = True
                skipinitialspace = False
                lineterminator = "\n"
                quoting = csv.QUOTE_MINIMAL

            return Forced()
        try:
            return csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|", " "])
        except Exception:
            return csv.excel

    @app.get("/")
    def index():
        return render_template("index.html")

    # ---- Corpora ----
    @app.get("/api/corpora")
    def api_list_corpora():
        return jsonify({"success": True, "corpora": list_corpora(app.config["DB_PATH"])})

    @app.post("/api/corpora")
    def api_create_corpus():
        data = request.get_json(force=True, silent=True) or {}
        name = (data.get("name") or "").strip()
        columns = data.get("columns") or []
        description = (data.get("description") or "").strip()

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400
        if not isinstance(columns, list) or any(not str(c).strip() for c in columns):
            return jsonify({"success": False, "error": "columns must be a list of non-empty strings"}), 400

        corpus = create_corpus(app.config["DB_PATH"], name=name, columns=[str(c).strip() for c in columns], description=description)
        return jsonify({"success": True, "corpus": corpus})

    @app.post("/api/corpora/import")
    def api_import_corpus():
        """
        Create a corpus from an uploaded UTF-8 CSV/TSV file.

        multipart/form-data:
          - file: the CSV/TSV
          - name: optional corpus name (default: filename)
          - delimiter: optional (',', '\\t', ';', '|') auto-detected if omitted
          - key_field: optional header field name to use as external key (default: 'key')
          - columns: optional comma-separated list of columns to import (default: all headers except key_field)
          - description: optional
        """
        up = request.files.get("file")
        if not up:
            return jsonify({"success": False, "error": "file is required"}), 400

        name = (request.form.get("name") or "").strip() or Path(up.filename or "import").stem
        description = (request.form.get("description") or "").strip()
        key_field = (request.form.get("key_field") or "key").strip() or "key"
        delimiter = (request.form.get("delimiter") or "").strip()
        if delimiter == "\\t":
            delimiter = "\t"
        delimiter = delimiter or None

        try:
            text = _decode_upload_to_text(up)
        except ValueError as e:
            return jsonify({"success": False, "error": str(e)}), 400

        sample = text[:4096]
        dialect = _sniff_dialect(sample, delimiter)

        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        if not reader.fieldnames:
            return jsonify({"success": False, "error": "missing header row"}), 400

        header = [h.strip() for h in reader.fieldnames if h]
        columns_form = (request.form.get("columns") or "").strip()
        if columns_form:
            columns = [c.strip() for c in columns_form.split(",") if c.strip()]
        else:
            columns = [h for h in header if h != key_field]
        if not columns:
            return jsonify({"success": False, "error": "no columns to import"}), 400

        corpus = create_corpus(app.config["DB_PATH"], name=name, columns=columns, description=description)

        # Insert rows (upsert by key when key_field present and non-empty).
        imported = 0
        upserted = 0
        for row in reader:
            key = (row.get(key_field) or "").strip()
            cells = {c: (row.get(c) or "") for c in columns}
            if key:
                upsert_row_by_key_in_corpus_table(app.config["DB_PATH"], corpus=corpus, key=key, cells=cells)
                upserted += 1
            else:
                create_row_in_corpus_table(app.config["DB_PATH"], corpus=corpus, key=None, cells=cells)
            imported += 1

        return jsonify({"success": True, "corpus": corpus, "imported": imported, "upserted": upserted})

    @app.post("/api/corpora/import/preview")
    def api_import_preview():
        """
        Preview an uploaded CSV/TSV to help the UI pick delimiter/columns.

        multipart/form-data:
          - file: required
          - delimiter: optional forced delimiter (',', '\\t', ';', '|', ' ')
        """
        up = request.files.get("file")
        if not up:
            return jsonify({"success": False, "error": "file is required"}), 400

        delimiter = (request.form.get("delimiter") or "").strip()
        if delimiter == "\\t":
            delimiter = "\t"
        delimiter = delimiter or None

        try:
            text = _decode_upload_to_text(up)
        except ValueError as e:
            return jsonify({"success": False, "error": str(e)}), 400

        sample = text[:4096]
        dialect = _sniff_dialect(sample, delimiter)
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        if not reader.fieldnames:
            return jsonify({"success": False, "error": "missing header row"}), 400

        header = [h.strip() for h in reader.fieldnames if h]
        # Count rows with a cap (avoid heavy work for huge files even within size limit).
        max_count = 5000
        count = 0
        for _ in reader:
            count += 1
            if count >= max_count:
                break

        return jsonify(
            {
                "success": True,
                "header": header,
                "delimiter": getattr(dialect, "delimiter", None),
                "row_count": count,
                "row_count_capped": count >= max_count,
                "max_upload_mb": _max_upload_bytes() // (1024 * 1024),
            }
        )

    @app.get("/api/corpora/<int:corpus_id>")
    def api_get_corpus(corpus_id: int):
        corpus = get_corpus(app.config["DB_PATH"], corpus_id)
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404
        return jsonify({"success": True, "corpus": corpus})

    @app.get("/api/corpora/<int:corpus_id>/export")
    def api_export_corpus(corpus_id: int):
        corpus = get_corpus(app.config["DB_PATH"], corpus_id)
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404

        delimiter = (request.args.get("delimiter") or "|")
        if delimiter == "\\t":
            delimiter = "\t"
        if delimiter not in {",", "\t", "|", ";"}:
            return jsonify({"success": False, "error": "unsupported delimiter"}), 400

        include_key = (request.args.get("include_key") or "1").strip() != "0"
        columns: list[str] = list(corpus.get("columns") or [])

        def iter_lines():
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
            header = (["key"] if include_key else []) + columns
            writer.writerow(header)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

            # Stream in pages to keep memory bounded.
            offset = 0
            page = 2000
            while True:
                rows_page, has_more = list_rows_in_corpus_table(
                    app.config["DB_PATH"],
                    corpus=corpus,
                    q="",
                    sort_by="id",
                    sort_dir="asc",
                    limit=page,
                    offset=offset,
                )
                for r in rows_page:
                    cells = r.get("cells") or {}
                    row_out = ([r.get("key") or ""] if include_key else []) + [cells.get(c, "") for c in columns]
                    writer.writerow(row_out)
                    yield buf.getvalue()
                    buf.seek(0)
                    buf.truncate(0)
                if not has_more or not rows_page:
                    break
                offset += page

        safe_name = "".join(ch for ch in (corpus.get("name") or f"corpus_{corpus_id}") if ch.isalnum() or ch in {"-", "_"}).strip("_-")
        filename = f"{safe_name or 'corpus'}_{corpus_id}.csv"

        return Response(
            iter_lines(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
        )

    @app.put("/api/corpora/<int:corpus_id>")
    def api_update_corpus(corpus_id: int):
        data = request.get_json(force=True, silent=True) or {}
        corpus = update_corpus(
            app.config["DB_PATH"],
            corpus_id=corpus_id,
            name=data.get("name"),
            description=data.get("description"),
            columns=data.get("columns"),
        )
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404
        return jsonify({"success": True, "corpus": corpus})

    @app.delete("/api/corpora/<int:corpus_id>")
    def api_delete_corpus(corpus_id: int):
        ok = delete_corpus(app.config["DB_PATH"], corpus_id)
        if not ok:
            return jsonify({"success": False, "error": "corpus not found"}), 404
        return jsonify({"success": True})

    # ---- Rows ----
    @app.get("/api/corpora/<int:corpus_id>/rows")
    def api_list_rows(corpus_id: int):
        q = (request.args.get("q") or "").strip()
        limit = int(request.args.get("limit") or 200)
        offset = int(request.args.get("offset") or 0)
        sort_by = (request.args.get("sort_by") or "id").strip()
        sort_dir = (request.args.get("sort_dir") or "asc").strip()

        corpus = get_corpus(app.config["DB_PATH"], corpus_id)
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404

        rows = list_rows_in_corpus_table(
            app.config["DB_PATH"],
            corpus=corpus,
            q=q,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )
        rows_list, has_more = rows
        return jsonify({"success": True, "corpus": corpus, "rows": rows_list, "offset": offset, "limit": limit, "has_more": has_more})

    @app.post("/api/corpora/<int:corpus_id>/rows")
    def api_create_row(corpus_id: int):
        data = request.get_json(force=True, silent=True) or {}
        key = (data.get("key") or "").strip()
        cells = data.get("cells") or {}

        corpus = get_corpus(app.config["DB_PATH"], corpus_id)
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404

        if not isinstance(cells, dict):
            return jsonify({"success": False, "error": "cells must be an object"}), 400

        row = create_row_in_corpus_table(app.config["DB_PATH"], corpus=corpus, key=key or None, cells=cells)
        return jsonify({"success": True, "row": row})

    @app.put("/api/corpora/<int:corpus_id>/rows/<int:row_id>")
    def api_update_row(corpus_id: int, row_id: int):
        data = request.get_json(force=True, silent=True) or {}
        corpus = get_corpus(app.config["DB_PATH"], corpus_id)
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404
        row = update_row_in_corpus_table(app.config["DB_PATH"], corpus=corpus, row_id=row_id, key=data.get("key"), cells=data.get("cells"))
        if not row:
            return jsonify({"success": False, "error": "row not found"}), 404
        return jsonify({"success": True, "row": row})

    @app.delete("/api/corpora/<int:corpus_id>/rows/<int:row_id>")
    def api_delete_row(corpus_id: int, row_id: int):
        corpus = get_corpus(app.config["DB_PATH"], corpus_id)
        if not corpus:
            return jsonify({"success": False, "error": "corpus not found"}), 404
        ok = delete_row_in_corpus_table(app.config["DB_PATH"], corpus=corpus, row_id=row_id)
        if not ok:
            return jsonify({"success": False, "error": "row not found"}), 404
        return jsonify({"success": True})

    return app


if __name__ == "__main__":
    app = create_app()
    # Default to a safe, non-debug run mode (works in restricted environments).
    debug = os.environ.get("FLASK_DEBUG", "").strip() in {"1", "true", "True"}
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=debug,
        use_reloader=False,
    )

