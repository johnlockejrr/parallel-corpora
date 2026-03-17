from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = 2


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def _reset_db_file(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        return
    # Remove DB + WAL/SHM (user ok with discarding test data).
    for path in (p, Path(str(p) + "-wal"), Path(str(p) + "-shm")):
        try:
            path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            # If locked by a running server, user should stop it and retry.
            pass


def init_db(db_path: str) -> None:
    # Hard reset when schema version doesn't match.
    if Path(db_path).exists():
        try:
            with _connect(db_path) as conn:
                r = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='meta';"
                ).fetchone()
                if not r:
                    raise RuntimeError("no meta table")
                v = conn.execute("SELECT value FROM meta WHERE key='schema_version';").fetchone()
                if not v or int(v["value"]) != SCHEMA_VERSION:
                    raise RuntimeError("schema mismatch")
        except Exception:
            _reset_db_file(db_path)

    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS corpora (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              description TEXT NOT NULL DEFAULT '',
              columns_json TEXT NOT NULL,
              table_name TEXT NOT NULL UNIQUE,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )


def _json_load(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def _safe_table_name(name: str) -> str:
    return "".join(ch for ch in name if ch.isalnum() or ch == "_")


def _new_corpus_table_name(corpus_id: int) -> str:
    token = secrets.token_hex(4)
    return _safe_table_name(f"corpus_{corpus_id}_{token}")


def _corpus_row_to_dict(r: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": r["id"],
        "name": r["name"],
        "description": r["description"],
        "columns": _json_load(r["columns_json"]) or [],
        "table_name": r["table_name"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def _row_to_dict(r: sqlite3.Row, corpus_id: int) -> dict[str, Any]:
    return {
        "id": r["id"],
        "corpus_id": corpus_id,
        "key": r["key"],
        "cells": _json_load(r["cells_json"]) or {},
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def create_corpus_table(db_path: str, table_name: str) -> None:
    tn = _safe_table_name(table_name)
    with _connect(db_path) as conn:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {tn} (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              key TEXT,
              cells_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_{tn}_updated ON {tn}(updated_at DESC, id DESC);
            CREATE INDEX IF NOT EXISTS idx_{tn}_key ON {tn}(key);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{tn}_key_present
              ON {tn}(key)
              WHERE key IS NOT NULL AND key != '';
            """
        )


def drop_corpus_table(db_path: str, table_name: str) -> None:
    tn = _safe_table_name(table_name)
    with _connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {tn}")


def list_corpora(db_path: str) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM corpora ORDER BY updated_at DESC, id DESC").fetchall()
        return [_corpus_row_to_dict(r) for r in rows]


def get_corpus(db_path: str, corpus_id: int) -> Optional[dict[str, Any]]:
    with _connect(db_path) as conn:
        r = conn.execute("SELECT * FROM corpora WHERE id = ?", (corpus_id,)).fetchone()
        return _corpus_row_to_dict(r) if r else None


def create_corpus(db_path: str, name: str, columns: list[str], description: str = "") -> dict[str, Any]:
    now = _utc_now_iso()
    cols = [c for c in (columns or []) if str(c).strip()]
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO corpora(name, description, columns_json, table_name, created_at, updated_at) VALUES(?,?,?,?,?,?)",
            (name, description or "", json.dumps(cols, ensure_ascii=False), "__pending__", now, now),
        )
        corpus_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        table_name = _new_corpus_table_name(corpus_id)
        conn.execute("UPDATE corpora SET table_name=? WHERE id=?", (table_name, corpus_id))
    create_corpus_table(db_path, table_name)
    return get_corpus(db_path, corpus_id)  # type: ignore[return-value]


def update_corpus(
    db_path: str,
    corpus_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
    columns: Optional[list[str]] = None,
) -> Optional[dict[str, Any]]:
    existing = get_corpus(db_path, corpus_id)
    if not existing:
        return None

    new_name = existing["name"] if name is None else str(name)
    new_desc = existing["description"] if description is None else str(description)
    new_cols = existing["columns"] if columns is None else [str(c).strip() for c in columns if str(c).strip()]
    now = _utc_now_iso()

    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE corpora SET name=?, description=?, columns_json=?, updated_at=? WHERE id=?",
            (new_name, new_desc, json.dumps(new_cols, ensure_ascii=False), now, corpus_id),
        )
    return get_corpus(db_path, corpus_id)


def delete_corpus(db_path: str, corpus_id: int) -> bool:
    corpus = get_corpus(db_path, corpus_id)
    if not corpus:
        return False
    drop_corpus_table(db_path, corpus["table_name"])
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM corpora WHERE id=?", (corpus_id,))
        return cur.rowcount > 0


def list_rows_in_corpus_table(
    db_path: str,
    corpus: dict[str, Any],
    q: str = "",
    sort_by: str = "id",
    sort_dir: str = "asc",
    limit: int = 200,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], bool]:
    tn = _safe_table_name(corpus["table_name"])
    corpus_id = int(corpus["id"])
    q = (q or "").strip()
    limit = max(1, min(int(limit), 2000))
    offset = max(0, int(offset))

    allowed_sort = {"id", "key", "created_at", "updated_at"}
    sort_by = sort_by if sort_by in allowed_sort else "id"
    sort_dir = "desc" if str(sort_dir).lower() == "desc" else "asc"

    where = ""
    params: list[Any] = []
    if q:
        where = "WHERE key LIKE ? ESCAPE '\\' OR cells_json LIKE ? ESCAPE '\\'"
        like = f"%{q}%"
        params.extend([like, like])

    # Fetch one extra row to know if there is a next page.
    sql = f"""
      SELECT * FROM {tn}
      {where}
      ORDER BY {sort_by} {sort_dir}, id {sort_dir}
      LIMIT ? OFFSET ?
    """
    params.extend([limit + 1, offset])

    with _connect(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        return ([_row_to_dict(r, corpus_id=corpus_id) for r in rows], has_more)


def create_row_in_corpus_table(
    db_path: str,
    corpus: dict[str, Any],
    key: str | None,
    cells: dict[str, Any],
) -> dict[str, Any]:
    tn = _safe_table_name(corpus["table_name"])
    corpus_id = int(corpus["id"])
    now = _utc_now_iso()
    key_norm = (key or "").strip()
    key_value = key_norm if key_norm else None
    with _connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {tn}(key, cells_json, created_at, updated_at) VALUES(?,?,?,?)",
            (key_value, json.dumps(cells or {}, ensure_ascii=False), now, now),
        )
        row_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        r = conn.execute(f"SELECT * FROM {tn} WHERE id=?", (row_id,)).fetchone()
        return _row_to_dict(r, corpus_id=corpus_id)  # type: ignore[arg-type]


def get_row_in_corpus_table(db_path: str, corpus: dict[str, Any], row_id: int) -> Optional[dict[str, Any]]:
    tn = _safe_table_name(corpus["table_name"])
    corpus_id = int(corpus["id"])
    with _connect(db_path) as conn:
        r = conn.execute(f"SELECT * FROM {tn} WHERE id=?", (row_id,)).fetchone()
        return _row_to_dict(r, corpus_id=corpus_id) if r else None


def update_row_in_corpus_table(
    db_path: str,
    corpus: dict[str, Any],
    row_id: int,
    key: Optional[str] = None,
    cells: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    existing = get_row_in_corpus_table(db_path, corpus, row_id)
    if not existing:
        return None

    tn = _safe_table_name(corpus["table_name"])
    corpus_id = int(corpus["id"])

    if key is None:
        new_key = existing["key"]
    else:
        key_norm = str(key).strip()
        new_key = key_norm if key_norm else None
    new_cells = existing["cells"] if cells is None else cells
    now = _utc_now_iso()

    with _connect(db_path) as conn:
        conn.execute(
            f"UPDATE {tn} SET key=?, cells_json=?, updated_at=? WHERE id=?",
            (new_key, json.dumps(new_cells or {}, ensure_ascii=False), now, row_id),
        )
    return get_row_in_corpus_table(db_path, corpus, row_id)


def delete_row_in_corpus_table(db_path: str, corpus: dict[str, Any], row_id: int) -> bool:
    tn = _safe_table_name(corpus["table_name"])
    with _connect(db_path) as conn:
        cur = conn.execute(f"DELETE FROM {tn} WHERE id=?", (row_id,))
        return cur.rowcount > 0


def get_row_by_key_in_corpus_table(db_path: str, corpus: dict[str, Any], key: str) -> Optional[dict[str, Any]]:
    tn = _safe_table_name(corpus["table_name"])
    corpus_id = int(corpus["id"])
    key_norm = str(key).strip()
    if not key_norm:
        return None
    with _connect(db_path) as conn:
        r = conn.execute(f"SELECT * FROM {tn} WHERE key=?", (key_norm,)).fetchone()
        return _row_to_dict(r, corpus_id=corpus_id) if r else None


def upsert_row_by_key_in_corpus_table(
    db_path: str,
    corpus: dict[str, Any],
    key: str,
    cells: dict[str, Any],
) -> dict[str, Any]:
    existing = get_row_by_key_in_corpus_table(db_path, corpus, key)
    if not existing:
        return create_row_in_corpus_table(db_path, corpus, key=key, cells=cells)
    return update_row_in_corpus_table(db_path, corpus, row_id=int(existing["id"]), key=key, cells=cells)  # type: ignore[return-value]

