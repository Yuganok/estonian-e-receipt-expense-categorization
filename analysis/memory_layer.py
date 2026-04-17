"""
SQLite-backed memory layer for manual category overrides.

Priority contract:
    manual memory override > rule-based classifier
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "output" / "manual_category_memory.db"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _normalize_store(value: str) -> str:
    return _normalize_text(value)


def _normalize_item_text(value: str) -> str:
    return _normalize_text(value)


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    if db_path is None or not str(db_path).strip():
        return DEFAULT_DB_PATH
    return Path(db_path).expanduser().resolve()


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path | None = None) -> Path:
    resolved = resolve_db_path(db_path)
    with _connect(resolved) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_category_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_norm TEXT NOT NULL,
                item_text_norm TEXT NOT NULL,
                manual_category TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_store TEXT NOT NULL DEFAULT '',
                last_item_text TEXT NOT NULL DEFAULT '',
                UNIQUE(store_norm, item_text_norm)
            )
            """
        )
        conn.commit()
    return resolved


def lookup_manual_category(store: str, item_text: str, db_path: str | Path | None = None) -> str | None:
    store_norm = _normalize_store(store)
    item_norm = _normalize_item_text(item_text)
    if not store_norm or not item_norm:
        return None

    resolved = init_db(db_path)
    with _connect(resolved) as conn:
        row = conn.execute(
            """
            SELECT manual_category
            FROM manual_category_overrides
            WHERE item_text_norm = ? AND store_norm IN (?, '*')
            ORDER BY CASE WHEN store_norm = ? THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (item_norm, store_norm, store_norm),
        ).fetchone()
    if not row:
        return None
    return str(row["manual_category"] or "").strip() or None


def upsert_manual_category(
    store: str,
    item_text: str,
    manual_category: str,
    db_path: str | Path | None = None,
    note: str = "",
    updated_at: str | None = None,
) -> bool:
    store_norm = _normalize_store(store)
    item_norm = _normalize_item_text(item_text)
    category = str(manual_category or "").strip()
    if not store_norm or not item_norm or not category:
        return False

    resolved = init_db(db_path)
    timestamp = str(updated_at or _utc_now_iso())
    with _connect(resolved) as conn:
        conn.execute(
            """
            INSERT INTO manual_category_overrides
                (store_norm, item_text_norm, manual_category, note, updated_at, created_at, last_store, last_item_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(store_norm, item_text_norm) DO UPDATE SET
                manual_category = excluded.manual_category,
                note = excluded.note,
                updated_at = excluded.updated_at,
                last_store = excluded.last_store,
                last_item_text = excluded.last_item_text
            """,
            (
                store_norm,
                item_norm,
                category,
                str(note or "").strip(),
                timestamp,
                timestamp,
                str(store or "").strip(),
                str(item_text or "").strip(),
            ),
        )
        conn.commit()
    return True


def delete_manual_category(store: str, item_text: str, db_path: str | Path | None = None) -> bool:
    store_norm = _normalize_store(store)
    item_norm = _normalize_item_text(item_text)
    if not store_norm or not item_norm:
        return False
    resolved = init_db(db_path)
    with _connect(resolved) as conn:
        cur = conn.execute(
            """
            DELETE FROM manual_category_overrides
            WHERE store_norm = ? AND item_text_norm = ?
            """,
            (store_norm, item_norm),
        )
        conn.commit()
    return cur.rowcount > 0


def _load_categorized_rows(categorized_csv: str | Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with open(categorized_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append({k: str(v or "") for k, v in row.items()})
    return rows


def _load_corrections_json(corrections_json_path: str | Path) -> list[dict[str, str]]:
    with open(corrections_json_path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        out.append({k: str(v or "") for k, v in item.items()})
    return out


def _session_row_key(row: dict[str, str]) -> str:
    return f"{str(row.get('receipt_id', '')).strip()}__{str(row.get('item_text', '')).strip()}"


def _norm_key_for_row(row: dict[str, str]) -> tuple[str, str] | None:
    store_norm = _normalize_store(row.get("store", ""))
    item_norm = _normalize_item_text(row.get("item_text", ""))
    if not store_norm or not item_norm:
        return None
    return store_norm, item_norm


def sync_session_corrections(
    categorized_csv: str | Path,
    corrections_json_path: str | Path,
    db_path: str | Path | None = None,
) -> dict[str, int]:
    """
    Sync UI corrections with SQLite memory for current session rows.

    Logic:
      - upsert corrections currently present in payload
      - if row had existing DB override but now absent in payload -> delete override
    """
    resolved = init_db(db_path)
    rows = _load_categorized_rows(categorized_csv)
    corrections = _load_corrections_json(corrections_json_path)

    key_to_row: dict[str, dict[str, str]] = {}
    for row in rows:
        k = _session_row_key(row)
        if k not in key_to_row:
            key_to_row[k] = row

    desired_overrides: dict[tuple[str, str], dict[str, str]] = {}
    for corr in corrections:
        manual_category = str(corr.get("manual_category", "")).strip()
        if not manual_category:
            continue
        row = key_to_row.get(_session_row_key(corr))
        if not row:
            continue
        norm_key = _norm_key_for_row(row)
        if not norm_key:
            continue
        desired_overrides[norm_key] = {
            "manual_category": manual_category,
            "note": str(corr.get("note", "")).strip(),
            "updated_at": str(corr.get("updated_at", "")).strip() or _utc_now_iso(),
            "store": str(row.get("store", "")).strip(),
            "item_text": str(row.get("item_text", "")).strip(),
        }

    session_norm_keys = {
        key
        for key in (_norm_key_for_row(row) for row in rows)
        if key is not None
    }

    existing_for_session: set[tuple[str, str]] = set()
    if session_norm_keys:
        with _connect(resolved) as conn:
            for store_norm, item_norm in session_norm_keys:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM manual_category_overrides
                    WHERE store_norm = ? AND item_text_norm = ?
                    """,
                    (store_norm, item_norm),
                ).fetchone()
                if row:
                    existing_for_session.add((store_norm, item_norm))

    upserted = 0
    deleted = 0
    for (store_norm, item_norm), payload in desired_overrides.items():
        ok = upsert_manual_category(
            store=payload["store"],
            item_text=payload["item_text"],
            manual_category=payload["manual_category"],
            db_path=resolved,
            note=payload["note"],
            updated_at=payload["updated_at"],
        )
        if ok:
            upserted += 1

    for store_norm, item_norm in (existing_for_session - set(desired_overrides.keys())):
        with _connect(resolved) as conn:
            cur = conn.execute(
                """
                DELETE FROM manual_category_overrides
                WHERE store_norm = ? AND item_text_norm = ?
                """,
                (store_norm, item_norm),
            )
            conn.commit()
        if cur.rowcount > 0:
            deleted += 1

    return {"upserted": upserted, "deleted": deleted}


def get_session_corrections(
    categorized_csv: str | Path,
    db_path: str | Path | None = None,
) -> list[dict[str, str]]:
    """
    Return correction rows compatible with existing UI schema:
        receipt_id, item_text, manual_category, note, updated_at
    """
    resolved = init_db(db_path)
    rows = _load_categorized_rows(categorized_csv)
    out: list[dict[str, str]] = []

    with _connect(resolved) as conn:
        for row in rows:
            store_norm = _normalize_store(row.get("store", ""))
            item_norm = _normalize_item_text(row.get("item_text", ""))
            if not store_norm or not item_norm:
                continue
            rec = conn.execute(
                """
                SELECT manual_category, note, updated_at
                FROM manual_category_overrides
                WHERE item_text_norm = ? AND store_norm IN (?, '*')
                ORDER BY CASE WHEN store_norm = ? THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (item_norm, store_norm, store_norm),
            ).fetchone()
            if not rec:
                continue
            out.append(
                {
                    "receipt_id": str(row.get("receipt_id", "")),
                    "item_text": str(row.get("item_text", "")),
                    "manual_category": str(rec["manual_category"] or ""),
                    "note": str(rec["note"] or ""),
                    "updated_at": str(rec["updated_at"] or ""),
                }
            )
    return out


def import_legacy_csv(corrections_csv: str | Path, db_path: str | Path | None = None) -> int:
    """
    One-time migration helper for old research/manual_corrections.csv.

    Expected headers:
      receipt_id, item_text, manual_category, note, updated_at
    Since legacy CSV has no store, imported rows are saved under store='*'.
    """
    csv_path = Path(corrections_csv)
    if not csv_path.exists():
        return 0
    imported = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            item_text = str(row.get("item_text", "")).strip()
            manual_category = str(row.get("manual_category", "")).strip()
            if not item_text or not manual_category:
                continue
            ok = upsert_manual_category(
                store="*",
                item_text=item_text,
                manual_category=manual_category,
                db_path=db_path,
                note=str(row.get("note", "")).strip(),
                updated_at=str(row.get("updated_at", "")).strip() or _utc_now_iso(),
            )
            if ok:
                imported += 1
    return imported


def _write_json(path: str | Path, payload: object) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual category SQLite memory layer")
    parser.add_argument("--db", default="", help="Path to SQLite DB (default: output/manual_category_memory.db)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get-session-corrections", help="Read DB overrides for session rows")
    p_get.add_argument("--categorized-csv", required=True)
    p_get.add_argument("--out-json", default="-", help="Output JSON path, or '-' for stdout")

    p_sync = sub.add_parser("sync-session-corrections", help="Sync session corrections to DB")
    p_sync.add_argument("--categorized-csv", required=True)
    p_sync.add_argument("--corrections-json", required=True)

    p_import = sub.add_parser("import-legacy-csv", help="Import old manual_corrections.csv")
    p_import.add_argument("--csv", required=True, help="Legacy CSV path")

    p_lookup = sub.add_parser("lookup", help="Debug lookup for one row")
    p_lookup.add_argument("--store", required=True)
    p_lookup.add_argument("--item-text", required=True)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    db_path = resolve_db_path(args.db)
    init_db(db_path)

    if args.cmd == "get-session-corrections":
        data = get_session_corrections(args.categorized_csv, db_path=db_path)
        if str(args.out_json).strip() == "-":
            print(json.dumps(data, ensure_ascii=False))
        else:
            _write_json(args.out_json, data)
            print(json.dumps({"ok": True, "count": len(data), "out_json": str(args.out_json)}, ensure_ascii=False))
        return

    if args.cmd == "sync-session-corrections":
        stats = sync_session_corrections(
            categorized_csv=args.categorized_csv,
            corrections_json_path=args.corrections_json,
            db_path=db_path,
        )
        print(json.dumps({"ok": True, **stats}, ensure_ascii=False))
        return

    if args.cmd == "import-legacy-csv":
        imported = import_legacy_csv(args.csv, db_path=db_path)
        print(json.dumps({"ok": True, "imported": imported}, ensure_ascii=False))
        return

    if args.cmd == "lookup":
        category = lookup_manual_category(args.store, args.item_text, db_path=db_path)
        print(json.dumps({"ok": True, "category": category or ""}, ensure_ascii=False))
        return

    raise SystemExit(1)


if __name__ == "__main__":
    main()
