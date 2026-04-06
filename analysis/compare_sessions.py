"""
compare_sessions.py
===================
Builds a standardized before/after comparison package for two session outputs.

Usage:
    python compare_sessions.py --old ../output/sessions/<old_id> --new ../output/sessions/<new_id>
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path


NUMERIC_SUMMARY_RE = re.compile(r"^\s*\d+\s+[\d.,]+\s*€\s+[\d.,]+\s*€\s*$")
QTY_ONLY_RE = re.compile(r"^\s*\d+(?:[.,]\d+)?\s*(?:tk|kg|g|l|ml)\s*$", re.IGNORECASE)
KNOWN_PATTERNS = [
    "Võiroos, 65g",
    "Küpsis pähkli kreemitäidisega",
    "Biskviidikook Iiri kreemiga",
    "Batoonike Kaseke KALEV 150g",
    "1 tk",
]


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _safe_float(value: str) -> float:
    try:
        return float(str(value).strip().replace(",", "."))
    except Exception:
        return 0.0


def _profile_items(session_dir: Path) -> dict:
    items_path = session_dir / "items_categorized.csv"
    if not items_path.exists():
        return {
            "rows_total": 0,
            "category_counts": {},
            "category_source_counts": {},
            "numeric_summary_rows": 0,
            "qty_only_rows": 0,
            "known_patterns": {k: 0 for k in KNOWN_PATTERNS},
        }

    rows = _read_csv(items_path)
    counts = Counter()
    source_counts = Counter()
    numeric_summary_rows = 0
    qty_only_rows = 0
    pattern_counts = {k: 0 for k in KNOWN_PATTERNS}

    for row in rows:
        category = (row.get("category") or "").strip() or "UNKNOWN"
        counts[category] += 1
        source = (row.get("category_source") or "").strip() or "unknown"
        source_counts[source] += 1
        text = (row.get("item_text") or row.get("name") or "").strip()
        if NUMERIC_SUMMARY_RE.match(text):
            numeric_summary_rows += 1
        if QTY_ONLY_RE.match(text):
            qty_only_rows += 1
        for pat in KNOWN_PATTERNS:
            if text == pat:
                pattern_counts[pat] += 1

    return {
        "rows_total": len(rows),
        "category_counts": dict(counts),
        "category_source_counts": dict(source_counts),
        "numeric_summary_rows": numeric_summary_rows,
        "qty_only_rows": qty_only_rows,
        "known_patterns": pattern_counts,
    }


def _read_quick_metrics(session_dir: Path) -> dict:
    metrics_path = session_dir / "research" / "metrics_summary.json"
    if not metrics_path.exists():
        return {
            "sample_size": 0,
            "accuracy": 0.0,
            "macro_f1": 0.0,
            "spend_error_pct": 0.0,
            "per_category": [],
        }
    with metrics_path.open(encoding="utf-8") as f:
        data = json.load(f)
    return {
        "sample_size": int(data.get("sample_size", 0) or 0),
        "accuracy": float(data.get("accuracy", 0.0) or 0.0),
        "macro_f1": float(data.get("macro_f1", 0.0) or 0.0),
        "spend_error_pct": float(data.get("spend_error_pct", 0.0) or 0.0),
        "per_category": data.get("per_category", []),
    }


def _format_pct(value: float) -> str:
    return f"{value:.2f}%"


def _build_report_text(payload: dict) -> str:
    old_profile = payload["old_profile"]
    new_profile = payload["new_profile"]
    old_eval = payload["old_quick_eval"]
    new_eval = payload["new_quick_eval"]
    deltas = payload["deltas"]

    lines = [
        "BEFORE vs AFTER (STANDARDIZED QUICK CHECK)",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"Old session: {payload['old_session']}",
        f"New session: {payload['new_session']}",
        "",
        "Data cleanliness:",
        f"- Rows total: {old_profile['rows_total']} -> {new_profile['rows_total']} (delta {deltas['rows_total']:+d})",
        "- Numeric summary rows: "
        f"{old_profile['numeric_summary_rows']} -> {new_profile['numeric_summary_rows']} "
        f"(delta {deltas['numeric_summary_rows']:+d})",
        f"- Quantity-only rows: {old_profile['qty_only_rows']} -> {new_profile['qty_only_rows']} "
        f"(delta {deltas['qty_only_rows']:+d})",
        "",
        "Quick quality snapshot (internal pilot eval):",
        f"- Sample size: {old_eval['sample_size']} -> {new_eval['sample_size']}",
        f"- Accuracy: {_format_pct(old_eval['accuracy'] * 100)} -> {_format_pct(new_eval['accuracy'] * 100)} "
        f"(delta {deltas['accuracy_pp']:+.2f} pp)",
        f"- Macro F1: {old_eval['macro_f1']:.3f} -> {new_eval['macro_f1']:.3f} "
        f"(delta {deltas['macro_f1_delta']:+.3f})",
        f"- Spend error: {_format_pct(old_eval['spend_error_pct'])} -> {_format_pct(new_eval['spend_error_pct'])} "
        f"(delta {deltas['spend_error_pp']:+.2f} pp)",
        "",
        "Known failure patterns count:",
    ]
    for pat in KNOWN_PATTERNS:
        old_v = int(old_profile["known_patterns"].get(pat, 0))
        new_v = int(new_profile["known_patterns"].get(pat, 0))
        lines.append(f"- {pat}: {old_v} -> {new_v} (delta {new_v - old_v:+d})")
    lines.extend(
        [
            "",
            "Category source distribution:",
        ]
    )
    source_keys = ["deposit", "rule_match", "fallback_food", "unknown"]
    for source in source_keys:
        old_v = int(old_profile.get("category_source_counts", {}).get(source, 0))
        new_v = int(new_profile.get("category_source_counts", {}).get(source, 0))
        lines.append(f"- {source}: {old_v} -> {new_v} (delta {new_v - old_v:+d})")
    lines.extend(
        [
            "",
            "Interpretation:",
            "- quick metrics are internal pilot indicators and must be reported as such in thesis text.",
            "- for strict scientific evaluation, use reviewed gold labels and full confusion analysis.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_before_after(old_session: Path, new_session: Path) -> dict:
    old_profile = _profile_items(old_session)
    new_profile = _profile_items(new_session)
    old_eval = _read_quick_metrics(old_session)
    new_eval = _read_quick_metrics(new_session)

    deltas = {
        "rows_total": int(new_profile["rows_total"] - old_profile["rows_total"]),
        "numeric_summary_rows": int(new_profile["numeric_summary_rows"] - old_profile["numeric_summary_rows"]),
        "qty_only_rows": int(new_profile["qty_only_rows"] - old_profile["qty_only_rows"]),
        "accuracy_pp": round((new_eval["accuracy"] - old_eval["accuracy"]) * 100.0, 4),
        "macro_f1_delta": round(new_eval["macro_f1"] - old_eval["macro_f1"], 6),
        "spend_error_pp": round(new_eval["spend_error_pct"] - old_eval["spend_error_pct"], 4),
    }

    return {
        "protocol": {
            "name": "internal_pilot_quick_eval",
            "source_metrics_file": "research/metrics_summary.json",
            "notes": (
                "quick metrics are internal pilot indicators (kiire hindamine / sisemine kontroll / pilootmõõdik), "
                "not a final strict gold-standard benchmark"
            ),
        },
        "old_session": str(old_session.resolve()),
        "new_session": str(new_session.resolve()),
        "old_profile": old_profile,
        "new_profile": new_profile,
        "old_quick_eval": old_eval,
        "new_quick_eval": new_eval,
        "deltas": deltas,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build standardized before/after quick comparison between sessions")
    parser.add_argument("--old", required=True, help="Path to old session folder")
    parser.add_argument("--new", required=True, help="Path to new session folder")
    parser.add_argument(
        "--out-dir",
        default="",
        help="Optional output directory (default: <new_session>/research)",
    )
    args = parser.parse_args()

    old_session = Path(args.old).resolve()
    new_session = Path(args.new).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else new_session / "research"
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = build_before_after(old_session, new_session)
    metrics_path = out_dir / "before_after_quick_metrics.json"
    report_path = out_dir / "before_after_quick_report.txt"

    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")

    report_text = _build_report_text(payload)
    with report_path.open("w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"[ok] Metrics JSON -> {metrics_path}")
    print(f"[ok] Report TXT  -> {report_path}")


if __name__ == "__main__":
    main()
