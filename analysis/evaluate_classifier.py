"""
Evaluate rule-based classifier quality on a manually labeled gold CSV.

Main metrics (6 spending categories): rows with true_category in MAIN_EVAL_CATEGORIES
and not deposit lines. DEPOSIT is reported separately when such rows exist.

Outputs:
  - evaluation_report.txt
  - evaluation_metrics.json
  - confusion_matrix.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_recall_fscore_support,
)

from categorize import MAIN_EVAL_CATEGORIES, classify


def as_bool(value: str | bool | int | None) -> bool:
    t = str(value or "").strip().lower()
    return t in {"1", "true", "yes", "y"}


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_confusion_csv(path: Path, labels: list[str], matrix: list[list[int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["true\\pred"] + labels)
        for idx, row in enumerate(matrix):
            writer.writerow([labels[idx], *row])


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def write_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def per_class_metrics(
    y_true: list[str], y_pred: list[str], labels: list[str]
) -> dict[str, dict[str, float]]:
    p, r, f1, support = precision_recall_fscore_support(
        y_true, y_pred, labels=labels, average=None, zero_division=0
    )
    out: dict[str, dict[str, float]] = {}
    for i, lab in enumerate(labels):
        out[lab] = {
            "precision": float(p[i]),
            "recall": float(r[i]),
            "f1": float(f1[i]),
            "support": int(support[i]),
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate classifier on manually labeled gold CSV.")
    parser.add_argument("--gold", required=True, help="Path to gold CSV with true labels")
    parser.add_argument(
        "--out",
        default="../output/evaluation",
        help="Output directory (default: ../output/evaluation)",
    )
    args = parser.parse_args()

    gold_path = Path(args.gold)
    out_dir = Path(args.out)
    if not gold_path.exists():
        raise FileNotFoundError(f"gold CSV not found: {gold_path}")

    eval_labels = list(MAIN_EVAL_CATEGORIES)
    rows = read_csv_rows(gold_path)

    y_true_main: list[str] = []
    y_pred_main: list[str] = []
    y_true_dep: list[str] = []
    y_pred_dep: list[str] = []

    skipped_no_label = 0
    skipped_no_text = 0
    skipped_unknown_label = 0

    for row in rows:
        item_text = str(row.get("item_text") or "").strip()
        true_category = str(row.get("true_category") or "").strip()
        if not item_text:
            skipped_no_text += 1
            continue
        if not true_category:
            skipped_no_label += 1
            continue

        is_dep = as_bool(row.get("is_deposit"))
        pred_category, _ = classify(item_text, is_dep)

        if is_dep or true_category == "DEPOSIT":
            y_true_dep.append("DEPOSIT")
            y_pred_dep.append(pred_category)
            continue

        if true_category not in eval_labels:
            skipped_unknown_label += 1
            continue

        y_true_main.append(true_category)
        y_pred_main.append(pred_category)

    if not y_true_main and not y_true_dep:
        raise RuntimeError("No valid labeled rows found. Check item_text/true_category columns.")

    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "evaluation_metrics.json"
    report_path = out_dir / "evaluation_report.txt"
    confusion_path = out_dir / "confusion_matrix.csv"
    timestamp = datetime.now(timezone.utc).isoformat()

    metrics: dict = {
        "generated_at_utc": timestamp,
        "gold_csv": str(gold_path.resolve()),
        "rows_total": len(rows),
        "rows_skipped_empty_item_text": skipped_no_text,
        "rows_skipped_empty_true_category": skipped_no_label,
        "rows_skipped_true_category_not_in_main_eval": skipped_unknown_label,
        "main_eval": {},
        "deposit_eval": None,
    }

    report_lines = [
        "Classifier evaluation report",
        "===========================",
        f"Generated (UTC): {timestamp}",
        f"Gold CSV: {gold_path.resolve()}",
        "",
        f"Rows total: {len(rows)}",
        f"Skipped (empty item_text): {skipped_no_text}",
        f"Skipped (empty true_category): {skipped_no_label}",
        f"Skipped (true_category not in 6-class scheme, non-deposit): {skipped_unknown_label}",
        "",
    ]

    if y_true_main:
        accuracy = float(accuracy_score(y_true_main, y_pred_main))
        macro_f1 = float(
            f1_score(y_true_main, y_pred_main, labels=eval_labels, average="macro", zero_division=0)
        )
        cm = confusion_matrix(y_true_main, y_pred_main, labels=eval_labels)
        cm_list = cm.tolist()
        per_class = per_class_metrics(y_true_main, y_pred_main, eval_labels)

        metrics["main_eval"] = {
            "rows_used": len(y_true_main),
            "labels": eval_labels,
            "accuracy": accuracy,
            "macro_f1": macro_f1,
            "per_class": per_class,
        }
        write_confusion_csv(confusion_path, eval_labels, cm_list)

        report_lines += [
            "--- Main evaluation (6 categories, deposit rows excluded) ---",
            f"Rows used: {len(y_true_main)}",
            f"Accuracy: {accuracy:.6f}",
            f"Macro-F1: {macro_f1:.6f}",
            "",
            "Per-class (precision / recall / F1 / support):",
        ]
        for lab in eval_labels:
            pc = per_class[lab]
            report_lines.append(
                f"  {lab}: P={pc['precision']:.4f} R={pc['recall']:.4f} "
                f"F1={pc['f1']:.4f} n={pc['support']}"
            )
        report_lines += ["", f"Confusion matrix CSV: {confusion_path.resolve()}"]
    else:
        report_lines.append("No rows for main (6-class) evaluation.")
        metrics["main_eval"] = {"rows_used": 0, "note": "no non-deposit labeled rows in scheme"}

    if y_true_dep:
        dep_pred_bin = ["DEPOSIT" if p == "DEPOSIT" else "NOT_DEPOSIT" for p in y_pred_dep]
        dep_correct = sum(1 for t, p in zip(y_true_dep, dep_pred_bin, strict=True) if t == p)
        dep_acc = dep_correct / len(y_true_dep)
        metrics["deposit_eval"] = {
            "rows_used": len(y_true_dep),
            "accuracy_deposit_vs_rest": dep_acc,
            "correct": dep_correct,
        }
        report_lines += [
            "",
            "--- Deposit lines (separate) ---",
            f"Rows used: {len(y_true_dep)}",
            f"Accuracy (DEPOSIT vs non-DEPOSIT prediction): {dep_acc:.6f}",
            f"Correct: {dep_correct} / {len(y_true_dep)}",
        ]
    else:
        report_lines.append("")
        report_lines.append("No deposit rows in gold file.")

    metrics["artifacts"] = {
        "evaluation_metrics_json": str(metrics_path.resolve()),
        "evaluation_report_txt": str(report_path.resolve()),
        "confusion_matrix_csv": str(confusion_path.resolve()) if y_true_main else None,
    }

    write_json(metrics_path, metrics)
    write_report(report_path, report_lines)

    print(f"Evaluation complete: {out_dir.resolve()}")
    if y_true_main:
        print(f"  Main eval rows: {len(y_true_main)}")
        print(f"  Accuracy: {metrics['main_eval']['accuracy']:.6f}")
        print(f"  Macro-F1: {metrics['main_eval']['macro_f1']:.6f}")
    if y_true_dep:
        print(f"  Deposit rows: {len(y_true_dep)} (see report for deposit accuracy)")


if __name__ == "__main__":
    main()
