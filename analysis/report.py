"""
report.py
=========
Generates the pilot study summary statistics used in the thesis.

Computes:
    - Total receipts and matched receipts
    - Total paid, deposits excluded, actual consumption spend
    - Mixed receipts (≥ 2 categories in one receipt)
    - Non-baseline rows and their share of total spend
    - Category breakdown table (net prices)

All figures match the pilot study results reported in Chapter 3.
"""

import csv
from collections import defaultdict
from pathlib import Path


BASELINE_CATEGORY = "Toidupoed"
DEPOSIT_CATEGORY = "DEPOSIT"
MUU_CATEGORY = "Muu"
SOURCE_ORDER = ["deposit", "rule_match", "fallback_food", "unknown"]


def _pct(part: float, whole: float) -> str:
    if whole <= 0:
        return "n/a"
    return f"{part / whole * 100:.1f}%"


def _pct_int(part: int, whole: int) -> str:
    if whole <= 0:
        return "n/a"
    return f"{part / whole * 100:.0f}%"


def _load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def generate_report(categorized_csv: Path, matched_csv: Path, out_dir: Path):
    """
    Read categorized items and match results, print summary statistics,
    and write report.txt + category_breakdown.csv.
    """
    items = _load_csv(categorized_csv)
    matched = _load_csv(matched_csv)

    # ── Basic counts ────────────────────────────────────────────────────
    total_receipts = len(matched)
    bank_matched = sum(1 for r in matched if r["match_confidence"] != "none")

    # Total paid (sum of receipt totals)
    total_paid = sum(float(r["receipt_total_eur"]) for r in matched)

    # Deposits (items flagged as DEPOSIT)
    deposit_items = [it for it in items if it["category"] == DEPOSIT_CATEGORY]
    total_deposit = sum(float(it["net_price"]) for it in deposit_items)

    # Non-deposit items only
    spend_items = [it for it in items if it["category"] != DEPOSIT_CATEGORY]
    total_spend = round(total_paid - total_deposit, 2)

    # ── Mixed receipts ──────────────────────────────────────────────────
    # A receipt is "mixed" if it contains items from ≥ 2 distinct non-deposit categories
    receipt_cats = defaultdict(set)
    for it in spend_items:
        receipt_cats[it["receipt_id"]].add(it["category"])

    mixed_receipts = sum(1 for cats in receipt_cats.values() if len(cats) >= 2)

    # ── Baseline error ──────────────────────────────────────────────────
    # Baseline assigns ALL supermarket spending to "Toidupoed".
    # Non-baseline rows = items NOT in "Toidukaubad ja alkoholivabad joogid"
    non_baseline = [
        it for it in spend_items
        if it["category"] != "Toidukaubad ja alkoholivabad joogid"
    ]
    non_baseline_count = len(non_baseline)
    non_baseline_eur = round(sum(float(it["net_price"]) for it in non_baseline), 2)
    total_items = len(spend_items)

    if total_receipts == 0 and total_items == 0:
        sep = "-" * 60
        msg = (
            f"{sep}\nPILOT STUDY REPORT - Receipt Expense Categorisation\n{sep}\n"
            "No data: no receipts and no product lines.\n"
            f"{sep}\n"
        )
        print(msg)
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "report.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(msg)
        breakdown_path = out_dir / "category_breakdown.csv"
        with open(breakdown_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["category", "amount_eur", "share_pct", "row_count"])
            w.writerow(["TOTAL", 0.0, 0.0, 0])
        source_breakdown_path = out_dir / "category_source_breakdown.csv"
        with open(source_breakdown_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["category_source", "amount_eur", "share_pct_spend", "row_count", "share_pct_rows"])
            for source in SOURCE_ORDER:
                w.writerow([source, 0.0, 0.0, 0, 0.0])
            w.writerow(["TOTAL", 0.0, 0.0, 0, 0.0])
        print(f"\n  Report saved -> {report_path}")
        print(f"  Category breakdown -> {breakdown_path}")
        print(f"  Category source breakdown -> {source_breakdown_path}")
        return

    # ── Category breakdown ──────────────────────────────────────────────
    cat_eur = defaultdict(float)
    cat_count = defaultdict(int)
    for it in spend_items:
        net = float(it["net_price"])
        cat_eur[it["category"]] += net
        cat_count[it["category"]] += 1

    # Round category sums and adjust last category so total == total_spend
    categories = sorted(cat_eur.items(), key=lambda x: -x[1])
    cat_eur_rounded = {cat: round(eur, 2) for cat, eur in categories}
    rounding_diff = round(total_spend - sum(cat_eur_rounded.values()), 2)
    # Apply rounding correction to the largest category
    if categories and abs(rounding_diff) > 0:
        largest_cat = categories[0][0]
        cat_eur_rounded[largest_cat] = round(cat_eur_rounded[largest_cat] + rounding_diff, 2)

    # ── Category source breakdown ───────────────────────────────────────
    src_eur = defaultdict(float)
    src_count = defaultdict(int)
    for it in items:
        source = str(it.get("category_source", "") or "").strip()
        if source not in SOURCE_ORDER:
            category = str(it.get("category", "") or "").strip()
            matched_rule = str(it.get("category_rule", "") or "").strip()
            if category == DEPOSIT_CATEGORY:
                source = "deposit"
            elif matched_rule:
                source = "rule_match"
            elif category == MUU_CATEGORY:
                source = "unknown"
            else:
                source = "fallback_food"
        net = float(it.get("net_price", 0) or 0)
        src_eur[source] += net
        src_count[source] += 1

    spend_src_eur = {
        source: round(src_eur.get(source, 0.0), 2)
        for source in SOURCE_ORDER
        if source != "deposit"
    }
    spend_rounding_diff = round(total_spend - sum(spend_src_eur.values()), 2)
    if abs(spend_rounding_diff) > 0:
        present_non_deposit = [s for s in ("rule_match", "fallback_food", "unknown") if abs(spend_src_eur[s]) > 0]
        correction_target = present_non_deposit[0] if present_non_deposit else "fallback_food"
        spend_src_eur[correction_target] = round(spend_src_eur[correction_target] + spend_rounding_diff, 2)

    source_rows_total = sum(src_count.get(source, 0) for source in SOURCE_ORDER)

    # --- Print summary (ASCII-only for Windows consoles) ----------------
    sep = "-" * 60
    lines = [
        sep,
        "PILOT STUDY REPORT - Receipt Expense Categorisation",
        sep,
        f"Total receipts:                  {total_receipts}",
        f"  Maxima:                        {sum(1 for r in matched if r['store'] == 'Maxima')}",
        f"  Rimi:                          {sum(1 for r in matched if r['store'] == 'Rimi')}",
        f"Matched to bank export:          {bank_matched} / {total_receipts}",
        sep,
        f"Total paid (receipt totals):     {total_paid:.2f} EUR",
        f"Packaging deposits (excluded):     {total_deposit:.2f} EUR",
        f"Actual consumption spend:        {total_spend:.2f} EUR",
        f"Product rows (excl. deposits):   {total_items}",
        sep,
        f"Mixed receipts (>= 2 categories): {mixed_receipts} / {total_receipts}"
        f" = {_pct_int(mixed_receipts, total_receipts)}",
        sep,
        f"Non-baseline rows:               {non_baseline_count} / {total_items}"
        f" = {_pct(non_baseline_count, total_items)}",
        f"Non-baseline spend:              {non_baseline_eur:.2f} / {total_spend:.2f} EUR"
        f" = {_pct(non_baseline_eur, total_spend)}",
        sep,
        "CATEGORY BREAKDOWN:",
    ]

    for cat, eur in sorted(cat_eur_rounded.items(), key=lambda x: -x[1]):
        pct = (eur / total_spend * 100) if total_spend > 0 else 0.0
        count = cat_count[cat]
        lines.append(f"  {cat:<45} {eur:>8.2f} EUR  {pct:>5.1f}%  {count:>4} rows")

    total_pct_label = "100.0%" if total_spend > 0 else "n/a"
    lines += [
        sep,
        f"  {'TOTAL':<45} {total_spend:>8.2f} EUR  {total_pct_label:>5}  {total_items:>4} rows",
        sep,
        "CATEGORY SOURCE BREAKDOWN:",
    ]
    for source in SOURCE_ORDER:
        count = int(src_count.get(source, 0))
        row_share = (count / source_rows_total * 100) if source_rows_total > 0 else 0.0
        if source == "deposit":
            amount = round(src_eur.get(source, 0.0), 2)
        else:
            amount = spend_src_eur[source]
        spend_share = (amount / total_paid * 100) if total_paid > 0 else 0.0
        lines.append(
            f"  {source:<18} rows {count:>4} ({row_share:>5.1f}%)"
            f"  amount {amount:>8.2f} EUR ({spend_share:>5.1f}%)"
        )
    lines += [
        sep,
        f"  {'TOTAL':<18} rows {source_rows_total:>4} (100.0%)"
        f"  amount {total_paid:>8.2f} EUR (100.0%, incl. deposits)",
        sep,
        "METHODOLOGY NOTES (thesis / interpretation):",
        "- fallback_food: residual bucket for normal supermarket lines with no keyword hit;",
        "  default category is food by convention, not verified ground-truth product typing.",
        f"- unknown: low-information / non-product parser rows; category {MUU_CATEGORY} (not food).",
        "- Refund/credit lines (e.g. 'received back' RU/EE) -> Muu via explicit keyword rule (rule_match).",
        sep,
    ]

    report_text = "\n".join(lines)
    print(report_text)

    # Write report.txt
    report_path = out_dir / "report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text + "\n")
    print(f"\n  Report saved -> {report_path}")

    # Write category_breakdown.csv
    breakdown_path = out_dir / "category_breakdown.csv"
    with open(breakdown_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["category", "amount_eur", "share_pct", "row_count"])
        for cat, eur in sorted(cat_eur_rounded.items(), key=lambda x: -x[1]):
            pct = round(eur / total_spend * 100, 1) if total_spend > 0 else 0.0
            w.writerow([cat, eur, pct, cat_count[cat]])
        total_share = 100.0 if total_spend > 0 else 0.0
        w.writerow(["TOTAL", total_spend, total_share, total_items])
    print(f"  Category breakdown -> {breakdown_path}")

    # Write category_source_breakdown.csv
    source_breakdown_path = out_dir / "category_source_breakdown.csv"
    with open(source_breakdown_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["category_source", "amount_eur", "share_pct_spend", "row_count", "share_pct_rows"])
        for source in SOURCE_ORDER:
            count = int(src_count.get(source, 0))
            row_share = round((count / source_rows_total * 100), 1) if source_rows_total > 0 else 0.0
            if source == "deposit":
                amount = round(src_eur.get(source, 0.0), 2)
            else:
                amount = spend_src_eur[source]
            spend_share = round((amount / total_paid * 100), 1) if total_paid > 0 else 0.0
            w.writerow([source, amount, spend_share, count, row_share])
        w.writerow(["TOTAL", total_paid, 100.0 if total_paid > 0 else 0.0, source_rows_total, 100.0 if source_rows_total > 0 else 0.0])
    print(f"  Category source breakdown -> {source_breakdown_path}")
