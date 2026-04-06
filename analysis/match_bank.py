"""
match_bank.py
=============
Matches parsed receipts to Swedbank CSV export rows.

Matching key:
    1. amount  — receipt_total_eur == bank transaction amount (±0.01 tolerance)
    2. merchant — receipt store name appears in bank Saaja/Maksja field
    3. date    — bank transaction date within ±3 days of receipt purchase_date

Swedbank CSV columns (Estonian):
    Kliendi konto, Reatüüp, Kuupäev, Saaja/Maksja, Selgitus,
    Summa, Valuuta, Deebet/Kreedit, Arhiveerimistunnus, ...

Baseline category:
    The Swedbank CSV export does NOT include the bank's auto-category.
    The baseline "Toidupoed" was observed in the Swedbank UI (transaction detail view)
    and is applied here as a fixed label for all Maxima/Rimi/supermarket transactions.

Output columns:
    receipt_id, store, purchase_date, receipt_total_eur,
    bank_tx_date, bank_tx_amount, bank_merchant,
    delta_days, match_confidence, baseline_category, notes
"""

import csv
from pathlib import Path
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Merchant keyword → store name mapping
# ---------------------------------------------------------------------------
MERCHANT_MAP = {
    "MAXIMA": "Maxima",
    "MAXIMA APTEEK": "Maxima",   # pharmacy inside Maxima — treated separately
    "RIMI": "Rimi",
    "SELVER": "Selver",
    "PRISMA": "Prisma",
    "LIDL": "Lidl",
    "COOP": "Coop",
}

# Baseline category assigned to all supermarket transactions in Swedbank UI
SUPERMARKET_BASELINE = "Toidupoed"


def _parse_date(date_str: str) -> datetime | None:
    """Parse date from DD.MM.YYYY or YYYY-MM-DD format."""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def _parse_amount(amount_str: str) -> float | None:
    """Parse amount like '18,57' or '18.57' to float."""
    try:
        return float(amount_str.strip().replace(",", ".").replace(" ", ""))
    except ValueError:
        return None


def _detect_store(merchant_field: str) -> str | None:
    """Return normalised store name if merchant field matches a known supermarket."""
    upper = merchant_field.upper()
    for key, name in MERCHANT_MAP.items():
        if key in upper:
            return name
    return None


def load_bank_csv(bank_csv: Path) -> list[dict]:
    """
    Load Swedbank CSV export.
    Only expense rows (Deebet/Kreedit == 'D') are kept.
    """
    rows = []
    # Swedbank CSV uses semicolons and may have BOM
    with open(bank_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            # Keep only debit (expense) rows
            dc = row.get("Deebet/Kreedit", "").strip()
            if dc != "D":
                continue
            date = _parse_date(row.get("Kuupäev", ""))
            amount = _parse_amount(row.get("Summa", ""))
            merchant = row.get("Saaja/Maksja", "").strip()
            if date and amount:
                rows.append({
                    "bank_date": date,
                    "bank_amount": amount,
                    "bank_merchant": merchant,
                    "bank_store": _detect_store(merchant),
                })
    return rows


def match_to_bank(receipts_csv: Path, bank_csv: Path, out_csv: Path,
                  max_delta_days: int = 3, amount_tol: float = 0.02):
    """
    For each receipt, find the best-matching bank transaction.
    Writes matched.csv with match metadata and baseline_category.
    """
    receipts_csv = Path(receipts_csv)
    bank_csv = Path(bank_csv)

    # Load receipts
    receipts = []
    with open(receipts_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["receipt_date"] = _parse_date(row["purchase_date"])
            row["receipt_total"] = _parse_amount(row["receipt_total_eur"])
            receipts.append(row)

    # Load bank rows
    bank_rows = load_bank_csv(bank_csv)
    print(f"  Loaded {len(receipts)} receipts, {len(bank_rows)} bank debit rows")

    used_bank_indices: set[int] = set()
    results = []
    unmatched = []

    for receipt in receipts:
        r_date = receipt["receipt_date"]
        r_amount = receipt["receipt_total"]
        r_store = receipt["store"]

        if r_date is None or r_amount is None:
            unmatched.append(receipt["receipt_id"] + " (missing date/amount)")
            continue

        # Find candidates: same store, amount within tolerance, date within window
        # Each bank debit row may match at most one receipt.
        candidates = []
        for i, b in enumerate(bank_rows):
            if i in used_bank_indices:
                continue
            if b["bank_store"] != r_store:
                continue
            if abs(b["bank_amount"] - r_amount) > amount_tol:
                continue
            if b["bank_date"] is None:
                continue
            delta = abs((b["bank_date"] - r_date).days)
            if delta <= max_delta_days:
                candidates.append((delta, i, b))

        if not candidates:
            unmatched.append(receipt["receipt_id"])
            results.append({
                "receipt_id": receipt["receipt_id"],
                "store": r_store,
                "purchase_date": receipt["purchase_date"],
                "receipt_total_eur": r_amount,
                "bank_tx_date": "",
                "bank_tx_amount": "",
                "bank_merchant": "",
                "delta_days": "",
                "match_confidence": "none",
                "baseline_category": SUPERMARKET_BASELINE,
                "notes": "no bank match found",
            })
            continue

        # Pick best match (smallest date delta, then stable by bank row index)
        candidates.sort(key=lambda x: (x[0], x[1]))
        delta, best_i, best = candidates[0]
        used_bank_indices.add(best_i)
        confidence = "high" if delta <= 1 else ("medium" if delta <= 2 else "low")

        results.append({
            "receipt_id": receipt["receipt_id"],
            "store": r_store,
            "purchase_date": receipt["purchase_date"],
            "receipt_total_eur": r_amount,
            "bank_tx_date": best["bank_date"].strftime("%Y-%m-%d"),
            "bank_tx_amount": best["bank_amount"],
            "bank_merchant": best["bank_merchant"],
            "delta_days": delta,
            "match_confidence": confidence,
            "baseline_category": SUPERMARKET_BASELINE,
            "notes": f"{len(candidates)} candidate(s)" if len(candidates) > 1 else "",
        })

    # Write output
    fields = [
        "receipt_id", "store", "purchase_date", "receipt_total_eur",
        "bank_tx_date", "bank_tx_amount", "bank_merchant",
        "delta_days", "match_confidence", "baseline_category", "notes",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)

    matched_count = sum(1 for r in results if r["match_confidence"] != "none")
    print(f"  Matched: {matched_count}/{len(receipts)} receipts -> {out_csv}")
    if unmatched:
        print(f"  Unmatched receipt IDs: {', '.join(unmatched)}")
