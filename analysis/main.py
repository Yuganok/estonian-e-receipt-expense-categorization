"""
Receipt Expense Categorisation Pipeline
========================================
Entry point. Run this to process all receipts and produce a report.

Usage:
    python main.py --receipts ./receipts --bank bank_export.csv --out ./output

Folder structure expected:
    receipts/
        Maxima/   ← PDF files under any subfolder, e.g. Maxima/YYYY-MM/*.pdf (recursive scan)
        Rimi/     ← JPG/PNG files (image-based, manual/OCR fallback)
    bank_export.csv  ← Swedbank CSV export (semicolon-separated)
"""

import sys


def _configure_stdio_utf8():
    """Avoid UnicodeEncodeError on Windows (cp1252) when printing paths with non-ASCII."""
    for stream in (sys.stdout, sys.stderr):
        if stream is None:
            continue
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_configure_stdio_utf8()

import argparse
import os
from pathlib import Path

from parse_receipts import parse_all_receipts
from categorize import categorize_items
from match_bank import match_to_bank
from report import generate_report


def parse_stores_arg(s: str) -> frozenset[str]:
    """Parse --stores: all, maxima, rimi, or comma-separated e.g. maxima,rimi."""
    t = (s or "all").strip().lower()
    if t in ("", "all", "both"):
        return frozenset({"maxima", "rimi"})
    parts = [p.strip().lower() for p in t.split(",") if p.strip()]
    allowed = {"maxima", "rimi"}
    unknown = set(parts) - allowed
    if unknown:
        raise ValueError(
            f"Unknown --stores values: {unknown}. Allowed: all, maxima, rimi, or maxima,rimi."
        )
    if not parts:
        return frozenset({"maxima", "rimi"})
    return frozenset(parts)


def main():
    parser = argparse.ArgumentParser(description="Receipt expense categorisation pipeline")
    parser.add_argument("--receipts", default="./receipts", help="Folder with receipt PDFs/images")
    parser.add_argument("--bank", default="./bank_export.csv", help="Swedbank CSV export file")
    parser.add_argument("--out", default="./output", help="Output folder for CSVs and report")
    parser.add_argument(
        "--maxima-purchases-csv",
        default="",
        help="Optional path to Maxima purchases.csv from downloader session; limits Maxima parsing to listed filePath rows",
    )
    parser.add_argument(
        "--stores",
        default="all",
        help="Which stores to parse: all, maxima, rimi, or maxima,rimi",
    )
    args = parser.parse_args()

    receipts_dir = Path(args.receipts)
    bank_csv = Path(args.bank)
    out_dir = Path(args.out)
    maxima_purchases_csv = Path(args.maxima_purchases_csv) if str(args.maxima_purchases_csv).strip() else None
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        stores = parse_stores_arg(args.stores)
    except ValueError as e:
        print(str(e))
        raise SystemExit(1) from e

    print("=" * 60)
    print("STEP 1: Parsing receipts")
    print("=" * 60)
    items_path = out_dir / "items_raw.csv"
    receipts_path = out_dir / "receipts.csv"
    parse_all_receipts(
        receipts_dir,
        items_path,
        receipts_path,
        stores,
        maxima_purchases_csv=maxima_purchases_csv,
    )

    print("\n" + "=" * 60)
    print("STEP 2: Categorising items (rule-based)")
    print("=" * 60)
    categorized_path = out_dir / "items_categorized.csv"
    categorize_items(items_path, categorized_path)

    print("\n" + "=" * 60)
    print("STEP 3: Matching receipts to bank transactions")
    print("=" * 60)
    matched_path = out_dir / "matched.csv"
    match_to_bank(receipts_path, bank_csv, matched_path)

    print("\n" + "=" * 60)
    print("STEP 4: Generating report")
    print("=" * 60)
    generate_report(categorized_path, matched_path, out_dir)

    print("\nDone! Results saved to:", out_dir)


if __name__ == "__main__":
    main()
