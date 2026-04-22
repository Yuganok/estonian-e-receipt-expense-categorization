"""
parse_receipts.py
=================
Extracts product rows (item_text, gross_price, discount, net_price)
from receipt files.

- Maxima: text-based PDF  → direct extraction via pypdf (searched recursively under receipts/Maxima/, e.g. Maxima/YYYY-MM/*.pdf)
- Rimi:   image (JPG/PNG) → manual CSV fallback (OCR not automated yet)

Output columns:
    receipt_id, store, purchase_date, receipt_total_eur,
    item_text, gross_price, discount, net_price, is_deposit
"""

import hashlib
import re
import csv
from pathlib import Path
from pypdf import PdfReader

RU_TOTAL = "\u0418\u0442\u043e\u0433\u043e"
RU_PURCHASE_DATE = "\u0414\u0430\u0442\u0430 \u043f\u043e\u043a\u0443\u043f\u043a\u0438"
RU_DISCOUNT = "\u0421\u043a\u0438\u0434\u043a\u0430"
RU_DISCOUNT_LOWER = "\u0441\u043a\u0438\u0434\u043a\u0430"
RU_PAID = "\u041e\u043f\u043b\u0430\u0447\u0435\u043d\u043e"
RU_CASHIER = "\u041a\u0430\u0441\u0441\u0438\u0440"
RU_EARNED = "\u0417\u0430\u0440\u0430\u0431\u043e\u0442\u0430\u043d\u043d"
RU_BALANCE = "\u041e\u0441\u0442\u0430\u0442\u043e\u043a"
RU_DISCOUNT_TOTAL = "\u0421\u043a\u0438\u0434\u043a\u0430 \u0438\u0442\u043e\u0433\u043e"
RU_TOTAL_ALT = "\u0412\u0441\u0435\u0433\u043e"
RU_NSO = "\u041d\u0421\u041e"
RU_WITHOUT_NSO = "\u0431\u0435\u0437 \u041d\u0421\u041e"
RU_METHOD = "\u041c\u0435\u0442\u043e\u0434"
RU_NUMBER = "\u2116"

TOTAL_LABELS = [
    RU_TOTAL,
    RU_TOTAL_ALT,
    "Kokku",
    "Summa kokku",
    "Total",
    "Grand total",
]
PURCHASE_DATE_LABELS = [
    RU_PURCHASE_DATE,
    "Ostu kuupäev",
    "Kuupäev",
    "Purchase date",
    "Date of purchase",
]
DISCOUNT_LABELS = [
    RU_DISCOUNT,
    RU_DISCOUNT_LOWER,
    "Allahindlus",
    "Soodustus",
    "Discount",
]


# ---------------------------------------------------------------------------
# Deposit detection
# Deposits (pant/tagatisraha) are excluded from spending analysis
# ---------------------------------------------------------------------------
DEPOSIT_PATTERNS = [
    r"metallpurk",
    r"pet pudel",
    r"pandipakend",
    r"pant",
    r"tagatisraha",
    r"plastist ühekorrapakend",
    r"metallist ühekorrapakend",
    r"rimi ostukott",          # reusable bag — borderline, kept as deposit-like
]

DEPOSIT_RE = re.compile("|".join(DEPOSIT_PATTERNS), re.IGNORECASE)


def is_deposit(item_text: str) -> bool:
    """Return True if this item is a refundable deposit, not a consumption expense."""
    return bool(DEPOSIT_RE.search(item_text))


def _labels_alt(labels: list[str]) -> str:
    """Build case-insensitive alternation group from label list."""
    return "(?:" + "|".join(re.escape(s) for s in labels) + ")"


def _maxima_receipt_id(pdf_path: Path, maxima_root: Path) -> str:
    """
    Unique receipt_id from path under Maxima/ (same stem in different months differs).
    Example: Maxima/2026-03/foo.pdf -> MAX_2026-03_foo
    """
    rel = pdf_path.resolve().relative_to(maxima_root.resolve())
    stem = rel.with_suffix("").as_posix().replace("/", "_").replace("\\", "_")
    safe = re.sub(r"[^\w\-.]", "_", stem)
    if not safe:
        safe = re.sub(r"[^\w\-.]", "_", pdf_path.stem)
    return f"MAX_{safe}"


def _single_receipt_id(pdf_path: Path) -> str:
    """
    Stable receipt id for manually provided single PDF paths.
    Includes a short path hash to avoid collisions on common filenames.
    """
    resolved = str(pdf_path.resolve())
    stem = re.sub(r"[^\w\-.]", "_", pdf_path.stem)
    short_hash = hashlib.sha1(resolved.encode("utf-8")).hexdigest()[:8]
    return f"MAN_{stem}_{short_hash}"


# ---------------------------------------------------------------------------
# Maxima PDF parser
# ---------------------------------------------------------------------------
def _extract_maxima(pdf_path: Path) -> dict:
    """
    Parse a Maxima e-receipt PDF.

    Receipt structure (text-based PDF):
        <item name>  <qty> tk/kg  <gross_price> €
        RU_DISCOUNT  -<discount> €        ← optional discount on previous item (literal in receipt text)
        ...
        RU_TOTAL  <total> €             ← literal in receipt text
        RU_PURCHASE_DATE: DD.MM.YY,    ← literal in receipt text

    Returns dict with keys:
        purchase_date, receipt_total, items
        items: list of {item_text, gross_price, discount, net_price, is_deposit}
    """
    reader = PdfReader(str(pdf_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    # Normalize: non-breaking space → space, "0, 10" → "0,10" (Maxima quirk)
    text = text.replace("\xa0", " ")
    text = re.sub(r"(\d),\s+(\d)", r"\1,\2", text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # --- Extract total ---
    total_re = re.compile(rf"{_labels_alt(TOTAL_LABELS)}\s*[:\-]?\s*([\d,]+)", re.IGNORECASE)
    total = None
    for line in lines:
        m = total_re.search(line)
        if m:
            total = float(m.group(1).replace(",", "."))
            break

    # --- Extract purchase date ---
    purchase_date_re = re.compile(
        rf"{_labels_alt(PURCHASE_DATE_LABELS)}\s*[:\-]?\s*(\d{{2}}\.\d{{2}}\.\d{{2}})",
        re.IGNORECASE,
    )
    purchase_date = None
    for line in lines:
        m = purchase_date_re.search(line)
        if m:
            raw = m.group(1)  # e.g. "06.02.26"
            d, mo, y = raw.split(".")
            purchase_date = f"20{y}-{mo}-{d}"
            break

    # --- Extract item rows ---
    items = []
    pending_item = None  # last item seen, waiting for a possible discount line

    # Pattern: any line ending with a price like "2,85 €" or "2,85"
    price_re = re.compile(r"^(.+?)\s+([\d]+[,\.][\d]+)\s*€?\s*$")
    discount_re = re.compile(rf"{_labels_alt(DISCOUNT_LABELS)}\s*[-−]?([\d,\.]+)", re.IGNORECASE)
    # Parser noise pattern observed in Maxima totals footer lines.
    summary_row_re = re.compile(r"^\s*\d+\s+[\d,.]+\s*€\s+[\d,.]+\s*€\s*$")
    # Some footer rows include an extra trailing amount and still pass price_re.
    summary_name_re = re.compile(r"^\s*\d+\s+[\d,.]+\s*€\s+[\d,.]+\s*€\s*$")
    qty_only_name_re = re.compile(r"^\s*\d+(?:[.,]\d+)?\s*(tk|kg|g|pakk)\s*$", re.IGNORECASE)
    skip_re = re.compile(
        rf"({RU_TOTAL}|{RU_PAID}|{RU_NSO}|{RU_WITHOUT_NSO}|{RU_METHOD}|{RU_CASHIER}|{RU_PURCHASE_DATE}|"
        rf"{RU_EARNED}|{RU_BALANCE}|Klienditugi|Maxima Eesti|Reg\. Nr|{RU_NUMBER}\.|"
        rf"Kopli|Madala|Tuulemaa|Narva|Tallinn|{RU_DISCOUNT_TOTAL}|"
        rf"Teile|ÜLD|{RU_TOTAL_ALT}|Raha|MAXIMA)",
        re.IGNORECASE,
    )

    def flush(item):
        """Compute net price and append to items list."""
        if item is None:
            return
        net = round(item["gross_price"] - item["discount"], 2)
        item["net_price"] = max(net, 0.0)
        item["is_deposit"] = is_deposit(item["item_text"])
        items.append(item)

    for line in lines:
        # Skip metadata / summary lines
        if skip_re.search(line):
            continue
        if summary_row_re.match(line):
            continue

        # Discount line → apply to previous item
        dm = discount_re.search(line)
        if dm and pending_item is not None:
            pending_item["discount"] = float(dm.group(1).replace(",", "."))
            continue

        # Try to parse as item line
        pm = price_re.match(line)
        if pm:
            name_raw = pm.group(1)
            price = float(pm.group(2).replace(",", "."))

            # Skip obviously bad matches (VAT table rows, etc.)
            if price > 60 or price < 0.01:
                flush(pending_item)
                pending_item = None
                continue

            # Remove quantity suffix like "1 tk", "2 tk", "0.718 kg"
            name = re.sub(r"\s+\d+[\.,]?\d*\s*(tk|kg)\s*$", "", name_raw).strip()

            # Skip parser artifacts: too short, numeric-only, or quantity placeholder rows.
            if (
                len(name) < 3
                or re.match(r"^\d[\d\s,\.]+$", name)
                or qty_only_name_re.match(name_raw)
                or summary_name_re.match(name_raw)
                or summary_name_re.match(name)
            ):
                flush(pending_item)
                pending_item = None
                continue

            flush(pending_item)
            pending_item = {
                "item_text": name,
                "gross_price": price,
                "discount": 0.0,
            }

    flush(pending_item)

    return {
        "purchase_date": purchase_date,
        "receipt_total": total,
        "items": items,
    }


def _extract_selver_from_text(text: str) -> dict:
    """
    Parse Selver-style text receipts with table columns:
      Toode | Kogus | Ühiku hind | Kokku
    """
    text = text.replace("\xa0", " ")
    text = re.sub(r"(\d),\s+(\d)", r"\1,\2", text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    total = None
    total_re = re.compile(r"^Kokku\s+([\d,]+)\s*€?$", re.IGNORECASE)
    for line in lines:
        m = total_re.search(line)
        if m:
            total = float(m.group(1).replace(",", "."))
            break

    purchase_date = None
    date_re = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")
    for line in lines:
        m = date_re.search(line)
        if m:
            d, mo, y = m.group(1).split(".")
            purchase_date = f"{y}-{mo}-{d}"
            break

    # Start from item table header.
    header_idx = -1
    for i, line in enumerate(lines):
        if all(tok in line for tok in ("Toode", "Kogus", "Ühiku hind", "Kokku")):
            header_idx = i
            break

    items: list[dict] = []
    if header_idx < 0:
        return {"purchase_date": purchase_date, "receipt_total": total, "items": items}

    stop_markers = (
        "Kokku KM-ta",
        "KM kokku",
        "Maksmisviis",
        "MAKSEKAART",
        "BOONUSRAHA",
        "KOKKU",
        "Kassa",
        "Tšeki nr",
        "Partnerkaart",
    )
    row_re = re.compile(
        r"^(.+?)\s+(\d+(?:[.,]\d+)?)\s+(\d+(?:[.,]\d+)?)\s+(\d+(?:[.,]\d+))\s*€?$",
        re.IGNORECASE,
    )

    for line in lines[header_idx + 1 :]:
        if any(line.startswith(marker) for marker in stop_markers):
            break
        if line.startswith("KM ") or line.startswith("KM"):
            break
        m = row_re.match(line)
        if not m:
            continue
        item_text = m.group(1).strip()
        if not item_text or item_text.lower() == "kokku":
            continue
        qty = float(m.group(2).replace(",", "."))
        unit_price = float(m.group(3).replace(",", "."))
        line_total = float(m.group(4).replace(",", "."))
        gross_price = round(line_total, 2)
        # Quantity is parsed mainly for validation/debug; output schema remains unchanged.
        _ = qty, unit_price
        items.append(
            {
                "item_text": item_text,
                "gross_price": gross_price,
                "discount": 0.0,
                "net_price": gross_price,
                "is_deposit": is_deposit(item_text),
            }
        )

    return {
        "purchase_date": purchase_date,
        "receipt_total": total,
        "items": items,
    }


def _extract_single_text_pdf(pdf_path: Path) -> dict:
    """
    Parse manually provided single text-PDF.
    Uses Selver table parser when pattern is detected; otherwise falls back to Maxima parser.
    """
    reader = PdfReader(str(pdf_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    text_low = text.lower()
    if ("selver" in text_low and "ühiku hind" in text_low and "kogus" in text_low) or (
        "toode" in text_low and "ühiku hind" in text_low and "maksmisviis" in text_low
    ):
        return _extract_selver_from_text(text)
    return _extract_maxima(pdf_path)


# ---------------------------------------------------------------------------
# Rimi image fallback
# Rimi receipts are image-based; until OCR is implemented we load from a
# manually created sidecar CSV placed next to the image file.
#
# Sidecar format (rimi_manual.csv in the Rimi folder):
#   receipt_id, purchase_date, receipt_total, item_text, gross_price, discount
# ---------------------------------------------------------------------------
def _load_rimi_manual(rimi_dir: Path) -> list[dict]:
    """
    Load manually entered Rimi receipt data from a sidecar CSV.
    Returns list of raw row dicts.
    """
    sidecar = rimi_dir / "rimi_manual.csv"
    if not sidecar.exists():
        print(f"  [WARN] No rimi_manual.csv found in {rimi_dir} — skipping Rimi receipts.")
        print("         Create rimi_manual.csv with columns:")
        print("         receipt_id,purchase_date,receipt_total,item_text,gross_price,discount")
        return []

    rows = []
    with open(sidecar, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["gross_price"] = float(row["gross_price"])
            row["discount"] = float(row.get("discount", 0) or 0)
            row["net_price"] = round(row["gross_price"] - row["discount"], 2)
            row["is_deposit"] = is_deposit(row["item_text"])
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def parse_all_receipts(
    receipts_dir: Path,
    items_out: Path,
    receipts_out: Path,
    stores: frozenset[str] | None = None,
    maxima_purchases_csv: Path | None = None,
    single_receipt_pdf: Path | None = None,
    single_receipt_store: str = "Selver",
    only_single_receipt: bool = False,
):
    """
    Walk receipts_dir, parse all receipts, write two CSV files:
        items_out    — one row per product line
        receipts_out — one row per receipt (for bank matching)

    stores: which chains to include (lowercase: maxima, rimi). None = all.
    maxima_purchases_csv: optional path to Maxima purchases.csv (session manifest).
        If provided and valid, only PDFs listed in this CSV are parsed for Maxima.
    single_receipt_pdf: optional absolute/relative path to one manually provided text-PDF.
    single_receipt_store: store label written to CSV for the single-receipt mode.
    only_single_receipt: if True, skip Maxima/Rimi directory scans and parse only single_receipt_pdf.
    """
    receipts_dir = Path(receipts_dir)
    single_receipt_pdf = Path(single_receipt_pdf) if single_receipt_pdf else None
    store_label = str(single_receipt_store or "Selver").strip() or "Selver"
    if stores is None:
        stores = frozenset({"maxima", "rimi"})
    all_items = []
    all_receipts = []

    # --- Single manually provided text-PDF ---
    if single_receipt_pdf:
        if not single_receipt_pdf.exists():
            raise FileNotFoundError(f"single receipt PDF not found: {single_receipt_pdf}")
        if single_receipt_pdf.suffix.lower() != ".pdf":
            raise ValueError(f"single receipt file must be a .pdf: {single_receipt_pdf}")
        rid = _single_receipt_id(single_receipt_pdf)
        print(f"  Parsing single receipt: {single_receipt_pdf} ...", end=" ")
        result = _extract_single_text_pdf(single_receipt_pdf)
        total = result["receipt_total"] or 0.0
        date = result["purchase_date"] or "unknown"
        all_receipts.append(
            {
                "receipt_id": rid,
                "store": store_label,
                "purchase_date": date,
                "receipt_total_eur": total,
                "source": "manual_pdf_text",
            }
        )
        for item in result["items"]:
            all_items.append(
                {
                    "receipt_id": rid,
                    "store": store_label,
                    "purchase_date": date,
                    "receipt_total_eur": total,
                    **item,
                }
            )
        print(f"OK ({len(result['items'])} items, total={total})")
        if not result["items"]:
            print("  [WARN] No item rows were extracted. Verify that the PDF has a readable text layer.")

    # --- Maxima PDFs ---
    maxima_dir = receipts_dir / "Maxima"
    if only_single_receipt:
        print("  [INFO] Single-receipt mode: skipping Maxima/Rimi folder scans.")
    elif "maxima" not in stores:
        print("  [INFO] Skipping Maxima (not selected in --stores)")
    elif maxima_dir.exists():
        pdf_files = _resolve_session_maxima_pdfs(maxima_dir, maxima_purchases_csv)
        if maxima_purchases_csv and pdf_files is not None:
            print(f"  Found {len(pdf_files)} Maxima PDF(s) from session manifest: {maxima_purchases_csv}")
        else:
            pdf_files = sorted(maxima_dir.glob("**/*.pdf"))
            print(f"  Found {len(pdf_files)} Maxima PDF(s) (recursive under Maxima/)")
        for pdf in pdf_files:
            rid = _maxima_receipt_id(pdf, maxima_dir)
            rel_display = pdf.resolve().relative_to(maxima_dir.resolve())
            print(f"    Parsing {rel_display} ...", end=" ")
            try:
                result = _extract_maxima(pdf)
                total = result["receipt_total"] or 0.0
                date = result["purchase_date"] or "unknown"
                all_receipts.append({
                    "receipt_id": rid,
                    "store": "Maxima",
                    "purchase_date": date,
                    "receipt_total_eur": total,
                    "source": "pdf_text",
                })
                for item in result["items"]:
                    all_items.append({
                        "receipt_id": rid,
                        "store": "Maxima",
                        "purchase_date": date,
                        "receipt_total_eur": total,
                        **item,
                    })
                print(f"OK ({len(result['items'])} items, total={total})")
            except Exception as e:
                print(f"ERROR: {e}")
    elif "maxima" in stores:
        print(f"  [INFO] No Maxima/ subfolder found in {receipts_dir}")

    # --- Rimi images (manual sidecar) ---
    rimi_dir = receipts_dir / "Rimi"
    if only_single_receipt:
        pass
    elif "rimi" not in stores:
        print("  [INFO] Skipping Rimi (not selected in --stores)")
    elif rimi_dir.exists():
        rimi_rows = _load_rimi_manual(rimi_dir)
        print(f"  Loaded {len(rimi_rows)} Rimi item row(s) from sidecar CSV")

        # Group by receipt_id to build receipts list
        rimi_receipts = {}
        for row in rimi_rows:
            rid = row["receipt_id"]
            if rid not in rimi_receipts:
                rimi_receipts[rid] = {
                    "receipt_id": rid,
                    "store": "Rimi",
                    "purchase_date": row["purchase_date"],
                    "receipt_total_eur": float(row["receipt_total"]),
                    "source": "manual",
                }
            all_items.append({
                "receipt_id": rid,
                "store": "Rimi",
                "purchase_date": row["purchase_date"],
                "receipt_total_eur": float(row["receipt_total"]),
                "item_text": row["item_text"],
                "gross_price": row["gross_price"],
                "discount": row["discount"],
                "net_price": row["net_price"],
                "is_deposit": row["is_deposit"],
            })
        all_receipts.extend(rimi_receipts.values())
    elif "rimi" in stores:
        print(f"  [INFO] No Rimi/ subfolder found in {receipts_dir}")

    # --- Write items CSV ---
    item_fields = [
        "receipt_id", "store", "purchase_date", "receipt_total_eur",
        "item_text", "gross_price", "discount", "net_price", "is_deposit",
    ]
    with open(items_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=item_fields)
        w.writeheader()
        w.writerows(all_items)
    print(f"\n  Items written: {len(all_items)} rows -> {items_out}")

    # --- Write receipts CSV ---
    receipt_fields = ["receipt_id", "store", "purchase_date", "receipt_total_eur", "source"]
    with open(receipts_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=receipt_fields)
        w.writeheader()
        w.writerows(all_receipts)
    print(f"  Receipts written: {len(all_receipts)} rows -> {receipts_out}")


def _resolve_session_maxima_pdfs(maxima_dir: Path, purchases_csv: Path | None) -> list[Path] | None:
    """
    Build Maxima PDF list from purchases.csv filePath column.
    Returns:
      - list[Path] when manifest exists and can be read (possibly empty),
      - None when manifest is absent/unreadable (caller should fallback to full recursive scan).
    """
    if purchases_csv is None:
        return None
    p = Path(purchases_csv)
    if not p.exists():
        print(f"  [WARN] Session manifest not found: {p} — fallback to full Maxima scan.")
        return None

    rows: list[dict] = []
    try:
        with open(p, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"  [WARN] Could not read session manifest {p}: {e} — fallback to full Maxima scan.")
        return None

    unique: dict[str, Path] = {}
    for row in rows:
        raw = (row.get("filePath") or "").strip()
        if not raw:
            continue
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = (maxima_dir.parent / candidate).resolve()
        else:
            candidate = candidate.resolve()
        if candidate.suffix.lower() != ".pdf":
            continue
        if not candidate.exists():
            continue
        # Restrict to Maxima folder to avoid accidental leakage.
        try:
            candidate.relative_to(maxima_dir.resolve())
        except Exception:
            continue
        unique[str(candidate)] = candidate

    return sorted(unique.values())
