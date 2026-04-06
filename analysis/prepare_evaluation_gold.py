"""
Prepare a reviewable gold CSV for classifier evaluation.

This utility samples rows from items_categorized.csv (preferred) or items_raw.csv
and pre-fills true_category with the current rule-based result. The reviewer can
then quickly correct only wrong rows instead of labeling everything from scratch.
"""

from __future__ import annotations

import argparse
import csv
import random
from collections import defaultdict
from pathlib import Path

from categorize import (
    DEFAULT_CATEGORY,
    MUU_CATEGORY,
    SOURCE_FALLBACK_FOOD,
    SOURCE_RULE_MATCH,
    SOURCE_UNKNOWN,
    classify,
)


def as_bool(value: str | bool | int | None) -> bool:
    t = str(value or "").strip().lower()
    return t in {"1", "true", "yes", "y"}


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def key_for(receipt_id: str, item_text: str) -> tuple[str, str]:
    return (str(receipt_id or "").strip(), str(item_text or "").strip())


def load_base_rows(items_categorized: Path | None, items_raw: Path | None) -> list[dict[str, str]]:
    if items_categorized:
        rows = read_csv_rows(items_categorized)
        out: list[dict[str, str]] = []
        for r in rows:
            item_text = str(r.get("item_text") or "").strip()
            if not item_text:
                continue
            category = str(r.get("category") or "").strip()
            if not category:
                category, _ = classify(item_text, as_bool(r.get("is_deposit")))
            out.append(
                {
                    "receipt_id": str(r.get("receipt_id") or "").strip(),
                    "item_text": item_text,
                    "is_deposit": "True" if as_bool(r.get("is_deposit")) else "False",
                    "true_category": category,
                    "suggested_category": category,
                    "category_source": str(r.get("category_source") or "").strip(),
                    "category_rule": str(r.get("category_rule") or "").strip(),
                    "source": "sampled",
                    "note": "",
                }
            )
        return out

    if items_raw:
        rows = read_csv_rows(items_raw)
        out = []
        for r in rows:
            item_text = str(r.get("item_text") or "").strip()
            if not item_text:
                continue
            is_dep = as_bool(r.get("is_deposit"))
            suggested, _ = classify(item_text, is_dep)
            out.append(
                {
                    "receipt_id": str(r.get("receipt_id") or "").strip(),
                    "item_text": item_text,
                    "is_deposit": "True" if is_dep else "False",
                    "true_category": suggested,
                    "suggested_category": suggested,
                    "category_source": "",
                    "category_rule": "",
                    "source": "sampled",
                    "note": "",
                }
            )
        return out

    raise ValueError("Provide --items-categorized or --items-raw.")


def load_manual_corrections(path: Path | None) -> dict[tuple[str, str], dict[str, str]]:
    if not path:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"manual corrections file not found: {path}")
    rows = read_csv_rows(path)
    out: dict[tuple[str, str], dict[str, str]] = {}
    for r in rows:
        receipt_id = str(r.get("receipt_id") or "").strip()
        item_text = str(r.get("item_text") or "").strip()
        manual_category = str(r.get("manual_category") or "").strip()
        if not item_text or not manual_category:
            continue
        out[key_for(receipt_id, item_text)] = {
            "manual_category": manual_category,
            "note": str(r.get("note") or "").strip(),
        }
    return out


def filter_non_deposit_rows(rows: list[dict[str, str]], include_deposit: bool) -> list[dict[str, str]]:
    if include_deposit:
        return list(rows)
    out: list[dict[str, str]] = []
    for r in rows:
        if as_bool(r.get("is_deposit")):
            continue
        if str(r.get("suggested_category") or "").strip() == "DEPOSIT":
            continue
        out.append(r)
    return out


def diverse_bucket(row: dict[str, str]) -> str:
    """Stratum for quota sampling (items_categorized with category_source)."""
    cat = str(row.get("suggested_category") or "").strip()
    src = str(row.get("category_source") or "").strip()
    if cat == "DEPOSIT":
        return "deposit"
    if src == SOURCE_RULE_MATCH and cat == DEFAULT_CATEGORY:
        return "rule_food"
    if src == SOURCE_FALLBACK_FOOD:
        return "fallback_food"
    if cat == "Majapidamis- ja puhastusvahendid":
        return "household"
    if cat == "Alkohol ja tubakas":
        return "alcohol"
    if cat == "Majapidamistehnika":
        return "appliances"
    if cat == "Lilled ja kingitused":
        return "flowers"
    if cat == MUU_CATEGORY or src == SOURCE_UNKNOWN:
        return "other"
    return "other"


def scale_diverse_targets(n: int) -> dict[str, int]:
    """Quota mix tuned for ~80 rows (food rule/fallback + rare categories)."""
    base = {
        "rule_food": 26,
        "fallback_food": 12,
        "household": 12,
        "alcohol": 7,
        "appliances": 7,
        "flowers": 5,
        "other": 11,
    }
    total_base = sum(base.values())
    scaled = {k: n * v / total_base for k, v in base.items()}
    floors = {k: int(scaled[k]) for k in base}
    rem = n - sum(floors.values())
    order = sorted(base.keys(), key=lambda k: scaled[k] - floors[k], reverse=True)
    for i in range(rem):
        floors[order[i % len(order)]] += 1
    return floors


def sample_rows_diverse(rows: list[dict[str, str]], n: int, seed: int) -> list[dict[str, str]]:
    if n <= 0:
        raise ValueError("--n must be > 0")
    if len(rows) <= n:
        return list(rows)

    rnd = random.Random(seed)
    targets = scale_diverse_targets(n)
    buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        buckets[diverse_bucket(r)].append(r)

    for pool in buckets.values():
        rnd.shuffle(pool)

    picked: list[dict[str, str]] = []
    picked_keys: set[tuple[str, str]] = set()

    for bucket, tgt in targets.items():
        pool = buckets[bucket]
        got = 0
        while got < tgt and pool:
            r = pool.pop()
            k = key_for(r["receipt_id"], r["item_text"])
            if k in picked_keys:
                continue
            picked_keys.add(k)
            picked.append(r)
            got += 1

    if len(picked) < n:
        remainder: list[dict[str, str]] = []
        for pool in buckets.values():
            remainder.extend(pool)
        rnd.shuffle(remainder)
        for r in remainder:
            if len(picked) >= n:
                break
            k = key_for(r["receipt_id"], r["item_text"])
            if k in picked_keys:
                continue
            picked_keys.add(k)
            picked.append(r)

    return picked[:n]


def sample_rows(
    rows: list[dict[str, str]],
    n: int,
    mode: str,
    seed: int,
) -> list[dict[str, str]]:
    if n <= 0:
        raise ValueError("--n must be > 0")
    if len(rows) <= n:
        return list(rows)

    rnd = random.Random(seed)
    if mode == "random":
        return rnd.sample(rows, n)
    if mode == "diverse":
        return sample_rows_diverse(rows, n, seed)

    # stratify (evenly by predicted category)
    buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        buckets[str(r.get("suggested_category") or "")].append(r)

    for category_rows in buckets.values():
        rnd.shuffle(category_rows)

    categories = sorted(buckets.keys())
    sampled: list[dict[str, str]] = []
    idx = 0
    while len(sampled) < n and categories:
        cat = categories[idx % len(categories)]
        if buckets[cat]:
            sampled.append(buckets[cat].pop())
        else:
            categories.remove(cat)
            idx -= 1
        idx += 1
    return sampled[:n]


def merge_manual(
    sampled: list[dict[str, str]],
    base_map: dict[tuple[str, str], dict[str, str]],
    manual_map: dict[tuple[str, str], dict[str, str]],
) -> list[dict[str, str]]:
    by_key = {key_for(r["receipt_id"], r["item_text"]): dict(r) for r in sampled}
    for k, corr in manual_map.items():
        current = by_key.get(k) or dict(
            base_map.get(
                k,
                {
                    "receipt_id": k[0],
                    "item_text": k[1],
                    "is_deposit": "False",
                    "true_category": "",
                    "suggested_category": "",
                    "category_source": "",
                    "category_rule": "",
                    "source": "manual_correction",
                    "note": "",
                },
            )
        )
        current["true_category"] = corr["manual_category"]
        current["source"] = "manual_correction"
        if corr["note"]:
            current["note"] = corr["note"]
        by_key[k] = current

    return sorted(
        by_key.values(),
        key=lambda r: (str(r.get("source") != "manual_correction"), r.get("receipt_id", ""), r.get("item_text", "")),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a reviewable gold CSV for evaluation.")
    parser.add_argument("--items-categorized", default="", help="Path to items_categorized.csv")
    parser.add_argument("--items-raw", default="", help="Path to items_raw.csv (fallback source)")
    parser.add_argument("--manual-corrections", default="", help="Optional path to manual_corrections.csv")
    parser.add_argument("--n", type=int, default=80, help="Target sample size (default: 80)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument(
        "--sample-mode",
        choices=["diverse", "stratify", "random"],
        default="diverse",
        help="diverse: quotas by category_source/category (needs items_categorized); "
        "stratify: round-robin by predicted category; random: uniform",
    )
    parser.add_argument(
        "--stratify",
        action="store_true",
        help="Shortcut for --sample-mode stratify",
    )
    parser.add_argument(
        "--include-deposit",
        action="store_true",
        help="Include DEPOSIT rows in the pool (default: exclude for 6-class gold)",
    )
    parser.add_argument(
        "--out",
        default="../output/evaluation/gold_to_review.csv",
        help="Output CSV path (default: ../output/evaluation/gold_to_review.csv)",
    )
    args = parser.parse_args()

    items_categorized = Path(args.items_categorized) if str(args.items_categorized).strip() else None
    items_raw = Path(args.items_raw) if str(args.items_raw).strip() else None
    manual_path = Path(args.manual_corrections) if str(args.manual_corrections).strip() else None
    out_path = Path(args.out)

    if items_categorized and not items_categorized.exists():
        raise FileNotFoundError(f"items_categorized file not found: {items_categorized}")
    if items_raw and not items_raw.exists():
        raise FileNotFoundError(f"items_raw file not found: {items_raw}")

    base_rows = load_base_rows(items_categorized, items_raw)
    if not base_rows:
        raise RuntimeError("No source rows found for preparation.")

    mode = "stratify" if args.stratify else args.sample_mode
    if mode == "diverse" and not items_categorized:
        print("Note: diverse sampling needs items_categorized.csv (category_source); using stratify.")
        mode = "stratify"

    pool = filter_non_deposit_rows(base_rows, include_deposit=args.include_deposit)
    if not pool:
        raise RuntimeError("No rows left after filters; try --include-deposit or another input file.")

    base_map = {key_for(r["receipt_id"], r["item_text"]): dict(r) for r in base_rows}
    sampled = sample_rows(pool, args.n, mode, args.seed)
    manual_map = load_manual_corrections(manual_path)
    final_rows = merge_manual(sampled, base_map, manual_map) if manual_map else sampled

    fieldnames = [
        "receipt_id",
        "item_text",
        "is_deposit",
        "true_category",
        "suggested_category",
        "category_source",
        "category_rule",
        "source",
        "note",
    ]
    write_csv_rows(out_path, fieldnames, final_rows)

    print(f"Prepared {len(final_rows)} rows -> {out_path}")
    print(f"  Source rows (all): {len(base_rows)}")
    print(f"  Pool after deposit filter: {len(pool)} (include_deposit={args.include_deposit})")
    print(f"  Requested sample size: {args.n}")
    if manual_map:
        print(f"  Manual corrections merged: {len(manual_map)}")
    print(f"  Sample mode: {mode}")
    print(f"  Seed: {args.seed}")


if __name__ == "__main__":
    main()
