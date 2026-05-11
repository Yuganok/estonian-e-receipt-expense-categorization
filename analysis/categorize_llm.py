from __future__ import annotations

import csv
from pathlib import Path

from evaluation_predictors import EvaluationPredictor, OllamaConfig
from categorize import (
    MAIN_EVAL_CATEGORIES,
    MUU_CATEGORY,
    SOURCE_DEPOSIT,
    SOURCE_FALLBACK_FOOD,
    SOURCE_MANUAL_MEMORY,
    SOURCE_UNKNOWN,
    _looks_like_unknown_row,
)

try:
    from memory_layer import lookup_manual_category
except ImportError:  # pragma: no cover - support package import from repo root
    from .memory_layer import lookup_manual_category


def _as_bool(value: str | bool | int | None) -> bool:
    t = str(value or "").strip().lower()
    return t in {"1", "true", "yes", "y"}


def categorize_items_llm(
    items_in: Path,
    items_out: Path,
    memory_db_path: Path | str | None = None,
    llm_provider: str = "gemini",
    llm_model: str = "gemini-2.5-flash",
    llm_timeout_seconds: float = 30.0,
) -> None:
    """
    Read items_raw.csv, add LLM category columns, write items_categorized.csv.
    Keeps the output shape compatible with existing report pipeline.
    """
    items_in = Path(items_in)
    items_out = Path(items_out)
    provider = str(llm_provider or "gemini").strip().lower() or "gemini"
    model = str(llm_model or "gemini-2.5-flash").strip() or "gemini-2.5-flash"

    predictor = EvaluationPredictor(
        approach="llm",
        labels=list(MAIN_EVAL_CATEGORIES),
        ollama=OllamaConfig(
            provider=provider,
            model=model,
            timeout_seconds=float(llm_timeout_seconds or 30.0),
            temperature=0.0,
        ),
    )

    rows: list[dict[str, str]] = []
    in_fieldnames: list[str] = []
    counts: dict[str, int] = {}
    batch_candidates: list[str] = []

    with items_in.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fieldnames = list(reader.fieldnames or [])
        for row in reader:
            rows.append(dict(row))
            item_text = str(row.get("item_text") or "").strip()
            store = str(row.get("store") or "").strip()
            is_dep = _as_bool(row.get("is_deposit"))
            manual_category = lookup_manual_category(
                store=store,
                item_text=item_text,
                db_path=memory_db_path,
            )
            if not item_text or manual_category or is_dep or _looks_like_unknown_row(item_text):
                continue
            batch_candidates.append(item_text)

    if provider in {"openai", "gemini", "deepseek", "claude"} and batch_candidates:
        predictor.prefill_batch_cache(batch_candidates)

    for row in rows:
        item_text = str(row.get("item_text") or "").strip()
        store = str(row.get("store") or "").strip()
        is_dep = _as_bool(row.get("is_deposit"))
        manual_category = lookup_manual_category(
            store=store,
            item_text=item_text,
            db_path=memory_db_path,
        )

        if manual_category:
            category = str(manual_category).strip() or MUU_CATEGORY
            category_rule = "manual_memory"
            category_source = SOURCE_MANUAL_MEMORY
        elif _looks_like_unknown_row(item_text) and not is_dep:
            category = MUU_CATEGORY
            category_rule = ""
            category_source = SOURCE_UNKNOWN
        else:
            category = predictor.predict(item_text, is_dep)
            if category == "DEPOSIT":
                category_rule = "is_deposit flag"
                category_source = SOURCE_DEPOSIT
            else:
                category_rule = f"llm:{provider}"
                category_source = SOURCE_FALLBACK_FOOD if category == MUU_CATEGORY else f"llm_{provider}"

        row["category"] = category
        row["category_rule"] = category_rule
        row["category_source"] = category_source
        counts[category] = counts.get(category, 0) + 1

    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = [c for c in in_fieldnames if c not in {"category", "category_rule", "category_source"}] + [
            "category",
            "category_rule",
            "category_source",
        ]

    with items_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    token_usage = predictor.token_usage.as_dict()
    print(f"  Categorised {len(rows)} items with {provider}/{model} -> {items_out}")
    print(
        "  LLM token usage:"
        f" prompt={token_usage['total_prompt_tokens']}"
        f" completion={token_usage['total_completion_tokens']}"
        f" avg_per_item={token_usage['avg_tokens_per_item']:.2f}"
    )
    print("  Category breakdown:")
    for cat, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"    {cat:<45} {count:>4} items")
