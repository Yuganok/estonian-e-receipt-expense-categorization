import csv
import argparse
from pathlib import Path

from evaluation_predictors import EvaluationPredictor, OllamaConfig
from categorize import MAIN_EVAL_CATEGORIES


def main():
    parser = argparse.ArgumentParser(description="Generate OpenAI predictions CSV")
    parser.add_argument("--batch", action="store_true", help="Enable single-call batch prefill mode")
    args = parser.parse_args()

    gold = Path("../output/evaluation/gold_to_review.csv")
    out = (
        Path("../output/evaluation/batch/preds_openai.csv")
        if args.batch
        else Path("../output/evaluation/preds_openai.csv")
    )
    out.parent.mkdir(parents=True, exist_ok=True)

    labels = list(MAIN_EVAL_CATEGORIES)
    predictor = EvaluationPredictor(
        approach="llm",
        labels=labels,
        ollama=OllamaConfig(
            provider="openai",
            model="gpt-5.4-mini",
            base_url="http://127.0.0.1:11434",
            timeout_seconds=60,
            temperature=0.0,
        ),
    )

    rows = list(csv.DictReader(gold.open("r", encoding="utf-8", newline="")))
    batch_items = []
    for r in rows:
        item = str(r.get("item_text") or "").strip()
        true = str(r.get("true_category") or "").strip()
        if not item or not true:
            continue
        batch_items.append(item)
    if args.batch:
        predictor.prefill_batch_cache(batch_items)

    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["receipt_id", "item_text", "true_category", "pred_category"],
        )
        w.writeheader()
        for i, r in enumerate(rows, 1):
            item = str(r.get("item_text") or "").strip()
            true = str(r.get("true_category") or "").strip()
            if not item or not true:
                continue
            pred = predictor.predict(item, False)
            w.writerow(
                {
                    "receipt_id": str(r.get("receipt_id") or "").strip(),
                    "item_text": item,
                    "true_category": true,
                    "pred_category": pred,
                }
            )
            if i % 25 == 0:
                print(f"OPENAI_PROGRESS {i}")

    print("DONE", out)


if __name__ == "__main__":
    main()
