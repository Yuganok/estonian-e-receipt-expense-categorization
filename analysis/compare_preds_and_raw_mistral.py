import csv
import json
from pathlib import Path
from urllib import request, error

from categorize import MAIN_EVAL_CATEGORIES, MUU_CATEGORY
from evaluation_predictors import EvaluationPredictor, OllamaConfig


LABELS = list(MAIN_EVAL_CATEGORIES)
OLLAMA_URL = "http://127.0.0.1:11434/api/generate"


def build_prompt_for_mistral(item_text: str) -> str:
    return (
        "You are a classifier for grocery store receipt line items.\n"
        "You MUST return ONLY one of these EXACT category strings, copied verbatim:\n"
        "- Toidukaubad ja alkoholivabad joogid\n"
        "- Alkohol ja tubakas\n"
        "- Majapidamis- ja puhastusvahendid\n"
        "- Majapidamistehnika\n"
        "- Lilled ja kingitused\n"
        "- Muu\n\n"
        "DO NOT translate. DO NOT create new categories. Copy the string exactly.\n\n"
        "Examples:\n"
        'Item: "Piim 3,2% 1L" -> {"category": "Toidukaubad ja alkoholivabad joogid"}\n'
        'Item: "ÕLUT SAKU 0.5L" -> {"category": "Alkohol ja tubakas"}\n'
        'Item: "PESUPULBER 3KG" -> {"category": "Majapidamis- ja puhastusvahendid"}\n\n'
        'Return ONLY JSON: {"category": "<exact string from list above>"}\n'
        f'Item: "{item_text}"'
    )


def mistral_raw_response(item_text: str):
    payload = {
        "model": "mistral:latest",
        "prompt": build_prompt_for_mistral(item_text),
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0},
    }
    req = request.Request(
        OLLAMA_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
    decoded = json.loads(body)
    raw = decoded.get("response", "")

    parsed_ok = False
    category = ""
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            parsed_ok = True
            category = str(parsed.get("category") or "").strip()
        except Exception:
            parsed_ok = False
    elif isinstance(raw, dict):
        parsed_ok = True
        category = str(raw.get("category") or "").strip()

    allowed = category in LABELS
    return raw, parsed_ok, allowed, category


def main():
    mistral_predictor = EvaluationPredictor(
        approach="llm",
        labels=LABELS,
        ollama=OllamaConfig(
            model="mistral:latest",
            base_url="http://127.0.0.1:11434",
            timeout_seconds=60,
            temperature=0.0,
        ),
    )
    llama_path = Path("../output/evaluation/preds_llama.csv")
    mistral_path = Path("../output/evaluation/preds_mistral.csv")

    llama_rows = list(csv.DictReader(llama_path.open("r", encoding="utf-8", newline="")))
    mistral_rows = list(csv.DictReader(mistral_path.open("r", encoding="utf-8", newline="")))

    if len(llama_rows) != len(mistral_rows):
        print(f"WARN: row count differs llama={len(llama_rows)} mistral={len(mistral_rows)}")

    by_key_mistral = {
        (str(r["receipt_id"]), str(r["item_text"])): r
        for r in mistral_rows
    }

    examples = []
    for lr in llama_rows:
        key = (str(lr["receipt_id"]), str(lr["item_text"]))
        mr = by_key_mistral.get(key)
        if not mr:
            continue

        true_cat = str(lr["true_category"])
        llama_pred = str(lr["pred_category"])
        mistral_pred = str(mr["pred_category"])

        if llama_pred == true_cat and mistral_pred != true_cat:
            raw, parsed_ok, allowed, raw_cat = mistral_raw_response(key[1])
            live_mistral_pred = MUU_CATEGORY
            live_error = ""
            try:
                live_mistral_pred = mistral_predictor.predict(key[1], False)
            except (error.URLError, TimeoutError, OSError, ValueError) as e:
                live_error = str(e)
            raw_preview = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False)
            if len(raw_preview) > 180:
                raw_preview = raw_preview[:180] + "..."
            examples.append(
                {
                    "receipt_id": key[0],
                    "item_text": key[1],
                    "true_category": true_cat,
                    "llama_pred": llama_pred,
                    "mistral_pred": mistral_pred,
                    "mistral_parsed_ok": parsed_ok,
                    "mistral_allowed_category": allowed,
                    "mistral_raw_category": raw_cat,
                    "mistral_raw_preview": raw_preview,
                    "mistral_live_predictor_pred": live_mistral_pred,
                    "mistral_live_predictor_error": live_error,
                }
            )

    print("TOTAL_EXAMPLES", len(examples))
    for i, e in enumerate(examples[:15], 1):
        print("---")
        print("IDX", i)
        print("ITEM", e["item_text"])
        print("TRUE", e["true_category"])
        print("LLAMA", e["llama_pred"])
        print("MISTRAL", e["mistral_pred"])
        print("MISTRAL_PARSED_OK", e["mistral_parsed_ok"])
        print("MISTRAL_ALLOWED_CATEGORY", e["mistral_allowed_category"])
        print("MISTRAL_RAW_CATEGORY", e["mistral_raw_category"])
        print("MISTRAL_LIVE_PREDICTOR_PRED", e["mistral_live_predictor_pred"])
        print("MISTRAL_LIVE_PREDICTOR_ERROR", e["mistral_live_predictor_error"])
        print("MISTRAL_RAW_RESPONSE", e["mistral_raw_preview"])


if __name__ == "__main__":
    main()
