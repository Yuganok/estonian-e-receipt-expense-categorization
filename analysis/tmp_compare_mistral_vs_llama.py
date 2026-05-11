import csv
import json
from urllib import request

from evaluation_predictors import EvaluationPredictor, OllamaConfig
from categorize import MAIN_EVAL_CATEGORIES, MUU_CATEGORY

GOLD = "../output/evaluation/gold_to_review.csv"
URL = "http://127.0.0.1:11434/api/generate"
LABELS = list(MAIN_EVAL_CATEGORIES)

PROMPT_PREFIX = """You are a classifier for grocery store receipt line items.
Classify the item into exactly one category.

Categories:
- Toidukaubad ja alkoholivabad joogid: food, beverages (non-alcoholic), snacks, dairy, meat, bread
- Alkohol ja tubakas: beer, wine, spirits, tobacco, cigarettes
- Majapidamis- ja puhastusvahendid: cleaning products, detergents, household supplies, paper products
- Majapidamistehnika: appliances, electronics, kitchen equipment
- Lilled ja kingitused: flowers, gifts, greeting cards
- Muu: anything that does not fit the above

Examples:
Item: "PIIM 2.5% 1L" -> {"category": "Toidukaubad ja alkoholivabad joogid"}
Item: "ÕLUT SAKU 0.5L" -> {"category": "Alkohol ja tubakas"}
Item: "PESUPULBER 3KG" -> {"category": "Majapidamis- ja puhastusvahendid"}
Item: "ROOS PUNANE" -> {"category": "Lilled ja kingitused"}

Return ONLY JSON: {"category": "<category>"}
"""


def mistral_raw(item_text: str):
    prompt = PROMPT_PREFIX + f'Item: "{item_text}"'
    payload = {
        "model": "mistral:latest",
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0},
    }
    try:
        req = request.Request(
            URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=35) as resp:
            body = resp.read().decode("utf-8")
        decoded = json.loads(body)
        raw = decoded.get("response", "")
        parsed_ok = False
        allowed = False
        category = MUU_CATEGORY
        parse_error = ""
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                parsed_ok = True
            except Exception as e:
                parsed = {}
                parse_error = str(e)
        elif isinstance(raw, dict):
            parsed = raw
            parsed_ok = True
        else:
            parsed = {}
        c = str(parsed.get("category", "")).strip() if isinstance(parsed, dict) else ""
        if c in LABELS:
            category = c
            allowed = True
        else:
            category = MUU_CATEGORY
        return category, raw, parsed_ok, allowed, parse_error
    except Exception as e:
        return MUU_CATEGORY, "", False, False, str(e)


def main():
    llama = EvaluationPredictor(
        approach="llm",
        labels=LABELS,
        ollama=OllamaConfig(
            model="llama3.2:3b",
            base_url="http://127.0.0.1:11434",
            timeout_seconds=35,
            temperature=0.0,
        ),
    )
    rows_all = list(csv.DictReader(open(GOLD, encoding="utf-8")))
    focus_categories = {
        "Majapidamis- ja puhastusvahendid",
        "Alkohol ja tubakas",
        "Muu",
        "Lilled ja kingitused",
        "Majapidamistehnika",
    }
    rows = [r for r in rows_all if str(r.get("true_category") or "").strip() in focus_categories]
    examples = []
    checked = 0
    for r in rows:
        item = str(r.get("item_text") or "").strip()
        true = str(r.get("true_category") or "").strip()
        if not item or not true:
            continue
        checked += 1
        print(f"PROGRESS {checked}", flush=True)
        llama_pred = llama.predict(item, False)
        mistral_pred, raw, parsed_ok, allowed, parse_error = mistral_raw(item)
        if llama_pred == true and mistral_pred != true:
            raw_preview = raw
            if isinstance(raw_preview, str) and len(raw_preview) > 160:
                raw_preview = raw_preview[:160] + "..."
            examples.append((item, true, llama_pred, mistral_pred, parsed_ok, allowed, parse_error, raw_preview))
        if len(examples) >= 6:
            break
        if checked >= 30:
            break

    print("TOTAL_EXAMPLES", len(examples))
    print("ROWS_CHECKED", checked)
    for idx, e in enumerate(examples[:6], 1):
        print("---")
        print("IDX", idx)
        print("ITEM", e[0])
        print("TRUE", e[1])
        print("LLAMA", e[2])
        print("MISTRAL", e[3])
        print("PARSED_OK", e[4])
        print("ALLOWED_CATEGORY", e[5])
        print("PARSE_ERROR", e[6])
        print("RAW_PREVIEW", e[7])


if __name__ == "__main__":
    main()
