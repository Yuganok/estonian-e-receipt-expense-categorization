"""
categorize.py
=============
Rule-based keyword classifier for Estonian supermarket receipt items.
Assigns each item to one of five COICOP-based spending categories.

Categories:
    1. Toidukaubad ja alkoholivabad joogid  (Food and non-alcoholic beverages)
    2. Alkohol ja tubakas                   (Alcohol and tobacco)
    3. Majapidamis- ja puhastusvahendid     (Household and cleaning supplies)
    4. Majapidamistehnika                   (Household appliances)
    5. Lilled ja kingitused                 (Flowers and gifts)
    DEPOSIT                                 (Refundable packaging deposit — excluded)
    Muu                                     (Other / uncategorised)

Logic:
    - Categories are checked in priority order (alcohol before food, etc.)
    - First matching category wins
    - Deposits are flagged separately (is_deposit column from parser)
"""

import csv
import re
from pathlib import Path

try:
    from memory_layer import lookup_manual_category
except ImportError:  # pragma: no cover - support package import from repo root
    from .memory_layer import lookup_manual_category


# ---------------------------------------------------------------------------
# Keyword dictionaries
# Each entry is a list of lowercase substrings. If any appears in the
# lowercased item name, that category is assigned.
# ---------------------------------------------------------------------------

RULES = [
    # ── DEPOSIT (checked first — always exclude from spending) ───────────
    ("DEPOSIT", [
        "metallpurk", "pet pudel", "pandipakend", "pant", "tagatisraha",
        "plastist ühekorrapakend", "metallist ühekorrapakend",
    ]),

    # ── ALCOHOL & TOBACCO ────────────────────────────────────────────────
    ("Alkohol ja tubakas", [
        "vein", "vahuvein", "punav.", "valgevein", "prosecco", "cava",
        "õlu", "beer", "ipa", "lager", "pale ale", "kronenbourg", "saku",
        "a. le coq", "vana tallinn", "whisky", "viski", "rum", "džinn",
        "gin", "vodka", "liköör", "likõõr", "konjak", "brendy",
        "siider", "cider", "long drink", "gin&tonic",
        "martini", "asti", "prosecco",
        "jägermeister", "bols", "captain morgan",
        "tubakas", "sigaret", "sigar",
        # Wine in Estonian receipts often appears as "KPN"/"KGT" prefix
        "kpn ar.", "kgt vein",
    ]),

    # ── HOUSEHOLD APPLIANCES ─────────────────────────────────────────────
    ("Majapidamistehnika", [
        "röster", "kohvimasin", "blender", "mikser", "veekeetja",
        "triikraud", "tolmuimeja", "pesumasin", "nõudepesumasin",
        "külmkapp", "pliit", "ahi", "mikrolaineahi",
        "õhupuhastaja", "ventilaator", "elektriradiaator",
        "võileivakõpsetaja", "sandwich maker", "waffle",
        "fritüür", "aeglane pliit", "slow cooker",
        "emelia", "tefal", "philips", "bosch", "samsung",
        # High-impact exception from QA: cookware appears in food by default.
        "pann",
    ]),

    # ── FLOWERS & GIFTS ──────────────────────────────────────────────────
    ("Lilled ja kingitused", [
        "lill", "õis", "tulp", "tulbikümp", "krüsanteem", "roos",
        "narciss", "nartsiss", "hüatsint", "aster",
        "lillekimp", "bukett", "õied",
        "kingitus", "gift",
    ]),

    # ── HOUSEHOLD & CLEANING SUPPLIES ────────────────────────────────────
    ("Majapidamis- ja puhastusvahendid", [
        # Paper products
        "wc-paber", "tualettpe", "tualettpaber", "salvrätik", "lehträtik",
        "zewa", "pehme", "paberrätik", "wc paber", "paber", "ostukott",
        # Cleaning
        "pesupulber", "pesukapslid", "pesugeel", "fabric softener",
        "nõudepesuvahend", "loputusvahend", "loputusaine",
        "puhastusvahend", "spray", "yleainesritulaator",
        "wc puhastusvahend", "aken", "klaasipuhastus",
        "prügikott", "kilekott", "suur kilekott", "väike kilekott",
        "õhuvärskendaja", "õhuvärsk", "air wick", "airwick",
        "freshener", "deodorant kodu",
        # Personal care / cosmetics
        "šampoon", "palsam", "juuksepalsam", "dušigeel", "dušikreem",
        "seep", "vedelseep", "hambapasta", "hambahari",
        "raseerimisvahend", "habemeajamisgeel",
        "päikesekreem", "niisutuskreem", "näokreem", "kehakreem",
        "meik", "huulepulk", "maskara", "toonijuus", "nivea",
        "tampoon", "hügieenisidemed", "alushügieensidemed",
        # Household goods / tableware from review set
        "kruus", "taldrik", "kauss", "kahvel", "kahvl", "lusikas", "lusik",
        "vatitikud", "vatitiku", "küünal", "pleed", "voodikate", "keedupott", "söögiriist",
        # Laundry / bags
        "rimi ostukott",
    ]),

    # ── FOOD & NON-ALCOHOLIC BEVERAGES (default / largest category) ──────
    # Listed explicitly to catch common cases; anything else also falls here
    ("Toidukaubad ja alkoholivabad joogid", [
        # Dairy
        "piim", "täispiim", "keefir", "hapukoor", "jogurt", "juust",
        "toorjuust", "kohuke", "koorejogu", "koorejogurt", "valgejuust",
        "ricotta", "mozzarella", "cheddar", "gouda", "emmental", "tilsit",
        "valio", "farmi", "alma", "tere",
        # Meat & fish
        "liha", "veiseliha", "sealiha", "kanafilee", "rinnafilee",
        "kana", "part", "kalkun", "lõhe", "heeringas", "tuunikala",
        "krabipulgad", "krevetid", "rannakarbid",
        "sink", "vorst", "viiner", "peekon", "salami", "karbonaat",
        "pelmeenid", "kotlet",
        # Bread & bakery
        "leib", "sai", "röstsai", "baguette", "ciabatta", "focaccia",
        "kukkel", "pirukad", "pirukas", "kringel", "sõõrik",
        "pannkook", "tort", "kook", "küpsis", "vahvl",
        "haputaina", "lehttaigen",
        # Produce
        "banaan", "õun", "pirn", "apelsin", "sidrun", "mango",
        "maasika", "mustika", "vaarika", "kiivi",
        "tomat", "kurk", "paprika", "sibul", "küüslauk",
        "kartul", "porgand", "kapsas",
        # Eggs
        "munad", "kanamunad",
        # Beverages (non-alcohol)
        "mineraalvesi", "vesi", "mahl", "kirsijook", "karastusjook",
        "coca-cola", "pepsi", "sprite", "fanta", "lipton",
        "energiajook", "red bull", "monster",
        "kohv", "tee", "kakao", "piimajook",
        "smuuti", "smoothie",
        # Dry goods
        "riis", "pasta", "makaronid", "spaghetti", "nuudlid",
        "jahu", "suhkur", "sool", "pipar",
        "õli", "päevalilleõli", "oliivõli",
        "äädikas", "ketšup", "sinep", "majonees",
        "moos", "mesi", "šokolaadikreem", "nutella",
        "granola", "müsli", "kaerahelbed",
        "konserv", "suppkonserv",
        # Snacks & sweets
        "šokolaad", "kommid", "karamell", "närimiskumm",
        "krõpsud", "krõps", "pähklid", "seemned",
        "batoonike", "twix", "snickers", "mars", "bounty",
        "milka", "ritter", "kalev", "skittles",
        # Known false positives from QA dataset that should stay in food.
        "juustupulgad", "võiroos", "biskviidikook", "kaseke",
        # Frozen
        "külmutatud", "külm.", "jäätis",
        # Ready meals
        "supp", "puder", "kiirnuudli", "ramen",
        "burger", "pitsa", "lasanje",
    ]),
]

DEFAULT_CATEGORY = "Toidukaubad ja alkoholivabad joogid"
MUU_CATEGORY = "Muu"

# Six spending categories used for thesis-style evaluation (DEPOSIT excluded).
MAIN_EVAL_CATEGORIES: tuple[str, ...] = (
    DEFAULT_CATEGORY,
    "Alkohol ja tubakas",
    "Majapidamis- ja puhastusvahendid",
    "Majapidamistehnika",
    "Lilled ja kingitused",
    MUU_CATEGORY,
)

# Receipt credit / refund lines (not grocery products) -> Muu + rule_match
_REFUND_LINE_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"получен\w*\s+обратно", re.IGNORECASE), "получено обратно"),
    (re.compile(r"\bвозврат\b", re.IGNORECASE), "возврат"),
    (re.compile(r"tagas[ie]\s+saadu", re.IGNORECASE), "tagasi saadud"),
    (re.compile(r"tagastat", re.IGNORECASE), "tagastatud"),
    (re.compile(r"tagasimakse", re.IGNORECASE), "tagasimakse"),
    (re.compile(r"raha\s+tagasi", re.IGNORECASE), "raha tagasi"),
]

# category_source labels for thesis-facing transparency
SOURCE_DEPOSIT = "deposit"
SOURCE_MANUAL_MEMORY = "manual_memory"
SOURCE_RULE_MATCH = "rule_match"
SOURCE_FALLBACK_FOOD = "fallback_food"
SOURCE_UNKNOWN = "unknown"

_UNIT_TOKENS = {
    "g", "kg", "mg", "ml", "cl", "l",
    "tk", "pcs", "pc", "x",
    "eur", "euro", "€",
}
_NOISE_WORDS = {
    "summa", "kokku", "allahindlus", "discount", "subtotal", "total",
    "rida", "line", "item", "qty", "quantity", "hind", "price",
    "km", "nso", "kassir", "cashier", "saldo", "balance",
    "makstud", "paid", "kaart", "card", "visa", "mastercard",
}
_NON_PRODUCT_TOKEN_RE = re.compile(r"^\d+(?:[.,]\d+)?$|^[€$£]+$|^[xX*+\-]+$")
_LEXICAL_TOKEN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-ž]{2,}", re.UNICODE)
_NOISE_LINE_RE = re.compile(
    r"^\s*(?:"
    r"\d+(?:[.,]\d+)?\s*(?:tk|kg|g|l|ml)\s*[\d.,]+\s*€?\s*[\d.,]+\s*€?"
    r"|[\d\s.,€$£:/\-]+"
    r")\s*$",
    re.IGNORECASE,
)
_STRICT_WORD_KEYWORDS = {
    "riis", "pann",
    "kruus", "taldrik", "kauss", "vatitikud", "vatitiku",
    "nivea", "küünal", "pleed", "voodikate", "keedupott",
}


def classify(item_text: str, already_deposit: bool) -> tuple[str, str]:
    """
    Return the spending category for an item.
    Deposits are always returned as DEPOSIT regardless of text match.
    Returns (category, matched_keyword).
    """
    if already_deposit:
        return "DEPOSIT", "is_deposit flag"

    raw = item_text or ""
    for rx, label in _REFUND_LINE_RULES:
        if rx.search(raw):
            return MUU_CATEGORY, label

    text_low = raw.lower()

    for category, keywords in RULES:
        for kw in keywords:
            if kw and _keyword_matches(text_low, kw.lower()):
                return category, kw

    # Default: assume food if nothing matched
    return DEFAULT_CATEGORY, ""


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _keyword_matches(text_low: str, keyword_low: str) -> bool:
    """
    Mixed strategy:
    - default keywords: substring match (legacy behavior)
    - conflict-prone short keywords (riis/pann): whole-word match only
    """
    if keyword_low in _STRICT_WORD_KEYWORDS:
        return bool(re.search(rf"(?<!\w){re.escape(keyword_low)}(?!\w)", text_low, re.IGNORECASE))
    return keyword_low in text_low


def _looks_like_unknown_row(item_text: str) -> bool:
    """
    Conservative unknown detector:
    - mark unknown only for clearly broken/non-product rows
    - keep normal unmatched product-like lines for fallback_food
    """
    normalized = _normalize_text(item_text)
    if not normalized:
        return True

    if _NOISE_LINE_RE.match(normalized):
        # If the line has a real lexical product token, keep as fallback_food.
        if _LEXICAL_TOKEN_RE.search(normalized):
            return False
        return True

    raw_tokens = re.split(r"\s+", normalized)
    lexical_tokens = []
    for token in raw_tokens:
        clean = re.sub(r"^[^\wÀ-ž]+|[^\wÀ-ž]+$", "", token).lower()
        if not clean:
            continue
        if _NON_PRODUCT_TOKEN_RE.match(clean):
            continue
        if clean in _UNIT_TOKENS or clean in _NOISE_WORDS:
            continue
        if _LEXICAL_TOKEN_RE.search(clean):
            lexical_tokens.append(clean)

    return len(lexical_tokens) == 0


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def categorize_items(items_in: Path, items_out: Path, memory_db_path: Path | str | None = None):
    """
    Read items_raw.csv, add 'category' column, write items_categorized.csv.
    """
    items_in = Path(items_in)
    items_out = Path(items_out)

    rows = []
    counts = {}
    in_fieldnames: list[str] = []

    with open(items_in, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fieldnames = list(reader.fieldnames or [])
        for row in reader:
            item_text = row.get("item_text", "")
            store = row.get("store", "")
            is_dep = row.get("is_deposit", "False").lower() in ("true", "1", "yes")
            manual_category = lookup_manual_category(
                store=store,
                item_text=item_text,
                db_path=memory_db_path,
            )
            if manual_category:
                cat = manual_category
                matched_kw = "manual_memory"
                source = SOURCE_MANUAL_MEMORY
            elif _looks_like_unknown_row(item_text):
                cat, matched_kw = classify(item_text, is_dep)
                if cat == "DEPOSIT":
                    source = SOURCE_DEPOSIT
                elif matched_kw:
                    source = SOURCE_RULE_MATCH
                else:
                    cat = MUU_CATEGORY
                    matched_kw = ""
                    source = SOURCE_UNKNOWN
            else:
                cat, matched_kw = classify(item_text, is_dep)
                if cat == "DEPOSIT":
                    source = SOURCE_DEPOSIT
                elif matched_kw:
                    source = SOURCE_RULE_MATCH
                else:
                    source = SOURCE_FALLBACK_FOOD
            row["category"] = cat
            row["category_rule"] = matched_kw
            row["category_source"] = source
            rows.append(row)
            counts[cat] = counts.get(cat, 0) + 1

    # Write output with category column added
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = [c for c in in_fieldnames if c not in {"category", "category_rule", "category_source"}] + [
            "category",
            "category_rule",
            "category_source",
        ]
    with open(items_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"  Categorised {len(rows)} items -> {items_out}")
    print("  Category breakdown:")
    for cat, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"    {cat:<45} {count:>4} items")
