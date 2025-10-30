# === fx_parse_SWAPS_auto.py (CLOUD VERSION) ===
"""
Оригінальна логіка парсингу SWAPS — без змін.
Вхід:  RAW-файл із Supabase Storage (bucket 'raw')
Вихід: таблиця 'rates' у Supabase Database
"""

import re
from supabase_io import get_raw_from_supabase, get_prev_rates, save_to_supabase

CHANNEL = "SWAPS"

# Твоя оригінальна регулярка та логіка парсингу
CURRENCY_RE = re.compile(
    r"""
    ^\s*
    (?:[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s*)?   # прапорці (опціонально)
    (?P<a>[A-Z]{3})\s*[-/]\s*(?P<b>[A-Z]{3})                              # пари типу USD-UAH або USD/EUR
    [^\d\r\n]*?
    (?P<buy>[0-9]+[.,][0-9]+)\s*/\s*(?P<sell>[0-9]+[.,][0-9]+)            # курси
    """,
    re.VERBOSE | re.MULTILINE | re.IGNORECASE,
)

def norm_price_auto(s: str):
    if s is None:
        return None
    s = str(s).replace(",", ".").replace(" ", "").strip()
    try:
        return float(s)
    except:
        return None

def is_rate_changed(new_rate, old_rate):
    if not old_rate:
        return True
    nb, ns = new_rate
    ob, os = old_rate
    try:
        return round(float(nb), 4) != round(float(ob), 4) or round(float(ns), 4) != round(float(os), 4)
    except:
        return True

def process_text(text: str, previous_rates: dict):
    rows, skipped = [], 0
    msg_id = "auto"
    version = "v1"
    published = edited = "auto"

    for line in text.splitlines():
        m = CURRENCY_RE.search(line)
        if not m:
            continue

        a, b = m.group("a").upper(), m.group("b").upper()
        buy, sell = norm_price_auto(m.group("buy")), norm_price_auto(m.group("sell"))
        comment = ""

        key = (a, b, comment)
        if not is_rate_changed((buy, sell), previous_rates.get(key)):
            skipped += 1
            continue

        previous_rates[key] = (buy, sell)
        rows.append([CHANNEL, msg_id, version, published, edited, a, b, buy, sell, comment])

    return rows, skipped

def parse_once():
    print(f"\n[RUN] 🔎 Парсинг {CHANNEL}")

    text = get_raw_from_supabase(f"{CHANNEL}_raw.txt")
    if not text:
        print(f"[WARN] RAW для {CHANNEL} не знайдено у Supabase Storage.")
        return

    previous_rates = get_prev_rates(CHANNEL)
    rows, skipped = process_text(text, previous_rates)
    count = save_to_supabase(rows, CHANNEL)

    print(f"[DONE] {CHANNEL}: додано {count}, пропущено {skipped}\n")

if __name__ == "__main__":
    parse_once()
