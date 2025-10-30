# === fx_parse_SWAPS_auto.py ===
"""
Базовий приклад чистого парсера SWAPS.
Логіка парсингу збережена повністю.
Вхід → RAW із Supabase, вихід → таблиця rates у Supabase.
"""

import re
from supabase_io import get_raw_from_supabase, get_prev_rates, save_to_supabase

CHANNEL = "SWAPS"

# Регулярка та парсинг залишаються ті самі, як у твоїй локальній версії
LINE_RE = re.compile(
    r"^\s*(USD|EUR|PLN|GBP|CHF|CAD|CZK|SEK|JPY|NOK|DKK)\s*[:\-]?\s*([0-9]+[.,][0-9]+)\s*/\s*([0-9]+[.,][0-9]+)",
    re.M | re.I
)

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
        m = LINE_RE.search(line)
        if not m:
            continue

        cur = m.group(1).upper()
        buy, sell = m.group(2).replace(",", "."), m.group(3).replace(",", ".")
        comment = ""

        key = (cur, "UAH", comment)
        if not is_rate_changed((buy, sell), previous_rates.get(key)):
            skipped += 1
            continue

        previous_rates[key] = (buy, sell)
        rows.append([CHANNEL, msg_id, version, published, edited, cur, "UAH", buy, sell, comment])

    return rows, skipped

def parse_once():
    print(f"\n[RUN] 🔎 Парсинг {CHANNEL}")
    raw = get_raw_from_supabase(f"{CHANNEL}_raw.txt")
    if not raw:
        print(f"[WARN] RAW для {CHANNEL} не знайдено у Storage.")
        return

    previous_rates = get_prev_rates(CHANNEL)
    rows, skipped = process_text(raw, previous_rates)
    count = save_to_supabase(rows, CHANNEL)

    print(f"[DONE] {CHANNEL}: додано {count}, пропущено {skipped}\n")

if __name__ == "__main__":
    parse_once()
