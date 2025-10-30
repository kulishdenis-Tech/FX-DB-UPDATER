# === fx_parse_SWAPS_auto.py (CLOUD STABLE) ===
"""
Повна оригінальна логіка парсингу SWAPS, адаптована під Supabase.
Вхід: RAW-файл із Supabase Storage (bucket 'raw')
Вихід: таблиця 'rates' у Supabase Database
"""

import re
from supabase_io import get_raw_from_supabase, get_prev_rates, save_to_supabase

CHANNEL = "SWAPS"

# === Регулярка з оригінального парсера ===
CURRENCY_RE = re.compile(
    r"""
    ^\s*
    (?:[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s*)?   # прапорці (опціонально)
    (?P<a>[A-Z]{3})\s*[-/]\s*(?P<b>[A-Z]{3})                              # пари типу USD-UAH
    [^\d\r\n]*?
    (?P<buy>[0-9]+[.,][0-9]+)\s*/\s*(?P<sell>[0-9]+[.,][0-9]+)            # курси
    """,
    re.VERBOSE | re.MULTILINE | re.IGNORECASE,
)

# === Допоміжні функції ===
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


# === Основна логіка парсингу ===
def process_text(text: str, previous_rates: dict):
    rows, skipped = [], 0

    # Типи полів під структуру таблиці Supabase
    channel_id = None     # FK, можна залишити None
    message_id = 0        # int8
    version = "v1"        # text
    published = None      # timestamp
    edited = None         # timestamp

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

        row = {
            "channel_id": channel_id,
            "message_id": message_id,
            "version": version,
            "published": published,
            "edited": edited,
            "currency_a": a,
            "currency_b": b,
            "buy": buy,
            "sell": sell,
            "comment": comment,
        }
        rows.append(row)

    return rows, skipped


# === Основний запуск ===
def parse_once():
    print(f"\n[RUN] 🔎 Парсинг {CHANNEL}")

    # 1️⃣ Отримання RAW з Supabase
    text = get_raw_from_supabase(f"{CHANNEL}_raw.txt")
    if not text:
        print(f"[WARN] RAW для {CHANNEL} не знайдено у Supabase Storage.")
        return

    # 2️⃣ Завантаження попередніх курсів
    previous_rates = get_prev_rates(CHANNEL)

    # 3️⃣ Парсинг тексту
    rows, skipped = process_text(text, previous_rates)

    print(f"[DEBUG] Перед записом у Supabase: {len(rows)} рядків")
    if rows:
        example = rows[0]
        print(f"[DEBUG] Приклад рядка для вставки: {example}")

    # 4️⃣ Запис у Supabase
    try:
        inserted = save_to_supabase(rows, CHANNEL)
        print(f"[OK] {CHANNEL} → додано {inserted}, пропущено {skipped}")
    except Exception as e:
        print(f"[ERROR] Не вдалося записати у Supabase ({CHANNEL}): {e}")


if __name__ == "__main__":
    parse_once()
