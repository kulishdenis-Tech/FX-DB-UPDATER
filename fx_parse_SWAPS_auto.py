# === fx_parse_SWAPS_auto_v2.py ===
"""
SWAPS Parser v2 — з повною структурою даних і підтримкою часів, версій, коментарів.
"""

import re
from datetime import datetime
from supabase_io import get_raw_from_supabase, get_prev_rates, save_to_supabase, get_channel_id

CHANNEL = "SWAPS"

# === Регулярні вирази ===
CURRENCY_RE = re.compile(
    r"(?mi)^\s*([A-Z]{3})\s*[-/]\s*([A-Z]{3})[^\d\n]*?([0-9]+[.,][0-9]+)\s*/\s*([0-9]+[.,][0-9]+)"
)

MESSAGE_ID_RE = re.compile(r"ID[:=]?\s*(\d+)")
VERSION_RE = re.compile(r"\bv(\d+)\b", re.IGNORECASE)
DATE_RE = re.compile(r"(\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})")

# === Нормалізація чисел ===
def to_float(value):
    try:
        return float(str(value).replace(",", ".").strip())
    except:
        return None

# === Визначення змін курсу ===
def is_changed(new, old):
    if not old:
        return True
    try:
        nb, ns = new
        ob, os = old
        return abs(nb - ob) > 0.001 or abs(ns - os) > 0.001
    except:
        return True

# === Парсинг тексту ===
def parse_text(text, prev_rates, channel_id):
    lines = text.splitlines()
    rows, skipped = [], 0
    message_id, version, published, edited = 0, "v1", None, None

    # Пошук додаткових даних у тексті
    msg_match = MESSAGE_ID_RE.search(text)
    ver_match = VERSION_RE.search(text)
    date_matches = DATE_RE.findall(text)

    if msg_match:
        message_id = int(msg_match.group(1))
    if ver_match:
        version = f"v{ver_match.group(1)}"
    if len(date_matches) >= 1:
        published = datetime.strptime(date_matches[0], "%Y-%m-%d %H:%M:%S")
    if len(date_matches) >= 2:
        edited = datetime.strptime(date_matches[1], "%Y-%m-%d %H:%M:%S")

    # Основна логіка по рядках
    for line in lines:
        m = CURRENCY_RE.search(line)
        if not m:
            continue

        a, b, buy, sell = m.groups()
        buy, sell = to_float(buy), to_float(sell)
        comment = ""

        key = (a, b, comment)
        if not is_changed((buy, sell), prev_rates.get(key)):
            skipped += 1
            continue

        prev_rates[key] = (buy, sell)

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


# === Основна функція ===
def parse_once():
    print(f"\n[RUN] 🔍 Парсинг {CHANNEL}")

    channel_id = get_channel_id(CHANNEL)
    print(f"[CLOUD] ✅ channel_id={channel_id}")

    raw_text = get_raw_from_supabase(f"{CHANNEL}_raw.txt")
    if not raw_text:
        print(f"[WARN] RAW {CHANNEL}_raw.txt порожній або не знайдено")
        return

    prev = get_prev_rates(CHANNEL)
    rows, skipped = parse_text(raw_text, prev, channel_id)

    print(f"[DEBUG] Готово до запису: {len(rows)} рядків (пропущено {skipped})")
    if rows:
        print(f"[DEBUG] Приклад рядка: {rows[0]}")

    inserted = save_to_supabase(rows, CHANNEL)
    print(f"[OK] {CHANNEL} → додано {inserted}, пропущено {skipped}")


if __name__ == "__main__":
    parse_once()
