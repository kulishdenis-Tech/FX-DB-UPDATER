# === fx_parse_SWAPS_auto.py ===
"""
CLOUD-версія класичного локального парсера SWAPS.
Логіка парсингу — 100% як у тебе.
Єдина різниця: дані читаються з Supabase RAW і пишуться у таблицю rates.
"""

import re
from supabase import create_client
import os

# === Налаштування Supabase ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")

client = create_client(SUPABASE_URL, SUPABASE_KEY)
CHANNEL = "SWAPS"

# === Твоя оригінальна регулярка ===
CURRENCY_RE = re.compile(
    r"(?mi)^\s*([A-Z]{3})\s*[-/–]\s*([A-Z]{3})[^\d\n]*?([0-9]+[.,][0-9]+)\s*/\s*([0-9]+[.,][0-9]+)"
)

def norm_price_auto(s: str):
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


# === Отримання channel_id (створює, якщо нема) ===
def get_channel_id(channel_name: str) -> int:
    data = client.table("channels").select("id").eq("name", channel_name).execute()
    if data.data:
        return data.data[0]["id"]
    new = client.table("channels").insert({"name": channel_name}).execute()
    return new.data[0]["id"]


# === Отримання RAW з Supabase ===
def get_raw_from_supabase(filename: str) -> str:
    try:
        res = client.storage.from_(RAW_BUCKET).download(filename)
        return res.decode("utf-8") if res else None
    except Exception as e:
        print(f"[ERROR] RAW не знайдено ({filename}): {e}")
        return None


# === Отримання попередніх курсів ===
def get_prev_rates(channel_id: int) -> dict:
    try:
        data = client.table("rates").select(
            "currency_a, currency_b, buy, sell, comment"
        ).eq("channel_id", channel_id).order("id", desc=True).limit(500).execute()

        result = {}
        for r in data.data:
            key = (r["currency_a"], r["currency_b"], r.get("comment", ""))
            result[key] = (r["buy"], r["sell"])
        return result
    except Exception as e:
        print(f"[ERROR] Отримання попередніх курсів: {e}")
        return {}


# === Основна функція парсингу ===
def parse_text(text, prev, channel_id):
    rows, skipped = [], 0
    msg_id, version = 0, "v1"
    published = edited = None

    for line in text.splitlines():
        m = CURRENCY_RE.search(line)
        if not m:
            continue

        a, b, buy, sell = m.groups()
        buy, sell = norm_price_auto(buy), norm_price_auto(sell)
        comment = ""

        key = (a, b, comment)
        if not is_rate_changed((buy, sell), prev.get(key)):
            skipped += 1
            continue
        prev[key] = (buy, sell)

        row = {
            "channel_id": channel_id,
            "message_id": msg_id,
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
    print(f"\n[RUN] 🔍 Парсинг {CHANNEL}")
    channel_id = get_channel_id(CHANNEL)
    print(f"[CLOUD] ✅ channel_id={channel_id}")

    text = get_raw_from_supabase(f"{CHANNEL}_raw.txt")
    if not text:
        print(f"[WARN] RAW {CHANNEL}_raw.txt порожній або не знайдено")
        return

    prev = get_prev_rates(channel_id)
    rows, skipped = parse_text(text, prev, channel_id)

    if not rows:
        print(f"[CLOUD] ⏩ Немає нових рядків ({CHANNEL})")
        return

    try:
        res = client.table("rates").insert(rows).execute()
        added = len(res.data) if res.data else 0
        print(f"[CLOUD] ✅ Додано {added} рядків ({CHANNEL}), пропущено {skipped}")
    except Exception as e:
        print(f"[ERROR] Не вдалося записати у Supabase: {e}")


if __name__ == "__main__":
    parse_once()
