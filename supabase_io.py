# === supabase_io.py (CLOUD FULL VERSION) ===
"""
Модуль для роботи з Supabase:
- отримання RAW файлів з bucket 'raw'
- читання попередніх курсів із таблиці rates
- запис нових рядків у таблицю rates
- керування таблицею channels
"""

import os
from supabase import create_client

# ініціалізація клієнта Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")

client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === 1️⃣ Отримання RAW файлу з bucket ===
def get_raw_from_supabase(filename: str) -> str:
    try:
        res = client.storage.from_(RAW_BUCKET).download(filename)
        text = res.decode("utf-8") if res else None
        if text:
            print(f"[CLOUD] ✅ RAW {filename} завантажено ({len(text)} символів)")
        else:
            print(f"[CLOUD] ⚠️ RAW {filename} порожній або не знайдено")
        return text
    except Exception as e:
        print(f"[ERROR] Не вдалося отримати RAW ({filename}): {e}")
        return None


# === 2️⃣ Отримання channel_id ===
def get_channel_id(channel_name: str) -> int:
    """
    Повертає ID каналу з таблиці channels.
    Якщо не існує — створює новий запис.
    """
    try:
        data = client.table("channels").select("id").eq("name", channel_name).execute()
        if data.data:
            return data.data[0]["id"]

        # створюємо, якщо не існує
        new = client.table("channels").insert({"name": channel_name}).execute()
        return new.data[0]["id"]
    except Exception as e:
        print(f"[ERROR] get_channel_id({channel_name}): {e}")
        return None


# === 3️⃣ Отримання попередніх курсів ===
def get_prev_rates(channel_name: str) -> dict:
    """
    Завантажує останні курси з таблиці rates для вказаного каналу.
    Використовується для перевірки дублів.
    """
    try:
        ch_id = get_channel_id(channel_name)
        data = client.table("rates").select(
            "currency_a, currency_b, buy, sell, comment"
        ).eq("channel_id", ch_id).order("id", desc=True).limit(500).execute()

        result = {}
        for row in data.data:
            key = (row["currency_a"], row["currency_b"], row.get("comment", ""))
            result[key] = (row["buy"], row["sell"])

        print(f"[CLOUD] 🔁 Завантажено {len(result)} попередніх курсів ({channel_name})")
        return result
    except Exception as e:
        print(f"[ERROR] get_prev_rates({channel_name}): {e}")
        return {}


# === 4️⃣ Запис нових рядків у таблицю rates ===
def save_to_supabase(rows: list, channel_name: str) -> int:
    """
    Записує нові курси у таблицю rates.
    Повертає кількість успішно доданих рядків.
    """
    if not rows:
        print(f"[CLOUD] ⏩ Немає нових рядків для {channel_name}")
        return 0

    try:
        ch_id = get_channel_id(channel_name)
        for r in rows:
            r["channel_id"] = ch_id

        resp = client.table("rates").insert(rows).execute()
        if resp.data:
            count = len(resp.data)
            print(f"[CLOUD] ✅ Додано {count} рядків ({channel_name})")
            return count
        else:
            print(f"[CLOUD] ⚠️ Відповідь без data для {channel_name}")
            return 0
    except Exception as e:
        print(f"[ERROR] Не вдалося записати у Supabase ({channel_name}): {e}")
        return 0
