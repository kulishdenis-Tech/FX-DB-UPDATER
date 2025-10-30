# === supabase_io.py ===
"""
Мінімальний модуль роботи з Supabase:
- get_raw_from_supabase(filename)  → читає сирий текст із bucket 'raw'
- get_prev_rates(channel)          → повертає попередні курси з таблиці 'rates'
- save_to_supabase(rows, channel)  → вставляє нові рядки в таблицю 'rates'
"""

import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")

client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_raw_from_supabase(filename: str) -> str:
    """Завантажує сирий файл з bucket 'raw'."""
    try:
        blob = client.storage.from_(RAW_BUCKET).download(filename)
        return blob.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[WARN] Не вдалося завантажити {filename} з Supabase: {e}")
        return ""

def get_prev_rates(channel: str, limit: int = 1000) -> dict:
    """Отримує останні курси з таблиці 'rates' для дедуплікації."""
    prev = {}
    try:
        resp = client.table("channels").select("id").eq("name", channel).execute()
        if not resp.data:
            return prev
        ch_id = resp.data[0]["id"]

        data = client.table("rates") \
            .select("currency_a,currency_b,buy,sell,comment") \
            .eq("channel_id", ch_id) \
            .order("published", desc=True) \
            .limit(limit) \
            .execute()

        for r in data.data or []:
            key = (r["currency_a"], r["currency_b"], r.get("comment", "").strip())
            prev[key] = (r["buy"], r["sell"])
    except Exception as e:
        print(f"[WARN] Не вдалося отримати попередні дані для {channel}: {e}")
    return prev

def save_to_supabase(rows: list, channel: str):
    """Записує розпарсені рядки у таблицю 'rates'."""
    if not rows:
        return 0

    try:
        ch = client.table("channels").select("id").eq("name", channel).execute()
        if ch.data:
            ch_id = ch.data[0]["id"]
        else:
            inserted = client.table("channels").insert({"name": channel}).execute()
            ch_id = inserted.data[0]["id"]

        payload = []
        for r in rows:
            payload.append({
                "channel_id": ch_id,
                "message_id": int(r[1]) if r[1] else None,
                "version": r[2],
                "published": r[3],
                "edited": r[4],
                "currency_a": r[5],
                "currency_b": r[6],
                "buy": float(r[7]) if r[7] else None,
                "sell": float(r[8]) if r[8] else None,
                "comment": r[9],
            })

        client.table("rates").insert(payload).execute()
        print(f"[CLOUD] {channel} → додано {len(rows)} записів у Supabase")
        return len(rows)

    except Exception as e:
        print(f"[ERROR] Не вдалося записати у Supabase ({channel}): {e}")
        return 0
