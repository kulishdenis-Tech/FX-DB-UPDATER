# === db_adapter_cloud.py ===
import os
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]


class SupabaseAdapter:
    """Supabase адаптер для FX-парсерів (REST API + авто-створення каналів)."""

    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("⚠️  Не знайдено ключі Supabase у .env")
        self.client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("[CLOUD] ✅ Підключено до Supabase")

    def insert_rates(self, channel, rows):
        """Записує курси валют у таблицю rates, створює канал якщо треба."""
        ch_id = self._get_or_create_channel(channel)
        inserted, skipped = 0, 0

        # Батчування (по 10 рядків за раз)
        batch_size = 10
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            payload = []
            for r in batch:
                record = {
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
                }
                payload.append(record)
            self.client.table("rates").insert(payload).execute()
            inserted += len(payload)

        print(f"[CLOUD] 🌐 {channel}: додано {inserted}, пропущено {skipped}")
        return inserted, skipped

    def _get_or_create_channel(self, name):
        resp = self.client.table("channels").select("id").eq("name", name).execute()
        if resp.data:
            return resp.data[0]["id"]
        insert = self.client.table("channels").insert({"name": name}).execute()
        return insert.data[0]["id"]
