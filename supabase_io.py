# === supabase_io.py ===
# Уніфікований модуль для роботи з Supabase
# Об'єднує Storage (RAW файли) + Database (rates таблиця)

import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
BUCKET_NAME = os.environ.get("RAW_BUCKET", "raw")  # Використовуємо RAW_BUCKET з Render або "raw" як fallback


# ============================================
# 📦 STORAGE OPERATIONS (RAW files)
# ============================================

def get_storage_client() -> Client:
    """Клієнт для роботи з Storage"""
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def download_text(name: str) -> str:
    """
    Завантажує файл з bucket 'raw' і повертає вміст як UTF-8 рядок.
    Якщо файл не існує — повертає порожній рядок.
    """
    sb = get_storage_client()
    try:
        data = sb.storage.from_(BUCKET_NAME).download(name)
        if not data:
            print(f"[SUPABASE] ⚠️ File not found: {name}", flush=True)
            return ""
        return data.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[SUPABASE] ⚠️ Download error for {name}: {e}", flush=True)
        return ""

def upload_text(name: str, content: str, upsert: bool = True) -> None:
    """
    Завантажує файл до Supabase bucket 'raw'.
    Якщо файл існує — перезаписує (x-upsert: true).
    """
    sb = get_storage_client()
    try:
        headers = {
            "content-type": "text/plain",
            "x-upsert": "true" if upsert else "false",
        }
        sb.storage.from_(BUCKET_NAME).upload(name, content.encode("utf-8"), headers)
        print(f"[SUPABASE] ✅ Uploaded: {name}", flush=True)
    except Exception as e:
        print(f"[SUPABASE] ❌ Upload error for {name}: {e}", flush=True)


# ============================================
# 💾 DATABASE OPERATIONS (rates table)
# ============================================

class SupabaseIO:
    """Уніфікований клас для роботи з Supabase DB"""
    
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("⚠️  Не знайдено ключі Supabase у .env")
        self.client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("[CLOUD] ✅ Підключено до Supabase", flush=True)
    
    def get_existing_records(self, channel):
        """
        Завантажує з БД список вже існуючих записів для каналу.
        Повертає set з кортежів: (message_id, version, currency_a, currency_b, buy, sell, edited)
        """
        ch_id = self._get_or_create_channel(channel)
        
        try:
            resp = self.client.table("rates").select(
                "message_id, version, currency_a, currency_b, buy, sell, edited"
            ).eq("channel_id", ch_id).execute()
            
            existing = set()
            for row in resp.data:
                key = (
                    row['message_id'],
                    row['version'],
                    row['currency_a'],
                    row['currency_b'],
                    float(row['buy']) if row['buy'] else None,
                    float(row['sell']) if row['sell'] else None,
                    row['edited']
                )
                existing.add(key)
            
            return existing
        except Exception as e:
            print(f"[CLOUD] ⚠️ Error getting existing records: {e}", flush=True)
            return set()
    
    def insert_rates(self, channel, rows):
        """Записує курси валют у таблицю rates, фільтруючи дублікати."""
        ch_id = self._get_or_create_channel(channel)
        
        # 1️⃣ Завантажуємо існуючі записи
        existing = self.get_existing_records(channel)
        
        # 2️⃣ Фільтруємо нові записи
        new_rows = []
        for r in rows:
            key = (
                int(r[1]) if r[1] else None,
                r[2],
                r[5],
                r[6],
                float(r[7]) if r[7] else None,
                float(r[8]) if r[8] else None,
                r[4]
            )
            if key not in existing:
                new_rows.append(r)
        
        if not new_rows:
            print(f"[CLOUD] 🌐 {channel}: всі {len(rows)} записів вже є в БД", flush=True)
            return 0, len(rows)
        
        # 3️⃣ Вставляємо тільки нові записи (батчування по 50)
        inserted, skipped = 0, len(rows) - len(new_rows)
        batch_size = 50
        for i in range(0, len(new_rows), batch_size):
            batch = new_rows[i:i+batch_size]
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
            try:
                self.client.table("rates").insert(payload).execute()
                inserted += len(payload)
            except Exception as e:
                print(f"[CLOUD] ⚠️ Batch error: {e}", flush=True)
                skipped += len(payload)
        
        print(f"[CLOUD] 🌐 {channel}: додано {inserted}, пропущено {skipped}", flush=True)
        return inserted, skipped
    
    def _get_or_create_channel(self, name):
        """Отримує або створює канал"""
        resp = self.client.table("channels").select("id").eq("name", name).execute()
        if resp.data:
            return resp.data[0]["id"]
        insert = self.client.table("channels").insert({"name": name}).execute()
        return insert.data[0]["id"]


# ============================================
# 🔍 UTILITY FUNCTIONS
# ============================================

def norm_price_auto(s: str):
    """Нормалізація ціни"""
    if not s:
        return None
    s = s.replace(",", ".").replace(" ", "").strip()
    try:
        return float(s)
    except:
        return None


def iter_message_blocks(lines, id_re):
    """Розбиття повідомлення на логічні блоки"""
    block = []
    for line in lines:
        if id_re.search(line) and block:
            yield block
            block = []
        block.append(line)
    if block:
        yield block

def detect_currency(line: str):
    """Автовизначення валюти"""
    currencies = ["USD", "EUR", "PLN", "GBP", "CHF", "CAD", "CZK", "SEK", "JPY", "NOK", "DKK"]
    for cur in currencies:
        if cur in line.upper():
            return cur
    return None


