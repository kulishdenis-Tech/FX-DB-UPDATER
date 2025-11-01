# === supabase_io.py ===
# Уніфікований модуль для роботи з Supabase
# Об'єднує Storage (RAW файли) + Database (rates таблиця)

import os
from supabase import create_client, Client
from dotenv import load_dotenv
from decimal import Decimal, InvalidOperation

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
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
            print(f"[SUPABASE] ⚠️ File not found: {name}")
            return ""
        return data.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[SUPABASE] ⚠️ Download error for {name}: {e}")
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
        print(f"[SUPABASE] ✅ Uploaded: {name}")
    except Exception as e:
        print(f"[SUPABASE] ❌ Upload error for {name}: {e}")


# ============================================
# 💾 DATABASE OPERATIONS (rates table)
# ============================================

class SupabaseIO:
    """Уніфікований клас для роботи з Supabase DB"""
    
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
            try:
                self.client.table("rates").insert(payload).execute()
                inserted += len(payload)
            except Exception as e:
                # Дублікат або інша помилка
                print(f"[CLOUD] ⚠️ Batch error: {e}")
                skipped += len(payload)
        
        print(f"[CLOUD] 🌐 {channel}: додано {inserted}, пропущено {skipped}")
        return inserted, skipped
    
    def get_last_rates(self, channel):
        """
        Отримує останні курси для каналу з Supabase БД.
        Повертає словник: {(currency_a, currency_b): (buy, sell)}
        """
        ch_id = self._get_or_create_channel(channel)
        
        try:
            # Отримуємо останні курси для кожної валютної пари
            resp = self.client.table("rates").select(
                "currency_a, currency_b, buy, sell"
            ).eq("channel_id", ch_id).execute()
            
            # Групуємо по парах і беремо останній (просто перший у відповіді)
            last_rates = {}
            for row in resp.data:
                pair_key = (row['currency_a'], row['currency_b'])
                # Якщо вже є пара, залишаємо першу (але це не критично)
                if pair_key not in last_rates:
                    last_rates[pair_key] = (row['buy'], row['sell'])
            
            print(f"[CLOUD] Останні курси в БД: {len(last_rates)} пар")
            return last_rates
        except Exception as e:
            print(f"[CLOUD] ⚠️ Error getting last rates: {e}")
            return {}
    
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

def is_rate_changed(new_rate, last_rate):
    """Порівнює курси числово, щоб ігнорувати 41.5 vs 41.50"""
    if not last_rate:
        return True
    buy_old, sell_old = last_rate
    _, _, buy_new, sell_new = new_rate
    try:
        b_old, s_old = Decimal(buy_old), Decimal(sell_old)
        b_new, s_new = Decimal(buy_new), Decimal(sell_new)
    except InvalidOperation:
        return buy_old != buy_new or sell_old != sell_new
    return (b_old != b_new) or (s_old != s_new)


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

