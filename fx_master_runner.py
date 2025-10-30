# === fx_master_runner.py ===
"""
FX Master Runner — мінімалістичний cloud-runner для Render.
- Інжектить у sys.modules "parser_utils_v2", "db_adapter_cloud", "version_control"
- Зчитує RAW з Supabase Storage (bucket=RAW_BUCKET)
- Імпортує 7 парсерів (fx_parse_*) та виконує parse_once()
- Зберігає дані у таблицю rates Supabase
- Виводить логи по кожному каналу
"""

import os, sys, types, importlib, traceback
from datetime import datetime

# ────────────────────────────────────────────────
# 0️⃣ ENV keys
# ────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RAW_BUCKET   = os.getenv("RAW_BUCKET", "raw")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ SUPABASE_URL / SUPABASE_KEY must be set in environment.")

# ────────────────────────────────────────────────
# 1️⃣ db_adapter_cloud
# ────────────────────────────────────────────────
db_adapter_cloud = types.ModuleType("db_adapter_cloud")
exec(r"""
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

class SupabaseAdapter:
    def __init__(self):
        self.client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("[CLOUD] ✅ Підключено до Supabase")

    def insert_rates(self, channel, rows):
        ch_id = self._get_or_create_channel(channel)
        inserted = 0
        batch_size = 10
        for i in range(0, len(rows), batch_size):
            payload = []
            for r in rows[i:i+batch_size]:
                payload.append({
                    "channel_id": ch_id,
                    "message_id": int(r[1]) if r[1] else None,
                    "version": r[2],
                    "published": r[3],
                    "edited": r[4],
                    "currency_a": r[5],
                    "currency_b": r[6],
                    "buy": float(r[7]) if r[7] is not None else None,
                    "sell": float(r[8]) if r[8] is not None else None,
                    "comment": r[9],
                })
            self.client.table("rates").insert(payload).execute()
            inserted += len(payload)
        return inserted, 0

    def _get_or_create_channel(self, name):
        resp = self.client.table("channels").select("id").eq("name", name).execute()
        if resp.data:
            return resp.data[0]["id"]
        ins = self.client.table("channels").insert({"name": name}).execute()
        return ins.data[0]["id"]
""", db_adapter_cloud.__dict__)
sys.modules["db_adapter_cloud"] = db_adapter_cloud

# ────────────────────────────────────────────────
# 2️⃣ version_control (дедуп з БД)
# ────────────────────────────────────────────────
version_control = types.ModuleType("version_control")
exec(r"""
import os
from db_adapter_cloud import SupabaseAdapter
_cloud = SupabaseAdapter()

def _channel_from_output_path(path: str) -> str:
    base = os.path.basename(path)
    if base.endswith("_parsed.csv"):
        return base[:-12]
    return base

def load_last_rates(output_file_path: str):
    ch_name = _channel_from_output_path(output_file_path)
    ch_id = _cloud._get_or_create_channel(ch_name)
    prev = {}
    resp = _cloud.client.table("rates") \
        .select("currency_a,currency_b,buy,sell,comment") \
        .eq("channel_id", ch_id) \
        .order("published", desc=True) \
        .limit(2000).execute()
    for r in resp.data or []:
        a, b = r["currency_a"], r["currency_b"]
        buy, sell = r["buy"], r["sell"]
        comment = (r.get("comment") or "").strip()
        prev[(a, b)] = (buy, sell)
        prev[(a, b, comment)] = (buy, sell)
    return prev

def is_rate_changed(new_rate, old_rate):
    if not old_rate:
        return True
    _, _, nb, ns = new_rate
    ob, os = old_rate
    try:
        return round(float(nb), 4) != round(float(ob), 4) or round(float(ns), 4) != round(float(os), 4)
    except:
        return True
""", version_control.__dict__)
sys.modules["version_control"] = version_control

# ────────────────────────────────────────────────
# 3️⃣ parser_utils_v2 (cloud-only)
# ────────────────────────────────────────────────
parser_utils_v2 = types.ModuleType("parser_utils_v2")
exec(rf"""
import os
from typing import List, Optional
from db_adapter_cloud import SupabaseAdapter
RAW_BUCKET = os.getenv("RAW_BUCKET", "{RAW_BUCKET}")
_cloud = SupabaseAdapter()
LAST_SAVE_STATS = {{}}  # channel -> dict(inserted, provided)

def save_rows(rows: List[list], output_file_ignored: str = ""):
    if not rows:
        return
    channel = rows[0][0] if rows else "UNKNOWN"
    try:
        inserted, skipped = _cloud.insert_rates(channel, rows)
        _channel_name = rows[0][0] if rows and len(rows[0]) > 0 else "UNKNOWN"
        print(f"[CLOUD] {{_channel_name:<12}} → додано: {{inserted}} (із {{len(rows)}})")
        LAST_SAVE_STATS[channel] = {{"inserted": inserted, "provided": len(rows)}}
    except Exception as e:
        print(f"[ERROR] Supabase insert: {{e}}")

def save_to_csv(*args, **kwargs):
    return

def norm_price_auto(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).replace(",", ".").replace(" ", "").strip()
    try:
        return float(s)
    except:
        return None

def detect_currency(line: str) -> Optional[str]:
    currencies = ["USD","EUR","PLN","GBP","CHF","CAD","CZK","SEK","JPY","NOK","DKK","UAH"]
    up = line.upper()
    for cur in currencies:
        if cur in up:
            return cur
    return None

def iter_message_blocks(lines: List[str], id_re):
    block = []
    for line in lines:
        if id_re.search(line) and block:
            yield block
            block = []
        block.append(line)
    if block:
        yield block

def read_raw_text(channel: str) -> Optional[str]:
    try:
        blob = _cloud.client.storage.from_(RAW_BUCKET).download(f"{{channel}}_raw.txt")
        return blob.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[WARN] RAW не знайдено у Storage ({{RAW_BUCKET}}/{{channel}}_raw.txt): {{e}}")
        return None
""", parser_utils_v2.__dict__)
sys.modules["parser_utils_v2"] = parser_utils_v2

# ────────────────────────────────────────────────
# 4️⃣ Канали та парсери
# ────────────────────────────────────────────────
CHANNELS = {
    "GARANT":       "fx_parse_GARANT_auto",
    "KIT_GROUP":    "fx_parse_KIT_GROUP_auto",
    "VALUTA_KIEV":  "fx_parse_VALUTA_KIEV_auto",
    "MIRVALUTY":    "fx_parse_MIRVALUTY_auto",
    "CHANGE_KYIV":  "fx_parse_CHANGE_KYIV_auto",
    "UACOIN":       "fx_parse_UACOIN_auto",
    "SWAPS":        "fx_parse_SWAPS_auto",
}

# ────────────────────────────────────────────────
# 5️⃣ Runner
# ────────────────────────────────────────────────
def run():
    print("\n=== 🌍 FX Master Runner (Supabase Cloud Mode) ===")
    print(f"[START] {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"[INFO] RAW bucket: {RAW_BUCKET}")
    print(f"[INFO] Каналів до обробки: {len(CHANNELS)}\n")

    total_ok = total_skip = total_err = 0

    for ch_name, module_name in CHANNELS.items():
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"[RUN] 🔎 Канал: {ch_name}")

        try:
            raw = parser_utils_v2.read_raw_text(ch_name)
            if not raw:
                print(f"[SKIP] RAW для {ch_name} відсутній — пропуск.\n")
                total_skip += 1
                continue

            mod = importlib.import_module(module_name)
            if not hasattr(mod, "parse_once"):
                print(f"[ERR] У модулі {module_name} відсутня parse_once().")
                total_skip += 1
                continue

            before = datetime.utcnow()
            mod.parse_once()
            after = datetime.utcnow()
            elapsed = (after - before).total_seconds()

            stats = parser_utils_v2.LAST_SAVE_STATS.get(ch_name, {})
            ins = stats.get("inserted", 0)
            prov = stats.get("provided", 0)

            print(f"[OK] ✅ {ch_name} → додано {ins} із {prov} | {elapsed:.1f}s\n")
            total_ok += 1

        except Exception as e:
            total_err += 1
            print(f"[ERROR] ❌ {ch_name}: {e}")
            traceback.print_exc()
            print()

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🏁 [DONE] {datetime.utcnow().strftime('%H:%M:%S')} UTC")
    print(f"📊 Підсумок: OK={total_ok} | SKIP={total_skip} | ERRORS={total_err}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

if __name__ == "__main__":
    run()
