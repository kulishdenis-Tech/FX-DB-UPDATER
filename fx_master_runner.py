# === fx_master_runner.py (autopatch version) ===
"""
FX Master Runner — Render Edition
• Сам підключає Supabase
• Інжектить parser_utils_v2/db_adapter_cloud/version_control
• Перед запуском автоматично патчить усі fx_parse_* парсери:
  - виправляє import pparser_utils_v2 → parser_utils_v2
  - прибирає локальні шляхи C:\Users\...
  - замінює open(..._raw.txt) на read_raw_text(channel)
"""

import os, sys, types, importlib, traceback, re
from datetime import datetime, timezone

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RAW_BUCKET   = os.getenv("RAW_BUCKET", "raw")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Missing SUPABASE_URL or SUPABASE_KEY environment vars.")

# ────────────────────────────────────────────────
# Supabase adapter
# ────────────────────────────────────────────────
db_adapter_cloud = types.ModuleType("db_adapter_cloud")
exec(r"""
from supabase import create_client

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
# parser_utils_v2 (Supabase mode)
# ────────────────────────────────────────────────
parser_utils_v2 = types.ModuleType("parser_utils_v2")
exec(rf"""
import os
from typing import Optional, List
from db_adapter_cloud import SupabaseAdapter

RAW_BUCKET = os.getenv("RAW_BUCKET", "{RAW_BUCKET}")
_cloud = SupabaseAdapter()
LAST_SAVE_STATS = {{}}

def save_rows(rows: List[list], output_file_ignored: str = ""):
    if not rows: return
    channel = rows[0][0] if rows else "UNKNOWN"
    try:
        inserted, _ = _cloud.insert_rates(channel, rows)
        print(f"[CLOUD] {{channel:<12}} → додано: {{inserted}} (із {{len(rows)}})")
        LAST_SAVE_STATS[channel] = {{"inserted": inserted, "provided": len(rows)}}
    except Exception as e:
        print(f"[ERROR] Supabase insert: {{e}}")

def save_to_csv(*args, **kwargs): pass

def norm_price_auto(s: str) -> Optional[float]:
    if s is None: return None
    s = str(s).replace(",", ".").strip()
    try: return float(s)
    except: return None

def read_raw_text(channel: str) -> Optional[str]:
    try:
        blob = _cloud.client.storage.from_(RAW_BUCKET).download(f"{{channel}}_raw.txt")
        return blob.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[WARN] RAW не знайдено ({{RAW_BUCKET}}/{{channel}}_raw.txt): {{e}}")
        return None
""", parser_utils_v2.__dict__)
sys.modules["parser_utils_v2"] = parser_utils_v2

# ────────────────────────────────────────────────
# autopatch fx_parse_*
# ────────────────────────────────────────────────
def autopatch_parser(file_path: str):
    """Виправляє імпорти і локальні шляхи."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            code = f.read()

        # 1. виправити pparser_utils_v2
        code = re.sub(r"\bpparser_utils_v2\b", "parser_utils_v2", code)

        # 2. прибрати C:\Users\...
        code = re.sub(r"[A-Z]:\\\\Users\\\\.*?RAW.*?\.txt", "", code)

        # 3. замінити open('...raw.txt') на read_raw_text(channel)
        code = re.sub(r"open\(.*?_raw\.txt.*?\)", "read_raw_text(channel)", code)

        # 4. зберегти змінений код
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(code)

    except Exception as e:
        print(f"[WARN] autopatch {file_path}: {e}")

# ────────────────────────────────────────────────
# канали
# ────────────────────────────────────────────────
CHANNELS = {
    "GARANT": "fx_parse_GARANT_auto",
    "KIT_GROUP": "fx_parse_KIT_GROUP_auto",
    "VALUTA_KIEV": "fx_parse_VALUTA_KIEV_auto",
    "MIRVALUTY": "fx_parse_MIRVALUTY_auto",
    "CHANGE_KYIV": "fx_parse_CHANGE_KYIV_auto",
    "UACOIN": "fx_parse_UACOIN_auto",
    "SWAPS": "fx_parse_SWAPS_auto",
}

# ────────────────────────────────────────────────
# runner
# ────────────────────────────────────────────────
def run():
    print("\n=== 🌍 FX Parser Cloud (autopatch mode) ===")
    print(f"[START] {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    total_ok = total_err = 0

    for ch, modname in CHANNELS.items():
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"[RUN] 🔎 Канал: {ch}")

        try:
            autopatch_parser(f"{modname}.py")
            mod = importlib.import_module(modname)

            if not hasattr(mod, "parse_once"):
                print(f"[ERR] ❌ parse_once() відсутня у {modname}")
                total_err += 1
                continue

            mod.parse_once()
            total_ok += 1

        except Exception as e:
            print(f"[ERROR] ❌ {ch}: {e}")
            traceback.print_exc()
            total_err += 1

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🏁 DONE | OK={total_ok} | ERRORS={total_err}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")


if __name__ == "__main__":
    run()
