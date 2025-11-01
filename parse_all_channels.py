# === parse_all_channels.py ===
# Launcher для запуску всіх cloud-парсерів на Render

import sys

CHANNELS = [
    ("swaps", "fx_parse_SWAPS_cloud"),
    ("garant", "fx_parse_GARANT_cloud"),
    ("mirvaluty", "fx_parse_MIRVALUTY_cloud"),
    ("kit_group", "fx_parse_KIT_GROUP_cloud"),
    ("change_kyiv", "fx_parse_CHANGE_KYIV_cloud"),
    ("valuta_kiev", "fx_parse_VALUTA_KIEV_cloud"),
    ("uacoin", "fx_parse_UACOIN_cloud"),
]

def main():
    print("=" * 70, flush=True)
    print("🚀 ЗАПУСК УСІХ ПАРСЕРІВ (CLOUD)", flush=True)
    print("=" * 70, flush=True)
    print("[CLOUD] Підключення до Supabase...", flush=True)
    
    total_inserted = 0
    total_skipped = 0
    
    for channel_name, module_name in CHANNELS:
        try:
            module = __import__(module_name)
            process_func = getattr(module, f"process_{channel_name}")
            result = process_func()  # Парсери повертають (inserted, skipped)
            if result:
                inserted, skipped = result
                total_inserted += inserted
                total_skipped += skipped
        except Exception as e:
            print(f"[ERROR] {channel_name}: {e}", flush=True)
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 70, flush=True)
    print(f"✅ Усі парсери завершили роботу | Додано: {total_inserted}, Пропущено: {total_skipped}", flush=True)
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
