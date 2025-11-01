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
    
    for channel_name, module_name in CHANNELS:
        print(f"\n[RUN] {channel_name.upper()}...", flush=True)
        
        try:
            module = __import__(module_name)
            process_func = getattr(module, f"process_{channel_name}")
            process_func()
        except Exception as e:
            print(f"[ERROR] {channel_name}: {e}", flush=True)
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 70, flush=True)
    print("✅ Усі парсери завершили роботу", flush=True)
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
