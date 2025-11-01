# === watch_raw_changes.py ===
# Автоматичний моніторинг папки RAW та запуск парсерів

import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# === Шляхи під твою структуру ===
RAW_DIR = r"C:\Users\kulis\Documents\Google drive\Exchange\RAW"
PY_DIR = r"C:\Users\kulis\Documents\Google drive\Exchange"

# === Мапа парсерів: файл RAW → відповідний Python-скрипт ===
FILE_MAP = {
    "GARANT_raw.txt": "fx_parse_GARANT_auto.py",
    "KIT_GROUP_raw.txt": "fx_parse_KIT_GROUP_auto.py",
    "VALUTA_KIEV_raw.txt": "fx_parse_VALUTA_KIEV_auto.py",
    "MIRVALUTY_raw.txt": "fx_parse_MIRVALUTY_auto.py",
    "CHANGE_KYIV_raw.txt": "fx_parse_CHANGE_KYIV_auto.py",
    "UACOIN_raw.txt": "fx_parse_UACOIN_auto.py",
    "SWAPS_raw.txt": "fx_parse_SWAPS_auto.py",
}

# === Основна логіка запуску ===
class WatchHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return
        
        filename = os.path.basename(event.src_path)
        if filename not in FILE_MAP:
            return
        
        parser_script = FILE_MAP[filename]
        print(f"\n[WATCHER] {time.strftime('%H:%M:%S')} → виявлено оновлення у {filename}")
        parser_path = os.path.join(PY_DIR, parser_script)
        
        try:
            subprocess.run(
                ["python", parser_path],
                check=False,
                shell=True,
                cwd=PY_DIR
            )
            print(f"[DONE] Парсер {parser_script} виконано успішно.")
        except Exception as e:
            print(f"[ERROR] Не вдалося запустити {parser_script}: {e}")

# === Запуск моніторингу ===
if __name__ == "__main__":
    print(f"[INIT] 🔍 Спостереження за RAW: {RAW_DIR}")
    print("[INFO] У разі зміни будь-якого файлу буде автоматично запущено відповідний парсер.\n")

    event_handler = WatchHandler()
    observer = Observer()
    observer.schedule(event_handler, RAW_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[STOP] Моніторинг зупинено користувачем.")
        observer.stop()
    observer.join()
