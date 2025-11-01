# === main_runner.py ===
# Запускає одночасно Telegram‑парсер і моніторинг RAW
# Безпечний режим для безкоштовного хостингу (Render / Railway)

import subprocess
import threading
import time
import datetime
import os
import sys
import signal

# --- Кольори логів (ANSI, щоб було видно і локально, і на Render) ---
class Colors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"

def log(message, color=Colors.GREEN):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{now}] {message}{Colors.RESET}", flush=True)

# --- Файли для запуску ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TG_SCRIPT = os.path.join(BASE_DIR, "telegram_fetcher.py")
RAW_WATCHER = os.path.join(BASE_DIR, "watch_raw_changes.py")

# --- Безпечний запуск з рестартом ---
def safe_run(script_path):
    while True:
        try:
            log(f"🚀 Запуск {os.path.basename(script_path)} ...", Colors.BLUE)
            subprocess.run(["python", script_path], cwd=BASE_DIR, check=True)
        except subprocess.CalledProcessError as e:
            log(f"❌ {os.path.basename(script_path)} завершився з кодом {e.returncode}", Colors.RED)
        except Exception as e:
            log(f"⚠️ Помилка у {os.path.basename(script_path)}: {e}", Colors.YELLOW)
        log(f"🔁 Перезапуск через 10 сек...", Colors.YELLOW)
        time.sleep(10)

# --- Основна функція ---
def main():
    log("=== 🧠 Старт системи: Telegram Fetcher + RAW Watcher ===", Colors.GREEN)
    log(f"📁 Робоча папка: {BASE_DIR}", Colors.BLUE)

    t1 = threading.Thread(target=safe_run, args=(TG_SCRIPT,), daemon=True)
    t2 = threading.Thread(target=safe_run, args=(RAW_WATCHER,), daemon=True)
    t1.start()
    t2.start()

    def shutdown_handler(sig, frame):
        log("🛑 Зупинення роботи користувачем (Ctrl+C або SIGTERM).", Colors.RED)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
