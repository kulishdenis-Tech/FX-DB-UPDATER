# === parse_all_channels.py (v9) ===
import sys, io, subprocess, os, time

# 🔧 Фікс кирилиці у Windows
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

BASE_DIR = r"C:\Users\kulis\Documents\Google drive\Exchange"
PARSERS = [
    ("GARANT", "fx_parse_GARANT_auto.py"),
    ("MIRVALUTY", "fx_parse_MIRVALUTY_auto.py"),
    ("KIT_GROUP", "fx_parse_KIT_GROUP_auto.py"),
    ("CHANGE_KYIV", "fx_parse_CHANGE_KYIV_auto.py"),
    ("VALUTA_KIEV", "fx_parse_VALUTA_KIEV_auto.py"),
    ("UACOIN", "fx_parse_UACOIN_auto.py"),
    ("SWAPS", "fx_parse_SWAPS_auto.py")
]

def run_parser(channel, script):
    print(f"\n🚀 Запуск парсера для {channel} ...")
    start = time.time()
    try:
        # 🧩 головний фікс: примусово встановлюємо UTF-8 середовище
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        result = subprocess.run(
            ["python", os.path.join(BASE_DIR, script)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env
        )

        duration = time.time() - start
        print(f"\n📄 [LOG] {channel} завершив роботу ({duration:.2f} сек)")
        print("─" * 60)
        print(result.stdout if result.stdout else "[без виводу]")
        print("─" * 60)

        if result.returncode != 0:
            print(f"❌ [ERROR] {channel}: парсер завершився з помилкою")
            print(result.stderr)
        else:
            print(f"✅ [OK] {channel} — успішно")

    except Exception as e:
        print(f"⚠️ [EXCEPTION] {channel}: {e}")


def main():
    print("=" * 70)
    print("🧩 ЗАПУСК УСІХ ПАРСЕРІВ TELEGRAM (GARANT, MIRVALUTY, KIT_GROUP)")
    print("=" * 70)

    for channel, script in PARSERS:
        run_parser(channel, script)
        time.sleep(1)

    print("\n🎯 Усі парсери завершили роботу.")
    print("=" * 70)


if __name__ == "__main__":
    main()
