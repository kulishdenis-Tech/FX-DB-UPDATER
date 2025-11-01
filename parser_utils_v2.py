# === parser_utils_v2.py — Утиліти з паралельним збереженням у Supabase і CSV ===

import csv
import os
from typing import List, Optional
from db_adapter_cloud import SupabaseAdapter   # ← зміна!
from datetime import datetime

# === Глобальна ініціалізація ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARSED_DIR = os.path.join(BASE_DIR, "PARSED")

cloud_adapter = SupabaseAdapter()
print("[INIT] ✅ Режим Dual Save: Supabase + CSV")


# === Основна функція збереження ===
def save_rows(rows: List[list], output_file: str):
    """
    Автоматичне паралельне збереження:
    - Supabase: основна база даних у хмарі
    - CSV: резерв/архів для локальної перевірки або логів
    """
    if not rows:
        return

    channel = rows[0][0] if rows else "UNKNOWN"

    # 1️⃣ Основне — запис у Supabase
    try:
        inserted, skipped = cloud_adapter.insert_rates(channel, rows)
        print(f"[CLOUD] {channel:<12} → {inserted} додано | {skipped} пропущено")
    except Exception as e:
        print(f"[ERROR] Supabase: {e}")

    # 2️⃣ Паралельно — запис у CSV
    try:
        save_to_csv(rows, output_file)
    except Exception as e:
        print(f"[ERROR] CSV: {e}")


def save_to_csv(rows: List[list], output_file: str):
    """Дублювання даних у CSV (уникнення дублікатів та дозапис)"""
    if not rows:
        return

    folder = os.path.dirname(output_file)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

    header = [
        "channel", "message_id", "version", "published", "edited",
        "currency_a", "currency_b", "buy", "sell", "comment"
    ]

    existing = set()
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 10:
                    key = tuple(row[:3] + row[5:8] + [row[9]])
                    existing.add(key)

    new_rows = []
    for r in rows:
        key = tuple([r[0], r[1], r[2], r[5], r[6], r[7], r[9]])
        if key not in existing:
            new_rows.append(r)

    if not new_rows:
        print(f"[CSV] {os.path.basename(output_file)} → без нових рядків")
        return

    mode = "a" if os.path.exists(output_file) else "w"
    with open(output_file, mode, newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if mode == "w":
            writer.writerow(header)
        writer.writerows(new_rows)

    print(f"[CSV] {os.path.basename(output_file)} → +{len(new_rows)} нових")


# === Додаткові утиліти (без змін) ===
def norm_price_auto(s: str) -> Optional[float]:
    """Нормалізація ціни"""
    if not s:
        return None
    s = s.replace(",", ".").replace(" ", "").strip()
    try:
        return float(s)
    except:
        return None


def detect_currency(line: str) -> Optional[str]:
    """Автовизначення валюти"""
    currencies = ["USD", "EUR", "PLN", "GBP", "CHF", "CAD", "CZK", "SEK", "JPY", "NOK", "DKK"]
    for cur in currencies:
        if cur in line.upper():
            return cur
    return None


def iter_message_blocks(lines: List[str], id_re):
    """Розбиття повідомлення на логічні блоки"""
    block = []
    for line in lines:
        if id_re.search(line) and block:
            yield block
            block = []
        block.append(line)
    if block:
        yield block
