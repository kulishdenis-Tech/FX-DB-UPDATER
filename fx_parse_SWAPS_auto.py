# === fx_parse_SWAPS_auto.py (v2.0 - Supabase + CSV) ===

import sys, io, os, re
from parser_utils_v2 import norm_price_auto, iter_message_blocks, save_rows  # ✅ новий імпорт
from version_control import load_last_rates, is_rate_changed

# 🔧 Windows: фікс кирилиці (щоб термінал не кракозябив)
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Налаштування ===
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
RAW_DIR    = os.path.join(BASE_DIR, "RAW")
PARSED_DIR = os.path.join(BASE_DIR, "PARSED")
os.makedirs(PARSED_DIR, exist_ok=True)

CHANNEL     = "SWAPS"
INPUT_FILE  = os.path.join(RAW_DIR,   f"{CHANNEL}_raw.txt")
OUTPUT_FILE = os.path.join(PARSED_DIR, f"{CHANNEL}_parsed.csv")

# === Регулярки ===
# 🇺🇸/🇺🇦 USD-UAH 42,10 / 42,45
LINE_RE = re.compile(
    r'[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s+'
    r'([A-Z]{3})[-/]([A-Z]{3})\s+([0-9.,]+)\s*/\s*([0-9.,]+)',
    re.U | re.I
)

# Метадані
ID_RE      = re.compile(r'\[MESSAGE_ID\]\s*(\d+)')
DATE_RE    = re.compile(r'\[DATE\]\s*([\d-]+\s[\d:]+)')
EDITED_RE  = re.compile(r'\[EDITED\]\s*([\d-]+\s[\d:]+)')
VER_RE     = re.compile(r'\[VERSION\]\s*(\S+)')

# Фільтрація шуму
HOT_KEYWORDS = [
    "від $2000", "Фіксація курсу", "оптовим курсом", "Tether", "USDT",
    "купюри", "ГАРНОМУ стані", "акці", "☎", "+380", "📱", "Telegram",
    "АДРЕСИ", "Графік роботи", "вул.", "Київ", "Палац", "метро", 
    "⌚", "📨", "🔐", "✅", "💰", "💸", "від 10 000"
]

def is_hot_offer(line: str) -> bool:
    """Фільтрує рекламні рядки"""
    ln = line.lower()
    return any(k.lower() in ln for k in HOT_KEYWORDS)

def clean_line(s: str) -> str:
    """Очистка тексту"""
    if not s:
        return s
    return (s.replace("–", "-")
             .replace("—", "-")
             .replace(",", ".")
             .strip())

def process_file():
    rows = []
    if not os.path.exists(INPUT_FILE):
        print(f"[WARN] Файл {INPUT_FILE} не знайдено")
        return rows, 0

    raw = open(INPUT_FILE, "r", encoding="utf-8").read().replace("[NO TEXT]", "")
    lines = raw.splitlines()
    blocks = iter_message_blocks(lines, ID_RE)
    previous_rates = load_last_rates(OUTPUT_FILE)

    # Сортуємо блоки за MESSAGE_ID
    sorted_blocks = []
    for b in blocks:
        mid = None
        for ln in b:
            m = ID_RE.search(ln)
            if m:
                mid = int(m.group(1))
                break
        if mid:
            sorted_blocks.append((mid, b))
    sorted_blocks.sort(key=lambda x: x[0])

    skipped = 0
    for _, block in sorted_blocks:
        msg_id = version = published = edited = None
        for ln in block:
            if not msg_id and (m := ID_RE.search(ln)):      msg_id = m.group(1)
            if not version and (v := VER_RE.search(ln)):    version = v.group(1)
            if not published and (d := DATE_RE.search(ln)): published = d.group(1)
            if not edited and (e := EDITED_RE.search(ln)):  edited = e.group(1)
        if not msg_id or not version or not published:
            continue
        if not edited:
            edited = published

        for ln in block:
            if is_hot_offer(ln):
                continue
            ln_clean = clean_line(ln)
            m = LINE_RE.search(ln_clean)
            if not m:
                continue

            cur_a = m.group(1).upper()
            cur_b = m.group(2).upper()
            buy  = norm_price_auto(m.group(3))
            sell = norm_price_auto(m.group(4))
            if not buy or not sell:
                continue

            comment = "" if "UAH" in (cur_a, cur_b) else f"крос-курс ({cur_a}/{cur_b})"

            pair_key = (cur_a, cur_b)
            new_rate = (cur_a, cur_b, buy, sell)

            if not is_rate_changed(new_rate, previous_rates.get(pair_key)):
                skipped += 1
                continue

            previous_rates[pair_key] = (buy, sell)
            rows.append([CHANNEL, msg_id, version, published, edited,
                         cur_a, cur_b, buy, sell, comment])

    return rows, skipped


def parse_once():
    """Основний запуск"""
    print(f"[RUN] Обробка RAW для каналу {CHANNEL}")
    rows, skipped = process_file()
    found = len(rows) + skipped
    cross_rates = [r for r in rows if "крос" in r[-1]]
    uah_rates = [r for r in rows if r[-1] == ""]

    print(f"[FOUND] У RAW: {found} | [NEW] {len(rows)} | [SKIPPED] {skipped}")
    print(f"  → UAH пар: {len(uah_rates)} | Крос-курсів: {len(cross_rates)}")

    # 🟢 Оновлене збереження
    save_rows(rows, OUTPUT_FILE)

    if not rows:
        print("[INFO] Нових рядків немає.")
    print("[DONE] ✅ Готово.")


if __name__ == "__main__":
    parse_once()
