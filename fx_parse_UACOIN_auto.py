# === fx_parse_UACOIN_auto.py (v1.1 - PRODUCTION READY) ===

import sys, io, os, re
from parser_utils_v2 import norm_price_auto, iter_message_blocks, save_rows
from version_control import load_last_rates, is_rate_changed

# 🔧 Windows: фікс кирилиці
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Налаштування ===
BASE_DIR   = r"C:\Users\kulis\Documents\Google drive\Exchange"
RAW_DIR    = os.path.join(BASE_DIR, "RAW")
PARSED_DIR = os.path.join(BASE_DIR, "PARSED")
os.makedirs(PARSED_DIR, exist_ok=True)

CHANNEL     = "UACOIN"
INPUT_FILE  = os.path.join(RAW_DIR,   f"{CHANNEL}_raw.txt")
OUTPUT_FILE = os.path.join(PARSED_DIR, f"{CHANNEL}_parsed.csv")

# === Регулярки ===
# Формат: USDUAH  42.05/42.20 (пробіли + слеш)
LINE_RE = re.compile(
    r'(USD|EUR|GBP|CHF|PLN|CAD|CZK|SEK|JPY|NOK|DKK)UAH\s+'
    r'([0-9.]+)/([0-9.]+)',
    re.I
)

# Формат: EURUSD  1.1650/1.1700
CROSS_RE = re.compile(
    r'(EUR)(USD)\s+'
    r'([0-9.]+)/([0-9.]+)',
    re.I
)

# Метадані
ID_RE      = re.compile(r'\[MESSAGE_ID\]\s*(\d+)')
DATE_RE    = re.compile(r'\[DATE\]\s*([\d-]+\s[\d:]+)')
EDITED_RE  = re.compile(r'\[EDITED\]\s*([\d-]+\s[\d:]+)')
VER_RE     = re.compile(r'\[VERSION\]\s*(\S+)')

# Фільтрація шуму
NOISE_KEYWORDS = [
    "uacoin.com.ua", "00.00", "0.00", "USDTUAH", 
    "+380", "☎", "www.", ".com", ".ua"
]

def is_noise(line: str) -> bool:
    """Фільтрує шумові рядки (сайти, телефони, нулі)"""
    ln = line.lower()
    return any(k.lower() in ln for k in NOISE_KEYWORDS)

def clean_line(s: str) -> str:
    """Базова очистка рядка"""
    if not s: 
        return s
    return s.replace(",", ".").strip()

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
        # Витягуємо метадані
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

        processed_lines = set()

        # === 1️⃣ КРОС-КУРС (EUR/USD) ===
        for ln in block:
            if is_noise(ln):
                continue
            
            ln_clean = clean_line(ln)
            m = CROSS_RE.search(ln_clean)
            if not m:
                continue

            cur_a = m.group(1).upper()  # EUR
            cur_b = m.group(2).upper()  # USD
            buy_raw = m.group(3)
            sell_raw = m.group(4)
            
            buy  = norm_price_auto(buy_raw)
            sell = norm_price_auto(sell_raw)
            
            if not buy or not sell:
                continue

            # Пропускаємо нульові курси
            if buy == 0 or sell == 0:
                continue

            comment = f"крос-курс ({cur_a}/{cur_b})"
            
            pair_key = (cur_a, cur_b)
            new_rate = (cur_a, cur_b, buy, sell)
            
            if not is_rate_changed(new_rate, previous_rates.get(pair_key)):
                skipped += 1
                continue

            previous_rates[pair_key] = (buy, sell)
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
            processed_lines.add(ln.strip())

        # === 2️⃣ UAH ПАРИ (USD/UAH, EUR/UAH тощо) ===
        for ln in block:
            if is_noise(ln):
                continue
            
            if ln.strip() in processed_lines:
                continue

            ln_clean = clean_line(ln)
            m = LINE_RE.search(ln_clean)
            if not m:
                continue

            cur_a = m.group(1).upper()
            buy_raw = m.group(2)
            sell_raw = m.group(3)
            
            buy  = norm_price_auto(buy_raw)
            sell = norm_price_auto(sell_raw)
            
            if not buy or not sell:
                continue

            # Пропускаємо нульові курси
            if buy == 0 or sell == 0:
                continue

            cur_b = "UAH"
            comment = ""

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
    """Головна функція запуску парсера"""
    print(f"[RUN] Обробка RAW для каналу {CHANNEL}")
    rows, skipped = process_file()
    found = len(rows) + skipped
    
    # Підрахунок крос-курсів та UAH пар
    cross_rates = [r for r in rows if r[-1].startswith("крос-курс")]
    uah_rates = [r for r in rows if r[-1] == ""]
    
    print(f"[FOUND] У RAW: {found} курсів")
    print(f"[NEW] Нових: {len(rows)} | [SKIPPED] Без змін: {skipped}")
    print(f"  → UAH пари: {len(uah_rates)} | Крос-курси: {len(cross_rates)}")
    
    # Зберігаємо результати
    save_rows(rows, OUTPUT_FILE)
    
    # Фінальне повідомлення
    if not rows:
        print("[INFO] Нових рядків немає.")
    print("[DONE] Готово.")

if __name__ == "__main__":
    parse_once()