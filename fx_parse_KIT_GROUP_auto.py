# === fx_parse_KIT_GROUP_auto.py (v14.2 - FIXED DEDUP) ===

import sys, io, os, re
from parser_utils_v2 import norm_price_auto, iter_message_blocks, save_rows, detect_currency
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

CHANNEL     = "KIT_GROUP"
INPUT_FILE  = os.path.join(RAW_DIR,   f"{CHANNEL}_raw.txt")
OUTPUT_FILE = os.path.join(PARSED_DIR, f"{CHANNEL}_parsed.csv")

# === Регулярки ===
# Формат: USD-UAH 42.00 / 42.25 (з слешем!)
LINE_RE = re.compile(
    r'(?:[🇪🇺🇺🇸🇬🇧🇨🇭🇵🇱🇨🇦🇨🇿🇸🇪🇯🇵🇳🇴🇩🇰🇺🇦]\s*)*'
    r'(USD|EUR|PLN|GBP|CHF|CAD|CZK|SEK|JPY|NOK|DKK|UAH)'
    r'[-\s]'
    r'(USD|EUR|PLN|GBP|CHF|CAD|CZK|SEK|JPY|NOK|DKK|UAH)\s+'
    r'([0-9]+[.,][0-9]+)\s*/\s*([0-9]+[.,][0-9]+)',
    re.U | re.I
)

# Метадані
ID_RE      = re.compile(r'\[MESSAGE_ID\]\s*(\d+)')
DATE_RE    = re.compile(r'\[DATE\]\s*([\d-]+\s[\d:]+)')
EDITED_RE  = re.compile(r'\[EDITED\]\s*([\d-]+\s[\d:]+)')
VER_RE     = re.compile(r'\[VERSION\]\s*(\S+)')

def clean_line(s: str) -> str:
    """Базова очистка рядка"""
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

        # Парсимо курси
        for ln in block:
            ln_clean = clean_line(ln)
            m = LINE_RE.search(ln_clean)
            if not m:
                continue

            cur_a = m.group(1).upper()
            cur_b = m.group(2).upper()
            buy_raw = m.group(3)
            sell_raw = m.group(4)
            
            buy  = norm_price_auto(buy_raw)
            sell = norm_price_auto(sell_raw)
            
            if not buy or not sell:
                continue

            # Визначаємо чи крос-курс
            is_cross = (cur_a != "UAH" and cur_b != "UAH")
            
            # Коментар: витягуємо решту рядка (від 500$, від 5000$ тощо)
            rest_of_line = ln[m.end():].strip()
            
            if is_cross:
                comment = f"крос-курс ({cur_a}/{cur_b})"
                if rest_of_line:
                    comment = f"{comment}, {rest_of_line}"
            else:
                comment = rest_of_line

            # ✅ ВИПРАВЛЕННЯ: Додаємо коментар до ключа антидубля
            # Це дозволяє зберігати різні спеціальні курси (від 500, від 1000, від 5000)
            pair_key = (cur_a, cur_b, comment)  # ← ЗМІНЕНО: додано comment
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
    cross_rates = [r for r in rows if 'крос-курс' in r[-1]]
    uah_rates = [r for r in rows if 'крос-курс' not in r[-1]]
    
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