# === fx_parse_VALUTA_KIEV_auto.py (v9.0 - ФІНАЛЬНА ВЕРСІЯ) ===

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

CHANNEL     = "VALUTA_KIEV"
INPUT_FILE  = os.path.join(RAW_DIR,   f"{CHANNEL}_raw.txt")
OUTPUT_FILE = os.path.join(PARSED_DIR, f"{CHANNEL}_parsed.csv")

# === Словник прапорців → валюти ===
FLAG_TO_CUR = {
    '🇪🇺': 'EUR', '🇺🇸': 'USD', '🇬🇧': 'GBP', '🇨🇭': 'CHF', 
    '🇵🇱': 'PLN', '🇨🇦': 'CAD', '🇨🇿': 'CZK', '🇸🇪': 'SEK',
    '🇯🇵': 'JPY', '🇳🇴': 'NOK', '🇩🇰': 'DKK', '🇺🇦': 'UAH'
}

# === Регулярки ===
# 1) КРОС-КУРСИ: 🇪🇺/🇺🇸EUR: 1,165 / 1,169
CROSS_RE = re.compile(
    r'[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s*'  # два прапорці
    r'(EUR|USD|GBP|CHF|PLN|CAD|CZK|SEK|JPY|NOK|DKK)\s*[:\s-]*'
    r'([0-9З.,]+)\s*/\s*([0-9З.,]+)',
    re.U | re.I
)

# 2) UAH ПАРИ: 🇺🇸/🇺🇦USD: 42,13 / 42,18
LINE_RE = re.compile(
    r'(?:[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s*)?'  # опціональні прапорці
    r'(USD|EUR|PLN|GBP|CHF|CAD|CZK|SEK|JPY|NOK|DKK|UAH)\s*[:\s-]*'
    r'([0-9З.,]+)\s*/\s*([0-9З.,]+)',
    re.U | re.I
)

# Метадані
ID_RE      = re.compile(r'\[MESSAGE_ID\]\s*(\d+)')
DATE_RE    = re.compile(r'\[DATE\]\s*([\d-]+\s[\d:]+)')
EDITED_RE  = re.compile(r'\[EDITED\]\s*([\d-]+\s[\d:]+)')
VER_RE     = re.compile(r'\[VERSION\]\s*(\S+)')

# Фільтрація
HOT_KEYWORDS = ["🔥", "спецкурс", "акці", "обмежен", "знижк", "promo", "продамо", "⚡️", 
                "usdt", "купуємо", "купимо", "💰", "%", "золото", "метал", "срібло"]

def is_hot_offer(line: str) -> bool:
    ln = line.lower()
    return any(k in ln for k in HOT_KEYWORDS)

def clean_line(s: str) -> str:
    if not s: 
        return s
    return (s.replace("–", "-")
             .replace("—", "-")
             .replace("⁄", "/")
             .replace(",", ".")
             .replace("З", "3").replace("з", "3")
             .replace("\u00A0", " ")
             .replace("：", ":")
             .replace("‐", "-")
             .strip())

def extract_flags(line: str):
    """
    ВИПРАВЛЕНА версія - витягує прапорці як ПАРИ Regional Indicator символів.
    Прапорці Unicode (🇪🇺, 🇺🇸) складаються з ДВОХ символів Regional Indicator.
    """
    flags = []
    i = 0
    while i < len(line):
        if i + 1 < len(line):
            char1 = line[i]
            char2 = line[i + 1]
            # Перевіряємо, чи обидва символи є Regional Indicator (U+1F1E6 - U+1F1FF)
            if (0x1F1E6 <= ord(char1) <= 0x1F1FF and 
                0x1F1E6 <= ord(char2) <= 0x1F1FF):
                flags.append(char1 + char2)  # Додаємо ПАРУ символів
                i += 2
                continue
        i += 1
    return flags

def process_file():
    rows = []
    if not os.path.exists(INPUT_FILE):
        print(f"[WARN] Файл {INPUT_FILE} не знайдено")
        return rows, 0

    raw = open(INPUT_FILE, "r", encoding="utf-8").read().replace("[NO TEXT]", "")
    lines = raw.splitlines()
    blocks = iter_message_blocks(lines, ID_RE)
    previous_rates = load_last_rates(OUTPUT_FILE)

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

        found_any = False
        processed_lines = set()

        # === 1️⃣ КРОС-КУРСИ (EUR/USD, GBP/USD тощо) ===
        for ln in block:
            if is_hot_offer(ln):
                continue
            
            ln_clean = clean_line(ln)
            m = CROSS_RE.search(ln_clean)
            if not m:
                continue

            code = m.group(1).upper()
            buy_raw, sell_raw = m.group(2), m.group(3)
            
            buy  = norm_price_auto(buy_raw)
            sell = norm_price_auto(sell_raw)
            
            if not buy or not sell:
                continue

            # Витягуємо валюти з прапорців (ВИПРАВЛЕНА функція)
            flags = extract_flags(ln)
            if len(flags) < 2:
                continue
            
            cur_flag1 = FLAG_TO_CUR.get(flags[0])
            cur_flag2 = FLAG_TO_CUR.get(flags[1])
            
            if not cur_flag1 or not cur_flag2:
                continue

            # ВИПРАВЛЕНА ЛОГІКА: Перший прапорець = базова валюта (currency_a)
            # Формат: 🇬🇧/🇺🇸USD означає GBP/USD (перший прапорець завжди база)
            if code == cur_flag2:
                # Код збігається з другим прапорцем - порядок правильний
                cur_a, cur_b = cur_flag1, cur_flag2  # ✅ GBP, USD
            elif code == cur_flag1:
                # Код збігається з першим прапорцем - також залишаємо порядок
                cur_a, cur_b = cur_flag1, cur_flag2  # ✅ EUR, USD
            else:
                # Код не збігається - беремо перший прапорець як базу
                cur_a, cur_b = cur_flag1, code

            # Перевіряємо крос-курс (без UAH)
            if cur_a == "UAH" or cur_b == "UAH":
                continue
            
            if cur_a == cur_b:
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
            found_any = True
            processed_lines.add(ln.strip())

        # === 2️⃣ UAH ПАРИ (USD/UAH, EUR/UAH тощо) ===
        for ln in block:
            if is_hot_offer(ln):
                continue
            
            if ln.strip() in processed_lines:
                continue

            ln_clean = clean_line(ln)
            
            # Пропускаємо явні крос-курси
            if re.match(r'[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}', ln):
                flags = extract_flags(ln)
                if len(flags) >= 2:
                    cur1 = FLAG_TO_CUR.get(flags[0])
                    cur2 = FLAG_TO_CUR.get(flags[1])
                    if cur1 and cur2 and cur1 != "UAH" and cur2 != "UAH":
                        continue  # Це крос-курс

            m = LINE_RE.search(ln_clean)
            if not m:
                continue

            cur_a = m.group(1).upper()
            buy_raw, sell_raw = m.group(2), m.group(3)
            
            buy  = norm_price_auto(buy_raw)
            sell = norm_price_auto(sell_raw)
            
            if not buy or not sell:
                continue

            cur_b = "UAH"
            
            if cur_a == "UAH":
                continue

            comment = ""

            pair_key = (cur_a, cur_b)
            new_rate = (cur_a, cur_b, buy, sell)
            
            if not is_rate_changed(new_rate, previous_rates.get(pair_key)):
                skipped += 1
                continue

            previous_rates[pair_key] = (buy, sell)
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
            found_any = True

    return rows, skipped

def parse_once():
    print(f"[RUN] Обробка RAW для каналу {CHANNEL}")
    rows, skipped = process_file()
    found = len(rows) + skipped
    
    # Підрахунок крос-курсів та UAH пар
    cross_rates = [r for r in rows if r[-1].startswith("крос-курс")]
    uah_rates = [r for r in rows if r[-1] == ""]
    
    print(f"[FOUND] У RAW: {found} курсів")
    print(f"[NEW] Нових: {len(rows)} | [SKIPPED] Без змін: {skipped}")
    print(f"  → UAH пари: {len(uah_rates)} | Крос-курси: {len(cross_rates)}")
    
    save_rows(rows, OUTPUT_FILE)
    if not rows:
        print("[INFO] Нових рядків немає.")
    else:
        print(f"[SAVED] {len(rows)} нових рядків у {OUTPUT_FILE}")
    print("[DONE] Готово.")

if __name__ == "__main__":
    parse_once()