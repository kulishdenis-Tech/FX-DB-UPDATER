# === fx_parse_VALUTA_KIEV_cloud.py ===
# Cloud версія парсера VALUTA_KIEV для Render

import sys, io, os, re
from supabase_io import SupabaseIO, download_text, norm_price_auto, iter_message_blocks, normalize_cross_rate

# 🔧 Windows: фікс кирилиці
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Конфіг ===
CHANNEL = "VALUTA_KIEV"

# === Словник прапорців → валюти ===
FLAG_TO_CUR = {
    '🇪🇺': 'EUR', '🇺🇸': 'USD', '🇬🇧': 'GBP', '🇨🇭': 'CHF', 
    '🇵🇱': 'PLN', '🇨🇦': 'CAD', '🇨🇿': 'CZK', '🇸🇪': 'SEK',
    '🇯🇵': 'JPY', '🇳🇴': 'NOK', '🇩🇰': 'DKK', '🇺🇦': 'UAH'
}

# === Регулярки ===
CROSS_RE = re.compile(
    r'[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s*'
    r'(EUR|USD|GBP|CHF|PLN|CAD|CZK|SEK|JPY|NOK|DKK)\s*[:\s-]*'
    r'([0-9З.,]+)\s*/\s*([0-9З.,]+)',
    re.U | re.I
)

LINE_RE = re.compile(
    r'(?:[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}\s*)?'
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
    """Витягує прапорці як ПАРИ Regional Indicator символів."""
    flags = []
    i = 0
    while i < len(line):
        if i + 1 < len(line):
            char1 = line[i]
            char2 = line[i + 1]
            if (0x1F1E6 <= ord(char1) <= 0x1F1FF and 
                0x1F1E6 <= ord(char2) <= 0x1F1FF):
                flags.append(char1 + char2)
                i += 2
                continue
        i += 1
    return flags

def process_valuta_kiev():
    """Обробка VALUTA_KIEV каналу з Supabase Storage"""
    
    db = SupabaseIO()
    filename = f"{CHANNEL}_raw.txt"
    raw_content = download_text(filename)
    
    if not raw_content:
        print(f"[WARN] Файл {filename} не знайдено в Supabase Storage", flush=True)
        return
    
    print(f"[CLOUD] ✅ Завантажено {len(raw_content)} символів з Supabase", flush=True)
    
    raw = raw_content.replace("[NO TEXT]", "")
    lines = raw.splitlines()
    blocks = iter_message_blocks(lines, ID_RE)
    
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
    
    rows = []
    
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

        processed_lines = set()

        # Крос-курси
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

            flags = extract_flags(ln)
            if len(flags) < 2:
                continue
            
            cur_flag1 = FLAG_TO_CUR.get(flags[0])
            cur_flag2 = FLAG_TO_CUR.get(flags[1])
            
            if not cur_flag1 or not cur_flag2:
                continue

            if code == cur_flag2:
                cur_a, cur_b = cur_flag1, cur_flag2
            elif code == cur_flag1:
                cur_a, cur_b = cur_flag1, cur_flag2
            else:
                cur_a, cur_b = cur_flag1, code

            if cur_a == "UAH" or cur_b == "UAH":
                continue
            
            if cur_a == cur_b:
                continue

            # Нормалізуємо напрямок крос-курсів (USD завжди другим)
            cur_a, cur_b, buy, sell = normalize_cross_rate(cur_a, cur_b, buy, sell)
            comment = "крос-курс"
            
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
            processed_lines.add(ln.strip())

        # UAH пари
        for ln in block:
            if is_hot_offer(ln):
                continue
            
            if ln.strip() in processed_lines:
                continue

            ln_clean = clean_line(ln)
            
            if re.match(r'[\U0001F1E6-\U0001F1FF]{2}\s*/\s*[\U0001F1E6-\U0001F1FF]{2}', ln):
                flags = extract_flags(ln)
                if len(flags) >= 2:
                    cur1 = FLAG_TO_CUR.get(flags[0])
                    cur2 = FLAG_TO_CUR.get(flags[1])
                    if cur1 and cur2 and cur1 != "UAH" and cur2 != "UAH":
                        continue

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
            
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
    
    if rows:
        cross_rates = [r for r in rows if "крос" in r[-1]]
        uah_rates = [r for r in rows if r[-1] == ""]
        inserted, skipped_db = db.insert_rates(CHANNEL, rows)
        
        print(f"{CHANNEL} | Знайдено: {len(rows)} (UAH: {len(uah_rates)}, Крос: {len(cross_rates)}) | Додано: {inserted}, Пропущено: {skipped_db}", flush=True)
    else:
        print(f"{CHANNEL} | Курсів не знайдено", flush=True)

if __name__ == "__main__":
    process_valuta_kiev()

