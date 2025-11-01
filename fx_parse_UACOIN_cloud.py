# === fx_parse_UACOIN_cloud.py ===
# Cloud версія парсера UACOIN для Render

import sys, io, os, re
from supabase_io import SupabaseIO, download_text, norm_price_auto, iter_message_blocks, normalize_cross_rate

# 🔧 Windows: фікс кирилиці
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Конфіг ===
CHANNEL = "UACOIN"

# === Регулярки ===
LINE_RE = re.compile(
    r'(USD|EUR|GBP|CHF|PLN|CAD|CZK|SEK|JPY|NOK|DKK)UAH\s+'
    r'([0-9.]+)/([0-9.]+)',
    re.I
)

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

def process_uacoin():
    """Обробка UACOIN каналу з Supabase Storage"""
    
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

        # Крос-курс
        for ln in block:
            if is_noise(ln):
                continue
            
            ln_clean = clean_line(ln)
            m = CROSS_RE.search(ln_clean)
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

            if buy == 0 or sell == 0:
                continue

            # Нормалізуємо напрямок крос-курсів (USD завжди другим)
            cur_a, cur_b, buy, sell = normalize_cross_rate(cur_a, cur_b, buy, sell)
            comment = "крос-курс"
            
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
            processed_lines.add(ln.strip())

        # UAH пари
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

            if buy == 0 or sell == 0:
                continue

            cur_b = "UAH"
            comment = ""
            
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
    
    print(f"[PARSED] Знайдено: {len(rows)} курсів", flush=True)
    
    if rows:
        cross_rates = [r for r in rows if r[-1].startswith("крос-курс")]
        uah_rates = [r for r in rows if r[-1] == ""]
        print(f"  → UAH пар: {len(uah_rates)} | Крос-курсів: {len(cross_rates)}", flush=True)
        
        inserted, skipped_db = db.insert_rates(CHANNEL, rows)
        print(f"[CLOUD] ✅ Записано в БД: {inserted} рядків", flush=True)
        if skipped_db > 0:
            print(f"[CLOUD] ⚠️ Пропущено дублікатів: {skipped_db}", flush=True)
    else:
        print("[INFO] Курсів не знайдено", flush=True)
    
    print("[DONE] ✅ Готово.", flush=True)

if __name__ == "__main__":
    process_uacoin()

