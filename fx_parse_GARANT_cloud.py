# === fx_parse_GARANT_cloud.py ===
# Cloud версія парсера GARANT для Render
# Читає з Supabase Storage, пише в Supabase DB

import sys, io, os, re
from supabase_io import SupabaseIO, download_text, norm_price_auto, iter_message_blocks, detect_currency, normalize_cross_rate

# 🔧 Windows: фікс кирилиці
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Конфіг ===
CHANNEL = "GARANT"

# === Регулярки ===
LINE_RE = re.compile(
    r'(?:(?:[A-Z]{3})|(?:[\U0001F1E6-\U0001F1FF]{2}))[:\s]*([0-9]+[.,][0-9]+)\s*[/\\]\s*([0-9]+[.,][0-9]+)(.*)$',
    re.U
)

# Крос-курси (розширена підтримка)
CROSS_RE = re.compile(
    r'(?:🇪🇺|🇺🇸|🇬🇧|🇨🇭|🇵🇱|🇨🇦|🇨🇿|🇸🇪|🇯🇵|🇳🇴|🇩🇰)?\s*'
    r'(EUR|USD|GBP|CHF|PLN|CAD|CZK|SEK|JPY|NOK|DKK)\s*'
    r'[/:\- ]\s*'
    r'(?:🇪🇺|🇺🇸|🇬🇧|🇨🇭|🇵🇱|🇨🇦|🇨🇿|🇸🇪|🇯🇵|🇳🇴|🇩🇰)?\s*'
    r'(EUR|USD|GBP|CHF|PLN|CAD|CZK|SEK|JPY|NOK|DKK).*?'
    r'([0-9]+[.,][0-9]+)\s* /\s*([0-9]+[.,][0-9]+)',
    re.U | re.I
)

# Метадані
ID_RE      = re.compile(r'\[MESSAGE_ID\]\s*(\d+)')
DATE_RE    = re.compile(r'\[DATE\]\s*([\d-]+\s[\d:]+)')
EDITED_RE  = re.compile(r'\[EDITED\]\s*([\d-]+\s[\d:]+)')
VER_RE     = re.compile(r'\[VERSION\]\s*(\S+)')

# Фільтрація гарячих пропозицій
HOT_KEYWORDS = ["🔥", "гаряч", "акці", "знижк", "прода", "hot", "promo"]

def is_hot_offer(line: str) -> bool:
    """Фільтрує гарячі пропозиції"""
    return any(k in line.lower() for k in HOT_KEYWORDS)

def clean_line(s: str) -> str:
    """Базова очистка рядка"""
    if not s: 
        return s
    return (s.replace("–", "-")
             .replace("—", "-")
             .replace(",", ".")
             .strip())

def process_garant():
    """Обробка GARANT каналу з Supabase Storage"""
    
    # 1️⃣ Підключаємося до Supabase
    db = SupabaseIO()
    
    # 2️⃣ Завантажуємо RAW з Supabase Storage
    filename = f"{CHANNEL}_raw.txt"
    raw_content = download_text(filename)
    
    if not raw_content:
        print(f"[WARN] Файл {filename} не знайдено в Supabase Storage", flush=True)
        return 0, 0
    
    # 3️⃣ Парсимо RAW
    raw = raw_content.replace("[NO TEXT]", "")
    # Очищення від зайвих символів
    lines = re.sub(r'[^\w\s\.,:/\-\[\]€$🇺🇸🇪🇺🇵🇱🇬🇧🇨🇭🇨🇦🇨🇿🇸🇪🇯🇵🇳🇴🇩🇰]', ' ', raw).splitlines()
    
    blocks = iter_message_blocks(lines, ID_RE)
    
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
    
    # 4️⃣ Витягуємо курси
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

        # === 1️⃣ КРОС-КУРСИ ===
        for ln in block:
            if is_hot_offer(ln):
                continue
            
            m = CROSS_RE.search(ln)
            if not m:
                continue

            cur_a = m.group(1).upper()
            cur_b = m.group(2).upper()
            buy  = norm_price_auto(m.group(3))
            sell = norm_price_auto(m.group(4))
            
            if not buy or not sell:
                continue

            # Нормалізуємо напрямок крос-курсів (USD завжди другим)
            cur_a, cur_b, buy, sell = normalize_cross_rate(cur_a, cur_b, buy, sell)
            comment = "крос-курс"
            
            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
            processed_lines.add(ln.strip())

        # === 2️⃣ UAH ПАРИ ===
        for ln in block:
            if is_hot_offer(ln):
                continue
            
            if ln.strip() in processed_lines:
                continue

            # Пропускаємо явні крос-курси
            if re.search(r'\b[A-Z]{3}\s*/\s*[A-Z]{3}\b', ln):
                continue

            ln_clean = clean_line(ln)
            m = LINE_RE.search(ln_clean)
            if not m:
                continue

            # Визначаємо валюту
            maybe_cur = None
            for k in ['USD','EUR','PLN','GBP','CHF','CAD','CZK','SEK','JPY','NOK','DKK']:
                if k in ln:
                    maybe_cur = k
                    break
            
            if not maybe_cur:
                maybe_cur = detect_currency(ln)
            
            if not maybe_cur:
                continue

            cur_a, cur_b = maybe_cur, "UAH"
            buy  = norm_price_auto(m.group(1))
            sell = norm_price_auto(m.group(2))
            comment = (m.group(3) or "").strip()
            
            if not buy or not sell:
                continue

            rows.append([CHANNEL, msg_id, version, published, edited,
                        cur_a, cur_b, buy, sell, comment])
    
    # 5️⃣ Зберігаємо в Supabase БД
    if rows:
        cross_rates = [r for r in rows if 'крос-курс' in r[-1]]
        uah_rates = [r for r in rows if 'крос-курс' not in r[-1]]
        inserted, skipped_db = db.insert_rates(CHANNEL, rows)
        
        print(f"{CHANNEL:12} | Знайдено: {len(rows):4} (UAH: {len(uah_rates):4}, Крос: {len(cross_rates):3}) | Додано: {inserted:4}, Пропущено: {skipped_db:4}", flush=True)
        return inserted, skipped_db
    else:
        print(f"{CHANNEL:12} | Курсів не знайдено", flush=True)
        return 0, 0

if __name__ == "__main__":
    process_garant()

