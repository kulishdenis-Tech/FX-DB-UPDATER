# === fx_parse_SWAPS_cloud.py ===
# Cloud версія парсера SWAPS для Render
# Читає з Supabase Storage, пише в Supabase DB

import sys, io, os, re
from supabase_io import SupabaseIO, download_text, norm_price_auto, iter_message_blocks, normalize_cross_rate

# 🔧 Windows: фікс кирилиці
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Конфіг ===
CHANNEL = "SWAPS"

# === Регулярки ===
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

def process_swaps():
    """Обробка SWAPS каналу з Supabase Storage"""
    
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
    lines = raw.splitlines()
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

            # Нормалізуємо напрямок крос-курсів (USD завжди другим)
            if cur_a != "UAH" and cur_b != "UAH":
                cur_a, cur_b, buy, sell = normalize_cross_rate(cur_a, cur_b, buy, sell)
                comment = "крос-курс"
            else:
                comment = ""

            rows.append([CHANNEL, msg_id, version, published, edited,
                         cur_a, cur_b, buy, sell, comment])
    
    # 5️⃣ Зберігаємо в Supabase БД
    if rows:
        cross_rates = [r for r in rows if "крос" in r[-1]]
        uah_rates = [r for r in rows if r[-1] == ""]
        inserted, skipped_db = db.insert_rates(CHANNEL, rows)
        
        print(f"{CHANNEL:12} | Знайдено: {len(rows):4} (UAH: {len(uah_rates):4}, Крос: {len(cross_rates):3}) | Додано: {inserted:4}, Пропущено: {skipped_db:4}", flush=True)
        return inserted, skipped_db
    else:
        print(f"{CHANNEL:12} | Курсів не знайдено", flush=True)
        return 0, 0

if __name__ == "__main__":
    process_swaps()

