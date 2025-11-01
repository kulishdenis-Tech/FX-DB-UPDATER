# === fx_parse_CHANGE_KYIV_auto.py (v2.2) ===
import sys, io, os, re
from parser_utils_v2 import norm_price_auto, iter_message_blocks, save_rows
from version_control import load_last_rates, is_rate_changed

# 🔧 Windows: фікс кирилиці
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# === Шляхи ===
BASE_DIR = r"C:\Users\kulis\Documents\Google drive\Exchange"
RAW_DIR = os.path.join(BASE_DIR, "RAW")
PARSED_DIR = os.path.join(BASE_DIR, "PARSED")
os.makedirs(PARSED_DIR, exist_ok=True)

CHANNEL = "CHANGE_KYIV"
INPUT_FILE = os.path.join(RAW_DIR, f"{CHANNEL}_raw.txt")
OUTPUT_FILE = os.path.join(PARSED_DIR, f"{CHANNEL}_parsed.csv")

# === Регулярка (UAH + кроси, стрілки, прапори) ===
LINE_RE = re.compile(
    r'(?:[🇪🇺🇺🇸🇬🇧🇨🇭🇵🇱🇨🇦🇨🇿🇸🇪🇯🇵🇳🇴🇩🇰🇺🇦]+\s*)?'
    r'(USD|EUR|PLN|GBP|CHF|CAD|CZK|SEK|JPY|NOK|DKK|UAH)'
    r'(?:\s*[🇪🇺🇺🇸🇬🇧🇨🇭🇵🇱🇨🇦🇨🇿🇸🇪🇯🇵🇳🇴🇩🇰🇺🇦]*)?\s*'
    r'(USD|EUR|PLN|GBP|CHF|CAD|CZK|SEK|JPY|NOK|DKK|UAH)\s*'
    r'([0-9]+[.,][0-9]+)(?:⬆️|⬇️)?\s*/\s*([0-9]+[.,][0-9]+)(?:⬆️|⬇️)?',
    re.U | re.I
)

# Метадані
ID_RE     = re.compile(r'\[MESSAGE_ID\]\s*(\d+)')
DATE_RE   = re.compile(r'\[DATE\]\s*([\d-]+\s[\d:]+)')
EDITED_RE = re.compile(r'\[EDITED\]\s*([\d-]+\s[\d:]+)')
VER_RE    = re.compile(r'\[VERSION\]\s*(\S+)')

HOT_KEYWORDS = ["🔥", "акці", "знижк", "promo", "продамо", "спецкурс"]

def is_hot_offer(line: str) -> bool:
    return any(k in line.lower() for k in HOT_KEYWORDS)

def clean_line(ln: str) -> str:
    """Уніфікує символи з Telegram."""
    return (
        ln.replace("–", "-")
          .replace("—", "-")
          .replace("⁄", "/")
          .replace(",", ".")
          .replace("\u00A0", " ")
          .replace("：", ":")
          .replace("‐", "-")
          .strip()
    )

# === Основна функція ===
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
            if not msg_id and (m := ID_RE.search(ln)):     msg_id = m.group(1)
            if not version and (v := VER_RE.search(ln)):   version = v.group(1)
            if not published and (d := DATE_RE.search(ln)):published = d.group(1)
            if not edited and (e := EDITED_RE.search(ln)): edited = e.group(1)
        if not msg_id or not version or not published:
            continue
        if not edited:
            edited = published

        found_any = False
        for ln in block:
            if is_hot_offer(ln):
                continue

            ln = clean_line(ln)
            m = LINE_RE.search(ln)
            if not m:
                continue

            cur_a, cur_b = m.group(1).upper(), m.group(2).upper()
            buy, sell = norm_price_auto(m.group(3)), norm_price_auto(m.group(4))
            comment = ""

            if not buy or not sell:
                continue

            # 🔄 Якщо гривня — робимо її другою (USD, UAH)
            if cur_a == "UAH" and cur_b != "UAH":
                cur_a, cur_b = cur_b, cur_a
            elif cur_b == "UAH" and cur_a != "UAH":
                pass  # вже ок

            # 💱 Крос-курси залишаємо як є
            if cur_a != "UAH" and cur_b != "UAH":
                comment = f"крос-курс ({cur_a}/{cur_b})"

            pair_key = (cur_a, cur_b)
            new_rate = (cur_a, cur_b, buy, sell)
            if not is_rate_changed(new_rate, previous_rates.get(pair_key)):
                skipped += 1
                continue
            previous_rates[pair_key] = (buy, sell)

            rows.append([
                CHANNEL, msg_id, version, published, edited,
                cur_a, cur_b, buy, sell, comment
            ])
            found_any = True

        if not found_any:
            continue

    return rows, skipped


def parse_once():
    print(f"[RUN] Обробка RAW для каналу {CHANNEL}")
    rows, skipped = process_file()
    found = len(rows) + skipped
    print(f"[FOUND] У RAW: {found} | [SKIPPED]: {skipped}")
    save_rows(rows, OUTPUT_FILE)


if __name__ == "__main__":
    parse_once()
