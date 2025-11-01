# === parser_utils.py ===
import os, re, csv

def norm_price_auto(s):
    if not s:
        return None
    s = s.replace(',', '.').strip()
    try:
        return round(float(s), 4)
    except:
        return None

def detect_currency(text):
    """Проста евристика для знаходження коду валюти за прапором."""
    flags = {
        '🇺🇸': 'USD', '🇪🇺': 'EUR', '🇵🇱': 'PLN', '🇬🇧': 'GBP',
        '🇨🇭': 'CHF', '🇨🇦': 'CAD', '🇨🇿': 'CZK', '🇸🇪': 'SEK',
        '🇯🇵': 'JPY', '🇳🇴': 'NOK', '🇩🇰': 'DKK'
    }
    for f, c in flags.items():
        if f in text:
            return c
    return None

def iter_message_blocks(lines, id_re):
    """Розділяє текстовий файл на блоки повідомлень за [MESSAGE_ID]."""
    blocks = []
    current = []
    for ln in lines:
        if id_re.search(ln):
            if current:
                blocks.append(current)
                current = []
        current.append(ln)
    if current:
        blocks.append(current)
    return blocks

def save_rows(rows, output_file):
    """
    Додає лише унікальні рядки.
    Унікальність визначається за: (channel, message_id, version, currency_a, currency_b, comment_norm)
    """
    if not rows:
        print("[INFO] Нових рядків немає.")
        return

    def norm_comment(c):
        return (c or "").strip().lower()

    existing_keys = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for r in reader:
                key = (r['channel'], r['message_id'], r['version'],
                       r['currency_a'], r['currency_b'], norm_comment(r.get('comment')))
                existing_keys.add(key)

    new_unique = []
    for r in rows:
        key = (r[0], r[1], r[2], r[5], r[6], norm_comment(r[9]))
        if key not in existing_keys:
            existing_keys.add(key)
            new_unique.append(r)

    if not new_unique:
        print("[INFO] Усі рядки вже існують — нових немає.")
        return

    new_file = not os.path.exists(output_file)
    with open(output_file, 'a', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        if new_file:
            w.writerow([
                'channel','message_id','version','published','edited',
                'currency_a','currency_b','buy','sell','comment'
            ])
        new_unique.sort(key=lambda r: (int(r[1]), r[2]))
        w.writerows(new_unique)

    print(f"[SAVED] {len(new_unique)} нових унікальних рядків у {output_file}")
