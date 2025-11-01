# === version_control.py (v10) ===
import csv
import os
from decimal import Decimal, InvalidOperation


def load_last_rates(csv_file):
    """Зчитує останні курси для швидкого порівняння"""
    last = {}
    if not os.path.exists(csv_file):
        return last
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            pair = (r['currency_a'], r['currency_b'])
            last[pair] = (r['buy'], r['sell'])
    return last


def is_rate_changed(new_rate, last_rate):
    """Порівнює курси числово, щоб ігнорувати 41.5 vs 41.50"""
    if not last_rate:
        return True
    buy_old, sell_old = last_rate
    _, _, buy_new, sell_new = new_rate
    try:
        b_old, s_old = Decimal(buy_old), Decimal(sell_old)
        b_new, s_new = Decimal(buy_new), Decimal(sell_new)
    except InvalidOperation:
        return buy_old != buy_new or sell_old != sell_new
    return (b_old != b_new) or (s_old != s_new)
