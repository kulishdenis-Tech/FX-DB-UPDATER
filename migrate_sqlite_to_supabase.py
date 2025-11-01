import sqlite3
from db_adapter_cloud import SupabaseAdapter

# Ініціалізація адаптера Supabase
cloud = SupabaseAdapter()

# Підключення до локальної SQLite
sqlite_path = "fx_parser.db"
conn = sqlite3.connect(sqlite_path)
cur = conn.cursor()

print("[MIGRATE] 🔍 Зчитую таблицю rates ...")
cur.execute("""
    SELECT c.name AS channel, r.message_id, r.version, r.published, r.edited,
           r.currency_a, r.currency_b, r.buy, r.sell, r.comment
    FROM rates r
    JOIN channels c ON r.channel_id = c.id
    ORDER BY r.id ASC
""")
rows = cur.fetchall()

print(f"[MIGRATE] ✅ Знайдено {len(rows)} рядків для перенесення")

# Форматування під insert_rates()
batched = []
for r in rows:
    # кожен r = (channel, msg_id, version, published, edited, curA, curB, buy, sell, comment)
    row = [
        r[0], str(r[1] or ""), r[2] or "", r[3] or "", r[4] or "",
        r[5] or "", r[6] or "", r[7] or 0, r[8] or 0, r[9] or ""
    ]
    batched.append(row)

print("[MIGRATE] 🚀 Починаю заливку у Supabase ...")

# пакетами по 100
batch_size = 100
for i in range(0, len(batched), batch_size):
    subset = batched[i:i+batch_size]
    ch = subset[0][0]
    try:
        cloud.insert_rates(ch, subset)
    except Exception as e:
        print(f"[ERROR] ❌ Помилка у пакеті {i}-{i+batch_size}: {e}")

print("[MIGRATE] 🟢 Міграцію завершено!")
conn.close()
