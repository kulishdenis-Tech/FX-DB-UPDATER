import os
import csv
import sqlite3
from datetime import datetime


class DatabaseAdapter:
    """SQLite адаптер для FX‑парсерів (підтримка структури з папкою PARSED)."""

    def __init__(self, base_dir=None):
        self.base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
        self.parsed_dir = os.path.join(self.base_dir, "PARSED")
        self.db_file = os.path.join(self.base_dir, "fx_parser.db")
        self.schema_file = os.path.join(self.base_dir, "fx_parser_schema.sql")

        # Ініціалізуємо базу
        self._ensure_database()
        self.conn = sqlite3.connect(self.db_file)

    # -----------------------------
    # Розділ 1. Ініціалізація
    # -----------------------------

    def _ensure_database(self):
        if not os.path.exists(self.db_file):
            print(f"[DB] Створюю нову базу: {self.db_file}")
            self._apply_schema()
        else:
            print(f"[DB] ✅ SQLite база існує: {self.db_file}")

    def _apply_schema(self):
        if not os.path.exists(self.schema_file):
            raise FileNotFoundError("fx_parser_schema.sql не знайдено")
        with open(self.schema_file, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        conn = sqlite3.connect(self.db_file)
        conn.executescript(schema_sql)
        conn.commit()
        conn.close()
        print("[DB] ✅ Схема застосована")

    # -----------------------------
    # Розділ 2. Загальні методи
    # -----------------------------

    def _get_channel_id(self, channel_name):
        cur = self.conn.execute("SELECT id FROM channels WHERE name = ?", (channel_name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur = self.conn.execute("INSERT INTO channels (name) VALUES (?)", (channel_name,))
        self.conn.commit()
        return cur.lastrowid

    # -----------------------------
    # Розділ 3. Основна логіка вставки
    # -----------------------------

    def insert_rates(self, channel, rows):
        """Вставляє записи з парсера (rows — список списків)"""
        ch_id = self._get_channel_id(channel)
        inserted, skipped = 0, 0

        for r in rows:
            try:
                self.conn.execute("""
                    INSERT INTO rates (
                        channel_id, message_id, version, published, edited,
                        currency_a, currency_b, buy, sell, comment
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ch_id, r[1], r[2], r[3], r[4],
                    r[5], r[6], r[7], r[8], r[9]
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1
                continue

        self.conn.commit()
        return inserted, skipped

    # -----------------------------
    # Розділ 4. Міграція CSV у БД
    # -----------------------------

    def migrate_csv(self, csv_path):
        channel_name = os.path.basename(csv_path).split("_")[0]
        print(f"[MIGRATE] {channel_name} → SQLite")

        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [[
                channel_name,
                row["message_id"], row["version"], row["published"], row["edited"],
                row["currency_a"], row["currency_b"], row["buy"], row["sell"], row["comment"]
            ] for row in reader]

        inserted, skipped = self.insert_rates(channel_name, rows)
        print(f"[MIGRATE] ✅ Додано: {inserted} | Пропущено: {skipped}\n")

    def migrate_all_parsed(self):
        """Імпортує всі CSV з папки PARSED"""
        if not os.path.isdir(self.parsed_dir):
            print(f"[ERROR] Папку PARSED не знайдено: {self.parsed_dir}")
            return

        files = [f for f in os.listdir(self.parsed_dir) if f.endswith(".csv")]
        if not files:
            print("[INFO] ⛔ CSV файлів не знайдено")
            return

        for f in files:
            self.migrate_csv(os.path.join(self.parsed_dir, f))

        print("[DONE] ✅ Всі CSV перенесено в SQLite")


# ============================================
# Автоматичне виконання при запуску напряму
# ============================================
if __name__ == "__main__":
    db = DatabaseAdapter()
    db.migrate_all_parsed()
