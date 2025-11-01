# FX-DB-UPDATER

Cloud парсер для Telegram-каналів валютних курсів.

## 📁 Структура

```
FX-DB-UPDATER/
├── supabase_io.py           # Уніфікований модуль для Supabase
├── fx_parse_SWAPS_auto.py   # Парсер для каналу SWAPS
├── requirements.txt         # Залежності
└── .env                     # Змінні оточення (не в Git!)
```

## 🚀 Як використовувати

### 1. Налаштування Supabase

Створи `.env` файл:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_service_role_key
```

### 2. Запуск парсера локально

```bash
python fx_parse_SWAPS_auto.py
```

### 3. Розгортання на Render

**Варіант A: Cron Job (рекомендовано)**

1. Відкрий https://dashboard.render.com
2. Натисни "New +" → **"Cron Job"**
3. Підключи GitHub репо: **kulishdenis-Tech/FX-DB-UPDATER**
4. Налаштуй:
   - **Name**: `fx-parser-swaps`
   - **Schedule**: `*/10 * * * *` (кожні 10 хвилин)
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python fx_parse_SWAPS_auto.py`
5. Додай Environment Variables (Supabase Dashboard → Settings → API):
   - `SUPABASE_URL` = `https://xxxxx.supabase.co`
   - `SUPABASE_KEY` = `service_role_key` (не anon key!)
6. Натисни "Create Cron Job"

**⚠️ Увага:** Якщо у тебе вже є Background Worker **FX-DB-UPDATER**, видали його перед створенням Cron Job!

## 📊 Що робить парсер

1. ✅ Читає RAW дані з Supabase Storage (`SWAPS_raw.txt`)
2. ✅ Парсить курси валют (USD, EUR, PLN тощо)
3. ✅ Витягує метадані (message_id, version, published, edited)
4. ✅ Фільтрує дублікати
5. ✅ Записує нові курси в таблицю `rates` у Supabase

## 🔧 Розробка

Парсер використовує **ті самі регулярки** що й локальна версія, але:
- Читає з **Supabase Storage** замість локальних файлів
- Записує тільки в **Supabase DB** (без CSV)
- Перевіряє дублікати через **БД запит**

## 📝 Логи

```
[CLOUD] ✅ Підключено до Supabase
[CLOUD] ✅ Завантажено 54234 символів з Supabase
[CLOUD] Останні курси в БД: 15 пар
[PARSED] Знайдено: 20 | Нових: 5 | Пропущено: 15
  → UAH пар: 3 | Крос-курсів: 2
[CLOUD] 🌐 SWAPS: додано 5, пропущено 15
[DONE] ✅ Готово.
```

