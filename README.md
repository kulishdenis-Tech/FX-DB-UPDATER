# FX-DB-UPDATER

Cloud парсер для Telegram-каналів валютних курсів.

## 📁 Структура

```
FX-DB-UPDATER/
├── supabase_io.py              # Уніфікований модуль для Supabase
├── fx_parse_SWAPS_auto.py      # Парсер для каналу SWAPS
├── requirements.txt            # Залежності
├── FIX_SUPABASE.md            # ⚠️ Інструкція для налаштування БД
├── fix_supabase_constraint.sql # SQL скрипт
└── .env                        # Змінні оточення (не в Git!)
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

**Cron Job (кожні 10 хвилин)**

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

### 4. Налаштування БД (ОДИН РАЗ)

⚠️ **ВАЖЛИВО!** Перед запуском парсера виконай інструкції з `FIX_SUPABASE.md`!

Потрібно видалити UNIQUE INDEX з таблиці `rates`, щоб парсер міг записувати **ВСЕ**.

## 🧠 Логіка роботи парсера

Парсер записує **ВСЕ** без фільтрів:
- ✅ Парсить **всі повідомлення** з RAW файлу
- ✅ Записує **всі версії редагувань**
- ✅ **НЕ фільтрує** дублікати
- ✅ Повна історія для аналізу

**Фільтрація відбувається в мобільному додатку (Flutter)** на рівні UI.

## 📊 Що робить парсер

1. ✅ Читає RAW дані з Supabase Storage (`SWAPS_raw.txt`)
2. ✅ Парсить курси валют (USD, EUR, PLN тощо)
3. ✅ Витягує метадані (message_id, version, published, edited)
4. ✅ Записує **ВСЕ** в таблицю `rates` у Supabase

## 🔧 Розробка

Парсер використовує **ті самі регулярки** що й локальна версія, але:
- Читає з **Supabase Storage** замість локальних файлів
- Записує в **Supabase DB** (без CSV)
- **БЕЗ фільтрації** - повна історія

## 📝 Логи

```
[CLOUD] ✅ Підключено до Supabase
[CLOUD] ✅ Завантажено 102048 символів з Supabase
[PARSED] Знайдено: 330 курсів
  → UAH пар: 264 | Крос-курсів: 66
[CLOUD] ✅ Записано в БД: 330 рядків
[DONE] ✅ Готово.
```
