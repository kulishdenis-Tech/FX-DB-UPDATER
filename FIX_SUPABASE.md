# 🔧 Інструкція з виправлення Supabase БД

## Проблема
Constraint `rates_uq_full` використовує `published` замість `edited`, що призводить до дублікатів помилок при вставці нових версій повідомлень.

## Рішення
Потрібно змінити constraint на `edited` (timestamp останньої зміни).

---

## 📋 Що зробити ВРУЧНУ:

### 1️⃣ Змінити constraint в Supabase
1. Відкрий **Supabase Dashboard** → твій проект
2. Перейди до **SQL Editor**
3. Скопіюй команду з файлу `fix_supabase_constraint.sql` і виконай:

```sql
ALTER TABLE rates DROP CONSTRAINT IF EXISTS rates_uq_full;
CREATE UNIQUE INDEX rates_uq_full ON rates(channel_id, message_id, currency_a, currency_b, buy, sell, edited);
```

4. Натисни **Run**

### 2️⃣ Очистити таблицю rates
Після зміни constraint, **видали всі дані** з таблиці:

```sql
TRUNCATE TABLE rates;
```

Або якщо TRUNCATE не працює:

```sql
DELETE FROM rates;
```

### 3️⃣ Перевірити constraint
Виконай цей запит щоб переконатись що constraint створений:

```sql
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'rates' 
AND indexname = 'rates_uq_full';
```

Маєш побачити:
```
rates_uq_full | CREATE UNIQUE INDEX rates_uq_full ON rates(channel_id, message_id, currency_a, currency_b, buy, sell, edited)
```

---

## ✅ Після цього:
Парсер автоматично почне працювати з новою логікою при наступному запуску Cron Job!

---

## 🔍 Що змінилось:
- **ДО:** `UNIQUE(channel_id, message_id, currency_a, currency_b, buy, sell, published)` ❌
- **ПІСЛЯ:** `UNIQUE(channel_id, message_id, currency_a, currency_b, buy, sell, edited)` ✅

Тепер кожна версія редагування повідомлення буде окремим унікальним записом!

