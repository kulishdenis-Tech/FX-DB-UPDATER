-- === truncate_rates.sql ===
-- Очищення таблиці rates перед першим запуском оновлених парсерів
-- 
-- ВАЖЛИВО: Це видалить ВСІ дані з таблиці rates!
-- Використовуйте лише для тестових запусків на чистій БД.

-- Видалити всі записи і скинути auto-increment (якщо є)
TRUNCATE TABLE rates RESTART IDENTITY CASCADE;

-- Перевірити, що таблиця пуста
SELECT COUNT(*) as remaining_rows FROM rates;

-- Має бути: remaining_rows = 0

