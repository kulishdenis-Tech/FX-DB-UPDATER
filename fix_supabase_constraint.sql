-- ============================================
-- FIX SUPABASE CONSTRAINT: rates_uq_full
-- ============================================
-- Проблема: зараз constraint на (published), але це не унікально
-- Рішення: змінити на (edited) - тоді кожна версія редагування буде окремим записом

-- 1. Видалити старий constraint
ALTER TABLE rates DROP CONSTRAINT IF EXISTS rates_uq_full;

-- 2. Створити новий constraint на edited
CREATE UNIQUE INDEX rates_uq_full ON rates(channel_id, message_id, currency_a, currency_b, buy, sell, edited);

-- Готово! Тепер кожна версія редагування (edited) буде окремим унікальним записом.

