# 💱 FX Parser (Render Deployment)

### 🧩 Опис

Хмарна версія парсера валютних каналів.  
Сервіс на **Render** автоматично:
1. зчитує RAW-файли з **Supabase Storage** (`bucket: raw`);
2. запускає 7 незалежних парсерів (`fx_parse_*`);
3. зберігає результати у таблицю **`rates`** в Supabase Database;
4. виводить детальний лог виконання у Render Logs.

---

### ⚙️ Структура проєкту

```
fx_master_runner.py      ← головний раннер (запускається Render)
fx_parse_GARANT_auto.py
fx_parse_KIT_GROUP_auto.py
fx_parse_VALUTA_KIEV_auto.py
fx_parse_MIRVALUTY_auto.py
fx_parse_CHANGE_KYIV_auto.py
fx_parse_UACOIN_auto.py
fx_parse_SWAPS_auto.py
requirements.txt
```

---

### 🚀 Розгортання на Render

1. Створи **Background Worker** на Render.
2. Завантаж код або підключи GitHub-репозиторій.
3. У **Settings → Environment** додай:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your_service_role_key
   RAW_BUCKET=raw
   ```
4. У полі **Start Command** вкажи:
   ```
   python fx_master_runner.py
   ```
5. Натисни **Deploy**.

---

### 📊 Приклад логів

```
=== 🌍 FX Master Runner (Supabase Cloud Mode) ===
[RUN] 🔎 Канал: GARANT
[CLOUD] GARANT       → додано: 7 (із 7)
[OK] ✅ GARANT → додано 7 із 7 | 2.4s
[RUN] 🔎 Канал: SWAPS
[CLOUD] SWAPS        → додано: 3 (із 3)
[OK] ✅ SWAPS → додано 3 із 3 | 1.1s
🏁 [DONE] 08:05:41 UTC
📊 Підсумок: OK=7 | SKIP=0 | ERRORS=0
```

---

### ✅ Готово

Render автоматично:
- підтягує нові RAW із Supabase,
- оновлює базу курсів у `rates`,
- показує результати у Render Logs.

---
