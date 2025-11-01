# === telegram_fetcher.py (v2) ===
# ✅ універсальний мультиканальний Telegram RAW fetcher
# підтримує і channel_id, і username (автоматично визначає через get_entity)

from telethon import TelegramClient, events
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import asyncio
import os
import re

# ========= CONFIG =========
load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE_NUMBER")

# 🔸 Список каналів (ID або username — працює обидва варіанти)
CHANNELS = {
    "MIRVALUTY": "@mirvaluty",
    "GARANT": "@obmen_kyiv",
    "KIT_GROUP": "@obmenka_kievua",
    "CHANGE_KYIV": "@kiev_change",
    "VALUTA_KIEV": "@valuta_kiev",
    "UACOIN": "@uacoin",
    "SWAPS": "@Obmen_usd"
}

BASE_DIR = r"C:\Users\kulis\Documents\Google drive\Exchange"
RAW_DIR = os.path.join(BASE_DIR, "RAW")
os.makedirs(RAW_DIR, exist_ok=True)

HISTORY_LIMIT = 200
TZ_KYIV = ZoneInfo("Europe/Kyiv")
# ===========================


# === 📦 Допоміжні функції ===
def now_str():
    return datetime.now(TZ_KYIV).strftime("%Y-%m-%d %H:%M:%S")


def local_dt(dt):
    if not dt:
        return ""
    return dt.astimezone(TZ_KYIV).strftime("%Y-%m-%d %H:%M:%S")


def output_path(channel_name: str):
    """Шлях до RAW-файлу"""
    return os.path.join(RAW_DIR, f"{channel_name}_raw.txt")


def prepend_to_file(path: str, text: str):
    """Додає блок на початок файлу"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        with open(path, "r", encoding="utf-8") as f:
            old = f.read()
        with open(path, "w", encoding="utf-8") as f:
            f.write(text + "\n" + old)


def save_raw_message(channel_name, message_id, version, date, edited, text):
    """Записує повідомлення у RAW-файл (нові зверху)"""
    path = output_path(channel_name)
    block = (
        "=" * 100 + "\n"
        + f"[CHANNEL] {channel_name}\n"
        + f"[MESSAGE_ID] {message_id}\n"
        + f"[VERSION] v{version}\n"
        + f"[DATE] {date}\n"
    )
    if edited:
        block += f"[EDITED] {edited}\n"
    block += (
        "-" * 100 + "\n"
        + (text.strip() if text else "[NO TEXT]") + "\n"
        + "=" * 100 + "\n\n"
    )
    prepend_to_file(path, block)
    print(f"[{now_str()}] [SAVED] {channel_name} id={message_id} v{version} "
          f"(дата: {date}{' | редаговано: ' + edited if edited else ''})")


def load_existing_versions(path: str):
    """Зчитує message_id → версію з RAW"""
    versions = {}
    if not os.path.exists(path):
        return versions
    msg_id = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("[MESSAGE_ID]"):
                found = re.findall(r"\d+", line)
                if found:
                    msg_id = found[0]
                    versions[msg_id] = versions.get(msg_id, 1)
            elif line.startswith("[VERSION]") and msg_id:
                found_v = re.findall(r"v(\d+)", line)
                if found_v:
                    versions[msg_id] = max(versions[msg_id], int(found_v[0]))
    return versions


# === 🔄 Основна логіка ===
async def parse_message(message, channel_name, versions):
    text = message.message or ""
    msg_id = str(message.id)
    date = local_dt(message.date)
    edited = local_dt(message.edit_date) if message.edit_date else ""

    # нова або оновлена версія
    if msg_id in versions and edited:
        versions[msg_id] += 1
    elif msg_id not in versions:
        versions[msg_id] = 1
    else:
        return

    save_raw_message(channel_name, msg_id, versions[msg_id], date, edited, text)


async def get_entity_safe(client, channel_ref):
    """Повертає Telegram entity, незалежно від того, username чи ID"""
    try:
        if isinstance(channel_ref, int) or str(channel_ref).startswith("-100"):
            return await client.get_input_entity(channel_ref)
        else:
            entity = await client.get_entity(channel_ref)
            return entity
    except Exception as e:
        print(f"[WARN] Не вдалося отримати entity для {channel_ref}: {e}")
        return None


async def initial_load(client, channel_name, entity, versions):
    print(f"[{now_str()}] [INIT] Завантажую {HISTORY_LIMIT} повідомлень із {channel_name}…")
    messages = [m async for m in client.iter_messages(entity, limit=HISTORY_LIMIT)]
    messages = sorted(messages, key=lambda x: x.date)

    for msg in messages:
        await parse_message(msg, channel_name, versions)

    print(f"[{now_str()}] [INIT READY] Початкове завантаження {channel_name} завершено.")


async def monitor_channel(client, channel_name, channel_ref):
    path = output_path(channel_name)
    versions = load_existing_versions(path)
    first_run = not os.path.exists(path) or os.path.getsize(path) == 0

    entity = await get_entity_safe(client, channel_ref)
    if not entity:
        print(f"[ERROR] Не можу отримати доступ до {channel_name}. Пропускаю.")
        return

    if first_run:
        await initial_load(client, channel_name, entity, versions)
    else:
        print(f"[{now_str()}] [READY] Моніторинг {channel_name} (відомих версій: {len(versions)})")

    # --- обробка нових / редагованих ---
    @client.on(events.NewMessage(chats=[entity]))
    async def new_handler(event):
        await parse_message(event.message, channel_name, versions)

    @client.on(events.MessageEdited(chats=[entity]))
    async def edit_handler(event):
        await parse_message(event.message, channel_name, versions)


async def main():
    session_name = "session_raw_multi"
    async with TelegramClient(session_name, API_ID, API_HASH) as client:
        print(f"[{now_str()}] [START] Telegram fetcher активовано.")
        for name, ref in CHANNELS.items():
            await monitor_channel(client, name, ref)
        print(f"[{now_str()}] [LISTENING] Всі канали запущено.")
        await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[{now_str()}] [EXIT] Зупинено користувачем.")
