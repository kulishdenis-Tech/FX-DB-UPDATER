import os
import asyncio
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage_utils import upload_text, download_text
from dotenv import load_dotenv

# ──────────────── CONFIG ────────────────
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

TG_API_ID = int(os.getenv("TG_API_ID", "6"))
TG_API_HASH = os.getenv("TG_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")
TG_USER_SESSION = os.getenv("TG_USER_SESSION")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

SESSION_NAME = "render_fetcher_session"

CHANNELS = {
    "MIRVALUTY": "@mirvaluty",
    "GARANT": "@obmen_kyiv",
    "KIT_GROUP": "@obmenka_kievua",
    "CHANGE_KYIV": "@kiev_change",
    "VALUTA_KIEV": "@valuta_kiev",
    "UACOIN": "@uacoin",
    "SWAPS": "@Obmen_usd"
}


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_block(channel: str, message_id: int, version: int, text: str, ts: datetime, edited_ts=None) -> str:
    lines = [
        f"[CHANNEL] {channel}",
        f"[MESSAGE_ID] {message_id}",
        f"[VERSION] v{version}",
        f"[DATE] {ts.strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    if edited_ts:
        lines.append(f"[EDITED] {edited_ts.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("-" * 100)
    lines.append(text or "")
    lines.append("=" * 120)
    return "\n".join(lines) + "\n"


def detect_next_version(existing: str, message_id: int) -> int:
    v = 0
    key = f"[MESSAGE_ID] {message_id}"
    for line in existing.splitlines():
        if line.strip() == key:
            v = max(v, 1)
        if v >= 1 and line.startswith("[VERSION] v"):
            try:
                num = int(line.split("v")[-1].strip())
                v = max(v, num)
            except:
                pass
    return v + 1 if v > 0 else 1


async def bootstrap_history(client: TelegramClient, name: str, username: str):
    filename = f"{name}_raw.txt"
    current = download_text(filename)
    if current:
        print(f"[INIT] {filename} вже існує, пропускаю початкове завантаження.")
        return

    print(f"[INIT] Завантажую історію {name}…")
    msgs = []
    async for m in client.iter_messages(username, limit=300):
        if not m.message:
            continue
        block = make_block(name, m.id, 1, m.message, m.date)
        msgs.append(block)
    msgs.reverse()
    upload_text(filename, "".join(msgs))
    print(f"[INIT] Snapshot {name} готовий ({len(msgs)} повідомлень).")


async def main():
    print("[START] Telegram Fetcher (Render, continuous mode)")
    if TG_USER_SESSION:
        client = TelegramClient(StringSession(TG_USER_SESSION), TG_API_ID, TG_API_HASH)
        await client.start()
        print("[MODE] ✅ USER session active")
    else:
        client = TelegramClient(SESSION_NAME, TG_API_ID, TG_API_HASH)
        await client.start(bot_token=TG_BOT_TOKEN)
        print("[MODE] ⚙️ BOT session active (обмежений доступ)")

    for name, username in CHANNELS.items():
        await bootstrap_history(client, name, username)

    print(f"[{now_str()}] [LISTENING] Всі канали запущено.")

    @client.on(events.NewMessage(chats=list(CHANNELS.values())))
    async def on_new_message(event):
        try:
            chat = await event.get_chat()
            alias = next((a for a, u in CHANNELS.items() if u.lower().lstrip("@") == chat.username.lower()), chat.username)
            filename = f"{alias}_raw.txt"
            existing = download_text(filename)
            version = detect_next_version(existing, event.id)
            block = make_block(alias, event.id, version, event.message.message or "", event.date)
            new_content = (existing + ("\n" if existing and not existing.endswith("\n") else "") + block)
            upload_text(filename, new_content)
            print(f"[NEW] {alias} #{event.id} v{version}")
        except Exception as e:
            print(f"[ERR] on_new_message: {e}")

    @client.on(events.MessageEdited(chats=list(CHANNELS.values())))
    async def on_edit_message(event):
        try:
            chat = await event.get_chat()
            alias = next((a for a, u in CHANNELS.items() if u.lower().lstrip("@") == chat.username.lower()), chat.username)
            filename = f"{alias}_raw.txt"
            existing = download_text(filename)
            version = detect_next_version(existing, event.id)
            block = make_block(alias, event.id, version, event.message.message or "", event.date, event.edit_date)
            new_content = (existing + ("\n" if existing and not existing.endswith("\n") else "") + block)
            upload_text(filename, new_content)
            print(f"[EDIT] {alias} #{event.id} v{version}")
        except Exception as e:
            print(f"[ERR] on_edit_message: {e}")

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"[{now_str()}] [EXIT] Зупинено користувачем.")