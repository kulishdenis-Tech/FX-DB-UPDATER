from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio

# 🔹 ВСТАВ СВОЇ ДАНІ (ті, що ти вже використовуєш локально)
API_ID = 22894376          # ← заміни на свій
API_HASH = "4f3d1c7322fe7690365eb3c650b5a0cd"  # ← заміни на свій

client = TelegramClient(StringSession(), API_ID, API_HASH)

async def main():
    print("🔹 Логін через користувача Telegram...")
    await client.start()  # запитає код авторизації через Telegram або SMS
    print("\n✅ Авторизація успішна!\n")
    print("📦 TG_USER_SESSION (скопіюй цей рядок і встав у Render):\n")
    print(StringSession.save(client.session))
    print("\n⚠️ Не передавай цей рядок нікому — він дає повний доступ до акаунту!")

    await client.disconnect()

asyncio.run(main())
