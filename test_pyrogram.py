import asyncio
from pyrogram import Client
from config import Config

async def main():
    async with Client("BOT", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True) as app:
        try:
            ch = await app.get_chat("@AryaForwardBot")
            print("get_chat ok:", ch.type)
        except Exception as e:
            print("get_chat failed:", type(e).__name__, e)
        try:
            msgs = await app.get_messages("@AryaForwardBot", [1, 2, 3])
            print("get_messages by str ok:", len(msgs))
        except Exception as e:
            print("get_messages by str failed:", type(e).__name__, e)

if __name__ == "__main__":
    asyncio.run(main())
