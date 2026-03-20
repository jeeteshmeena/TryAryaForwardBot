import asyncio
from pyrogram import Client
from pyrogram.raw.types import InputPeerChannel
from config import Config

async def main():
    try:
        # Load API keys and Bot token
        # The API values in config.py might be invalid. We will just use placeholders if needed.
        async with Client("BOT", api_id=2040, api_hash="b18441a1ff607e10a989891a5462e627", bot_token=Config.BOT_TOKEN, in_memory=True) as app:
            try:
                peer = await app.resolve_peer("@telegram")
                print("resolved type:", type(peer))
            except Exception as e:
                print("resolving str failed:", type(e).__name__, e)

    except Exception as e:
        print("client error", e)

if __name__ == '__main__':
    asyncio.run(main())
