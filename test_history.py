import asyncio
import pyrogram
from config import Config

async def test():
    bot = pyrogram.Client(
        "Auto-Forward-Bot",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        bot_token=Config.BOT_TOKEN
    )
    await bot.start()
    
    try:
        print("Testing get_chat_history for @telegram ...")
        async for m in bot.get_chat_history("@telegram", limit=2):
            print(f"Got message {m.id}")
            
    except Exception as e:
        print(f"Exception: {type(e).__name__} - {e}")
            
    await bot.stop()

if __name__ == '__main__':
    asyncio.run(test())
