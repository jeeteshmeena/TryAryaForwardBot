import asyncio
import pyrogram
from database import db
from config import Config

async def test():
    await db.connect()
    bot = pyrogram.Client(
        "Auto-Forward-Bot",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        bot_token=Config.BOT_TOKEN
    )
    await bot.start()
    
    chat_id = "testpublicchannel" # wait I don't know the user's chat_id. Let me use some public channel like "Telegram"
    chat_id = "Telegram"
    
    lo, hi = 1, 9_999_999
    print(f"Testing binary search on {chat_id}")
    for i in range(25):
        if hi - lo <= 50: break
        mid = (lo + hi) // 2
        try:
            print(f"Trying {mid}")
            probe = await bot.get_messages(chat_id, [mid])
            if not isinstance(probe, list): probe = [probe]
            if any(m and getattr(m, 'empty', True) is False for m in probe):
                print("Exists!")
                lo = mid
            else:
                print("Empty!")
                hi = mid
        except Exception as e:
            print(f"Exception at {mid}: {type(e).__name__} - {e}")
            hi = mid
            
    print(f"Found top_id: {hi}")
    await bot.stop()

if __name__ == '__main__':
    asyncio.run(test())
