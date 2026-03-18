import asyncio
from pyrogram import Client
from config import Config

async def main():
    config = Config()
    client = Client(
        "my_test_session",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.USER_SESSION
    )
    await client.start()
    
    # get latest message from a known bot chat, e.g., @BotFather
    target_bot = "@BotFather"
    msgs = []
    async for m in client.get_chat_history(target_bot, limit=1):
        msgs.append(m)
        
    m = msgs[0]
    print(f"Message ID: {m.id}, Chat ID: {m.chat.id}")
    
    try:
        await client.copy_message("me", m.chat.id, m.id)
        print("copy_message SUCCESS")
    except Exception as e:
        print(f"copy_message FAIL: {type(e).__name__} - {e}")
        
    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
