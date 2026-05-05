import asyncio
from pyrogram import Client

bot_token = '8620223217:AAGhKHLni-GiLfwOQAfzMnODw-VtgFrOHIs'

async def main():
    try:
        app = Client("test_bot_session2", api_id=34925580, api_hash='49e4edbc419d87675bf76a6467c786cd', bot_token=bot_token)
        await app.start()
        print("Logged in as", (await app.get_me()).first_name)
        
        target = 5123283499
        print("Sending to", target)
        
        with open("[ 10 - The Return ].txt", "w") as f: f.write("test")
        msg = await app.send_document(target, "[ 10 - The Return ].txt")
        print("Sent filename:", msg.document.file_name)
        
        with open("10 - The Return.txt", "w") as f: f.write("test")
        msg2 = await app.send_document(target, "10 - The Return.txt")
        print("Sent filename 2:", msg2.document.file_name)
        
        await app.stop()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(main())
