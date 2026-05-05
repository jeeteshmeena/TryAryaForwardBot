import asyncio
from pyrogram import Client
from config import Config

async def main():
    try:
        app = Client("my_account")
        await app.start()
        print("Logged in as", (await app.get_me()).first_name)
        
        with open("[ 10 - The Return ].txt", "w") as f:
            f.write("test")
            
        msg = await app.send_document("me", "[ 10 - The Return ].txt")
        print("Sent filename:", msg.document.file_name)
        
        with open("10 - The Return.txt", "w") as f:
            f.write("test")
        
        msg2 = await app.send_document("me", "10 - The Return.txt")
        print("Sent filename 2:", msg2.document.file_name)
        
        await app.stop()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(main())
