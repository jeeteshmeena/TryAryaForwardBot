import asyncio
from pyrogram import Client

bot_token = '8620223217:AAGhKHLni-GiLfwOQAfzMnODw-VtgFrOHIs'

async def main():
    try:
        app = Client("test_bot_session_video", api_id=34925580, api_hash='49e4edbc419d87675bf76a6467c786cd', bot_token=bot_token)
        await app.start()
        print("Logged in as", (await app.get_me()).first_name)
        
        target = 5123283499
        print("Sending to", target)
        
        # create a dummy video file
        import os
        os.system('ffmpeg -f lavfi -i color=c=black:s=100x100:d=1 -c:v libx264 -y "10 - The Return.mp4" 2> NUL')
        os.system('ffmpeg -f lavfi -i color=c=black:s=100x100:d=1 -c:v libx264 -y "[ 10 - The Return ].mp4" 2> NUL')
        
        msg = await app.send_video(target, "[ 10 - The Return ].mp4")
        print("Sent filename:", msg.video.file_name)
        
        msg2 = await app.send_video(target, "10 - The Return.mp4")
        print("Sent filename 2:", msg2.video.file_name)
        
        await app.stop()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(main())
