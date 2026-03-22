import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from pyrogram import idle
from bot import Bot

# Global statistics for the session
START_TIME = time.time()
TOTAL_FILES_FWD = 0
TOTAL_DOWNLOADS = 0
TOTAL_UPLOADS   = 0
TOTAL_BYTES_TRANSFERRED = 0

def get_uptime():
    elapsed = time.time() - START_TIME
    days, rem = divmod(elapsed, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"

async def web_server():
    async def handle(request):
        uptime = get_uptime()
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Bot Status</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background-color: #f0f2f5;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                }}
                .container {{
                    background-color: white;
                    padding: 40px;
                    border-radius: 12px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    text-align: center;
                    max-width: 400px;
                    width: 100%;
                }}
                h1 {{
                    color: #1a73e8;
                    margin-bottom: 20px;
                }}
                p {{
                    color: #555;
                    font-size: 18px;
                    margin: 10px 0;
                }}
                .status-active {{
                    color: #28a745;
                    font-weight: bold;
                }}
                .footer {{
                    margin-top: 30px;
                    font-size: 14px;
                    color: #888;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Bot is Running</h1>
                <p>Status: <span class="status-active">Active</span></p>
                <p>Uptime: {uptime}</p>
                <div class="footer">
                    Powered by Aryᴀ Bᴏᴛ
                </div>
            </div>
        </body>
        </html>
        """
        return web.Response(text=html_content, content_type='text/html')

    app = web.Application()
    app.add_routes([web.get('/', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"Web server started on port {port}")

async def ping_server():
    while True:
        await asyncio.sleep(300) # Ping every 5 minutes
        try:
            port = int(os.environ.get('PORT', 8080))
            url = f'http://127.0.0.1:{port}'
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    logging.info(f"Self-ping to {url}: Status {resp.status}")
        except Exception as e:
            logging.error(f"Self-ping failed: {e}")

async def sync_global_stats():
    from database import db
    global TOTAL_FILES_FWD, TOTAL_DOWNLOADS, TOTAL_UPLOADS, TOTAL_BYTES_TRANSFERRED
    
    # Load past stats and add any that accumulated during the first few ms of boot
    st_doc = await db.db.global_stats.find_one({"_id": "stats"})
    if st_doc:
        TOTAL_FILES_FWD += st_doc.get("fwd", 0)
        TOTAL_DOWNLOADS += st_doc.get("dl", 0)
        TOTAL_UPLOADS += st_doc.get("ul", 0)
        TOTAL_BYTES_TRANSFERRED += st_doc.get("bytes", 0)
        
    while True:
        await asyncio.sleep(10)
        try:
            await db.db.global_stats.update_one(
                {"_id": "stats"},
                {"$set": {
                    "fwd": TOTAL_FILES_FWD,
                    "dl": TOTAL_DOWNLOADS,
                    "ul": TOTAL_UPLOADS,
                    "bytes": TOTAL_BYTES_TRANSFERRED
                }},
                upsert=True
            )
        except Exception as e:
            logging.error(f"Stats sync error: {e}")

async def main():
    # ── Startup cleanup — remove any leftover partial download files ─────────
    import shutil
    downloads_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
    if os.path.exists(downloads_dir):
        shutil.rmtree(downloads_dir, ignore_errors=True)
        logging.info("Cleared leftover downloads/ folder from previous session.")
    os.makedirs(downloads_dir, exist_ok=True)
    # ────────────────────────────────────────────────────────────────────────

    bot = Bot()
    await bot.start()

    # Start web server
    await web_server()

    # Start background tasks
    asyncio.create_task(ping_server())
    asyncio.create_task(sync_global_stats())

    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
