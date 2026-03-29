import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from pyrogram import idle
from bot import Bot

# Calculate uptime
START_TIME = time.time()

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

async def stats_tracker():
    import psutil
    from database import db
    last_io = psutil.net_io_counters()
    while True:
        await asyncio.sleep(60)
        curr_io = psutil.net_io_counters()
        diff = (curr_io.bytes_recv - last_io.bytes_recv) + (curr_io.bytes_sent - last_io.bytes_sent)
        if diff > 0:
            try:
                await db.update_global_stats(total_data_usage_bytes=diff)
            except Exception as e:
                logging.error(f"Global stats update error: {e}")
        last_io = curr_io

async def main():
    bot = Bot()
    await bot.start()

    try:
        from plugins.share_bot import start_share_bot
        await start_share_bot()
    except Exception as e:
        logging.error(f"Failed to init share bot: {e}")

    # Start web server
    await web_server()

    # Start self-ping task
    asyncio.create_task(ping_server())
    
    # Start stats tracking task
    asyncio.create_task(stats_tracker())

    await idle()
    try:
        from plugins.share_bot import share_clients
        for c in share_clients.values():
            try: await c.stop()
            except: pass
    except Exception: pass
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
