import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from pyrogram import idle
from bot import Bot

# Global statistics for the session (synced with DB)
START_TIME = time.time()
TOTAL_FILES_FWD = 0
TOTAL_DOWNLOADS = 0
TOTAL_UPLOADS   = 0
TOTAL_BYTES_TRANSFERRED = 0
LAST_SYNCED_STATS = {}

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
    from database import db
    global TOTAL_FILES_FWD, TOTAL_DOWNLOADS, TOTAL_UPLOADS, TOTAL_BYTES_TRANSFERRED, LAST_SYNCED_STATS
    while True:
        await asyncio.sleep(60) # Ping & DB Sync every 1 min
        try:
            port = int(os.environ.get('PORT', 8080))
            url = f'http://127.0.0.1:{port}'
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    pass
        except Exception:
            pass
        
        # Sync stats to DB
        try:
            diff_fwd = TOTAL_FILES_FWD - LAST_SYNCED_STATS.get("fwd", 0)
            diff_dn  = TOTAL_DOWNLOADS - LAST_SYNCED_STATS.get("dn", 0)
            diff_up  = TOTAL_UPLOADS - LAST_SYNCED_STATS.get("up", 0)
            diff_bt  = TOTAL_BYTES_TRANSFERRED - LAST_SYNCED_STATS.get("bt", 0)
            
            inc_dict = {}
            if diff_fwd > 0: inc_dict["TOTAL_FILES_FWD"] = diff_fwd
            if diff_dn > 0:  inc_dict["TOTAL_DOWNLOADS"] = diff_dn
            if diff_up > 0:  inc_dict["TOTAL_UPLOADS"] = diff_up
            if diff_bt > 0:  inc_dict["TOTAL_BYTES_TRANSFERRED"] = diff_bt
            
            if inc_dict:
                await db.update_bot_stats(**inc_dict)
                LAST_SYNCED_STATS["fwd"] = TOTAL_FILES_FWD
                LAST_SYNCED_STATS["dn"]  = TOTAL_DOWNLOADS
                LAST_SYNCED_STATS["up"]  = TOTAL_UPLOADS
                LAST_SYNCED_STATS["bt"]  = TOTAL_BYTES_TRANSFERRED
        except Exception as e:
            logging.error(f"Stat sync failed: {e}")

async def sync_stats_now():
    from database import db
    global TOTAL_FILES_FWD, TOTAL_DOWNLOADS, TOTAL_UPLOADS, TOTAL_BYTES_TRANSFERRED, LAST_SYNCED_STATS
    try:
        diff_fwd = TOTAL_FILES_FWD - LAST_SYNCED_STATS.get("fwd", 0)
        diff_dn  = TOTAL_DOWNLOADS - LAST_SYNCED_STATS.get("dn", 0)
        diff_up  = TOTAL_UPLOADS - LAST_SYNCED_STATS.get("up", 0)
        diff_bt  = TOTAL_BYTES_TRANSFERRED - LAST_SYNCED_STATS.get("bt", 0)
        inc_dict = {}
        if diff_fwd > 0: inc_dict["TOTAL_FILES_FWD"] = diff_fwd
        if diff_dn > 0:  inc_dict["TOTAL_DOWNLOADS"] = diff_dn
        if diff_up > 0:  inc_dict["TOTAL_UPLOADS"] = diff_up
        if diff_bt > 0:  inc_dict["TOTAL_BYTES_TRANSFERRED"] = diff_bt
        if inc_dict:
            await db.update_bot_stats(**inc_dict)
    except Exception:
        pass

async def main():
    from database import db
    global TOTAL_FILES_FWD, TOTAL_DOWNLOADS, TOTAL_UPLOADS, TOTAL_BYTES_TRANSFERRED, LAST_SYNCED_STATS
    
    # Init stats from DB
    stats = await db.get_bot_stats()
    TOTAL_FILES_FWD = stats.get("TOTAL_FILES_FWD", 0)
    TOTAL_DOWNLOADS = stats.get("TOTAL_DOWNLOADS", 0)
    TOTAL_UPLOADS   = stats.get("TOTAL_UPLOADS", 0)
    TOTAL_BYTES_TRANSFERRED = stats.get("TOTAL_BYTES_TRANSFERRED", 0)
    
    LAST_SYNCED_STATS = {
        "fwd": TOTAL_FILES_FWD,
        "dn": TOTAL_DOWNLOADS,
        "up": TOTAL_UPLOADS,
        "bt": TOTAL_BYTES_TRANSFERRED
    }
    
    bot = Bot()
    await bot.start()

    # Start web server
    await web_server()

    # Start self-ping & sync task
    asyncio.create_task(ping_server())

    from plugins.taskjob import resume_task_jobs
    from plugins.old_taskjob import resume_oldtaskjobs
    from plugins.jobs import resume_live_jobs
    asyncio.create_task(resume_task_jobs(_bot=bot))
    asyncio.create_task(resume_oldtaskjobs(_bot=bot))
    asyncio.create_task(resume_live_jobs(_bot=bot))

    await idle()
    await sync_stats_now()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
