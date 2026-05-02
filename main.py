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


async def main():
    bot = Bot()
    await bot.start()

    # Persist actual startup time to DB immediately — this makes uptime
    # in the Status section always count from real bot start, not from the
    # first time a user opens the Status page (lazy-init issue).
    try:
        from database import db as _startdb
        await _startdb.stats.update_one(
            {'_id': 'bot_stats'},
            {'$set': {'bot_start_time': START_TIME}},
            upsert=True
        )
        logging.info(f"[Startup] bot_start_time persisted to DB: {START_TIME}")
    except Exception as _e:
        logging.warning(f"[Startup] Could not persist start time: {_e}")

    try:
        from plugins.share_bot import start_share_bot
        await start_share_bot()
    except Exception as e:
        logging.error(f"Failed to init share bots: {e}")

    if os.environ.get("DELIVERY_ONLY", "0") in ("1", "true", "True"):
        logging.info("Running in DELIVERY ONLY mode. Main bot features disabled.")
        await idle()
        try:
            from plugins.share_bot import share_clients
            for c in share_clients.values():
                try: await c.stop()
                except: pass
        except Exception: pass
        await bot.stop()
        return

    # Register DB channel auto-index listener on main bot
    try:
        from plugins.db_scanner import _try_auto_index
        from pyrogram import filters as _f
        @bot.on_message(_f.channel & (_f.audio | _f.document | _f.video | _f.voice))
        async def _auto_index_handler(client, message):
            asyncio.create_task(_try_auto_index(client, message))
        logging.info("DB channel auto-index listener registered")
    except Exception as e:
        logging.warning(f"Could not register auto-index listener: {e}")

    # Start web server
    await web_server()

    # Start self-ping task
    asyncio.create_task(ping_server())

    # Start system resource monitor (auto-pause on RAM/CPU overload)
    try:
        from plugins.sysmon import start_monitor
        start_monitor(bot)
        logging.info("System resource monitor started.")
    except Exception as e:
        logging.warning(f"Could not start system monitor: {e}")

    # ── Staggered job resumption (prevents FloodWait on restart) ────────────
    async def _staggered_resume():
        await asyncio.sleep(5)   # let bot fully connect first
        try:
            from plugins.jobs import resume_live_jobs
            await resume_live_jobs(stagger_secs=2.0)
        except Exception as e:
            logging.warning(f"LiveJob resume error: {e}")
        await asyncio.sleep(5)   # gap between job types
        try:
            from plugins.multijob import resume_multi_jobs
            await resume_multi_jobs(stagger_secs=3.0)
        except Exception as e:
            logging.warning(f"MultiJob resume error: {e}")
        await asyncio.sleep(5)
        # ── Cleaner job auto-resume ──────────────────────────────────────────
        # Any cleaner jobs still marked 'running'/'queued' in DB when the bot
        # died are orphaned (their asyncio Tasks are gone). Relaunch them now.
        # Without this they appear 'Running' forever but process nothing.
        try:
            from plugins.cleaner import _cl_run_job, _cl_tasks, _cl_paused, _cl_bot_ref
            from database import db as _db
            orphaned = await _db.db.cleaner_jobs.find(
                {"status": {"$in": ["running", "queued"]}}
            ).to_list(length=None)
            logging.info(f"[Startup] Resuming {len(orphaned)} orphaned cleaner job(s)...")
            for _job in orphaned:
                _jid = _job["job_id"]
                if _jid in _cl_tasks and not _cl_tasks[_jid].done():
                    continue  # already has a live task
                _cl_paused[_jid] = asyncio.Event()
                _cl_paused[_jid].set()
                _cl_bot_ref[_jid] = bot
                await _db.db.cleaner_jobs.update_one(
                    {"job_id": _jid},
                    {"$set": {"status": "queued", "error": "Resuming after restart..."}}
                )
                _cl_tasks[_jid] = asyncio.create_task(_cl_run_job(_jid, bot))
                logging.info(f"[Startup] Resumed cleaner job {_jid}")
                await asyncio.sleep(2)  # stagger restarts to avoid hammering Telegram
        except Exception as e:
            logging.warning(f"Cleaner resume error: {e}")
        # ── Live Batch job auto-resume ───────────────────────────────────────
        # live_batch jobs marked 'running' lose their asyncio Task on restart.
        # Without this they appear Running forever but process nothing, and users
        # must manually pause+resume to kick them back to life.
        await asyncio.sleep(5)
        try:
            from plugins.live_batch import _lb_run_job, _lb_tasks, _lb_paused
            from database import db as _db
            lb_orphaned = await _db.db["live_batch_jobs"].find(
                {"status": {"$in": ["running", "queued"]}}
            ).to_list(length=None)
            logging.info(f"[Startup] Resuming {len(lb_orphaned)} orphaned Live Batch job(s)...")
            for _lbjob in lb_orphaned:
                _lbjid = _lbjob["job_id"]
                if _lbjid in _lb_tasks and not _lb_tasks[_lbjid].done():
                    continue  # already has a live task, skip
                if _lbjid not in _lb_paused:
                    _lb_paused[_lbjid] = asyncio.Event()
                _lb_paused[_lbjid].set()   # ensure not paused
                _lb_tasks[_lbjid] = asyncio.create_task(_lb_run_job(_lbjid))
                logging.info(f"[Startup] Resumed Live Batch job {_lbjid}")
                await asyncio.sleep(2)  # stagger restarts
        except Exception as e:
            logging.warning(f"Live Batch resume error: {e}")
        logging.info("Staggered job resumption complete.")

    asyncio.create_task(_staggered_resume())

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
