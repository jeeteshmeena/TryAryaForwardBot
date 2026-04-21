import asyncio
import logging
import os
from pyrogram import Client, compose, filters
from config import Config
from database import db
from utils import setup_ask_router

# Setup basic logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Wait, we need the Management Bot token if it's separate from the connected bots.
async def main():
    logger.info("Initializing Premium Ecosystem Database...")
    await db.connect()

    config_vars = ["API_ID", "API_HASH", "MGMT_BOT_TOKEN", "MONGO_URI", "DATABASE_NAME"]
    missing = [c for c in config_vars if not getattr(Config, c)]
    if missing:
        logger.error(f"Missing required configs: {', '.join(missing)}")
        return
    
    apps = []

    # ── 1. Load Management Bot ──
    try:
        from pyrogram import Client
        from utils import setup_ask_router
        mgmt_bot = Client(
            name="mgmt_bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.MGMT_BOT_TOKEN,
            plugins=dict(root="plugins.mgmt"),
            in_memory=False
        )
        setup_ask_router(mgmt_bot)
        db.mgmt_client = mgmt_bot
        apps.append(mgmt_bot)
    except Exception as e:
        logger.error(f"Failed to load mgmt_bot: {e}")

    # ── 2. Load Store Bots ──
    try:
        from plugins.userbot.market_seller import market_clients, _process_start, _process_screenshot, _process_callback, _process_text, _process_my_stories, _process_chat_member
        from pyrogram.handlers import MessageHandler, CallbackQueryHandler, ChatMemberUpdatedHandler
        from utils import setup_ask_router
        
        bots = await db.db.premium_bots.find().to_list(length=None)
        
        for b in bots:
            tok = b.get('token')
            if not tok: continue
            
            logger.info(f"Loading Market Bot: {b.get('username')}")
            cli = Client(
                name=f"market_{b['id']}", 
                api_id=Config.API_ID, 
                api_hash=Config.API_HASH, 
                bot_token=tok, 
                in_memory=False
            )
            setup_ask_router(cli)
            cli.add_handler(MessageHandler(_process_start, filters.command("start") & filters.private))
            cli.add_handler(MessageHandler(_process_my_stories, filters.command(["mystories", "stories"]) & filters.private))
            cli.add_handler(MessageHandler(_process_screenshot, filters.photo & filters.private))
            cli.add_handler(MessageHandler(_process_text, filters.text & filters.private))
            cli.add_handler(CallbackQueryHandler(_process_callback, filters.regex(r'^mb#')))
            cli.add_handler(ChatMemberUpdatedHandler(_process_chat_member))
            market_clients[str(b['id'])] = cli
            apps.append(cli)
    except Exception as e:
         logger.error(f"Failed loading connected bots: {e}")

    if not apps:
        logger.error("No bots to run. Exiting.")
        return

    logger.info("Starting up Premium Ecosystem and Warming Cache...")
    from pyrogram import idle

    for app in apps:
        await app.start()
        
        # Background task to warm up cache completely on any fresh restart/VPS migration
        async def warm(client):
            try:
                from pyrogram.errors import FloodWait
                async for _ in client.get_dialogs(limit=500):
                    pass
                logger.info(f"[{client.name}] Successfully warmed up peer cache!")
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.debug(f"[{client.name}] Dialogs warmup interrupted: {e}")
                
        asyncio.create_task(warm(app))

    # Keep bots running
    # ── Auto-resume Cleaner Jobs that were running before shutdown ──
    async def _resume_cleaner_jobs():
        """
        On bot restart, any cleaner jobs still marked 'running' or 'queued'
        in DB are orphaned — their asyncio Tasks are gone. Re-launch them now.
        Without this, they appear 'Running' forever but process nothing.
        """
        try:
            await asyncio.sleep(5)  # wait for bots to be fully ready first
            from plugins.cleaner import _cl_run_job, _cl_tasks, _cl_paused, _cl_bot_ref
            import asyncio as _aio

            # Use the first available market bot as notification client
            notify_bot = None
            for _cli in apps:
                if hasattr(_cli, 'me') and getattr(getattr(_cli, 'me', None), 'is_bot', False):
                    notify_bot = _cli
                    break
            if not notify_bot and apps:
                notify_bot = apps[0]

            orphaned = await db.db.cleaner_jobs.find(
                {"status": {"$in": ["running", "queued"]}}
            ).to_list(length=None)

            logger.info(f"[Startup] Resuming {len(orphaned)} orphaned cleaner job(s)...")
            for job in orphaned:
                jid = job["job_id"]
                if jid in _cl_tasks and not _cl_tasks[jid].done():
                    continue  # already running
                _cl_paused[jid] = _aio.Event()
                _cl_paused[jid].set()
                # Restore bot ref
                _cl_bot_ref[jid] = notify_bot
                # Mark queued so UI shows correct state while waiting for semaphore slot
                await db.db.cleaner_jobs.update_one(
                    {"job_id": jid},
                    {"$set": {"status": "queued", "error": "Resuming after restart..."}}
                )
                _cl_tasks[jid] = _aio.create_task(_cl_run_job(jid, notify_bot))
                logger.info(f"[Startup] Resumed cleaner job {jid}")
        except Exception as e:
            logger.warning(f"[Startup] Cleaner resume failed: {e}")

    asyncio.create_task(_resume_cleaner_jobs())
    await idle()

    
    # Graceful shutdown
    for app in apps:
        await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down Ecosystem...")
