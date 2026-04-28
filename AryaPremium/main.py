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
        from plugins.userbot.market_seller import market_clients, _process_start, _process_screenshot, _process_callback, _process_text, _process_media, _process_my_stories, _process_chat_member
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
            # Media handler (feedback + screenshot) — must come before _process_screenshot
            cli.add_handler(MessageHandler(_process_media, (filters.photo | filters.video | filters.animation | filters.document | filters.voice | filters.audio) & filters.private))
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
    await idle()

    # Graceful shutdown
    for app in apps:
        await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down Ecosystem...")
