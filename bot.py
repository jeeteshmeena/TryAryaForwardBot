import os
import time
os.environ['TZ'] = 'Asia/Kolkata'
if hasattr(time, 'tzset'):
    time.tzset()

import asyncio
import logging 
import logging.config
from database import db 
from config import Config  
from pyrogram import Client, __version__
from pyrogram.raw.all import layer 
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait 

logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

import concurrent.futures

class Bot(Client): 
    def __init__(self):
        global BOT_INSTANCE
        BOT_INSTANCE = self
        
        try:
            loop = asyncio.get_running_loop()
            loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=100))
        except RuntimeError:
            pass
        super().__init__(
            Config.BOT_SESSION,
            api_hash=Config.API_HASH,
            api_id=Config.API_ID,
            plugins={
                "root": "plugins"
            },
            workers=50,
            bot_token=Config.BOT_TOKEN,
            max_concurrent_transmissions=50
        )
        self.log = logging

    async def start(self):
        await super().start()
        me = await self.get_me()
        logging.info(f"{me.first_name} with for pyrogram v{__version__} (Layer {layer}) started on @{me.username}.")
        self.id = me.id
        self.username = me.username
        self.first_name = me.first_name
        self.set_parse_mode(ParseMode.DEFAULT)
        text = "**๏[-ิ_•ิ]๏ bot restarted !**"
        logging.info(text)

        # Check if database URI is default broken one
        if "mongodb+srv://chhjgjkkjhkjhkjh@cluster0.xowzpr4.mongodb.net/" in Config.DATABASE_URI:
             logging.error("You have not set the DATABASE environment variable. The bot will not function correctly.")
             return

        try:
            success = failed = 0
            users = await db.get_all_frwd()
            async for user in users:
               chat_id = user['user_id']
               try:
                  await self.send_message(chat_id, text)
                  success += 1
               except FloodWait as e:
                  await asyncio.sleep(e.value + 1)
                  await self.send_message(chat_id, text)
                  success += 1
               except Exception:
                  failed += 1

            if (success + failed) != 0:
               await db.rmve_frwd(all=True)
               logging.info(f"Restart message status"
                     f"success: {success}"
                     f"failed: {failed}")
        except Exception as e:
            logging.error(f"Failed to send restart messages or connect to DB: {e}")

    async def stop(self, *args):
        msg = f"@{self.username} stopped. Bye."
        await super().stop()
        logging.info(msg)
