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

class Bot(Client): 
    def __init__(self):
        super().__init__(
            Config.BOT_SESSION,
            api_hash=Config.API_HASH,
            api_id=Config.API_ID,
            plugins={
                "root": "plugins"
            },
            workers=50,
            bot_token=Config.BOT_TOKEN
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

        # Ensure MongoDB indices exist for fast queries
        try:
            await db.ensure_indexes()
            logging.info("MongoDB indices verified/created successfully.")
        except Exception as e:
            logging.warning(f"Could not create MongoDB indices: {e}")

        try:
            from plugins.regix import resume_manual_jobs
            await resume_manual_jobs(self)
        except Exception as e:
            logging.error(f"Failed to resume manual jobs: {e}")

    async def stop(self, *args):
        msg = f"@{self.username} stopped. Bye."
        await super().stop()
        logging.info(msg)
