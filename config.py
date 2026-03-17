from os import environ
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load from .env file if present (local dev)
except ImportError:
    pass  # dotenv not installed: rely on system env vars

class Config:
    API_ID   = int(environ.get("API_ID", 1234567))
    API_HASH = environ.get("API_HASH", "your_api_hash_here")
    BOT_TOKEN   = environ.get("BOT_TOKEN", "your_bot_token_here")
    BOT_SESSION = environ.get("BOT_SESSION", "bot")
    DATABASE_URI  = environ.get("DATABASE", "your_mongodb_uri_here")
    DATABASE_NAME = environ.get("DATABASE_NAME", "forward-bot")
    BOT_OWNER_ID  = [int(i) for i in environ.get("BOT_OWNER_ID", "0").split()]

class temp(object):
    lock = {}
    CANCEL = {}
    PAUSE = {}
    forwardings = 0
    BANNED_USERS = []
    IS_FRWD_CHAT = []

