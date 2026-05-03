import os
import re 
import sys
import typing
import asyncio 
import logging 
from database import db 
from config import Config, temp
from pyrogram import Client, filters
from pyrogram.raw.all import layer
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message 
from pyrogram.errors.exceptions.bad_request_400 import AccessTokenExpired, AccessTokenInvalid
from pyrogram.errors import FloodWait
from config import Config
from translation import Translation

from typing import Union, Optional, AsyncGenerator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Client Cache ──────────────────────────────────────────────────────────────
# Prevents AUTH_KEY_DUPLICATED when multiple jobs use the same userbot account.
# Maps session_name → running Pyrogram Client.
_client_cache: dict = {}
_client_refcount: dict = {}
_cache_lock: asyncio.Lock | None = None

def _get_cache_lock() -> asyncio.Lock:
    global _cache_lock
    if _cache_lock is None:
        _cache_lock = asyncio.Lock()
    return _cache_lock

async def release_client(session_name: str):
    """Called when a job finishes — decrements refcount, and only stops GC if 0."""
    lock = _get_cache_lock()
    async with lock:
        refs = _client_refcount.get(session_name, 0)
        if refs > 1:
            _client_refcount[session_name] -= 1
            logger.info(f"[ClientCache] Decremented {session_name}: now {_client_refcount[session_name]} refs")
            return

        # Refcount hits 0, fully release
        _client_refcount.pop(session_name, None)
        client = _client_cache.pop(session_name, None)
        if client:
            try:
                await client.stop()
            except Exception:
                pass
            logger.info(f"[ClientCache] Released & Stopped: {session_name}")

BTN_URL_REGEX = re.compile(r"(\[([^\[]+?)]\[buttonurl:/{0,2}(.+?)(:same)?])")
BOT_TOKEN_TEXT = "<b>1) create a bot using @BotFather\n2) Then you will get a message with bot token\n3) Forward that message to me</b>"
SESSION_STRING_SIZE = 351

async def _schedule_delete(bot, chat_id, message_id, delay=43200): # 12 hours
    await asyncio.sleep(delay)
    try:
        await bot.delete_messages(chat_id, message_id)
    except Exception:
        pass

async def start_clone_bot(FwdBot, data=None, force_restart=False):
   """Start a Pyrogram client with deduplication — if a running client for
   this session already exists in the cache, return it immediately without
   starting a duplicate (which would cause AUTH_KEY_DUPLICATED)."""
   cache_key = FwdBot.name   # e.g. "userbot_7307208383" or "BOT"
   lock = _get_cache_lock()

   async with lock:
       curr_refs = _client_refcount.get(cache_key, 0)
       
       if force_restart:
           if curr_refs > 0:
               # If heavily utilized, do NOT forcefully restart and kill others!
               logger.warning(f"[ClientCache] Force restart ignored for {cache_key} (in use by {curr_refs} jobs).")
               _client_refcount[cache_key] = curr_refs + 1
               return _client_cache[cache_key]
           else:
               # Explicitly bypass cache to force a fresh connection since nobody uses it
               logger.warning(f"[ClientCache] Force restart requested for {cache_key}, popping from cache.")
               _client_refcount.pop(cache_key, None)
               old_client = _client_cache.pop(cache_key, None)
               if old_client:
                   try: await old_client.stop()
                   except Exception: pass
               
       existing = _client_cache.get(cache_key)
       if existing is not None:
           # Verify the cached client is still alive using a cheap MTProto Ping
           # (NOT get_me/GetFullUser which causes FLOOD_WAIT_X when many jobs restart together)
           try:
               from pyrogram.raw.functions import Ping
               await asyncio.wait_for(existing.invoke(Ping(ping_id=0)), timeout=10)
               logger.debug(f"[ClientCache] Reusing existing client: {cache_key}")
               _client_refcount[cache_key] = _client_refcount.get(cache_key, 1) + 1
               return existing   # ← return cached, skip new start entirely
           except Exception as e:
               err_str = str(e).lower()
               # If it's a timeout or other active jobs exist, do NOT wipe it
               if isinstance(e, asyncio.TimeoutError) or "timeout" in err_str or curr_refs > 0:
                   logger.warning(f"[ClientCache] Cached client {cache_key} failed ping ({e}) but assumed alive/in-use. Returning existing.")
                   _client_refcount[cache_key] = _client_refcount.get(cache_key, 1) + 1
                   return existing
               # Dead — clean up and fall through to start a fresh one
               logger.warning(f"[ClientCache] Cached client {cache_key} dead ({e}), restarting.")
               _client_cache.pop(cache_key, None)
               _client_refcount.pop(cache_key, None)
               try:
                   await existing.stop()
               except Exception:
                   pass

       # Removed is_userbot registering share_handlers as it causes duplicate message 
       # bugs. Delivery requests should be exclusively managed by official Share Bots.

       await FwdBot.start()
       _client_cache[cache_key] = FwdBot
       _client_refcount[cache_key] = 1
       logger.info(f"[ClientCache] Started & cached: {cache_key} (refs 1)")

   # Warm up peer cache in background — do NOT block here on 1GB VPS
   try:
       me = await asyncio.wait_for(FwdBot.get_me(), timeout=10)
       if not getattr(me, 'is_bot', False):
           async def _warm():
               try:
                   async for _ in FwdBot.get_dialogs(limit=30): pass
               except Exception:
                   pass
           asyncio.ensure_future(_warm())
   except Exception:
       pass
   return FwdBot

async def iter_messages(
    self,
    chat_id: Union[int, str], 
    limit: int, 
    offset: int = 0,
    search: str = None,
    filter: "typing.Any" = None,
    continuous: bool = False,
    reverse_order: bool = False
) -> Optional[AsyncGenerator["typing.Any", None]]:
    """Iterate through a chat sequentially. Bot-safe implementation."""
    import pyrogram
    
    # Detect if this client is a normal bot — bots CANNOT use get_chat_history (user-only API).
    me = await self.get_me()
    is_bot = getattr(me, 'is_bot', False)
    
    chat = await self.get_chat(chat_id)
    is_channel_or_supergroup = chat.type in [
        pyrogram.enums.ChatType.CHANNEL,
        pyrogram.enums.ChatType.SUPERGROUP,
    ]
    
    # Lock in numeric ID to prevent string resolution bugs later
    if str(chat_id).lower() not in ("me", "saved"):
        chat_id = chat.id

    BATCH_SIZE = 200  # Max IDs per get_messages call

    offset = offset if offset else getattr(self, "offset", 0)
    limit = limit if limit else getattr(self, "limit", getattr(self, "last_msg_id", 0))

    # 1. Determine REAL upper bound (top_id)
    if limit > 0 and limit != 10000000:
        top_id = limit
    else:
        # Binary search to find top message ID
        lo, hi = 1, 9_999_999
        for _ in range(25):
            if hi - lo <= BATCH_SIZE:
                break
            mid = (lo + hi) // 2
            try:
                probe = await self.get_messages(chat_id, [mid])
                if not isinstance(probe, list): probe = [probe]
                if any(m and not m.empty for m in probe):
                    lo = mid
                else:
                    hi = mid
            except Exception as e:
                import logging
                err_str = str(e).upper()
                if "PEER" in err_str or "CHANNEL" in err_str or "ACCESS" in err_str:
                    logging.getLogger(__name__).error(f"Binary search failed on {chat_id}: {e}")
                    raise e
                hi = mid
        top_id = hi

    # 2. Determine bounds
    start_id = max(1, offset if offset > 0 else 1)
    end_id = top_id

    if not reverse_order:
        # ── Old to New: ascend ──
        current = start_id
        while current <= end_id:
            batch_end_val = min(current + BATCH_SIZE - 1, end_id)
            batch_ids = list(range(current, batch_end_val + 1))
            
            try:
                msgs = await self.get_messages(chat_id, batch_ids)
            except FloodWait as e:
                await asyncio.sleep(e.value + 2)
                continue 

            if not isinstance(msgs, list):
                msgs = [msgs]
            
            valid = []
            for m in msgs:
                if not m or m.empty: continue
                if isinstance(chat_id, int) and getattr(m, 'chat', None) and m.chat.id != chat_id:
                    continue
                valid.append(m)
                
            valid.sort(key=lambda m: m.id)
            
            for message in valid:
                yield message
                
            current = batch_end_val + 1
    else:
        # ── New to Old: descend (iter_messages default) ──
        # (If offset is passed, start from there going downwards)
        if start_id > 1:
            # Normal descend starts from top_id
            start_desc = end_id
            end_desc = start_id
        else:
            start_desc = end_id
            end_desc = 1
            
        current = start_desc
        while current >= end_desc:
            batch_start_val = max(current - BATCH_SIZE + 1, end_desc)
            batch_ids = list(range(batch_start_val, current + 1))
            
            try:
                msgs = await self.get_messages(chat_id, batch_ids)
            except FloodWait as e:
                await asyncio.sleep(e.value + 2)
                continue 
            
            if not isinstance(msgs, list):
                msgs = [msgs]
                
            valid = []
            for m in msgs:
                if not m or m.empty: continue
                if isinstance(chat_id, int) and getattr(m, 'chat', None) and m.chat.id != chat_id:
                    continue
                valid.append(m)
                
            valid.sort(key=lambda m: m.id, reverse=True)
            
            for message in valid:
                yield message
                
            current = batch_start_val - 1

Client.iter_messages = iter_messages


class CLIENT: 
  def __init__(self):
     self.api_id = Config.API_ID
     self.api_hash = Config.API_HASH
    
  def client(self, data, user=None):
     if user == None and data.get('is_bot') == False:
        sname = f"userbot_{data.get('id', 'temp')}"
        return Client(sname, self.api_id, self.api_hash, session_string=data.get('session'), max_concurrent_transmissions=50, in_memory=True)
     elif user == True:
        # data is session string directly, use memory temporarily (wait, add_session handles this right after)
        return Client("temp_userbot_creation", self.api_id, self.api_hash, session_string=data, max_concurrent_transmissions=50, in_memory=True)
     
     if user != False:
        token = data.get('token')
        sname = f"bot_{data.get('id', 'temp')}"
        return Client(sname, self.api_id, self.api_hash, bot_token=token, max_concurrent_transmissions=50, in_memory=True)
     else:
        token = data
        sname = f"bot_{str(token).split(':')[0] if ':' in str(token) else 'temp'}"
        
     return Client(sname, self.api_id, self.api_hash, bot_token=token, in_memory=True, max_concurrent_transmissions=50)
  async def add_bot(self, bot, message):
     user_id = int(message.from_user.id)
     msg = await bot.ask(chat_id=user_id, text=BOT_TOKEN_TEXT)
     
     if msg.text:
         asyncio.create_task(_schedule_delete(bot, user_id, msg.id, 43200))
         
     if getattr(msg, 'text', None) and any(x in msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
        return await msg.reply('<b>process cancelled !</b>')
     elif not msg.forward_date:
       return await msg.reply_text("<b>This is not a forward message</b>")
     elif str(msg.forward_from.id) != "93372553":
       return await msg.reply_text("<b>This message was not forward from bot father</b>")
     bot_token = re.findall(r'\d[0-9]{8,10}:[0-9A-Za-z_-]{35}', msg.text, re.IGNORECASE)
     bot_token = bot_token[0] if bot_token else None
     if not bot_token:
       return await msg.reply_text("<b>There is no bot token in that message</b>")
     try:
       _client = await start_clone_bot(self.client(bot_token, False), True)
     except Exception as e:
       await msg.reply_text(f"<b>BOT ERROR:</b> `{e}`")
     _bot = _client.me
     details = {
       'id': _bot.id,
       'is_bot': True,
       'user_id': user_id,
       'name': _bot.first_name,
       'token': bot_token,
       'username': _bot.username 
     }
     res = await db.add_bot(details)
     return res
    
  async def add_session(self, bot, message):
     user_id = int(message.from_user.id)
     text = "<b>⚠️ DISCLAIMER ⚠️</b>\n\n<code>You can use your userbot account for forwarding messages from private chats or restricted channels.\nPlease sign in with your phone number at your own risk. There is a chance your account may get banned. My developer is not responsible if your account gets banned.</code>"
     await bot.send_message(user_id, text=text)
     msg = await bot.ask(chat_id=user_id, text="<b>Send your phone number with country code (e.g. +1234567890).\n\n/cancel - cancel the process</b>")
     if msg.text:
         asyncio.create_task(_schedule_delete(bot, user_id, msg.id, 43200))
         
     if getattr(msg, 'text', None) and any(x in msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
        return await msg.reply('<b>process cancelled !</b>')
     phone_number = msg.text.strip()
     import pyrogram
     temp_client = Client("temp_session", in_memory=True, api_id=int(self.api_id), api_hash=self.api_hash)
     try:
        await temp_client.connect()
        code = await temp_client.send_code(phone_number)
        otp_msg = await bot.ask(chat_id=user_id, text="<b>Send the OTP you received (e.g. 1 2 3 4 5 if code is 12345).\n\n/cancel - cancel the process</b>")
        if getattr(otp_msg, 'text', None) and any(x in otp_msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
           await temp_client.disconnect()
           return await bot.send_message(user_id, '<b>process cancelled !</b>')
        
        otp = otp_msg.text.replace(" ", "")
        try:
           await temp_client.sign_in(phone_number, code.phone_code_hash, otp)
        except pyrogram.errors.SessionPasswordNeeded:
           pwd_msg = await bot.ask(chat_id=user_id, text="<b>Your account has 2FA enabled. Send your password.\n\n/cancel - cancel the process</b>")
           if getattr(pwd_msg, 'text', None) and any(x in pwd_msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
              await temp_client.disconnect()
              return await bot.send_message(user_id, '<b>process cancelled !</b>')
           await temp_client.check_password(pwd_msg.text)
        
        session_string = await temp_client.export_session_string()
        await temp_client.disconnect()
        
        client = await start_clone_bot(self.client(session_string, True), True)
        user = client.me
        details = {
          'id': user.id,
          'is_bot': False,
          'user_id': user_id,
          'name': user.first_name,
          'session': session_string,
          'username': user.username
        }
        res = await db.add_bot(details)
        return res
     except Exception as e:
        try:
            await temp_client.disconnect()
        except:
            pass
        await bot.send_message(user_id, f"<b>USER BOT ERROR:</b> `{e}`")
        return False
    
@Client.on_message(filters.private & filters.command('reset'))
async def forward_tag(bot, m):
   default = await db.get_configs("01")
   temp.CONFIGS[m.from_user.id] = default
   await db.update_configs(m.from_user.id, default)
   await m.reply("successfully settings reseted ✔️")

@Client.on_message(filters.command('resetall') & filters.user(Config.BOT_OWNER_ID))
async def resetall(bot, message):
  users = await db.get_all_users()
  sts = await message.reply("**processing**")
  TEXT = "total: {}\nsuccess: {}\nfailed: {}\nexcept: {}"
  total = success = failed = already = 0
  ERRORS = []
  async for user in users:
      user_id = user['id']
      default = await get_configs(user_id)
      default['db_uri'] = None
      total += 1
      if total %10 == 0:
         await sts.edit(TEXT.format(total, success, failed, already))
      try: 
         await db.update_configs(user_id, default)
         success += 1
      except Exception as e:
         ERRORS.append(e)
         failed += 1
  if ERRORS:
     await message.reply(ERRORS[:100])
  await sts.edit("completed\n" + TEXT.format(total, success, failed, already))
  
async def get_configs(user_id):
  #configs = temp.CONFIGS.get(user_id)
  #if not configs:
  configs = await db.get_configs(user_id)
  #temp.CONFIGS[user_id] = configs 
  return configs
                          
async def update_configs(user_id, key, value):
  current = await db.get_configs(user_id)
  if key in ['caption', 'duplicate', 'download', 'db_uri', 'duration', 'forward_tag', 'protect', 'file_size', 'size_limit', 'extension', 'keywords', 'button', 'bot_mode']:
     current[key] = value
  else: 
     current['filters'][key] = value
 # temp.CONFIGS[user_id] = value
  await db.update_configs(user_id, current)
    
def parse_buttons(text, markup=True):
    buttons = []
    for match in BTN_URL_REGEX.finditer(text):
        n_escapes = 0
        to_check = match.start(1) - 1
        while to_check > 0 and text[to_check] == "\\":
            n_escapes += 1
            to_check -= 1

        if n_escapes % 2 == 0:
            if bool(match.group(4)) and buttons:
                buttons[-1].append(InlineKeyboardButton(
                    text=match.group(2),
                    url=match.group(3).replace(" ", "")))
            else:
                buttons.append([InlineKeyboardButton(
                    text=match.group(2),
                    url=match.group(3).replace(" ", ""))])
    if markup and buttons:
       buttons = InlineKeyboardMarkup(buttons)
    return buttons if buttons else None