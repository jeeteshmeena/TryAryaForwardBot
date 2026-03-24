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

BTN_URL_REGEX = re.compile(r"(\[([^\[]+?)]\[buttonurl:/{0,2}(.+?)(:same)?])")
BOT_TOKEN_TEXT = "<b>1) create a bot using @BotFather\n2) Then you will get a message with bot token\n3) Forward that message to me</b>"
SESSION_STRING_SIZE = 351

async def _schedule_delete(bot, chat_id, message_id, delay=43200): # 12 hours
    await asyncio.sleep(delay)
    try:
        await bot.delete_messages(chat_id, message_id)
    except Exception:
        pass

async def start_clone_bot(FwdBot, data=None):
   await FwdBot.start()
   #
   async def iter_messages(
      self, 
      chat_id: Union[int, str], 
      limit: int, 
      offset: int = 0,
      search: str = None,
      filter: "types.TypeMessagesFilter" = None,
      continuous: bool = False,
      reverse_order: bool = False
      ) -> Optional[AsyncGenerator["types.Message", None]]:
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

        BATCH_SIZE = 200  # Max IDs per get_messages call

        offset = offset if offset else getattr(self, "offset", 0)
        limit = limit if limit else getattr(self, "limit", getattr(self, "last_msg_id", 0))

        BATCH_SIZE = 200

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
                except Exception:
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
                    await asyncio.sleep(e.value + 1)
                    continue
                except Exception:
                    msgs = []
                    
                if not isinstance(msgs, list):
                    msgs = [msgs]
                valid = [m for m in msgs if m and not m.empty]
                valid.sort(key=lambda m: m.id)
                
                for message in valid:
                    yield message
                    
                current = batch_end_val + 1
        else:
            # ── New to Old: descend ──
            current = end_id
            while current >= start_id:
                batch_start_val = max(start_id, current - BATCH_SIZE + 1)
                batch_ids = list(range(batch_start_val, current + 1))
                batch_ids.reverse()
                
                try:
                    msgs = await self.get_messages(chat_id, batch_ids)
                except FloodWait as e:
                    await asyncio.sleep(e.value + 1)
                    continue
                except Exception:
                    msgs = []
                
                if not isinstance(msgs, list):
                    msgs = [msgs]
                valid = [m for m in msgs if m and not m.empty]
                valid.sort(key=lambda m: m.id, reverse=True)
                
                for message in valid:
                    yield message
                    
                current = batch_start_val - 1
   #
   FwdBot.iter_messages = iter_messages
   return FwdBot


class CLIENT: 
  def __init__(self):
     self.api_id = Config.API_ID
     self.api_hash = Config.API_HASH
    
  def client(self, data, user=None):
     if user == None and data.get('is_bot') == False:
        return Client("USERBOT", self.api_id, self.api_hash, session_string=data.get('session'))
     elif user == True:
        return Client("USERBOT", self.api_id, self.api_hash, session_string=data)
     elif user != False:
        data = data.get('token')
     return Client("BOT", self.api_id, self.api_hash, bot_token=data, in_memory=True)
  
  async def add_bot(self, bot, message):
     user_id = int(message.from_user.id)
     msg = await bot.ask(chat_id=user_id, text=BOT_TOKEN_TEXT)
     
     if msg.text:
         asyncio.create_task(_schedule_delete(bot, user_id, msg.id, 43200))
         
     if msg.text=='/cancel':
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
         
     if msg.text == '/cancel':
        return await msg.reply('<b>process cancelled !</b>')
     phone_number = msg.text.strip()
     import pyrogram
     temp_client = Client("temp_session", in_memory=True, api_id=int(self.api_id), api_hash=self.api_hash)
     try:
        await temp_client.connect()
        code = await temp_client.send_code(phone_number)
        otp_msg = await bot.ask(chat_id=user_id, text="<b>Send the OTP you received (e.g. 1 2 3 4 5 if code is 12345).\n\n/cancel - cancel the process</b>")
        if otp_msg.text == '/cancel':
           await temp_client.disconnect()
           return await bot.send_message(user_id, '<b>process cancelled !</b>')
        
        otp = otp_msg.text.replace(" ", "")
        try:
           await temp_client.sign_in(phone_number, code.phone_code_hash, otp)
        except pyrogram.errors.SessionPasswordNeeded:
           pwd_msg = await bot.ask(chat_id=user_id, text="<b>Your account has 2FA enabled. Send your password.\n\n/cancel - cancel the process</b>")
           if pwd_msg.text == '/cancel':
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
  if key in ['caption', 'duplicate', 'download', 'db_uri', 'duration', 'forward_tag', 'protect', 'file_size', 'size_limit', 'extension', 'keywords', 'button']:
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
