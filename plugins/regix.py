import os
import sys 
import math
import time
import asyncio 
import logging
import re
from .utils import STS
from database import db 
from .test import CLIENT , start_clone_bot
from config import Config, temp
from translation import Translation
from pyrogram import Client, filters 
#from pyropatch.utils import unpack_new_file_id
from pyrogram.errors import FloodWait, MessageNotModified, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message 

CLIENT = CLIENT()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
TEXT = Translation.TEXT

@Client.on_callback_query(filters.regex(r'^start_public'))
async def pub_(bot, message):
    user = message.from_user.id
    temp.CANCEL[user] = False
    frwd_id = message.data.split("_")[2]
    if temp.lock.get(user) and str(temp.lock.get(user))=="True":
      return await message.answer("please wait until previous task complete", show_alert=True)
    sts = STS(frwd_id)
    if not sts.verify():
      await message.answer("your are clicking on my old button", show_alert=True)
      return await message.message.delete()
    i = sts.get(full=True)
    if i.TO in temp.IS_FRWD_CHAT:
      return await message.answer("In Target chat a task is progressing. please wait until task complete", show_alert=True)
    m = await msg_edit(message.message, "<code>verifying your data's, please wait.</code>")
    _bot, caption, forward_tag, data, protect, button = await sts.get_data(user)
    download_mode = data.get('download', False)
    if not _bot:
      return await msg_edit(m, "<code>You didn't added any bot. Please add a bot using /settings !</code>", wait=True)
    try:
      client = await start_clone_bot(CLIENT.client(_bot))
    except Exception as e:  
      return await m.edit(e)
    await msg_edit(m, "<code>processing..</code>")
    try: 
       # Just check if we can access messages. If continuous, limit might be huge.
       await client.get_messages(sts.get("FROM"), 1)
    except:
       await msg_edit(m, f"**Source chat may be a private channel / group. Use userbot (user must be member over there) or  if Make Your [Bot](t.me/{_bot['username']}) an admin over there**", retry_btn(frwd_id), True)
       return await stop(client, user)
    try:
       k = await client.send_message(i.TO, "Testing")
       await k.delete()
    except:
       await msg_edit(m, f"**Please Make Your [UserBot / Bot](t.me/{_bot['username']}) Admin In Target Channel With Full Permissions**", retry_btn(frwd_id), True)
       return await stop(client, user)
    temp.forwardings += 1
    await db.add_frwd(user)
    await send(client, user, "<b>ғᴏʀᴡᴀʀᴅɪɴɢ sᴛᴀʀᴛᴇᴅ <a href=https://t.me/MeJeetX>Aryᴀ Bᴏᴛ</a></b>")
    sts.add(time=True)
    sleep_duration = data.get('duration', 1)
    if sleep_duration <= 0: sleep_duration = 1 if _bot['is_bot'] else 10
    sleep = sleep_duration
    await msg_edit(m, "<code>Processing...</code>") 
    temp.IS_FRWD_CHAT.append(i.TO)
    temp.lock[user] = locked = True
    if locked:
        try:
          MSG = []
          pling=0
          await edit(m, 'Progressing', 10, sts)
          print(f"Starting Forwarding Process... From :{sts.get('FROM')} To: {sts.get('TO')} Totel: {sts.get('limit')} stats : {sts.get('skip')})")

          # Use getattr to safely check for 'continuous' attribute since old STS objects might not have it
          is_continuous = getattr(sts, 'continuous', False)

          # --- Concurrent Download/Upload Worker Pool Setup ---
          MAX_WORKERS = 5
          task_queue = asyncio.Queue(maxsize=10) # Bounded queue so we don't fetch millions of messages and OOM

          async def copy_worker():
              while True:
                  task = await task_queue.get()
                  if task is None:
                      break
                  bot_client, task_details, task_m, task_sts, task_download_mode, attempt = task
                  try:
                      await copy(bot_client, task_details, task_m, task_sts, task_download_mode, attempt)
                      task_sts.add('total_files')
                  except Exception as e:
                      logger.error(f"Worker copy failed: {e}")
                  finally:
                      task_queue.task_done()

          workers = [asyncio.create_task(copy_worker()) for _ in range(MAX_WORKERS)]
          # ---------------------------------------------------

          async for message in client.iter_messages(
            client,
            chat_id=sts.get('FROM'), 
            limit=int(sts.get('limit')), 
            offset=int(sts.get('skip')) if sts.get('skip') else 0,
            continuous=is_continuous,
            reverse_order=data.get('reverse_order', False)
            ):
                if await is_cancelled(client, user, m, sts):
                   return
                pling += 1
                if pling % 5 == 0:
                   await edit(m, 'Progressing', 10, sts)
                # Check message type filtering
                is_filtered = False
                _filters = data.get('filters', [])

                if message.empty or message.service:
                    sts.add('deleted')
                    continue
                elif getattr(message, 'text', None) and not message.media and 'text' in _filters:
                    is_filtered = True
                elif getattr(message, 'poll', None) and 'poll' in _filters:
                    is_filtered = True
                elif getattr(message, 'audio', None) and 'audio' in _filters:
                    is_filtered = True
                elif getattr(message, 'voice', None) and 'voice' in _filters:
                    is_filtered = True
                elif getattr(message, 'video', None) and 'video' in _filters:
                    is_filtered = True
                elif getattr(message, 'photo', None) and 'photo' in _filters:
                    is_filtered = True
                elif getattr(message, 'document', None) and 'document' in _filters:
                    is_filtered = True
                elif getattr(message, 'animation', None) and 'animation' in _filters:
                    is_filtered = True
                elif getattr(message, 'sticker', None) and 'sticker' in _filters:
                    is_filtered = True
                else:
                    # check extensions and keywords
                    media_obj = getattr(message, message.media.value if message.media else '', None)
                    file_name = getattr(media_obj, 'file_name', '') if media_obj else ''
                    
                    extensions = data.get('extensions')
                    if extensions and file_name:
                        if any(file_name.endswith(ext.strip()) for ext in extensions):
                            is_filtered = True
                            
                    keywords = data.get('keywords')
                    if keywords and file_name:
                        if not any(kw.strip().lower() in file_name.lower() for kw in keywords):
                            is_filtered = True
                            
                    # File Size Limit
                    size_limit = data.get('media_size')
                    if not is_filtered and size_limit and hasattr(media_obj, 'file_size'):
                        file_size = getattr(media_obj, 'file_size', 0)
                        if file_size:
                            limit_size = size_limit[0]
                            limit_type = size_limit[1]
                            limit_bytes = limit_size * 1024 * 1024
                            if limit_type == True and file_size <= limit_bytes:
                                 is_filtered = True 
                            elif limit_type == False and file_size >= limit_bytes:
                                 is_filtered = True 
                                 
                if is_filtered:
                    sts.add('filtered')
                    continue

                sts.add('fetched')
                if forward_tag:
                   MSG.append(message.id)
                   notcompleted = len(MSG)
                   completed = sts.get('total') - sts.get('fetched')
                   if ( notcompleted >= 100 
                        or completed <= 100): 
                      await forward(client, MSG, m, sts, protect)
                      sts.add('total_files', notcompleted)
                      await asyncio.sleep(10)
                      MSG = []
                else:
                    _filters = data.get('filters', [])
                    new_caption = custom_caption(message, caption)
                    if (message.audio or message.video or message.photo or message.document) and 'rm_caption' in _filters:
                        new_caption = ""

                    # Apply Replacements
                    replacements = data.get('replacements', {})
                    if replacements and new_caption:
                        for old_txt, new_txt in replacements.items():
                            try:
                                new_caption = re.sub(old_txt, new_txt, new_caption, flags=re.IGNORECASE)
                            except Exception:
                                new_caption = new_caption.replace(old_txt, new_txt)
                    
                    details = {"msg_id": message.id, "media": media(message), "caption": new_caption, 'button': button, "protect": protect}
                    # Put task in queue instead of waiting for it sequentially
                    await task_queue.put((client, details, m, sts, download_mode, 0))
                    # Note: sts.add('total_files') is now handled inside the worker after success
                    await asyncio.sleep(sleep)
                    
          # --- Wait for all pending tasks to finish before completing ---
          if not is_continuous:
              await task_queue.join()
          
          # Tell workers to stop
          for _ in range(MAX_WORKERS):
              await task_queue.put(None)
          await asyncio.gather(*workers)
          # -------------------------------------------------------------
          
        except Exception as e:
            await msg_edit(m, f'<b>ERROR:</b>\n<code>{e}</code>', wait=True)
            if sts.TO in temp.IS_FRWD_CHAT:
                temp.IS_FRWD_CHAT.remove(sts.TO)
            return await stop(client, user)
            
        if sts.TO in temp.IS_FRWD_CHAT:
            temp.IS_FRWD_CHAT.remove(sts.TO)

        # 🔔 Detailed Completion Notification
        summary = (
            f"<b>✅ Batch Forwarding Completed!</b>\n\n"
            f"<b>📊 Summary:</b>\n"
            f" ┣ <b>Fetched:</b> <code>{sts.get('fetched')}</code>\n"
            f" ┣ <b>Forwarded:</b> <code>{sts.get('total_files')}</code>\n"
            f" ┣ <b>Duplicates skipped:</b> <code>{sts.get('duplicate')}</code>\n"
            f" ┣ <b>Filtered out:</b> <code>{sts.get('filtered')}</code>\n"
            f" ┗ <b>Deleted sources:</b> <code>{sts.get('deleted')}</code>\n"
        )
        try:
            await bot.send_message(user, summary)
        except Exception:
            pass

        await edit(m, 'Completed', "completed", sts) 
        await stop(client, user)
            
async def copy(bot, msg, m, sts, download=False, attempt=0):
   try:                                  
     if msg.get("media") and msg.get("caption") and not download:
        await bot.send_cached_media(
              chat_id=sts.get('TO'),
              file_id=msg.get("media"),
              caption=msg.get("caption"),
              reply_markup=msg.get('button'),
              protect_content=msg.get("protect"))
     elif not download:
        await bot.copy_message(
              chat_id=sts.get('TO'),
              from_chat_id=sts.get('FROM'),    
              caption=msg.get("caption"),
              message_id=msg.get("msg_id"),
              reply_markup=msg.get('button'),
              protect_content=msg.get("protect"))
     else:
        raise Exception("DownloadModeEnabled")
   except FloodWait as e:
     await edit(m, 'Progressing', e.value, sts)
     await asyncio.sleep(e.value)
     await edit(m, 'Progressing', 10, sts)
     await copy(bot, msg, m, sts, download, attempt)
   except Exception as e:
     if attempt < 3 and "RESTRICTED" not in str(e).upper() and "DOWNLOAD" not in str(e).upper() and "PROTECTED" not in str(e).upper():
         await asyncio.sleep(2)
         return await copy(bot, msg, m, sts, download, attempt + 1)
         
     if "RESTRICTED" in str(e).upper() or "DOWNLOADMODEENABLED" in str(e).upper() or "PROTECTED" in str(e).upper() or "CHAT_FORWARDS_RESTRICTED" in str(e).upper() or "MESSAGE_PROTECTED" in str(e).upper():
         try:
             import os
             print(f"Downloading message {msg.get('msg_id')} due to restriction...")
             message = await bot.get_messages(sts.get('FROM'), msg.get("msg_id"))
             if message.empty: raise Exception("MessageEmpty")
             
             if message.media:
                 # Preserve original file name from message; fall back to safe unique name
                 media_obj = getattr(message, message.media.value, None) if message.media else None
                 original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                 
                 if original_name:
                     # Keep original name but prefix with msg_id to avoid caching clashes
                     safe_name = f"downloads/{message.id}_{original_name}"
                 elif message.audio or message.voice:
                     safe_name = f"downloads/{message.id}.ogg"
                 elif message.video or message.video_note:
                     safe_name = f"downloads/{message.id}.mp4"
                 elif message.photo:
                     safe_name = f"downloads/{message.id}.jpg"
                 elif message.animation:
                     safe_name = f"downloads/{message.id}.gif"
                 else:
                     safe_name = f"downloads/{message.id}"
                     
                 file_path = await bot.download_media(message, file_name=safe_name)
                 if not file_path: raise Exception("DownloadFailed")
                 
                 kwargs = {
                     "chat_id": sts.get("TO"),
                     "caption": msg.get("caption"),
                     "reply_markup": msg.get("button"),
                     "protect_content": msg.get("protect")
                 }
                 if message.photo:
                     await bot.send_photo(photo=file_path, **kwargs)
                 elif message.video:
                     await bot.send_video(video=file_path, file_name=original_name or None, **kwargs)
                 elif message.document:
                     await bot.send_document(document=file_path, file_name=original_name or None, **kwargs)
                 elif message.audio:
                     await bot.send_audio(audio=file_path, file_name=original_name or None, **kwargs)
                 elif message.voice:
                     await bot.send_voice(voice=file_path, **kwargs)
                 elif message.video_note:
                     await bot.send_video_note(video_note=file_path, **kwargs)
                 elif message.animation:
                     await bot.send_animation(animation=file_path, **kwargs)
                 elif message.sticker:
                     await bot.send_sticker(sticker=file_path, **kwargs)
                 else:
                     await bot.copy_message(chat_id=sts.get("TO"), from_chat_id=sts.get("FROM"), message_id=msg.get("msg_id"))
                 
                 try:
                     if os.path.exists(file_path):
                         os.remove(file_path)
                 except: pass
             else:
                 await bot.send_message(
                     chat_id=sts.get("TO"),
                     text=message.text.html if message.text else "",
                     reply_markup=msg.get("button"),
                     protect_content=msg.get("protect")
                 )
         except FloodWait as e2:
             await edit(m, 'Progressing', e2.value, sts)
             await asyncio.sleep(e2.value)
             await edit(m, 'Progressing', 10, sts)
             await copy(bot, msg, m, sts, download)
         except Exception as e2:
             print(f"Fallback failed for message {msg.get('msg_id')}: {e2}")
             sts.add('deleted')
     else:
         print(f"Failed to copy message {msg.get('msg_id')}: {e}")
         sts.add('deleted')
        
async def forward(bot, msg, m, sts, protect):
   try:                             
     await bot.forward_messages(
           chat_id=sts.get('TO'),
           from_chat_id=sts.get('FROM'), 
           protect_content=protect,
           message_ids=msg)
   except FloodWait as e:
     await edit(m, 'Progressing', e.value, sts)
     await asyncio.sleep(e.value)
     await edit(m, 'Progressing', 10, sts)
     await forward(bot, msg, m, sts, protect)
   except Exception as e:
      print(f"Failed to forward messages {msg}: {e}")
      sts.add('deleted')

PROGRESS = """
📈 Percetage: {0} %

♻️ Feched: {1}

♻️ Fowarded: {2}

♻️ Remaining: {3}

♻️ Stataus: {4}

⏳️ ETA: {5}
"""

async def msg_edit(msg, text, button=None, wait=None):
    try:
        return await msg.edit(text, reply_markup=button)
    except MessageNotModified:
        pass 
    except FloodWait as e:
        if wait:
           await asyncio.sleep(e.value)
           return await msg_edit(msg, text, button, wait)
        
async def edit(msg, title, status, sts):
   i = sts.get(full=True)
   status = 'Forwarding' if status == 10 else f"Sleeping {status} s" if str(status).isnumeric() else status
   # Handle division by zero if total is 0 (which happens if infinite/continuous without known total)
   total = float(i.total) if float(i.total) > 0 else 1.0
   percentage = "{:.0f}".format(float(i.fetched)*100/total)
   
   now = time.time()
   diff = now - float(i.start)
   speed = i.fetched / diff if diff > 0 else 0
   time_to_completion = int(round((i.total - i.fetched) / speed * 1000)) if speed > 0 else 0
   pct = int(percentage)
   
   # Progress bar styling
   filled  = pct // 10          # 10 blocks total → each block = 10%
   empty   = 10 - filled
   bar     = "▰" * filled + "▱" * empty
   progress_str = f"[{bar}] {pct}%"
   
   # Replace the bottom button text with the progress bar
   button =  [[InlineKeyboardButton(progress_str, f'fwrdstatus#{status}#{time_to_completion}#{percentage}#{i.id}')]]
   
   # Time formatter
   estimated_total_time = TimeFormatter(milliseconds=time_to_completion)
   estimated_total_time = estimated_total_time if estimated_total_time != '' else '0 s'

   # 7 formatting slots in TEXT now: fetched, total_files, duplicate, skip, deleted, status, ETA
   text = TEXT.format(i.fetched, i.total_files, i.duplicate, i.skip, i.deleted, status, estimated_total_time)
   
   if status in ["cancelled", "completed"]:
      # Completed state button override with Support text
      button = [[
          InlineKeyboardButton('✦ 𝐒𝐮𝐩𝐩𝐨𝐫𝐭 ✦', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
          InlineKeyboardButton('✦ 𝐔𝐩𝐝𝐚𝐭𝐞𝐬 ✦', url='https://t.me/MeJeetX')
      ]]
   else:
      button.append([InlineKeyboardButton('• ᴄᴀɴᴄᴇʟ', 'terminate_frwd')])
      
   await msg_edit(msg, text, InlineKeyboardMarkup(button))
   
async def is_cancelled(client, user, msg, sts):
   if temp.CANCEL.get(user)==True:
      temp.IS_FRWD_CHAT.remove(sts.TO)
      await edit(msg, "Cancelled", "completed", sts)
      await send(client, user, "<b>❌ Forwarding Process Cancelled</b>")
      await stop(client, user)
      return True 
   return False 

async def stop(client, user):
   try:
     await client.stop()
   except:
     pass 
   await db.rmve_frwd(user)
   temp.forwardings -= 1
   temp.lock[user] = False 
    
async def send(bot, user, text):
   try:
      await bot.send_message(user, text=text)
   except:
      pass 
     
def custom_caption(msg, caption):
  if msg.media:
    if (msg.video or msg.document or msg.audio or msg.photo):
      media = getattr(msg, msg.media.value, None)
      if media:
        file_name = getattr(media, 'file_name', '')
        file_size = getattr(media, 'file_size', '')
        fcaption = getattr(msg, 'caption', '')
        if fcaption:
          fcaption = fcaption.html
        if caption:
          return caption.format(filename=file_name, size=get_size(file_size), caption=fcaption)
        return fcaption
  return None

def get_size(size):
  units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"]
  size = float(size)
  i = 0
  while size >= 1024.0 and i < len(units):
     i += 1
     size /= 1024.0
  return "%.2f %s" % (size, units[i]) 

def media(msg):
  if msg.media:
     media = getattr(msg, msg.media.value, None)
     if media:
        return getattr(media, 'file_id', None)
  return None 

def TimeFormatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "d, ") if days else "") + \
        ((str(hours) + "h, ") if hours else "") + \
        ((str(minutes) + "m, ") if minutes else "") + \
        ((str(seconds) + "s, ") if seconds else "") + \
        ((str(milliseconds) + "ms, ") if milliseconds else "")
    return tmp[:-2]

def retry_btn(id):
    return InlineKeyboardMarkup([[InlineKeyboardButton('♻️ RETRY ♻️', f"start_public_{id}")]])

@Client.on_callback_query(filters.regex(r'^terminate_frwd$'))
async def terminate_frwding(bot, m):
    user_id = m.from_user.id 
    temp.lock[user_id] = False
    temp.CANCEL[user_id] = True 
    await m.answer("Forwarding cancelled !", show_alert=True)
          
@Client.on_callback_query(filters.regex(r'^fwrdstatus'))
async def status_msg(bot, msg):
    _, status, est_time, percentage, frwd_id = msg.data.split("#")
    sts = STS(frwd_id)
    if not sts.verify():
       fetched, forwarded, remaining = 0
    else:
       fetched, forwarded = sts.get('fetched'), sts.get('total_files')
       remaining = fetched - forwarded 
    est_time = TimeFormatter(milliseconds=est_time)
    est_time = est_time if (est_time != '' or status not in ['completed', 'cancelled']) else '0 s'
    return await msg.answer(PROGRESS.format(percentage, fetched, forwarded, remaining, status, est_time), show_alert=True)
                  
@Client.on_callback_query(filters.regex(r'^close_btn$'))
async def close(bot, update):
    await update.answer()
    await update.message.delete()
