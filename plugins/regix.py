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

          # --- Strictly Ordered Pipelined Setup ---
          # Workers are only needed for DOWNLOAD mode (large file transfers).
          # For simple copy_message / send_cached_media, we bypass the pipeline
          # and send directly + sequentially to guarantee correct ordering.
          MAX_WORKERS = 1  # Reduced to 1: file downloads are sequential, no race conditions
          task_queue = asyncio.Queue(maxsize=5) 
          upload_queue = asyncio.Queue(maxsize=5) 

          async def copy_worker():
              while True:
                  task = await task_queue.get()
                  if task is None: break
                  seq_idx, bot_client, task_details, task_m, task_sts, task_download_mode, attempt = task
                  try:
                      await copy(bot_client, task_details, task_m, task_sts, task_download_mode, attempt, seq_idx, upload_queue)
                  except Exception as e:
                      logger.error(f"Worker copy failed: {e}")
                      # On failure, insert a dummy skip so sequencer doesn't hang
                      await upload_queue.put((seq_idx, 'skip', {}, None))
                  finally:
                      task_queue.task_done()

          async def uploader_worker():
              expected_seq = 0
              buffer = {}
              while True:
                  item = await upload_queue.get()
                  if item is None: break
                  seq, action, params, local_file = item
                  buffer[seq] = (action, params, local_file)
                  
                  while expected_seq in buffer:
                      act, prm, fpath = buffer.pop(expected_seq)
                      if act != 'skip':
                          try:
                              if act == 'send_photo': await client.send_photo(**prm)
                              elif act == 'send_video': await client.send_video(**prm)
                              elif act == 'send_document': await client.send_document(**prm)
                              elif act == 'send_audio': await client.send_audio(**prm)
                              elif act == 'send_voice': await client.send_voice(**prm)
                              elif act == 'send_video_note': await client.send_video_note(**prm)
                              elif act == 'send_animation': await client.send_animation(**prm)
                              elif act == 'send_sticker': await client.send_sticker(**prm)
                              elif act == 'copy_message': await client.copy_message(**prm)
                              elif act == 'send_cached_media': await client.send_cached_media(**prm)
                              elif act == 'send_message': await client.send_message(**prm)
                              
                              sts.add('total_files')
                          except FloodWait as fw:
                              # Handle uploader side floodwaits directly by sleeping and retrying
                              await asyncio.sleep(fw.value + 2)
                              try:
                                  if act == 'send_photo': await client.send_photo(**prm)
                                  elif act == 'send_video': await client.send_video(**prm)
                                  elif act == 'send_document': await client.send_document(**prm)
                                  elif act == 'send_audio': await client.send_audio(**prm)
                                  elif act == 'send_voice': await client.send_voice(**prm)
                                  elif act == 'send_video_note': await client.send_video_note(**prm)
                                  elif act == 'send_animation': await client.send_animation(**prm)
                                  elif act == 'send_sticker': await client.send_sticker(**prm)
                                  elif act == 'copy_message': await client.copy_message(**prm)
                                  elif act == 'send_cached_media': await client.send_cached_media(**prm)
                                  elif act == 'send_message': await client.send_message(**prm)
                                  sts.add('total_files')
                              except Exception as e:
                                  print(f"Uploader fallback error: {e}")
                                  sts.add('deleted')
                          except Exception as e:
                              # Handle uploader fallback gracefully.
                              # If copy_message fails (Private chats, Bot DMs, Restricted, Error), we download/upload.
                              if act in ('copy_message', 'send_cached_media'):
                                  try:
                                      import os
                                      fallback_msg = prm.get('raw_message') or await client.get_messages(prm.get('from_chat_id'), prm.get('message_id'))
                                      if fallback_msg.media:
                                          safe_name = f"downloads/{fallback_msg.id}"
                                          dp = await client.download_media(fallback_msg, file_name=safe_name)
                                          if not dp: raise Exception("DownloadFailed")
                                          if getattr(fallback_msg, 'photo', None): await client.send_photo(chat_id=prm.get('chat_id'), photo=dp, caption=prm.get('caption'))
                                          elif getattr(fallback_msg, 'video', None): await client.send_video(chat_id=prm.get('chat_id'), video=dp, caption=prm.get('caption'))
                                          elif getattr(fallback_msg, 'document', None): await client.send_document(chat_id=prm.get('chat_id'), document=dp, caption=prm.get('caption'))
                                          elif getattr(fallback_msg, 'audio', None): await client.send_audio(chat_id=prm.get('chat_id'), audio=dp, caption=prm.get('caption'))
                                          elif getattr(fallback_msg, 'voice', None): await client.send_voice(chat_id=prm.get('chat_id'), voice=dp, caption=prm.get('caption'))
                                          if os.path.exists(dp): os.remove(dp)
                                      else:
                                          await client.send_message(chat_id=prm.get('chat_id'), text=fallback_msg.text.html if fallback_msg.text else "")
                                      sts.add('total_files')
                                  except Exception as e2:
                                      print(f"Uploader fallback error: {e2}")
                                      sts.add('deleted')
                              else:
                                  print(f"Uploader error payload: {prm}, e: {e}")
                                  sts.add('deleted')
                              
                      if fpath:
                          try:
                              if os.path.exists(fpath): os.remove(fpath)
                          except: pass
                          
                      expected_seq += 1
                  
                  upload_queue.task_done()

          workers = [asyncio.create_task(copy_worker()) for _ in range(MAX_WORKERS)]
          uploader = asyncio.create_task(uploader_worker())
          # ---------------------------------------------------
          
          seq_counter = 0
          smart_order = data.get('smart_order', True)
          # SMART ORDER: batch size is 10 — large enough to catch most source mismatches
          SORT_WINDOW = 10 if smart_order else 1
          sort_buffer = []
          
          def extract_sort_index(message):
              import re
              text = ""
              if message.media:
                  media_obj = getattr(message, message.media.value if message.media else '', None)
                  if media_obj and hasattr(media_obj, 'file_name') and media_obj.file_name:
                      text = media_obj.file_name
              if not text and message.caption:
                  text = message.caption
              if not text and message.text:
                  text = message.text
                  
              if not text:
                  return float('inf')
                  
              text_clean = text.replace(',', '')
              # Find standalone numbers first
              matches = re.findall(r'\b(\d+)\b', text_clean)
              if matches:
                  # Return last standalone number (often episode number)
                  return int(matches[-1]) 
                  
              # Fallback to any number sequence
              matches = re.findall(r'(\d+)', text_clean)
              if matches:
                  return int(matches[-1])
                  
              return float('inf')
          
          async def flush_buffer():
              nonlocal seq_counter, sort_buffer
              if not sort_buffer: return
              
              if smart_order:
                  # Sort by extracted numerical content (like episode numbers), fallback to message.id
                  # e.g. "Episode 45" before "Episode 46" even if Telegram ID for 45 is higher
                  sort_buffer.sort(key=lambda item: (extract_sort_index(item[0]), item[0].id))
              
              for message, forward_tag, new_caption, protect, download_mode, sleep in sort_buffer:
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
                        MSG.clear()
                  else:
                      # DIRECT SEQUENTIAL SEND — bypass the concurrent pipeline entirely
                      # This is the ONLY way to guarantee ordering for copy_message.
                      # The pipeline approach (task_queue + workers) inherently races
                      # even with a sequence-buffer in the uploader.
                      details = {"msg_id": message.id, "media": media(message), "caption": new_caption, 'button': button, "protect": protect, "text": message.text.html if message.text else "", "raw_message": message}
                      
                      if download_mode:
                          # Download mode: use the worker pipeline (slow, benefits from async)
                          await task_queue.put((seq_counter, client, details, m, sts, download_mode, 0))
                          seq_counter += 1
                      else:
                          # Non-download mode: SEND DIRECTLY, skip the pipeline completely
                          try:
                              await copy(client, details, m, sts, False, 0, seq_counter, upload_queue)
                              # For direct sending, drain the upload_queue immediately after each copy
                              while not upload_queue.empty():
                                  ui = upload_queue.get_nowait()
                                  if ui is None: break
                                  _, act, prm, fpath = ui
                                  if act != 'skip':
                                      try:
                                          if act == 'send_photo': await client.send_photo(**prm)
                                          elif act == 'send_video': await client.send_video(**prm)
                                          elif act == 'send_document': await client.send_document(**prm)
                                          elif act == 'send_audio': await client.send_audio(**prm)
                                          elif act == 'send_voice': await client.send_voice(**prm)
                                          elif act == 'send_video_note': await client.send_video_note(**prm)
                                          elif act == 'send_animation': await client.send_animation(**prm)
                                          elif act == 'send_sticker': await client.send_sticker(**prm)
                                          elif act == 'copy_message': await client.copy_message(**prm)
                                          elif act == 'send_cached_media': await client.send_cached_media(**prm)
                                          elif act == 'send_message': await client.send_message(**prm)
                                          sts.add('total_files')
                                      except FloodWait as fw:
                                          await asyncio.sleep(fw.value + 2)
                                          try:
                                              if act == 'copy_message': await client.copy_message(**prm)
                                              elif act == 'send_cached_media': await client.send_cached_media(**prm)
                                              sts.add('total_files')
                                          except Exception: sts.add('deleted')
                                      except Exception as e:
                                          print(f"Direct send error: {e}")
                                          if act in ('copy_message', 'send_cached_media'):
                                              print(f"Falling back to download for msg {prm.get('message_id')} due to {e}")
                                              await copy(client, details, m, sts, True, 0, seq_counter, upload_queue)
                                          else:
                                              sts.add('deleted')
                                  if fpath:
                                      try:
                                          import os
                                          if os.path.exists(fpath): os.remove(fpath)
                                      except: pass
                              seq_counter += 1
                          except Exception as e:
                              print(f"Direct copy error: {e}")
                              sts.add('deleted')
                              seq_counter += 1
                      
                      if sleep > 0:
                          await asyncio.sleep(sleep)
              sort_buffer.clear()

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
                
                # Determine message's generic type
                msg_type = 'text'
                if getattr(message, 'poll', None): msg_type = 'poll'
                elif getattr(message, 'audio', None): msg_type = 'audio'
                elif getattr(message, 'voice', None): msg_type = 'voice'
                elif getattr(message, 'video', None): msg_type = 'video'
                elif getattr(message, 'photo', None): msg_type = 'photo'
                elif getattr(message, 'document', None): msg_type = 'document'
                elif getattr(message, 'animation', None): msg_type = 'animation'
                elif getattr(message, 'sticker', None): msg_type = 'sticker'
                
                if msg_type in _filters:
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

                # Compute caption & replacements for this message before buffering
                _filters = data.get('filters', [])
                new_caption = custom_caption(message, caption)
                if (message.audio or message.video or message.photo or message.document) and 'rm_caption' in _filters:
                    new_caption = ""

                replacements = data.get('replacements', {})
                if replacements and new_caption:
                    for old_txt, new_txt in replacements.items():
                        try:
                            new_caption = re.sub(old_txt, new_txt, new_caption, flags=re.IGNORECASE)
                        except Exception:
                            new_caption = new_caption.replace(old_txt, new_txt)
                
                sort_buffer.append((message, forward_tag, new_caption, protect, download_mode, sleep))
                
                # Flush only when we have a full SORT_WINDOW batch (or immediately if smart is OFF)
                if len(sort_buffer) >= SORT_WINDOW:
                    await flush_buffer()
                    
          # --- Flush remaining messages that didn't fill a complete window ---
          await flush_buffer()
                    
          # --- Wait for all pending tasks to finish before completing ---
          if not is_continuous:
              await task_queue.join()
          
          # Tell workers to stop
          for _ in range(MAX_WORKERS):
              await task_queue.put(None)
          await asyncio.gather(*workers)
          
          # Tell uploader to stop
          await upload_queue.put(None)
          await uploader
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
            
async def copy(bot, msg, m, sts, download=False, attempt=0, seq_index=None, upload_queue=None):
   try:                                  
     if msg.get("media") and msg.get("caption") and not download:
        kwargs = {
            "chat_id": sts.get('TO'),
            "file_id": msg.get("media"),
            "caption": msg.get("caption"),
            "reply_markup": msg.get('button'),
            "protect_content": msg.get("protect"),
            "raw_message": msg.get("raw_message")
        }
        await upload_queue.put((seq_index, 'send_cached_media', kwargs, None))
     elif not download:
        kwargs = {
            "chat_id": sts.get('TO'),
            "from_chat_id": sts.get('FROM'),    
            "caption": msg.get("caption"),
            "message_id": msg.get("msg_id"),
            "reply_markup": msg.get('button'),
            "protect_content": msg.get("protect"),
            "raw_message": msg.get("raw_message")
        }
        await upload_queue.put((seq_index, 'copy_message', kwargs, None))
     else:
        raise Exception("DownloadModeEnabled")
   except FloodWait as e:
     await edit(m, 'Progressing', e.value, sts)
     await asyncio.sleep(e.value)
     await edit(m, 'Progressing', 10, sts)
     await copy(bot, msg, m, sts, download, attempt, seq_index, upload_queue)
   except Exception as e:
     if attempt < 3 and "RESTRICTED" not in str(e).upper() and "DOWNLOAD" not in str(e).upper() and "PROTECTED" not in str(e).upper() and "FALLBACK" not in str(e).upper():
         await asyncio.sleep(2)
         return await copy(bot, msg, m, sts, download, attempt + 1, seq_index, upload_queue)
         
     if "RESTRICTED" in str(e).upper() or "DOWNLOAD" in str(e).upper() or "PROTECTED" in str(e).upper() or "FALLBACK" in str(e).upper():
         try:
             import os
             print(f"Downloading message {msg.get('msg_id')} due to restriction...")
             message = msg.get("raw_message") or await bot.get_messages(sts.get('FROM'), msg.get("msg_id"))
             if message.empty or message.service: raise Exception("MessageEmpty")
             
             if message.media:
                 # Preserve original file name from message; fall back to safe unique name
                 media_obj = getattr(message, message.media.value, None) if message.media else None
                 original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                 
                 if original_name:
                     safe_name = f"downloads/{message.id}_{original_name}"
                 elif getattr(message, 'audio', None) or getattr(message, 'voice', None):
                     safe_name = f"downloads/{message.id}.ogg"
                 elif getattr(message, 'video', None) or getattr(message, 'video_note', None):
                     safe_name = f"downloads/{message.id}.mp4"
                 elif getattr(message, 'photo', None):
                     safe_name = f"downloads/{message.id}.jpg"
                 elif getattr(message, 'animation', None):
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
                 
                 if getattr(message, 'photo', None):
                     await upload_queue.put((seq_index, 'send_photo', {"photo": file_path, **kwargs}, file_path))
                 elif getattr(message, 'video', None):
                     await upload_queue.put((seq_index, 'send_video', {"video": file_path, "file_name": original_name or None, **kwargs}, file_path))
                 elif getattr(message, 'document', None):
                     await upload_queue.put((seq_index, 'send_document', {"document": file_path, "file_name": original_name or None, **kwargs}, file_path))
                 elif getattr(message, 'audio', None):
                     await upload_queue.put((seq_index, 'send_audio', {"audio": file_path, "file_name": original_name or None, **kwargs}, file_path))
                 elif getattr(message, 'voice', None):
                     await upload_queue.put((seq_index, 'send_voice', {"voice": file_path, **kwargs}, file_path))
                 elif getattr(message, 'video_note', None):
                     await upload_queue.put((seq_index, 'send_video_note', {"video_note": file_path, **kwargs}, file_path))
                 elif getattr(message, 'animation', None):
                     await upload_queue.put((seq_index, 'send_animation', {"animation": file_path, **kwargs}, file_path))
                 elif getattr(message, 'sticker', None):
                     await upload_queue.put((seq_index, 'send_sticker', {"sticker": file_path, **kwargs}, file_path))
                 else:
                     # Attempt to just copy message if somehow media type is completely missing.
                     c_kwargs = {"chat_id": sts.get("TO"), "from_chat_id": sts.get("FROM"), "message_id": msg.get("msg_id")}
                     await upload_queue.put((seq_index, 'copy_message', c_kwargs, file_path))
             else:
                 snd_kwargs = {
                     "chat_id": sts.get("TO"),
                     "text": msg.get("text") or "",
                     "reply_markup": msg.get("button"),
                     "protect_content": msg.get("protect")
                 }
                 await upload_queue.put((seq_index, 'send_message', snd_kwargs, None))
         except FloodWait as e2:
             await edit(m, 'Progressing', e2.value, sts)
             await asyncio.sleep(e2.value)
             await edit(m, 'Progressing', 10, sts)
             await copy(bot, msg, m, sts, download, attempt, seq_index, upload_queue)
         except Exception as e2:
             print(f"Fallback failed for message {msg.get('msg_id')}: {e2}")
             await upload_queue.put((seq_index, 'skip', {}, None))
             sts.add('deleted')
     else:
         print(f"Failed to copy message {msg.get('msg_id')}: {e}")
         await upload_queue.put((seq_index, 'skip', {}, None))
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
   user_id = int(str(sts.id).split('-')[0])
   
   if temp.PAUSE.get(user_id) == True:
      status = 'Paused'
   else:
      status = 'Forwarding' if status == 10 else f"Sleeping {status} s" if str(status).isnumeric() else status
   # Handle division by zero if total is 0 (which happens if infinite/continuous without known total)
   total = float(i.total) if float(i.total) > 0 else 1.0
   percentage = "{:.0f}".format(float(i.total_files)*100/total)
   
   now = time.time()
   diff = now - float(i.start)
   speed = i.total_files / diff if diff > 0 else 0
   time_to_completion = int(round((i.total - i.total_files) / speed * 1000)) if speed > 0 else 0
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
      if temp.PAUSE.get(user_id) == True:
          button.append([
              InlineKeyboardButton('▶ ʀᴇsᴜᴍᴇ', 'resume_frwd'), 
              InlineKeyboardButton('• ᴄᴀɴᴄᴇʟ', 'terminate_frwd')
          ])
      else:
          button.append([
              InlineKeyboardButton('⏸ ᴘᴀᴜsᴇ', 'pause_frwd'), 
              InlineKeyboardButton('• ᴄᴀɴᴄᴇʟ', 'terminate_frwd')
          ])
      
   await msg_edit(msg, text, InlineKeyboardMarkup(button))
   
async def is_cancelled(client, user, msg, sts):
   if temp.CANCEL.get(user)==True:
      temp.IS_FRWD_CHAT.remove(sts.TO)
      await edit(msg, "Cancelled", "completed", sts)
      await send(client, user, "<b>❌ Forwarding Process Cancelled</b>")
      await stop(client, user)
      return True 
      
   while temp.PAUSE.get(user) == True:
      await asyncio.sleep(2)
      if temp.CANCEL.get(user) == True:
          return await is_cancelled(client, user, msg, sts)
          
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

@Client.on_callback_query(filters.regex(r'^pause_frwd$'))
async def pause_frwding(bot, m):
    user_id = m.from_user.id 
    temp.PAUSE[user_id] = True 
    await m.answer("Forwarding paused!", show_alert=True)

@Client.on_callback_query(filters.regex(r'^resume_frwd$'))
async def resume_frwding(bot, m):
    user_id = m.from_user.id 
    temp.PAUSE[user_id] = False 
    await m.answer("Forwarding resumed!", show_alert=True)
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
