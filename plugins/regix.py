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
TEXT_BATCH = Translation.TEXT_BATCH
TEXT_LIVE = Translation.TEXT_LIVE

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
          # Start channel-side progress bar (awaits so errors surface before the loop starts)
          _dest_chat = int(sts.get('TO'))
          _total_msgs = int(sts.get('limit')) if sts.get('limit') else 0
          try:
              await channel_progress_start(client, _dest_chat, _total_msgs)
          except Exception as _pg_e:
              logger.warning(f"[ChannelProgress] Could not start: {_pg_e}")

          # Use getattr to safely check for 'continuous' attribute since old STS objects might not have it
          is_continuous = getattr(sts, 'continuous', False)

          # --- Strictly Ordered Pipelined Setup ---
          # Workers are only needed for DOWNLOAD mode (large file transfers).
          # For simple copy_message / send_cached_media, we bypass the pipeline
          # and send directly + sequentially to guarantee correct ordering.
          # Using 2 workers provides parallelism while preserving order via the
          # sequence-numbered uploader queue. 5 sometimes overwhelms Telegram rate limits.
          MAX_WORKERS = 2
          task_queue = asyncio.Queue(maxsize=100)  # Larger buffer prevents blocking 
          upload_queue = asyncio.Queue(maxsize=100)

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

          async def execute_upload_action(act, prm, fpath, sts_obj):
              # Execute the requested transmission action with retries for timeout
              for attempt in range(3):
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
                      sts_obj.add('total_files')
                      return True, None
                  except FloodWait as fw:
                      await asyncio.sleep(fw.value + 2)
                      continue
                  except Exception as e:
                      err_msg = str(e).upper()
                      if "TIMEOUT" in err_msg or "CONNECTION" in err_msg:
                          await asyncio.sleep(5)
                          continue # Retry sending
                      
                      # On first failure due to missing file, trying to fallback
                      print(f"Direct send error: {e}")
                      # sts_obj.add('deleted')
                      return False, e
              
              print(f"Max retries reached. Action failed.")
              # sts_obj.add('deleted')
              return False, Exception("Max retries")

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
                          success, err_e = await execute_upload_action(act, prm, fpath, sts)
                          if not success:
                              # Handle uploader fallback for restricted content graciously
                              print(f"Uploader error: {prm} - {err_e}")
                              sts.add('deleted')
                              
                      if fpath:
                          try:
                              import os
                              if os.path.exists(fpath): os.remove(fpath)
                          except: pass
                          
                      expected_seq += 1
                  
                  upload_queue.task_done()

          workers = []
          uploader = None
          if download_mode:
              workers = [asyncio.create_task(copy_worker()) for _ in range(MAX_WORKERS)]
              uploader = asyncio.create_task(uploader_worker())
          # ---------------------------------------------------
          
          seq_counter = 0
          smart_order = data.get('smart_order', True)
          # SMART ORDER: Buffer up to 500 messages to sort accurately via NLP
          limit_val = int(sts.get('limit') or 1)
          SORT_WINDOW = min(limit_val, 500) if smart_order else 1
          sort_buffer = []
          
          async def flush_buffer():
              nonlocal seq_counter, sort_buffer
              if not sort_buffer: return
              
              if smart_order:
                  import re
                  def _smart_sort_key(item):
                      msg = item[0]
                      media_obj = getattr(msg, msg.media.value if msg.media else '', None) if msg.media else None
                      filename = getattr(media_obj, 'file_name', '') if media_obj else ''
                      caption = msg.caption or getattr(msg.text, 'html', str(msg.text)) if msg.text else ''
                      
                      search_txt = f"{filename} {caption}".lower()
                      
                      # Heuristic 1: Explicit markers (Ep, Part, Chapter)
                      m1 = re.search(r'(?:ep|episode|part|ch|chapter|e)\s*[-_:]?\s*0*(\d+)', search_txt)
                      if m1: return (0, int(m1.group(1)), msg.id)
                      
                      # Heuristic 2: Trailing numerics isolated in filename
                      m2 = re.findall(r'(?<!\d)0*(\d+)(?!\d)', str(filename))
                      if m2: return (1, int(m2[-1]), msg.id)
                      
                      # Fallback
                      return (2, msg.id, msg.id)
                      
                  sort_buffer.sort(key=_smart_sort_key)
              
              for message, forward_tag, new_caption, new_text, is_text_replaced, protect, download_mode, sleep in sort_buffer:
                  sts.add('fetched')
                  if forward_tag:
                     MSG.append(message.id)
                     notcompleted = len(MSG)
                     if notcompleted >= 100:
                        await forward(client, MSG, m, sts, protect)
                        sts.add('total_files', notcompleted)
                        await asyncio.sleep(10)
                        MSG.clear()
                  else:
                      # DIRECT SEQUENTIAL SEND — bypass the concurrent pipeline entirely
                      # This is the ONLY way to guarantee ordering for copy_message.
                      # The pipeline approach (task_queue + workers) inherently races
                      # even with a sequence-buffer in the uploader.
                      details = {"msg_id": message.id, "media": media(message), "caption": new_caption, "is_text_replaced": is_text_replaced, 'button': button, "protect": protect, "text": new_text if new_text is not None else (getattr(message.text, "html", str(message.text)) if message.text else "")}
                      
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
                                      suc, err = await execute_upload_action(act, prm, fpath, sts)
                                      if not suc and err:
                                          if "RESTRICTED" in str(err).upper() or "PROTECTED" in str(err).upper():
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

          # Topic (thread) filter helper — same logic as jobs.py / multijob.py
          def _msg_in_topic(msg, thread_id: int) -> bool:
              """Return True if msg belongs to the given topic/thread."""
              try:
                  tid = getattr(msg, 'message_thread_id', None)
                  if tid is None:
                      tid = getattr(msg, 'reply_to_top_message_id', None)
                  if tid is not None and int(tid) == thread_id:
                      return True
                  if int(msg.id) == thread_id:
                      return True  # The root message of the topic itself
              except Exception:
                  pass
              return False

          # Inline topic filter value for this job
          _from_thread = data.get('from_thread', None)
          if _from_thread:
              try: _from_thread = int(_from_thread)
              except: _from_thread = None

          # Handle Bot DM fetching logic
          from_chat = sts.get('FROM')
          me = await client.get_me()
          if from_chat == me.id or from_chat == me.username:
              from_chat = user

          async for message in client.iter_messages(
            chat_id=from_chat, 
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
                if pling % 10 == 0:
                    _fwded = int(sts.get('total_files') or 0)
                    asyncio.create_task(channel_progress_update(client, _dest_chat, _fwded, _total_msgs))
                # Topic (thread) filtering — skip messages not in the requested topic
                if _from_thread and not _msg_in_topic(message, _from_thread):
                    sts.add('filtered')
                    continue
                # Check message type filtering
                is_filtered = False
                _disabled_types = data.get('filters', [])  # list of disabled type names
                _configs_filters = data.get('configs_filters', {})  # dict with rm_caption, links, etc.

                if message.empty or message.service:
                    sts.add('deleted')
                    continue

                # Link removal for pure text messages handled via new_text below
                
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
                
                if msg_type in _disabled_types:
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
                # _configs_filters is a dict with rm_caption, links, etc.
                _remove_links_flag = 'links' in _disabled_types  # strip links from captions of allowed media
                new_caption = custom_caption(message, caption, apply_smart_clean=_configs_filters.get('rm_caption', False), remove_links_flag=_remove_links_flag)

                replacements = data.get('replacements', {})
                if replacements and new_caption:
                    for old_txt, new_txt in replacements.items():
                        if old_txt is None: continue
                        new_txt_safe = "" if new_txt is None else str(new_txt)
                        try:
                            new_caption = re.sub(str(old_txt), new_txt_safe, str(new_caption), flags=re.IGNORECASE)
                        except Exception:
                            new_caption = str(new_caption).replace(str(old_txt), new_txt_safe)
                
                new_text = None
                is_text_replaced = False
                if not message.media:
                    new_text = getattr(message.text, "html", str(message.text)) if message.text else ""
                    if _remove_links_flag and new_text:
                        new_text = remove_all_links(new_text)
                        is_text_replaced = True
                    
                    if replacements and new_text:
                        orig_text = new_text
                        for old_txt, new_txt in replacements.items():
                            if old_txt is None: continue
                            new_txt_safe = "" if new_txt is None else str(new_txt)
                            try: new_text = re.sub(str(old_txt), new_txt_safe, str(new_text), flags=re.IGNORECASE)
                            except Exception: new_text = str(new_text).replace(str(old_txt), new_txt_safe)
                        if orig_text != new_text: is_text_replaced = True
                        
                sort_buffer.append((message, forward_tag, new_caption, new_text, is_text_replaced, protect, download_mode, sleep))
                
                # Flush only when we have a full SORT_WINDOW batch (or immediately if smart is OFF)
                if len(sort_buffer) >= SORT_WINDOW:
                    await flush_buffer()
                    
          # --- Flush remaining messages that didn't fill a complete window ---
          await flush_buffer()
                    
          # --- Wait for all pending tasks to finish before completing ---
          if download_mode:
              if not is_continuous:
                  await task_queue.join()
              
              # Tell workers to stop
              for _ in range(MAX_WORKERS):
                  await task_queue.put(None)
              await asyncio.gather(*workers)
              
              # Tell uploader to stop
              await upload_queue.put(None)
              if uploader:
                  await uploader
          
          # -------------------------------------------------------------
          # Flush any remaining forward_tag messages directly
          if MSG:
              await forward(client, MSG, m, sts, protect)
              sts.add('total_files', len(MSG))
              MSG.clear()

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
        # Finalize channel progress bar
        _fwded_final = int(sts.get('total_files') or 0)
        asyncio.create_task(channel_progress_done(client, _dest_chat, _fwded_final, _total_msgs, cancelled=False))
        await stop(client, user)
            
async def copy(bot, msg, m, sts, download=False, attempt=0, seq_index=None, upload_queue=None):
   try:                                  
     if msg.get("media") and msg.get("caption") and not download:
        kwargs = {
            "chat_id": sts.get('TO'),
            "file_id": msg.get("media"),
            "caption": msg.get("caption"),
            "reply_markup": msg.get('button'),
            "protect_content": msg.get("protect")
        }
        await upload_queue.put((seq_index, 'send_cached_media', kwargs, None))
     elif not download:
        if not msg.get("media") and msg.get("is_text_replaced"):
            if not msg.get("text") or not msg.get("text").strip():
                return await upload_queue.put((seq_index, 'skip', {}, None))
            kwargs = {
                "chat_id": sts.get('TO'),
                "text": msg.get("text"),
                "reply_markup": msg.get('button'),
                "protect_content": msg.get("protect")
            }
            await upload_queue.put((seq_index, 'send_message', kwargs, None))
        else:
            kwargs = {
                "chat_id": sts.get('TO'),
                "from_chat_id": sts.get('FROM'),    
                "caption": msg.get("caption"),
                "message_id": msg.get("msg_id"),
                "reply_markup": msg.get('button'),
                "protect_content": msg.get("protect")
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
             message = await bot.get_messages(sts.get('FROM'), msg.get("msg_id"))
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
                     
                 file_path = None
                 for _ in range(3):
                     try:
                         file_path = await bot.download_media(message, file_name=safe_name)
                         if file_path: break
                     except FloodWait as fw:
                         await asyncio.sleep(fw.value + 2)
                     except Exception as dl_e:
                         err_dl = str(dl_e).upper()
                         if "TIMEOUT" in err_dl or "CONNECTION" in err_dl:
                             await asyncio.sleep(5)
                             continue
                         break
                         
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
                 if msg.get("is_text_replaced") and not msg.get("media"):
                     if not msg.get("text") or not msg.get("text").strip():
                         sts.add('deleted')
                         return await upload_queue.put((seq_index, 'skip', {}, None))
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
             f_err = str(e2).upper()
             if "TIMEOUT" in f_err or "CONNECTION" in f_err:
                 if attempt < 3:
                     await asyncio.sleep(5)
                     return await copy(bot, msg, m, sts, download, attempt + 1, seq_index, upload_queue)
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

PROGRESS_BATCH = """
📈 ᴘᴇʀᴄᴇɴᴛᴀɢᴇ: {0} %

♻️ Fᴇᴛᴄʜᴇᴅ: {1}

♻️ Fᴏʀᴡᴀʀᴅᴇᴅ: {2}

♻️ Rᴇᴍᴀɪɴɪɴɢ: {3}

♻️ Sᴛᴀᴛᴜs: {4}

⏳️ ETA: {5}
"""

PROGRESS_LIVE = """
📈 ᴘᴇʀᴄᴇɴᴛᴀɢᴇ: {0} %

♻️ Fᴇᴛᴄʜᴇᴅ: {1}

♻️ Fᴏʀᴡᴀʀᴅᴇᴅ: {2}

♻️ Rᴇᴍᴀɪɴɪɴɢ: {3}

♻️ Sᴛᴀᴛᴜs: {4}
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

   is_continuous = getattr(sts, 'continuous', False)
   if is_continuous:
       text = TEXT_LIVE.format(i.fetched, i.total_files, i.duplicate, i.skip, i.deleted, status)
   else:
       text = TEXT_BATCH.format(i.fetched, i.total_files, i.duplicate, i.skip, i.deleted, status, estimated_total_time)
   
   if status in ["cancelled", "completed"]:
      # Completed state button override with Support text
      button = [[
          InlineKeyboardButton('💬 Sᴜᴘᴘᴏʀᴛ Gʀᴏᴜᴘ', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
          InlineKeyboardButton('📢 Uᴘᴅᴀᴛᴇs', url='https://t.me/MeJeetX')
      ]]
   else:
      if temp.PAUSE.get(user_id) == True:
          button.append([
              InlineKeyboardButton('▶️ Rᴇsᴜᴍᴇ', 'resume_frwd'), 
              InlineKeyboardButton('⛔ Cᴀɴᴄᴇʟ', 'terminate_frwd')
          ])
      else:
          button.append([
              InlineKeyboardButton('⏸ Pᴀᴜsᴇ', 'pause_frwd'), 
              InlineKeyboardButton('⛔ Cᴀɴᴄᴇʟ', 'terminate_frwd')
          ])
      
   await msg_edit(msg, text, InlineKeyboardMarkup(button))
   
async def is_cancelled(client, user, msg, sts):
   if temp.CANCEL.get(user)==True:
      temp.IS_FRWD_CHAT.remove(sts.TO)
      await edit(msg, "Cancelled", "completed", sts)
      await send(client, user, "<b>❌ Forwarding Process Cancelled</b>")
      # Mark channel progress as cancelled
      try:
          _fwded = int(sts.get('total_files') or 0)
          _tot   = int(sts.get('limit') or 0)
          asyncio.create_task(channel_progress_done(client, int(sts.TO), _fwded, _tot, cancelled=True))
      except Exception:
          pass
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

# ─────────────────────────────────────────────────────────────────────────────
# Channel-side Live Progress Bar
# Shows a progress message in the DESTINATION channel during forwarding.
# The message is auto-deleted 3 minutes after completion.
# ─────────────────────────────────────────────────────────────────────────────

# Stores {dest_chat_id: message_id} for the active progress message
_channel_progress_msgs: dict = {}

def _build_channel_progress_text(forwarded: int, total: int, status: str = "forwarding") -> str:
    """Build a clean progress bar text for the destination channel."""
    if total and total > 0:
        pct = min(int(forwarded * 100 / total), 100)
    else:
        pct = 0
    filled = pct // 5   # 20 blocks total → each = 5%
    empty  = 20 - filled
    bar = "█" * filled + "░" * empty
    if status == "done":
        heading = "✅ <b>Forwarding Complete!</b>"
        status_line = f"<b>All {forwarded} files forwarded successfully.</b>"
    elif status == "cancelled":
        heading = "❌ <b>Forwarding Cancelled</b>"
        status_line = f"<b>Stopped at {forwarded} / {total if total else '?'} files.</b>"
    else:
        heading = "📤 <b>Bot is forwarding, please wait…</b>"
        status_line = f"<b>Files:</b> <code>{forwarded}</code> / <code>{total if total else '?'}</code>"
    return (
        f"{heading}\n\n"
        f"<code>[{bar}]</code>  <b>{pct}%</b>\n"
        f"{status_line}\n\n"
        f"<i>Powered by Arya Forward Bot</i>"
    )

async def channel_progress_start(client, dest_chat: int, total: int, thread_id: int = None) -> None:
    """Send the initial progress message to the destination channel and pin it."""
    try:
        text = _build_channel_progress_text(0, total, "forwarding")
        kw = {"text": text, "parse_mode": "html"}
        if thread_id:
            kw["message_thread_id"] = thread_id
        msg = await client.send_message(dest_chat, **kw)
        _channel_progress_msgs[dest_chat] = msg.id
        # Auto-pin the progress message so it stays visible
        try:
            await client.pin_chat_message(dest_chat, msg.id, disable_notification=True)
        except Exception:
            pass  # If pinning fails (e.g., no admin rights), continue silently
    except Exception as e:
        logger.warning(f"[ChannelProgress] Could not send to {dest_chat}: {e}")

async def channel_progress_update(client, dest_chat: int, forwarded: int, total: int) -> None:
    """Edit the progress message in the destination channel."""
    msg_id = _channel_progress_msgs.get(dest_chat)
    if not msg_id:
        return
    try:
        text = _build_channel_progress_text(forwarded, total, "forwarding")
        await client.edit_message_text(dest_chat, msg_id, text, parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
    except (MessageNotModified, Exception):
        pass

async def channel_progress_done(client, dest_chat: int, forwarded: int, total: int,
                                  cancelled: bool = False, auto_delete_secs: int = 180) -> None:
    """Edit the progress message to show completion, unpin it, delete it, then send completion message."""
    msg_id = _channel_progress_msgs.pop(dest_chat, None)
    status = "cancelled" if cancelled else "done"

    if msg_id:
        try:
            text = _build_channel_progress_text(forwarded, total, status)
            await client.edit_message_text(dest_chat, msg_id, text, parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
        except Exception:
            pass
        # Unpin the progress message now that forwarding is done
        try:
            await client.unpin_chat_message(dest_chat, msg_id)
        except Exception:
            pass
        # Schedule auto-delete of progress message after delay
        async def _delete_later():
            await asyncio.sleep(auto_delete_secs)
            try:
                await client.delete_messages(dest_chat, msg_id)
            except Exception:
                pass
        asyncio.create_task(_delete_later())

    # Send completion message to destination channel (only when not cancelled)
    if not cancelled:
        try:
            completion_text = (
                "<i>Hey, the story is complete. Hope you like it \U0001faf6\U0001f3fb.</i>\n\n"
                "<u>If you're looking for another story, then try… @StoriesByJeetXNew</u>"
            )
            await client.send_message(dest_chat, completion_text, parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
        except Exception:
            pass
     
import re

def smart_clean_caption(caption: str) -> str:
    if not caption:
        return ""
    
    cleaned = str(caption)
    # Remove common audio/video codecs and group tags attached to the extension
    cleaned = re.sub(r'(?i)(AAC[0-9.]*|H\.?264|H\.?265|x264|x265|HEVC).*?(\.mkv|\.mp4|\.avi|\.webm|\.flv)', '', cleaned)
    # Remove isolated extensions
    cleaned = re.sub(r'(?i)(\.mkv|\.mp4|\.avi|\.webm|\.flv)', '', cleaned)
    # Remove trailing group tags like -Siddh_12
    cleaned = re.sub(r'(-[a-zA-Z0-9_]+)(\s*)$', r'\2', cleaned)
    # Remove prominent promotional channel intros
    cleaned = re.sub(r'(?i)(⚡️.*?Join Us.*|@\w+)', '', cleaned)
    
    return cleaned.strip()

def remove_all_links(text: str) -> str:
    if not text:
        return ""
    # Strip HTML anchor tags entirely, or maybe keep their inner text?
    # Keeping inner text: replacing <a href="...">Text</a> with Text
    text = re.sub(r'(?i)<a\s+href="[^"]*".*?>(.*?)</a>', r'\1', text)
    # Remove raw URLs - enhanced regex
    text = re.sub(r'(?i)\b(?:https?://|www\.)[^\s]+', '', text)
    text = re.sub(r'(?i)\bt\.me/[^\s]+', '', text)
    # Remove mentions
    text = re.sub(r'(?i)@\w+', '', text)
    return text.strip()

def custom_caption(msg, caption, apply_smart_clean=False, remove_links_flag=False):
  if not msg.media: return None
  if not (msg.video or msg.document or msg.audio or msg.photo): return None
  
  media = getattr(msg, msg.media.value, None)
  if not media: return None
  
  file_name = getattr(media, 'file_name', '')
  file_size = getattr(media, 'file_size', 0)
  
  fcaption = getattr(msg, 'caption', '')
  if fcaption: fcaption = getattr(fcaption, 'html', str(fcaption))
  
  if apply_smart_clean == 2:
      # Wipe All Captions. Block it completely.
      return ""
      fcaption = ""
  elif apply_smart_clean is True:
      # Smart Clean: Remove target patterns
      fcaption = smart_clean_caption(fcaption)
  elif apply_smart_clean is False:
      # Keep Original
      pass

  if remove_links_flag and fcaption:
      fcaption = remove_all_links(fcaption)

  # If an explicit custom template exists, format it
  if caption:
      # Using format carefully in case template uses generic syntax
      try:
          return caption.format(filename=file_name, size=get_size(file_size), caption=fcaption)
      except:
          return caption  # Fallback if bad format
          
  # No template provided
  if apply_smart_clean is True:
      # Smart clean modified the caption, return the new cleaned text
      return fcaption if fcaption else ""
  else:
      # apply_smart_clean is False => keep original 
      # BUT if links were removed, we must return the modified version, not None
      if remove_links_flag:
          return fcaption if fcaption else ""
      return None  # None tells Pyrogram to keep the original untouched

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
    return InlineKeyboardMarkup([[InlineKeyboardButton('♻️ Rᴇᴛʀʏ ♻️', f"start_public_{id}")]])

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
    is_continuous = getattr(sts, 'continuous', False)
    if is_continuous:
       return await msg.answer(PROGRESS_LIVE.format(percentage, fetched, forwarded, remaining, status), show_alert=True)
    else:
       est_time = TimeFormatter(milliseconds=est_time)
       est_time = est_time if (est_time != '' or status not in ['completed', 'cancelled']) else '0 s'
       return await msg.answer(PROGRESS_BATCH.format(percentage, fetched, forwarded, remaining, status, est_time), show_alert=True)
                  
@Client.on_callback_query(filters.regex(r'^close_btn$'))
async def close(bot, update):
    await update.answer()
    await update.message.delete()
