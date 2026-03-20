import re, shutil

# ═══════════════════════════════════════════════════
# PATCH regix.py
# ═══════════════════════════════════════════════════
with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    regix = f.read()

# FIX 1: Block-links should NOT delete file — only strip caption/links
# Currently: if _has_link: sts.add('filtered'); continue  -> drops entire message
# Fix: strip the link from caption instead of dropping
old_link_block = """                # ── Strict link filter ──────────────────────────────────────────
                # If 'links' filter is disabled, block any message containing URLs.
                # Checks text content, captions, AND Pyrogram message entities (text_link, url, mention).
                _link_disabled = data.get('block_links', False)
                if not is_filtered and _link_disabled:
                    _has_link = False
                    for _fld in ('text', 'caption'):
                        _content = getattr(message, _fld, None)
                        if _content:
                            _raw = _content.html if hasattr(_content, 'html') else str(_content)
                            if re.search(r'(https?://\\S+|t\\.me/\\S+|@[A-Za-z0-9_]{4,}|\\b(?:www\\.|bit\\.ly/|youtu\\.be/)\\S+|\\b[\\w.-]+\\.(?:com|net|org|io|co|me|tv|gg|app|xyz|info|news|link|site)(?:/\\S*)?\\b)', _raw, re.IGNORECASE):
                                _has_link = True; break
                    if not _has_link:
                        for _efld in ('entities', 'caption_entities'):
                            for _e in (getattr(message, _efld, None) or []):
                                if getattr(_e, 'type', '') in ('url', 'text_link', 'mention', 'bot_command'):
                                    _has_link = True; break
                            if _has_link: break
                    if _has_link:
                        sts.add('filtered')
                        continue"""

new_link_block = """                # ── Link filter — strip link from caption, never drop the file ──
                _link_disabled = data.get('block_links', False)
                if not is_filtered and _link_disabled:
                    _LINK_STRIP_RE = re.compile(
                        r'(https?://\\S+|t\\.me/\\S+|@[A-Za-z0-9_]{4,}'
                        r'|\\b(?:www\\.|bit\\.ly/|youtu\\.be/)\\S+'
                        r'|\\b[\\w.-]+\\.(?:com|net|org|io|co|me|tv|gg|app|xyz|info|news|link|site)(?:/\\S*)?\\b)',
                        re.IGNORECASE)
                    # For pure text messages with ONLY a link and no media — skip those
                    if not message.media:
                        _txt = str(getattr(message, 'text', '') or '')
                        if _LINK_STRIP_RE.search(_txt):
                            sts.add('filtered'); continue
                    # For media messages — strip the link/URL from caption, keep the file"""

if old_link_block in regix:
    regix = regix.replace(old_link_block, new_link_block)
    print("Fixed link filter in regix.py")
else:
    print("WARN: could not find link filter block in regix.py")

# FIX 2: Service messages counted as "deleted" — should be silently skipped
old_service = """                if message.empty or message.service:
                    sts.add('deleted')
                    continue"""

new_service = """                if message.empty or message.service:
                    continue  # silently skip system/service messages, not "deleted" """

if old_service in regix:
    regix = regix.replace(old_service, new_service)
    print("Fixed service-message counting as deleted in regix.py")
else:
    print("WARN: could not find service-msg block in regix.py")

# FIX 3: copy() function in regix.py — bad file_name building causes renamed uploads
# safe_name = f"downloads/{message.id}_{original_name}" → becomes the upload filename
# Fix: download to a tmp folder, pass display_name separately for upload
old_copy_dl = """              if message.media:
                  # Preserve original file name from message; fall back to safe unique name
                  media_obj = getattr(message, message.media.value, None) if message.media else None
                  original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                  
                  if original_name:
                      safe_name = f\"downloads/{message.id}_{original_name}\"
                  elif getattr(message, 'audio', None) or getattr(message, 'voice', None):
                      safe_name = f\"downloads/{message.id}.ogg\"
                  elif getattr(message, 'video', None) or getattr(message, 'video_note', None):
                      safe_name = f\"downloads/{message.id}.mp4\"
                  elif getattr(message, 'photo', None):
                      safe_name = f\"downloads/{message.id}.jpg\"
                  elif getattr(message, 'animation', None):
                      safe_name = f\"downloads/{message.id}.gif\"
                  else:
                      safe_name = f\"downloads/{message.id}\"
                      
                  file_path = await bot.download_media(message, file_name=safe_name)
                  if not file_path: raise Exception(\"DownloadFailed\")
                  
                  kwargs = {
                      \"chat_id\": sts.get(\"TO\"),
                      \"caption\": msg.get(\"caption\"),
                      \"reply_markup\": msg.get(\"button\"),
                      \"protect_content\": msg.get(\"protect\")
                  }
                  
                  if getattr(message, 'photo', None):
                      await upload_queue.put((seq_index, 'send_photo', {\"photo\": file_path, **kwargs}, file_path))
                  elif getattr(message, 'video', None):
                      await upload_queue.put((seq_index, 'send_video', {\"video\": file_path, \"file_name\": original_name or None, **kwargs}, file_path))
                  elif getattr(message, 'document', None):
                      await upload_queue.put((seq_index, 'send_document', {\"document\": file_path, \"file_name\": original_name or None, **kwargs}, file_path))
                  elif getattr(message, 'audio', None):
                      await upload_queue.put((seq_index, 'send_audio', {\"audio\": file_path, \"file_name\": original_name or None, **kwargs}, file_path))
                  elif getattr(message, 'voice', None):
                      await upload_queue.put((seq_index, 'send_voice', {\"voice\": file_path, **kwargs}, file_path))
                  elif getattr(message, 'video_note', None):
                      await upload_queue.put((seq_index, 'send_video_note', {\"video_note\": file_path, **kwargs}, file_path))
                  elif getattr(message, 'animation', None):
                      await upload_queue.put((seq_index, 'send_animation', {\"animation\": file_path, **kwargs}, file_path))
                  elif getattr(message, 'sticker', None):
                      await upload_queue.put((seq_index, 'send_sticker', {\"sticker\": file_path, **kwargs}, file_path))
                  else:
                      # Attempt to just copy message if somehow media type is completely missing.
                      c_kwargs = {\"chat_id\": sts.get(\"TO\"), \"from_chat_id\": sts.get(\"FROM\"), \"message_id\": msg.get(\"msg_id\")}
                      await upload_queue.put((seq_index, 'copy_message', c_kwargs, file_path))"""

new_copy_dl = """              if message.media:
                  # Get the DISPLAY name (what Telegram shows in the UI)
                  media_obj = getattr(message, message.media.value, None) if message.media else None
                  display_name = getattr(media_obj, 'file_name', None) if media_obj else None
                  if display_name:
                      import re as _re3
                      display_name = _re3.sub(r'[\\\\/*?"<>|]', '', display_name).strip() or None
                  # Download to isolated folder — let Pyrogram use whatever name it wants on disk
                  safe_dir = f\"downloads/{message.id}\"
                  os.makedirs(safe_dir, exist_ok=True)
                  file_path = await bot.download_media(message, file_name=safe_dir + \"/\")
                  if not file_path: raise Exception(\"DownloadFailed\")
                  
                  kwargs = {
                      \"chat_id\": sts.get(\"TO\"),
                      \"caption\": msg.get(\"caption\"),
                      \"reply_markup\": msg.get(\"button\"),
                      \"protect_content\": msg.get(\"protect\")
                  }
                  # Pass display_name as file_name= so Telegram shows correct name on upload
                  if getattr(message, 'photo', None):
                      await upload_queue.put((seq_index, 'send_photo', {\"photo\": file_path, **kwargs}, safe_dir))
                  elif getattr(message, 'video', None):
                      await upload_queue.put((seq_index, 'send_video', {\"video\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))
                  elif getattr(message, 'document', None):
                      await upload_queue.put((seq_index, 'send_document', {\"document\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))
                  elif getattr(message, 'audio', None):
                      await upload_queue.put((seq_index, 'send_audio', {\"audio\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))
                  elif getattr(message, 'voice', None):
                      await upload_queue.put((seq_index, 'send_voice', {\"voice\": file_path, **kwargs}, safe_dir))
                  elif getattr(message, 'video_note', None):
                      await upload_queue.put((seq_index, 'send_video_note', {\"video_note\": file_path, **kwargs}, safe_dir))
                  elif getattr(message, 'animation', None):
                      await upload_queue.put((seq_index, 'send_animation', {\"animation\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))
                  elif getattr(message, 'sticker', None):
                      await upload_queue.put((seq_index, 'send_sticker', {\"sticker\": file_path, **kwargs}, safe_dir))
                  else:
                      c_kwargs = {\"chat_id\": sts.get(\"TO\"), \"from_chat_id\": sts.get(\"FROM\"), \"message_id\": msg.get(\"msg_id\")}
                      await upload_queue.put((seq_index, 'copy_message', c_kwargs, safe_dir))"""

if old_copy_dl in regix:
    regix = regix.replace(old_copy_dl, new_copy_dl)
    print("Fixed copy() download filename in regix.py")
else:
    print("WARN: could not find copy() download block in regix.py")

# FIX 4: uploader_worker cleanup — it does os.remove(fpath) but we now pass a dir
# Change file cleanup to shutil.rmtree
old_fpath_rm = """                       if fpath:
                           try:
                               if os.path.exists(fpath): os.remove(fpath)
                           except: pass"""

new_fpath_rm = """                       if fpath:
                           try:
                               import shutil as _shu
                               if os.path.isdir(fpath): _shu.rmtree(fpath, ignore_errors=True)
                               elif os.path.exists(fpath): os.remove(fpath)
                           except: pass"""

count = regix.count(old_fpath_rm)
if count > 0:
    regix = regix.replace(old_fpath_rm, new_fpath_rm)
    print(f"Fixed {count} fpath cleanup(s) in regix.py")
else:
    print("WARN: could not find fpath cleanup in regix.py")

with open('plugins/regix.py', 'w', encoding='utf-8') as f:
    f.write(regix)

# ═══════════════════════════════════════════════════
# PATCH taskjob.py — fix fetch on public channels (username issue)
# ═══════════════════════════════════════════════════
with open('plugins/taskjob.py', 'r', encoding='utf-8') as f:
    taskjob = f.read()

# FIX 5: taskjob fetch — same is_private_src bug causes instant done on public channels
old_tj_fetch = """            is_private_src = not str(fc).startswith('-')
            try:
                if not is_bot or is_private_src:
                    col = []
                    async for msg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_SIZE):
                        if msg.id < current: break
                        col.append(msg)
                    msgs = list(reversed(col))
                else:
                    msgs = await client.get_messages(fc, batch_ids)
                    if not isinstance(msgs, list): msgs = [msgs]
            except FloodWait as fw: await asyncio.sleep(fw.value + 2); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f\"[TaskJob {job_id}] Fetch {current}: {e}\")
                current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue"""

new_tj_fetch = """            try:
                msgs = []
                fetch_ok = False
                # Try get_messages first (works for bots on all public channels)
                try:
                    msgs = await client.get_messages(fc, batch_ids)
                    if not isinstance(msgs, list): msgs = [msgs]
                    fetch_ok = True
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2); continue
                except Exception as ge:
                    logger.warning(f\"[TaskJob {job_id}] get_messages failed @ {current}: {ge}\")
                # Fallback: get_chat_history (for userbots and bot DMs)
                if not fetch_ok:
                    try:
                        col = []
                        async for hmsg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_SIZE):
                            if hmsg.id < current: break
                            col.append(hmsg)
                        msgs = list(reversed(col))
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as he:
                        logger.warning(f\"[TaskJob {job_id}] history fallback failed @ {current}: {he}\")
                if not fetch_ok:
                    current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f\"[TaskJob {job_id}] Fetch outer exception {current}: {e}\")
                current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue"""

if old_tj_fetch in taskjob:
    taskjob = taskjob.replace(old_tj_fetch, new_tj_fetch)
    print("Fixed taskjob.py fetch logic")
else:
    print("WARN: could not find taskjob fetch block")

# FIX 6: taskjob _send_one download fallback — same filename bug
old_tj_dl = """                orig = getattr(mo, 'file_name', None) if mo else None
                if orig:
                    import re
                    orig = re.sub(r'[\\\\/*?:\"<>|]', \"\", orig)
                safe = f\"downloads/{msg.id}_{orig}\" if orig else f\"downloads/{msg.id}\"
                fp = await client.download_media(msg, file_name=safe)
                if not fp: raise Exception(\"DownloadFailed\")
                kw = {\"chat_id\": to_chat, \"caption\": caption if caption is not None else (msg.caption or \"\")}
                if msg.photo:       await client.send_photo(photo=fp, **kw)
                elif msg.video:     await client.send_video(video=fp, file_name=orig, **kw)
                elif msg.document:  await client.send_document(document=fp, file_name=orig, **kw)
                elif msg.audio:     await client.send_audio(audio=fp, file_name=orig, **kw)
                elif msg.voice:     await client.send_voice(voice=fp, **kw)
                elif msg.animation: await client.send_animation(animation=fp, **kw)
                elif msg.sticker:   await client.send_sticker(sticker=fp, **kw)
                if os.path.exists(fp): os.remove(fp)
                return True"""

new_tj_dl = """                # display_name = Telegram UI name (what user sees), may differ from disk name
                display_name = getattr(mo, 'file_name', None) if mo else None
                if display_name:
                    import re as _re4
                    display_name = _re4.sub(r'[\\\\/*?:\"<>|]', '', display_name).strip() or None
                import shutil as _shu2
                safe_dir = f\"downloads/{msg.id}\"
                os.makedirs(safe_dir, exist_ok=True)
                fp = await client.download_media(msg, file_name=safe_dir + \"/\")
                if not fp: raise Exception(\"DownloadFailed\")
                kw = {\"chat_id\": to_chat, \"caption\": caption if caption is not None else (str(msg.caption) if msg.caption else \"\")}
                try:
                    if msg.photo:       await client.send_photo(photo=fp, **kw)
                    elif msg.video:     await client.send_video(video=fp, file_name=display_name, **kw)
                    elif msg.document:  await client.send_document(document=fp, file_name=display_name, **kw)
                    elif msg.audio:     await client.send_audio(audio=fp, file_name=display_name, **kw)
                    elif msg.voice:     await client.send_voice(voice=fp, **kw)
                    elif msg.animation: await client.send_animation(animation=fp, file_name=display_name, **kw)
                    elif msg.sticker:   await client.send_sticker(sticker=fp, **kw)
                finally:
                    _shu2.rmtree(safe_dir, ignore_errors=True)
                return True"""

if old_tj_dl in taskjob:
    taskjob = taskjob.replace(old_tj_dl, new_tj_dl)
    print("Fixed taskjob.py download filename")
else:
    print("WARN: could not find taskjob download block")

with open('plugins/taskjob.py', 'w', encoding='utf-8') as f:
    f.write(taskjob)

print("All patches applied.")
