with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: copy() download — replace the bad safe_name logic with folder-based download
old_block = (
    "             if message.media:\n"
    "                  # Preserve original file name from message; fall back to safe unique name\n"
    "                  media_obj = getattr(message, message.media.value, None) if message.media else None\n"
    "                  original_name = getattr(media_obj, 'file_name', None) if media_obj else None\n"
    "                  \n"
    "                  if original_name:\n"
    "                      safe_name = f\"downloads/{message.id}_{original_name}\"\n"
    "                  elif getattr(message, 'audio', None) or getattr(message, 'voice', None):\n"
    "                      safe_name = f\"downloads/{message.id}.ogg\"\n"
    "                  elif getattr(message, 'video', None) or getattr(message, 'video_note', None):\n"
    "                      safe_name = f\"downloads/{message.id}.mp4\"\n"
    "                  elif getattr(message, 'photo', None):\n"
    "                      safe_name = f\"downloads/{message.id}.jpg\"\n"
    "                  elif getattr(message, 'animation', None):\n"
    "                      safe_name = f\"downloads/{message.id}.gif\"\n"
    "                  else:\n"
    "                      safe_name = f\"downloads/{message.id}\"\n"
    "                      \n"
    "                  file_path = await bot.download_media(message, file_name=safe_name)\n"
    "                  if not file_path: raise Exception(\"DownloadFailed\")\n"
    "                  \n"
    "                  kwargs = {\n"
    "                      \"chat_id\": sts.get(\"TO\"),\n"
    "                      \"caption\": msg.get(\"caption\"),\n"
    "                      \"reply_markup\": msg.get(\"button\"),\n"
    "                      \"protect_content\": msg.get(\"protect\")\n"
    "                  }\n"
    "                  \n"
    "                  if getattr(message, 'photo', None):\n"
    "                      await upload_queue.put((seq_index, 'send_photo', {\"photo\": file_path, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'video', None):\n"
    "                      await upload_queue.put((seq_index, 'send_video', {\"video\": file_path, \"file_name\": original_name or None, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'document', None):\n"
    "                      await upload_queue.put((seq_index, 'send_document', {\"document\": file_path, \"file_name\": original_name or None, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'audio', None):\n"
    "                      await upload_queue.put((seq_index, 'send_audio', {\"audio\": file_path, \"file_name\": original_name or None, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'voice', None):\n"
    "                      await upload_queue.put((seq_index, 'send_voice', {\"voice\": file_path, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'video_note', None):\n"
    "                      await upload_queue.put((seq_index, 'send_video_note', {\"video_note\": file_path, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'animation', None):\n"
    "                      await upload_queue.put((seq_index, 'send_animation', {\"animation\": file_path, **kwargs}, file_path))\n"
    "                  elif getattr(message, 'sticker', None):\n"
    "                      await upload_queue.put((seq_index, 'send_sticker', {\"sticker\": file_path, **kwargs}, file_path))\n"
    "                  else:\n"
    "                      # Attempt to just copy message if somehow media type is completely missing.\n"
    "                      c_kwargs = {\"chat_id\": sts.get(\"TO\"), \"from_chat_id\": sts.get(\"FROM\"), \"message_id\": msg.get(\"msg_id\")}\n"
    "                      await upload_queue.put((seq_index, 'copy_message', c_kwargs, file_path))"
)

new_block = (
    "             if message.media:\n"
    "                  # display_name = what Telegram UI shows (may differ from actual disk filename)\n"
    "                  media_obj = getattr(message, message.media.value, None) if message.media else None\n"
    "                  display_name = getattr(media_obj, 'file_name', None) if media_obj else None\n"
    "                  if display_name:\n"
    "                      import re as _re3\n"
    "                      display_name = _re3.sub(r'[\\\\/*?\"<>|]', '', display_name).strip() or None\n"
    "                  # Download to isolated folder - Pyrogram keeps its internal name on disk\n"
    "                  safe_dir = f\"downloads/{message.id}\"\n"
    "                  os.makedirs(safe_dir, exist_ok=True)\n"
    "                  file_path = await bot.download_media(message, file_name=safe_dir + \"/\")\n"
    "                  if not file_path: raise Exception(\"DownloadFailed\")\n"
    "                  \n"
    "                  kwargs = {\n"
    "                      \"chat_id\": sts.get(\"TO\"),\n"
    "                      \"caption\": msg.get(\"caption\"),\n"
    "                      \"reply_markup\": msg.get(\"button\"),\n"
    "                      \"protect_content\": msg.get(\"protect\")\n"
    "                  }\n"
    "                  # Pass display_name as file_name= so Telegram shows the correct name on upload\n"
    "                  if getattr(message, 'photo', None):\n"
    "                      await upload_queue.put((seq_index, 'send_photo', {\"photo\": file_path, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'video', None):\n"
    "                      await upload_queue.put((seq_index, 'send_video', {\"video\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'document', None):\n"
    "                      await upload_queue.put((seq_index, 'send_document', {\"document\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'audio', None):\n"
    "                      await upload_queue.put((seq_index, 'send_audio', {\"audio\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'voice', None):\n"
    "                      await upload_queue.put((seq_index, 'send_voice', {\"voice\": file_path, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'video_note', None):\n"
    "                      await upload_queue.put((seq_index, 'send_video_note', {\"video_note\": file_path, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'animation', None):\n"
    "                      await upload_queue.put((seq_index, 'send_animation', {\"animation\": file_path, \"file_name\": display_name, **kwargs}, safe_dir))\n"
    "                  elif getattr(message, 'sticker', None):\n"
    "                      await upload_queue.put((seq_index, 'send_sticker', {\"sticker\": file_path, **kwargs}, safe_dir))\n"
    "                  else:\n"
    "                      c_kwargs = {\"chat_id\": sts.get(\"TO\"), \"from_chat_id\": sts.get(\"FROM\"), \"message_id\": msg.get(\"msg_id\")}\n"
    "                      await upload_queue.put((seq_index, 'copy_message', c_kwargs, safe_dir))"
)

if old_block in content:
    content = content.replace(old_block, new_block)
    print("Fixed copy() download block")
else:
    print(f"WARN: old_block not found (len={len(old_block)})")
    # Try to debug
    idx = content.find("safe_name = f\"downloads/{message.id}_{original_name}\"")
    print(f"  Found 'safe_name' at char {idx}")
    idx2 = content.find("# Preserve original file name from message")
    print(f"  Found comment at char {idx2}")

# Fix 2: uploader cleanup — replace os.remove(fpath) with smart dir/file remover
old_cleanup = (
    "                       if fpath:\n"
    "                           try:\n"
    "                               if os.path.exists(fpath): os.remove(fpath)\n"
    "                           except: pass"
)
new_cleanup = (
    "                       if fpath:\n"
    "                           try:\n"
    "                               import shutil as _shu\n"
    "                               if os.path.isdir(fpath): _shu.rmtree(fpath, ignore_errors=True)\n"
    "                               elif os.path.exists(fpath): os.remove(fpath)\n"
    "                           except: pass"
)
cnt = content.count(old_cleanup)
if cnt > 0:
    content = content.replace(old_cleanup, new_cleanup)
    print(f"Fixed {cnt} fpath cleanup instance(s)")
else:
    print("WARN: fpath cleanup not found")
    idx = content.find("os.path.exists(fpath): os.remove(fpath)")
    print(f"  found 'os.remove(fpath)' at index {idx}")
    # Print context
    print(repr(content[max(0,idx-100):idx+100]))

with open('plugins/regix.py', 'w', encoding='utf-8') as f:
    f.write(content)
