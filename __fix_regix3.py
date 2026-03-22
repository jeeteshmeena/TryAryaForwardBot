with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Lines 509-565 (0-indexed: 508-564)
# Build the start and end line numbers of the section we need to replace
# Find "# Preserve original file name" line
start_line = None
end_line = None
for i, line in enumerate(lines):
    if '# Preserve original file name from message' in line:
        start_line = i
    if start_line and 'copy_message' in line and "await upload_queue.put" in line and i > start_line + 30:
        end_line = i
        break

print(f"start_line={start_line}, end_line={end_line}")
print("START:", repr(lines[start_line]))
print("END:  ", repr(lines[end_line]))

# The "if message.media:" line is 2 lines before start 
# Actually it's the line before start_line
print("BEFORE:", repr(lines[start_line - 1]))

# Build replacement lines (same indentation as existing lines)
base_indent = "             "   # 13 spaces (for "if message.media:")  
inner_indent = "                 "  # 17 spaces (for content inside if)
inner2_indent = "                     "  # 21 spaces (for upload_queue.put lines)

new_lines = [
    f"{inner_indent}# display_name = what Telegram UI shows (may differ from actual disk filename)\n",
    f"{inner_indent}media_obj = getattr(message, message.media.value, None) if message.media else None\n",
    f"{inner_indent}display_name = getattr(media_obj, 'file_name', None) if media_obj else None\n",
    f"{inner_indent}if display_name:\n",
    f"{inner2_indent}import re as _re3\n",
    f'{inner2_indent}display_name = _re3.sub(r\'[\\\\/*?"<>|]\', \'\', display_name).strip() or None\n',
    f"{inner_indent}# Download to isolated folder - Pyrogram keeps its internal name on disk\n",
    f'{inner_indent}safe_dir = f"downloads/{{message.id}}"\n',
    f"{inner_indent}os.makedirs(safe_dir, exist_ok=True)\n",
    f'{inner_indent}file_path = await bot.download_media(message, file_name=safe_dir + "/")\n',
    f'{inner_indent}if not file_path: raise Exception("DownloadFailed")\n',
    f"{inner_indent}\n",
    f"{inner_indent}kwargs = {{\n",
    f'{inner2_indent}"chat_id": sts.get("TO"),\n',
    f'{inner2_indent}"caption": msg.get("caption"),\n',
    f'{inner2_indent}"reply_markup": msg.get("button"),\n',
    f'{inner2_indent}"protect_content": msg.get("protect")\n',
    f"{inner_indent}}}\n",
    f"{inner_indent}# Pass display_name as file_name= so Telegram shows correct name on upload\n",
    f"{inner_indent}if getattr(message, 'photo', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_photo\', {{"photo": file_path, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'video', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_video\', {{"video": file_path, "file_name": display_name, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'document', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_document\', {{"document": file_path, "file_name": display_name, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'audio', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_audio\', {{"audio": file_path, "file_name": display_name, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'voice', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_voice\', {{"voice": file_path, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'video_note', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_video_note\', {{"video_note": file_path, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'animation', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_animation\', {{"animation": file_path, "file_name": display_name, **kwargs}}, safe_dir))\n',
    f"{inner_indent}elif getattr(message, 'sticker', None):\n",
    f'{inner2_indent}await upload_queue.put((seq_index, \'send_sticker\', {{"sticker": file_path, **kwargs}}, safe_dir))\n',
    f"{inner_indent}else:\n",
    f'{inner2_indent}c_kwargs = {{"chat_id": sts.get("TO"), "from_chat_id": sts.get("FROM"), "message_id": msg.get("msg_id")}}\n',
    f'{inner2_indent}await upload_queue.put((seq_index, \'copy_message\', c_kwargs, safe_dir))\n',
]

# Replace lines from start_line to end_line (inclusive)
lines = lines[:start_line] + new_lines + lines[end_line + 1:]

# Fix 2: fpath cleanup — replace os.remove(fpath) with smart dir/file cleanup
for i, line in enumerate(lines):
    if 'os.path.exists(fpath): os.remove(fpath)' in line:
        ind = len(line) - len(line.lstrip())
        sp = ' ' * ind
        lines[i] = (
            f"{sp}import shutil as _shu\n"
            f"{sp}if os.path.isdir(fpath): _shu.rmtree(fpath, ignore_errors=True)\n"
            f"{sp}elif os.path.exists(fpath): os.remove(fpath)\n"
        )
        print(f"Fixed fpath cleanup at line {i}")

with open('plugins/regix.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Done.")
