"""
Audio Cleaner & Renamer - v2
============================
Downloads audio files strictly in order, cleans them with FFmpeg (noise reduction,
metadata stripping), applies fresh metadata, renames them using sequential numbers,
and uploads them to the destination.

Fixed in v2:
  - Client init now correctly uses start_clone_bot
  - Cover download uses the correct client instance
  - Metadata steps are individual prompts (not pipe format)
  - upload uses the same client that downloaded
"""
import os
import re
import time
import uuid
import asyncio
import logging
import shutil
import subprocess
import datetime
from database import db
from .test import CLIENT, start_clone_bot
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove,
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()
COLL = "cleaner_jobs"

_cl_tasks: dict[str, asyncio.Task] = {}
_cl_paused: dict[str, asyncio.Event] = {}
_cl_waiter: dict[int, asyncio.Future] = {}
_cl_bot_ref: dict[str, object] = {}   # job_id -> bot instance for notifications
MAX_CONCURRENT = 1
_cl_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# ─── DB Helpers ──────────────────────────────────────────────────────────────
async def _cl_save_job(job: dict):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)

async def _cl_get_job(jid: str):
    return await db.db[COLL].find_one({"job_id": jid})

async def _cl_get_all_jobs(uid: int):
    return [j async for j in db.db[COLL].find({"user_id": uid})]

async def _cl_delete_job(jid: str):
    await db.db[COLL].delete_one({"job_id": jid})

async def _cl_update_job(jid: str, kw: dict):
    await db.db[COLL].update_one({"job_id": jid}, {"$set": kw})

async def _cl_get_defaults(uid: int) -> dict:
    u = await db.db.users.find_one({"id": uid})
    return u.get("cleaner_defaults", {}) if u else {}

async def _cl_save_default(uid: int, key: str, val):
    await db.db.users.update_one({"id": uid}, {"$set": {f"cleaner_defaults.{key}": val}}, upsert=True)


# ─── Ask Flow ────────────────────────────────────────────────────────────────
@Client.on_message(filters.private, group=-16)
async def _cl_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _cl_waiter:
        fut = _cl_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation

async def _cl_ask(bot, user_id, text, reply_markup=None, timeout=300):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    old = _cl_waiter.pop(user_id, None)
    if old and not old.done(): old.cancel()
    _cl_waiter[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _cl_waiter.pop(user_id, None)
        raise

def _parse_link(text):
    text = text.strip().rstrip('/')
    if text.isdigit(): return None, int(text)
    m = re.search(r'https?://t\.me/c/(\d+)(?:/\d+)?/(\d+)', text)
    if m: return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.search(r'https?://t\.me/([^/]+)(?:/\d+)?/(\d+)', text)
    if m: return m.group(1), int(m.group(2))
    return None, None

def _ist_now() -> datetime.datetime:
    return datetime.datetime.now(IST_OFFSET)


def _sz(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b/1024:.1f} KB"
    if b < 1073741824: return f"{b/1048576:.1f} MB"
    return f"{b/1073741824:.2f} GB"

def _tm(s):
    s = max(0, int(s))
    if s < 60: return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"

# ─── Info Text Builder ───────────────────────────────────────────────────────
def _build_cl_info(job: dict) -> str:
    status = job.get("status", "stopped")
    name = job.get("base_name", "Cleaner")
    done = job.get("files_done", 0)
    total = max(job.get("total_files", 1), 1)
    err = job.get("error", "")
    start_num = job.get("starting_number", 1)

    pct = int((done / total) * 100) if total else 0
    filled = int(18 * pct / 100)
    bar = f"[{'█' * filled}{'░' * (18 - filled)}] {pct}%"

    ic = {"running":"🔄","paused":"⏸","completed":"✅","failed":"⚠️","stopped":"🔴","queued":"⏳"}.get(status, "❔")
    
    # ETA
    eta_str = ""
    start_ts = job.get("phase_start_ts", 0) or 0
    if status == "running" and done > 0 and start_ts > 0:
        elapsed = time.time() - start_ts
        rate = elapsed / done
        remaining = rate * (total - done)
        eta_str = f"\n  ⏱ <b>ETA:</b> ~{_tm(remaining)}"
    
    lines = [
        f"{ic} <b>🧹 {name}</b>  [{job.get('job_id')[-6:]}]",
        f"  Status: <b>{status.title()}</b>",
        f"  <code>{bar}</code>",
        "",
        f"  📁 <b>Processed:</b> {done}/{total} files",
        f"  🔢 <b>Range:</b> {name} {start_num} → {name} {start_num + total - 1}",
        f"  🎨 <b>Artist:</b> {job.get('artist', '—')}",
        f"  💿 <b>Album:</b> {job.get('album', '—')}",
        f"  🗓 <b>Year:</b> {job.get('year', '—')}",
        f"  🖼 <b>Cover:</b> {'✅ Set' if job.get('cover_file_id') else '—'}",
        f"  🎯 <b>Target:</b> {job.get('target_title', '?')}",
    ]
    if eta_str: lines.append(eta_str)
    if err:
        lines.append(f"\n  ⚠️ <b>Error:</b> <code>{err[:200]}</code>")
    
    lines.append(f"\n  <i>Last refreshed: {_ist_now().strftime('%I:%M %p IST')}</i>")
    return "\n".join(lines)


# ─── FFmpeg Engine ───────────────────────────────────────────────────────────
async def _process_audio_ffmpeg(input_path, output_path, cover_path, meta: dict):
    """
    Runs FFmpeg to clean audio:
    - -af afftdn=nf=-25 (Noise reduction)
    - Re-encodes to libmp3lame 128k
    - Strips all original metadata (-map_metadata -1)
    - Adds new Artist, Title, Year.
    """
    cmd = ["ffmpeg", "-y", "-hide_banner"]
    cmd += ["-i", input_path]
    
    if cover_path and os.path.exists(cover_path):
        cmd += ["-i", cover_path]
        cmd += ["-map", "0:a:0", "-map", "1:v:0"]
        cmd += ["-c:v", "mjpeg", "-id3v2_version", "3"]
        cmd += ["-metadata:s:v", "title=Album cover", "-metadata:s:v", "comment=Cover (front)"]
    else:
        cmd += ["-map", "0:a:0"]

    cmd += ["-af", "afftdn=nf=-25"]
    cmd += ["-c:a", "libmp3lame", "-b:a", "128k"]
    cmd += ["-map_metadata", "-1"]

    for k, v in meta.items():
        if v: cmd += ["-metadata", f"{k}={v}"]

    cmd.append(output_path)

    loop = asyncio.get_event_loop()
    def _sync_run():
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300)
            return result.returncode, result.stderr.decode('utf-8', errors='replace')
        except Exception as e:
            return -1, str(e)
    
    try:
        rc, stderr = await loop.run_in_executor(None, _sync_run)
        if rc != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
            return False, stderr[-1000:]
        return True, ""
    except Exception as e:
        return False, str(e)


# ─── Job Runner ──────────────────────────────────────────────────────────────
async def _cl_run_job(job_id: str, bot=None):
    """Main cleaner job coroutine. bot = the main Pyrogram bot client for DM notifications."""
    async with _cl_semaphore:
        while True:
            ev = _cl_paused.get(job_id)
            if ev and not ev.is_set():
                await ev.wait()

            job = await _cl_get_job(job_id)
            if not job or job.get("status") in ("completed", "failed", "stopped"):
                return

            await _cl_update_job(job_id, {"status": "running", "error": "", "phase_start_ts": time.time()})
            uid = job["user_id"]
            
            # Recover bot ref if not passed (e.g. after resume)
            if bot is None:
                bot = _cl_bot_ref.get(job_id)
            else:
                _cl_bot_ref[job_id] = bot
            
            # ── Init correct download client ──
            acc_id = job.get("account_id")
            client = None
            try:
                acc = await db.get_bot(uid, acc_id)
                if not acc:
                    await _cl_update_job(job_id, {"status": "failed", "error": "Account not found in DB"})
                    if bot:
                        try: await bot.send_message(uid, "⚠️ <b>Cleaner failed:</b> Account not found in DB.")
                        except: pass
                    return
                pyrogram_client = _CLIENT.client(acc)
                try:
                    client = await start_clone_bot(pyrogram_client)
                except Exception as start_err:
                    # If already started, try to use as-is
                    if "already" in str(start_err).lower() or "connected" in str(start_err).lower():
                        client = pyrogram_client
                    else:
                        raise
            except Exception as e:
                err_msg = f"Client init failed: {e}"
                await _cl_update_job(job_id, {"status": "failed", "error": err_msg})
                if bot:
                    try: await bot.send_message(uid, f"⚠️ <b>Cleaner failed:</b> <code>{err_msg[:300]}</code>")
                    except: pass
                return

            # Setup
            from_ch = job["from_chat"]
            dest_ch = job["dest_chat"]
            sid = job["start_id"]
            eid = job["end_id"]
            done = job.get("files_done", 0)
            
            base_name = job.get("base_name", "Cleaned")
            art = job.get("artist", "")
            yr  = str(job.get("year", "") or "")
            alb = job.get("album", "") or art
            gen = job.get("genre", "")
            cov_fid = job.get("cover_file_id", "")
            curr_num = job.get("starting_number", 1) + done

            # ── Download cover image using the MAIN BOT (file_id is scoped to main bot token) ──
            # NEVER use the clone client here — file_ids are bot-specific.
            local_cover = os.path.abspath(f"temp_cover_{job_id}.jpg")
            if cov_fid and not os.path.exists(local_cover):
                dl_client = bot if bot else client   # main bot preferred
                try:
                    dl = await dl_client.download_media(cov_fid, file_name=local_cover)
                    # Validate: must actually exist AND be a real image (>1KB)
                    if not dl or not os.path.exists(local_cover) or os.path.getsize(local_cover) < 1024:
                        logger.warning(f"[Cleaner {job_id}] Cover download produced invalid file, disabling cover.")
                        try:
                            if os.path.exists(local_cover): os.remove(local_cover)
                        except: pass
                        local_cover = None
                except Exception as e:
                    logger.warning(f"[Cleaner {job_id}] Cover dl fail: {e}")
                    local_cover = None
            elif not cov_fid:
                local_cover = None

            fail_count = 0
            phase_start = time.time()
            job_failed = False  # flag to break outer while True loop on fatal errors

            def _extract_ep_label(fname: str) -> str:
                """
                Extract an episode number or range from a filename for output naming.
                - 'Shadow 388-389.mp3'   -> '388-389'
                - 'Shadow 567 to 677.mp3'-> '567 to 677'
                - 'Shadow 86 (1).mp3'    -> '86'
                - 'Shadow 201.mp3'       -> '201'
                - 'Shadow.mp3'           -> '' (no episode found)
                """
                import re as _re
                base = _re.sub(r'\.\w{2,4}$', '', fname)        # strip extension
                base = _re.sub(r'\s*\(\d+\)\s*$', '', base).strip()  # strip (1),(2) copy markers
                # Range: '388-389' or '567 to 677'
                m = _re.search(r'\b(\d{1,4})\s*(?:-|to)\s*(\d{1,4})\b', base, _re.IGNORECASE)
                if m:
                    a, b = int(m.group(1)), int(m.group(2))
                    if 0 < a < 5000 and a <= b < 5000:
                        sep = m.group(0)[len(m.group(1)):-len(m.group(2))].strip()
                        return f"{a} {sep} {b}" if 'to' in sep.lower() else f"{a}-{b}"
                # Single episode number (not a year)
                nums = [int(x) for x in _re.findall(r'\b(\d{1,4})\b', base)
                        if 0 < int(x) < 5000 and not (1900 <= int(x) <= 2100)]
                if nums:
                    return str(nums[-1])
                return ''  # no episode found — fall back to sequential

            # Loop through all message IDs
            for msg_id in range(sid + done, eid + 1):
                ev = _cl_paused.get(job_id)
                if ev and not ev.is_set():
                    break  # pause triggered

                job = await _cl_get_job(job_id)
                if job.get("status") == "stopped":
                    break

                try:
                    msg = await client.get_messages(from_ch, msg_id)
                    if not msg or msg.empty or not (msg.audio or msg.voice or msg.document):
                        continue

                    # ── Determine output title: preserve original episode/range if present ──
                    media_obj = msg.audio or msg.voice or msg.document
                    orig_fn = getattr(media_obj, 'file_name', None) or ""
                    ep_label = _extract_ep_label(orig_fn) if orig_fn else ''
                    if ep_label:
                        clean_title = f"{base_name} {ep_label}"
                        # Do NOT increment curr_num when using extracted label
                    else:
                        clean_title = f"{base_name} {curr_num}"
                        curr_num += 1   # Only advance sequential counter for non-labeled files

                    clean_file = f"{clean_title}.mp3"
                    
                    meta = {
                        "title":  clean_title,
                        "artist": art,
                        "album":  alb or art,
                        "year":   yr,
                        "genre":  gen,
                    }

                    # Download
                    in_path  = os.path.abspath(f"temp_cl_in_{job_id}_{msg_id}.tmp")
                    out_path = os.path.abspath(f"temp_cl_out_{job_id}_{msg_id}.mp3")
                    
                    dl_path = await client.download_media(msg, file_name=in_path)
                    if not dl_path or not os.path.exists(str(dl_path)):
                        continue

                    # Process with FFmpeg
                    ok, err = await _process_audio_ffmpeg(str(dl_path), out_path, local_cover, meta)
                    
                    try: os.remove(str(dl_path))
                    except: pass

                    if not ok:
                        raise Exception(f"FFmpeg Edit Failed: {err[:500]}")

                    # Upload using the same client
                    thumb = local_cover if (local_cover and os.path.exists(local_cover)) else None
                    for attempt in range(5):
                        try:
                            await client.send_audio(
                                chat_id=dest_ch,
                                audio=out_path,
                                caption=f"**{clean_title}**",
                                title=clean_title,
                                performer=art,
                                file_name=clean_file,
                                thumb=thumb,
                            )
                            break
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2)
                        except Exception as ue:
                            if attempt >= 4:
                                raise Exception(f"Upload failed: {ue}")
                            await asyncio.sleep(3)

                    try: os.remove(out_path)
                    except: pass

                    # ── Only advance curr_num if we used sequential numbering ──
                    if not ep_label:
                        pass  # curr_num already incremented above
                    done += 1
                    fail_count = 0
                    await _cl_update_job(job_id, {"files_done": done})
                    await asyncio.sleep(0.5)

                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2)
                    continue  # retry same msg
                except Exception as e:
                    fail_count += 1
                    logger.error(f"[Cleaner {job_id}] Error at msg {msg_id}: {e}")
                    if fail_count > 3:
                        err_msg = f"Failed {fail_count}x at msg {msg_id}: {str(e)[:200]}"
                        await _cl_update_job(job_id, {"status": "failed", "error": err_msg})
                        if bot:
                            try: await bot.send_message(uid, f"⚠️ <b>Cleaner job failed:</b>\n<code>{err_msg[:400]}</code>")
                            except: pass
                        job_failed = True
                        break   # break inner for-loop
                    await asyncio.sleep(5)
            
            # ── End of loop logic ──
            if job_failed:
                # Fatal failure already logged above, break outer loop
                break
            
            job = await _cl_get_job(job_id)
            if job.get("status") == "failed":
                pass  # already logged
            elif job.get("status") == "stopped":
                pass
            elif _cl_paused.get(job_id) and not _cl_paused[job_id].is_set():
                await _cl_update_job(job_id, {"status": "paused"})
            else:
                # Finished entirely
                await _cl_update_job(job_id, {"status": "completed", "error": ""})
                if bot:
                    try:
                        await bot.send_message(
                            uid,
                            f"<b>🎉 Cleaner Job Completed!</b>\n\n"
                            f"<b>🧹 Job Name:</b> {base_name}\n"
                            f"<b>📄 Files Cleaned & Renamed:</b> {done} / {job.get('total_files', 0)}\n"
                            f"<b>🎯 Range Covered:</b> <code>{base_name} {job.get('starting_number')}</code> ➠ <code>{base_name} {curr_num - 1}</code>\n\n"
                            f"<i>All files scrubbed, re-encoded (128kbps), metadata sanitized, and uploaded.</i>"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to send cleaner report: {e}")
            
            # Cleanup
            try:
                if local_cover and os.path.exists(local_cover): os.remove(local_cover)
            except: pass
            _cl_bot_ref.pop(job_id, None)
            break


# ─── UI Callback Handlers ────────────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^cl#(main|new|view|pause|resume|stop|del|cfg)"))
async def _cl_callbacks(bot, update: CallbackQuery):
    uid = update.from_user.id
    data = update.data.split("#")
    action = data[1]

    if action == "main":
        jobs = await _cl_get_all_jobs(uid)
        active = [j for j in jobs if j.get("status") not in ("completed", "stopped", "failed")]
        kb = [[InlineKeyboardButton("➕ Sᴛᴀʀᴛ Nᴇᴡ Cʟᴇᴀɴᴇʀ Jᴏʙ", callback_data="cl#new")]]
        
        kb.append([
            InlineKeyboardButton("⚙️ Sᴇᴛ Cᴏᴠᴇʀ", callback_data="cl#cfg#cover"),
            InlineKeyboardButton("⚙️ Sᴇᴛ Aʀᴛɪsᴛ", callback_data="cl#cfg#artist"),
        ])
        
        row = []
        for i, j in enumerate(active):
            name = j.get('base_name', 'Job')[:12]
            row.append(InlineKeyboardButton(f"🧹 {name}", callback_data=f"cl#view#{j['job_id']}"))
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="settings#main")])
        
        df = await _cl_get_defaults(uid)
        txt = (
            "<b><u>🧹 Aᴜᴅɪᴏ Cʟᴇᴀɴᴇʀ & Rᴇɴᴀᴍᴇʀ</u></b>\n\n"
            "This system strips background noise, cleans corrupted metadata, "
            "forces 128kbps standard formats, and strictly renames sequential files.\n\n"
            f"<b>Global Defaults:</b>\n"
            f"  • Artist: {df.get('artist', '<i>None</i>')}\n"
            f"  • Cover Art: {'<i>Saved</i> ✅' if df.get('cover') else '<i>None</i>'}\n"
        )
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

    elif action == "cfg":
        cfg_type = data[2]
        ask_txt = f"Send the new default <b>{cfg_type.title()}</b>" + (" (send a Photo/image for cover art)" if cfg_type == "cover" else "") + "\n<i>Send /skip to clear.</i>"
        try:
            resp = await _cl_ask(bot, uid, ask_txt, timeout=120)
            if not resp: raise asyncio.TimeoutError
            txt = (resp.text or "").strip()
            
            if txt.lower() == "/skip":
                await _cl_save_default(uid, cfg_type, "")
            else:
                if cfg_type == "cover" and (resp.photo or resp.document):
                    fid = (resp.photo or resp.document).file_id
                    await _cl_save_default(uid, cfg_type, fid)
                else:
                    await _cl_save_default(uid, cfg_type, txt)
            
            try: await resp.delete()
            except: pass
            await bot.send_message(uid, f"✅ Default <b>{cfg_type}</b> updated!")
            
        except asyncio.TimeoutError:
            await bot.send_message(uid, "<i>Timed out.</i>")
            
        # Re-render main
        update.data = "cl#main"
        return await _cl_callbacks(bot, update)

    elif action == "new":
        from pyrogram.types import ReplyKeyboardRemove
        try:
            await update.message.delete()
        except:
            pass
        asyncio.create_task(_create_cl_flow(bot, uid))
        return True

    elif action == "view":
        jid = data[2]
        job = await _cl_get_job(jid)
        if not job: return await update.answer("Job not found.", show_alert=True)
        
        st = job.get("status")
        kb = []
        if st in ("running", "queued"):
            kb.append([
                InlineKeyboardButton("⏸ Pᴀᴜsᴇ", callback_data=f"cl#pause#{jid}"),
                InlineKeyboardButton("⏹ Sᴛᴏᴘ", callback_data=f"cl#stop#{jid}")
            ])
        elif st == "paused":
            kb.append([
                InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ", callback_data=f"cl#resume#{jid}"),
                InlineKeyboardButton("⏹ Sᴛᴏᴘ", callback_data=f"cl#stop#{jid}")
            ])
        
        kb.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ", callback_data=f"cl#view#{jid}")])
        if st in ("completed", "stopped", "failed"):
            kb.append([InlineKeyboardButton("🗑 Dᴇʟᴇᴛᴇ Rᴇᴄᴏʀᴅ", callback_data=f"cl#del#{jid}")])
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="cl#main")])
        
        try: await update.message.edit_text(_build_cl_info(job), reply_markup=InlineKeyboardMarkup(kb))
        except: pass

    elif action == "pause":
        jid = data[2]
        if jid in _cl_paused: _cl_paused[jid].clear()
        await _cl_update_job(jid, {"status": "paused"})
        update.data = f"cl#view#{jid}"
        return await _cl_callbacks(bot, update)

    elif action == "resume":
        jid = data[2]
        await _cl_update_job(jid, {"status": "queued"})
        if jid not in _cl_paused: _cl_paused[jid] = asyncio.Event()
        _cl_paused[jid].set()
        if jid not in _cl_tasks or _cl_tasks[jid].done():
            _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid, bot))
        update.data = f"cl#view#{jid}"
        return await _cl_callbacks(bot, update)

    elif action == "stop":
        jid = data[2]
        await _cl_update_job(jid, {"status": "stopped"})
        if jid in _cl_paused: _cl_paused[jid].set()
        if jid in _cl_tasks and not _cl_tasks[jid].done():
            _cl_tasks[jid].cancel()
        update.data = f"cl#view#{jid}"
        return await _cl_callbacks(bot, update)

    elif action == "del":
        jid = data[2]
        await _cl_delete_job(jid)
        update.data = "cl#main"
        return await _cl_callbacks(bot, update)


async def _create_cl_flow(bot, user_id):
    old = _cl_waiter.pop(user_id, None)
    if old and not old.done(): old.cancel()

    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    SKIP_BTN   = KeyboardButton("⏭ Sᴋɪᴘ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")
    markup_b   = ReplyKeyboardMarkup([[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    markup_s   = ReplyKeyboardMarkup([[SKIP_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)

    def _cancel(txt): return txt.strip().startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt
    def _skip(txt):   return "⏭" in txt or "Sᴋɪᴘ" in txt or txt.strip().lower() == "/skip"
    def _undo(txt):   return "↩️" in txt or "Uɴᴅᴏ" in txt

    # ── Step 1: Account ───────────────────────────────────────────
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id, "<b>❌ No accounts found. Add one in /settings.</b>")

    def _acc_label(a):
        kind = "Bot" if a.get("is_bot", True) else "Userbot"
        name = a.get("username") or a.get("name", "Unknown")
        return f"{kind}: {name} [{a['id']}]"

    acc_btns = [[KeyboardButton(_acc_label(a))] for a in accounts]
    acc_btns.append([CANCEL_BTN])
    
    r_acc = await _cl_ask(bot, user_id,
        "<b>🧹 Create Cleaner Job — Step 1/8</b>\n\nChoose the <b>account</b> to read from:",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))
    if _cancel(r_acc.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in (r_acc.text or "") and "]" in (r_acc.text or ""):
        try: acc_id = int(r_acc.text.split('[')[-1].split(']')[0])
        except: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
    
    # ── Step 2: Start link ───────────────────────────────────────
    r_start = await _cl_ask(bot, user_id,
        "<b>»  Step 2/8</b>\n\nSend the <b>Start Message Link</b> (first file):",
        reply_markup=ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))
    if _cancel(r_start.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    from_chat, sid = _parse_link(r_start.text or "")

    # ── Step 3: End link ─────────────────────────────────────────
    r_end = await _cl_ask(bot, user_id,
        "<b>»  Step 3/8</b>\n\nSend the <b>End Message Link</b> (last file):",
        reply_markup=markup_b)
    if _cancel(r_end.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    _, eid = _parse_link(r_end.text or "")
    if sid and eid and sid > eid: sid, eid = eid, sid

    # ── Step 4: Destination ──────────────────────────────────────
    channels = await db.get_user_channels(user_id)
    dest_chat = None
    ch = None
    if channels:
        ch_kb = [[KeyboardButton(f"📢 {ch['title']}")] for ch in channels]
        ch_kb.append([KeyboardButton("⏭ Skip (Send to DM)")])
        ch_kb.append([CANCEL_BTN])
        r_dest = await _cl_ask(bot, user_id,
            "<b>»  Step 4/8</b>\n\nSelect <b>destination channel</b> for cleaned files:",
            reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
        if _cancel(r_dest.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        
        if "Skip" not in (r_dest.text or ""):
            title = (r_dest.text or "").replace("📢 ", "").strip()
            ch = next((c for c in channels if c["title"] == title), None)
            if ch: dest_chat = int(ch["chat_id"])
    if not dest_chat:
        dest_chat = user_id

    # ── Step 5: Base Name ────────────────────────────────────────
    r_base = await _cl_ask(bot, user_id,
        "<b>»  Step 5/8</b>\n\nSend the <b>Base Name</b> for the files.\n"
        "<i>Example: Send <code>Saaya</code> → outputs <code>Saaya 1.mp3</code>, <code>Saaya 2.mp3</code>...</i>",
        reply_markup=markup_b)
    if _cancel(r_base.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    base_name = re.sub(r'[<>:"/\\|?*]', '_', (r_base.text or "Cleaned").strip())

    # ── Step 6: Starting Number ──────────────────────────────────
    r_num = await _cl_ask(bot, user_id,
        "<b>»  Step 6/8</b>\n\nSend the <b>Starting Number</b>.\n"
        "<i>Example: Send <code>1</code> for Saaya 1, or <code>201</code> for Saaya 201...</i>",
        reply_markup=markup_b)
    if _cancel(r_num.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    start_num = int((r_num.text or "1").strip()) if (r_num.text or "").strip().isdigit() else 1

    # ── Step 7: Metadata (individual prompts) ────────────────────
    df = await _cl_get_defaults(user_id)
    adv_artist = df.get("artist", "")
    adv_year   = df.get("year", "")
    adv_album  = df.get("album", "")
    adv_genre  = df.get("genre", "")
    adv_cover  = df.get("cover", "")

    r_art = await _cl_ask(bot, user_id,
        f"<b>»  Step 7a/8 — Artist Name</b>\n\n"
        f"Enter the <b>Artist</b> name.\n"
        f"<i>Default: {adv_artist or 'None'}. Skip to keep.</i>",
        reply_markup=markup_s)
    if _cancel(r_art.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if not _skip(r_art.text or ""): adv_artist = (r_art.text or "").strip()

    r_alb = await _cl_ask(bot, user_id,
        f"<b>»  Step 7b/8 — Album Name</b>\n\n"
        f"Enter the <b>Album</b> name.\n"
        f"<i>Default: Story name / artist. Skip to use Artist name.</i>",
        reply_markup=markup_s)
    if _cancel(r_alb.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if not _skip(r_alb.text or ""): adv_album = (r_alb.text or "").strip()
    if not adv_album: adv_album = adv_artist

    r_yr = await _cl_ask(bot, user_id,
        f"<b>»  Step 7c/8 — Year</b>\n\n"
        f"Enter the <b>Release Year</b> (e.g. <code>2024</code>).\n"
        f"<i>Default: {adv_year or 'None'}. Skip to leave empty.</i>",
        reply_markup=markup_s)
    if _cancel(r_yr.text or ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if not _skip(r_yr.text or ""): adv_year = (r_yr.text or "").strip()

    # ── Step 8: Cover Image ──────────────────────────────────────
    r_cov = await _cl_ask(bot, user_id,
        f"<b>»  Step 8/8 — Cover Image</b>\n\n"
        f"Send a <b>photo/image</b> to use as the album cover art for all files.\n"
        f"<i>{'Current default cover is set. ' if adv_cover else ''}Skip to {'keep existing' if adv_cover else 'use no cover'}.</i>",
        reply_markup=markup_s,
        timeout=300)
    if _cancel((r_cov.text or "") if r_cov else ""): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    
    if r_cov and not _skip(r_cov.text or ""):
        if r_cov.photo:
            adv_cover = r_cov.photo.file_id
        elif r_cov.document and 'image' in (r_cov.document.mime_type or ''):
            adv_cover = r_cov.document.file_id

    # ── Create Job ───────────────────────────────────────────────
    job_id = str(uuid.uuid4())
    total_range = (eid - sid) + 1 if (sid and eid) else 0
    job = {
        "job_id": job_id, "user_id": user_id, "status": "queued",
        "from_chat": from_chat, "dest_chat": dest_chat,
        "start_id": sid, "end_id": eid,
        "total_files": total_range, "files_done": 0,
        "base_name": base_name, "starting_number": start_num,
        "artist": adv_artist,
        "year": adv_year,
        "album": adv_album,
        "genre": adv_genre,
        "cover_file_id": adv_cover,
        "account_id": sel_acc.get("id") or acc_id,
        "is_bot": sel_acc.get("is_bot", True),
        "created_at": _ist_now().strftime('%Y-%m-%d %H:%M:%S'),
        "target_title": "DM" if dest_chat == user_id else (ch.get("title", "Channel") if ch else "Channel"),
        "phase_start_ts": 0,
    }
    await _cl_save_job(job)
    await bot.send_message(
        user_id,
        f"<b>✅ Cleaner Job Queued!</b>\n"
        f"Name: <code>{base_name}</code>\n"
        f"Files: <code>{sid}</code> → <code>{eid}</code> (~{total_range} msgs)\n"
        f"Numbering: {base_name} <b>{start_num}</b> → {base_name} <b>{start_num + total_range - 1}</b>\n"
        f"Artist: {adv_artist or '—'}  |  Cover: {'✅ Set' if adv_cover else '—'}",
        reply_markup=ReplyKeyboardRemove()
    )
    
    _cl_paused[job_id] = asyncio.Event()
    _cl_paused[job_id].set()
    _cl_bot_ref[job_id] = bot  # store so resume can notify too
    _cl_tasks[job_id] = asyncio.create_task(_cl_run_job(job_id, bot))
