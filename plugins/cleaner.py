"""
Audio Cleaner & Renamer - v1
============================
Downloads audio files strictly in order, cleans them with FFmpeg (noise reduction,
metadata stripping), applies fresh metadata, renames them using sequential numbers,
and uploads them to the destination.
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
    Message, CallbackQuery, ReplyKeyboardMarkup
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()
COLL = "cleaner_jobs"

_cl_tasks: dict[str, asyncio.Task] = {}
_cl_paused: dict[str, asyncio.Event] = {}
_cl_waiter: dict[int, asyncio.Future] = {}
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

    pct = int((done / total) * 100) if total else 0
    filled = int(18 * pct / 100)
    bar = f"[{'█' * filled}{'░' * (18 - filled)}] {pct}%"

    ic = {"running":"🔄","paused":"⏸","completed":"✅","failed":"⚠️","stopped":"🔴","queued":"⏳"}.get(status, "❔")
    
    lines = [
        f"{ic} <b>🧹 {name}</b>  [{job.get('job_id')[-6:]}]",
        f"  Status: <b>{status.title()}</b>",
        f"  <code>{bar}</code>",
        "",
        f"  📁 <b>Processed:</b> {done}/{total} files",
        f"  📝 <b>Pattern:</b> {job.get('base_name')} {'{number}'}",
        f"  🎯 <b>Target:</b> {job.get('dest_chat')}"
    ]
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

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
            return False, stderr.decode('utf-8', errors='replace')[-1000:]
        return True, ""
    except Exception as e:
        return False, str(e)


# ─── Job Runner ──────────────────────────────────────────────────────────────
async def _cl_run_job(job_id: str):
    async with _cl_semaphore:
        while True:
            ev = _cl_paused.get(job_id)
            if ev and not ev.is_set():
                await ev.wait()

            job = await _cl_get_job(job_id)
            if not job or job.get("status") in ("completed", "failed", "stopped"):
                return

            await _cl_update_job(job_id, {"status": "running", "error": ""})
            uid = job["user_id"]
            
            # Init client
            acc_id = job.get("account_id")
            if acc_id == "bot":
                client = _CLIENT
            else:
                client = await start_clone_bot(await db.get_bot(uid, acc_id))
            if not getattr(client, "is_initialized", False):
                if not client.is_connected:
                    try: await client.start()
                    except: pass
            
            # Setup
            from_ch = job["from_chat"]
            dest_ch = job["dest_chat"]
            sid = job["start_id"]
            eid = job["end_id"]
            done = job.get("files_done", 0)
            
            base_name = job.get("base_name", "Cleaned")
            art = job.get("artist", "")
            yr  = job.get("year", "")
            cov = job.get("cover_file_id", "")
            curr_num = job.get("starting_number", 1) + done

            # Download Cover once if needed
            local_cover = f"temp_cover_{job_id}.jpg"
            if cov and not os.path.exists(local_cover):
                try:
                    await _CLIENT.download_media(cov, file_name=local_cover)
                except Exception as e:
                    logger.warning(f"Cleaner Cover dl fail: {e}")
                    cov = None

            fail_count = 0
            
            # Loop
            for msg_id in range(sid + done, eid + 1):
                ev = _cl_paused.get(job_id)
                if ev and not ev.is_set():
                    break # pause triggered

                job = await _cl_get_job(job_id)
                if job.get("status") == "stopped":
                    break

                try:
                    msg = await client.get_messages(from_ch, msg_id)
                    if not msg or msg.empty or not (msg.audio or msg.voice or msg.document):
                        continue

                    clean_title = f"{base_name} {curr_num}"
                    clean_file  = f"{clean_title}.mp3"
                    
                    album_val = job.get("album") or art
                    year_val = str(job.get("year", yr) or yr or "")
                    
                    meta = {
                        "title": clean_title,
                        "artist": art,
                        "album": album_val,
                        "year": year_val
                    }
                    if job.get("genre"):
                        meta["genre"] = str(job.get("genre"))

                    # Download
                    in_path  = f"temp_cl_in_{job_id}_{msg_id}.tmp"
                    out_path = f"temp_cl_out_{job_id}_{msg_id}.mp3"
                    
                    dl_path = await client.download_media(msg, file_name=in_path)
                    if not dl_path:
                        continue

                    # Process
                    ok, err = await _process_audio_ffmpeg(dl_path, out_path, local_cover if cov else None, meta)
                    
                    try: os.remove(dl_path)
                    except: pass

                    if not ok:
                        raise Exception(f"FFmpeg Edit Failed: {err}")

                    # Upload
                    await client.send_audio(
                        chat_id=dest_ch,
                        audio=out_path,
                        caption=f"**{clean_title}**",
                        title=clean_title,
                        performer=art,
                        file_name=clean_file
                    )

                    try: os.remove(out_path)
                    except: pass

                    # Success!
                    done += 1
                    curr_num += 1
                    fail_count = 0
                    await _cl_update_job(job_id, {"files_done": done})

                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2)
                    continue # retry same
                except Exception as e:
                    fail_count += 1
                    if fail_count > 3:
                        await _cl_update_job(job_id, {"status": "failed", "error": f"Failed repeatedly at msg {msg_id}: {str(e)}"})
                        break
                    await asyncio.sleep(5)
            
            # End of loop logic
            job = await _cl_get_job(job_id)
            if job.get("status") == "failed":
                pass
            elif job.get("status") == "stopped":
                pass
            elif _cl_paused.get(job_id) and not _cl_paused[job_id].is_set():
                await _cl_update_job(job_id, {"status": "paused"})
            else:
                # Finished entirely
                await _cl_update_job(job_id, {"status": "completed", "error": ""})
                
                # Send Final Report
                try:
                    await _CLIENT.send_message(
                        uid,
                        f"<b>🎉 Cʟᴇᴀɴᴇʀ Jᴏʙ Cᴏᴍᴘʟᴇᴛᴇᴅ!</b>\n\n"
                        f"<b>🧹 Jᴏʙ Nᴀᴍᴇ:</b> {base_name}\n"
                        f"<b>📄 Fɪʟᴇs Cʟᴇᴀɴᴇᴅ & Rᴇɴᴀᴍᴇᴅ:</b> {done} / {job.get('total_files', 0)}\n"
                        f"<b>🎯 Rᴀɴɢᴇ Cᴏᴠᴇʀᴇᴅ:</b> <code>{base_name} {job.get('starting_number')}</code> ➠ <code>{base_name} {curr_num - 1}</code>\n\n"
                        f"<i>All files successfully scrubbed of corruption, re-encoded (128kbps), metadata sanitized, and uploaded.</i>"
                    )
                except Exception as e:
                    logger.warning(f"Failed to send cleaner report: {e}")
            
            try:
                if os.path.exists(local_cover): os.remove(local_cover)
            except: pass
            
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
        
        # Default Settings Row
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
        ask_msg = await bot.send_message(uid, f"Send the new default **{cfg_type.title()}** (or /skip to clear):")
        try:
            resp = await _cl_ask(bot, uid, 120)
            if not resp: raise asyncio.TimeoutError
            txt = (resp.text or "").strip()
            
            if txt.lower() == "/skip":
                await _cl_save_default(uid, cfg_type, "")
            else:
                if cfg_type == "cover" and resp.photo:
                    await _cl_save_default(uid, cfg_type, resp.photo.file_id)
                else:
                    await _cl_save_default(uid, cfg_type, txt)
            
            await ask_msg.delete()
            try: await resp.delete()
            except: pass
            
        except asyncio.TimeoutError:
            await ask_msg.edit_text("<i>Timed out.</i>")
            
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
            _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid))
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
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")

    def _cancel(txt): return txt.strip().startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt
    def _undo(txt):   return txt.strip().startswith("/undo") or "↩️" in txt or "Uɴᴅᴏ" in txt

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
    
    r_acc = await _cl_ask(bot, user_id, "<b>🧹 Create Cleaner Job — Step 1/7</b>\n\nChoose the <b>account</b> to read from:", 
                          reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))
    if _cancel(r_acc.text): return await bot.send_message(user_id, "<i>Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in r_acc.text and "]" in r_acc.text:
        try: acc_id = int(r_acc.text.split('[')[-1].split(']')[0])
        except Exception: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
    
    # ── Step 2: Start link ───────────────────────────────────────
    r_start = await _cl_ask(bot, user_id, "<b>»  Step 2/7</b>\n\nSend the <b>Start Message Link</b>:", reply_markup=ReplyKeyboardRemove())
    if _cancel(r_start.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if _undo(r_start.text): return await bot.send_message(user_id, "<b>Undo not supported at first logic step. Use Cancel.</b>", reply_markup=ReplyKeyboardRemove())
    from_chat, sid = _parse_link(r_start.text)

    # ── Step 3: End link ─────────────────────────────────────────
    markup_b = ReplyKeyboardMarkup([[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    r_end = await _cl_ask(bot, user_id, "<b>»  Step 3/7</b>\n\nSend the <b>End Message Link</b>:", reply_markup=markup_b)
    if _cancel(r_end.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if _undo(r_end.text):
        r_start = await _cl_ask(bot, user_id, "<b>»  Step 2/7 (REDO)</b>\n\nSend the <b>Start Message Link</b>:", reply_markup=ReplyKeyboardRemove())
        if _cancel(r_start.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        from_chat, sid = _parse_link(r_start.text)
        r_end = await _cl_ask(bot, user_id, "<b>»  Step 3/7</b>\n\nSend the <b>End Message Link</b>:", reply_markup=markup_b)
        if _cancel(r_end.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())

    _, eid = _parse_link(r_end.text)
    if sid > eid: sid, eid = eid, sid

    # ── Step 4: Destination ──────────────────────────────────────
    channels = await db.get_user_channels(user_id)
    dest_chat = None
    if channels:
        ch_kb = [[KeyboardButton(f"📢 {ch['title']}")] for ch in channels]
        ch_kb.append([KeyboardButton("⏭ Skip (DM only)")])
        ch_kb.append([UNDO_BTN, CANCEL_BTN])
        r_dest = await _cl_ask(bot, user_id, "<b>»  Step 4/7</b>\n\nSelect destination channel:", reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
        if _cancel(r_dest.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(r_dest.text):
            r_end = await _cl_ask(bot, user_id, "<b>»  Step 3/7 (REDO)</b>\n\nSend the <b>End Message Link</b>:", reply_markup=markup_b)
            if _cancel(r_end.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
            _, eid = _parse_link(r_end.text)
            if sid > eid: sid, eid = eid, sid
            r_dest = await _cl_ask(bot, user_id, "<b>»  Step 4/7</b>\n\nSelect destination channel:", reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
            if _cancel(r_dest.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        
        if "Skip" not in r_dest.text:
            title = r_dest.text.replace("📢 ","").strip()
            ch = next((c for c in channels if c["title"] == title), None)
            if ch: dest_chat = int(ch["chat_id"])
    if not dest_chat:
        dest_chat = user_id

    # ── Step 5: Base Name ────────────────────────────────────────
    r_base = await _cl_ask(bot, user_id, "<b>»  Step 5/7</b>\n\nSend <b>Base Name</b> for the files\n<i>(e.g., Send `Saaya` -> outputs `Saaya 1.mp3`)</i>", reply_markup=markup_b)
    if _cancel(r_base.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if _undo(r_base.text):
        return await bot.send_message(user_id, "<b>‣ Please restart the flow to undo further back.</b>", reply_markup=ReplyKeyboardRemove())
    base_name = re.sub(r'[<>:"/\\|?*]', '_', r_base.text.strip())

    # ── Step 6: Advanced Metadata ────────────────────────────────
    markup_m = ReplyKeyboardMarkup([["⏭ Skip (Use Defaults)"], [UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    r_meta = await _cl_ask(bot, user_id, "<b>»  Step 6/7</b>\n\nAdvanced Metadata Configuration.\nSend details in format: <code>Artist | Year | Album | Genre</code>\n\n<i>Or click Skip to use global defaults.</i>", reply_markup=markup_m)
    if _cancel(r_meta.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if _undo(r_meta.text):
        r_base = await _cl_ask(bot, user_id, "<b>»  Step 5/7 (REDO)</b>\n\nSend <b>Base Name</b>:", reply_markup=markup_b)
        if _cancel(r_base.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        base_name = re.sub(r'[<>:"/\\|?*]', '_', r_base.text.strip())
        r_meta = await _cl_ask(bot, user_id, "<b>»  Step 6/7</b>\n\nAdvanced Metadata Configuration.\nSend: <code>Artist | Year | Album | Genre</code>", reply_markup=markup_m)
        if _cancel(r_meta.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())

    df = await _cl_get_defaults(user_id)
    adv_artist = df.get('artist', 'Arya Audio')
    adv_year = df.get('year', '')
    adv_album = getattr(df, 'get', lambda x,y:y)('album', '')
    
    if "Skip" not in r_meta.text:
        parts = [p.strip() for p in r_meta.text.split('|')]
        if len(parts) > 0 and parts[0]: adv_artist = parts[0]
        if len(parts) > 1 and parts[1]: adv_year   = parts[1]
        if len(parts) > 2 and parts[2]: adv_album  = parts[2]

    # ── Step 7: Starting Number ──────────────────────────────────
    r_num = await _cl_ask(bot, user_id, "<b>»  Step 7/7</b>\n\nSend <b>Starting Number</b> (e.g. `1` or `201`)", reply_markup=markup_b)
    if _cancel(r_num.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
    if _undo(r_num.text):
        r_meta = await _cl_ask(bot, user_id, "<b>»  Step 6/7 (REDO)</b>\n\nAdvanced Metadata Configuration:", reply_markup=markup_m)
        if _cancel(r_meta.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        if "Skip" not in r_meta.text:
            parts = [p.strip() for p in r_meta.text.split('|')]
            if len(parts) > 0 and parts[0]: adv_artist = parts[0]
            if len(parts) > 1 and parts[1]: adv_year   = parts[1]
            if len(parts) > 2 and parts[2]: adv_album  = parts[2]
        r_num = await _cl_ask(bot, user_id, "<b>»  Step 7/7</b>\n\nSend <b>Starting Number</b>:", reply_markup=markup_b)
        if _cancel(r_num.text): return await bot.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())

    start_num = int(r_num.text.strip() if r_num.text.strip().isdigit() else 1)

    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id, "user_id": user_id, "status": "queued",
        "from_chat": from_chat, "dest_chat": dest_chat,
        "start_id": sid, "end_id": eid,
        "total_files": (eid - sid) + 1, "files_done": 0,
        "base_name": base_name, "starting_number": start_num,
        "artist": adv_artist,
        "year": adv_year,
        "album": adv_album,
        "cover_file_id": df.get('cover', ''),
        "account_id": sel_acc.get("id") or acc_id,
        "is_bot": sel_acc.get('is_bot', True),
        "created_at": _ist_now().strftime('%Y-%m-%d %H:%M:%S'),
        "target_title": "DM" if dest_chat == user_id else getattr(ch, "title", "Channel") if dest_chat else "DM",
        "metadata": {"artist": adv_artist, "year": adv_year, "album": adv_album}
    }
    await _cl_save_job(job)
    await bot.send_message(user_id, f"<b>✅ Cleaner Job Queued!</b>\nName: <code>{base_name}</code>\nMetadata: Art: {adv_artist} | Yr: {adv_year}", reply_markup=ReplyKeyboardRemove())
    
    _cl_paused[job_id] = asyncio.Event()
    _cl_paused[job_id].set()
    _cl_tasks[job_id] = asyncio.create_task(_cl_run_job(job_id))
    
    class FakeUpdate: pass
    fake = FakeUpdate()
    fake.from_user = type('obj', (object,), {'id': user_id})()
    fake.message = None
    fake.data = f"cl#view#{job_id}"
    await _cl_callbacks(bot, fake)
