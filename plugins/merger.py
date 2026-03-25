"""
Merger Plugin — v4
==================
Multi-Job-style UI for merging media files from channel ranges.

Two sections accessible from Settings (Merger Mode):
  🎵 Audio Merge  — merges audio files (MP3, AAC, FLAC, WAV, OGG, etc.)
  🎬 Video Merge  — merges video files (MP4, MKV, AVI, etc.)

Each section has Multi-Job-style controls:
  ▶️ Start | ⏸ Pause | ▶️ Resume | ⏹ Stop | ℹ️ Info | ✏️ Name | 🗑 Delete

Also accessible directly via /merge command.
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
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

# ─── In-memory registries ────────────────────────────────────────────────────
_mg_tasks: dict[str, asyncio.Task] = {}
_mg_paused: dict[str, asyncio.Event] = {}
_mg_waiter: dict[int, asyncio.Future] = {}
_mg_global_lock = asyncio.Lock()


# ─── Future-based ask ────────────────────────────────────────────────────────
@Client.on_message(filters.private, group=-14)
async def _mg_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _mg_waiter:
        fut = _mg_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)


async def _mg_ask(bot, user_id, text, reply_markup=None, timeout=300):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    old = _mg_waiter.pop(user_id, None)
    if old and not old.done(): old.cancel()
    _mg_waiter[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _mg_waiter.pop(user_id, None)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DB
# ══════════════════════════════════════════════════════════════════════════════
COLL = "mergejobs"

async def _db_save(job):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)

async def _db_get(jid):
    return await db.db[COLL].find_one({"job_id": jid})

async def _db_list(uid, mtype=None):
    q = {"user_id": uid}
    if mtype: q["merge_type"] = mtype
    return [j async for j in db.db[COLL].find(q)]

async def _db_del(jid):
    await db.db[COLL].delete_one({"job_id": jid})

async def _db_up(jid, **kw):
    await db.db[COLL].update_one({"job_id": jid}, {"$set": kw})


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
def _bar(c, t, w=20):
    if t <= 0: return "[" + "░"*w + "] 0%"
    p = min(100, int(c/t*100)); f = int(w*c/t)
    return f"[{'█'*f}{'░'*(w-f)}] {p}%"

def _sz(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b/1024:.1f} KB"
    if b < 1073741824: return f"{b/1048576:.1f} MB"
    return f"{b/1073741824:.2f} GB"

def _spd(bps):
    if bps < 1024: return f"{bps:.0f} B/s"
    if bps < 1048576: return f"{bps/1024:.1f} KB/s"
    return f"{bps/1048576:.1f} MB/s"

def _tm(s):
    s = max(0, int(s))
    if s < 60: return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"

def _emoji(st):
    return {"downloading":"⬇️","merging":"🔀","uploading":"⬆️","done":"✅",
            "error":"⚠️","stopped":"🔴","paused":"⏸","queued":"⏳"}.get(st, "❓")

def _check_ffmpeg():
    return shutil.which("ffmpeg") is not None

def _parse_link(text):
    text = text.strip()
    if text.isdigit(): return None, int(text)
    m = re.match(r'https?://t\.me/c/(\d+)/(\d+)', text)
    if m: return int(m.group(1)), int(m.group(2))
    m = re.match(r'https?://t\.me/([^/]+)/(\d+)', text)
    if m: return m.group(1), int(m.group(2))
    return None, None


# ══════════════════════════════════════════════════════════════════════════════
# FFmpeg
# ══════════════════════════════════════════════════════════════════════════════
def _probe(fp):
    info = {"type": "audio", "codec": ""}
    try:
        r = subprocess.run(
            ["ffprobe","-v","quiet","-show_entries","stream=codec_type,codec_name",
             "-of","csv=p=0",fp], capture_output=True, text=True, timeout=30)
        for line in r.stdout.strip().split("\n"):
            parts = line.strip().split(",")
            if len(parts) >= 2:
                if parts[1] == "video": info["type"] = "video"; info["codec"] = parts[0]
                elif parts[1] == "audio" and not info["codec"]: info["codec"] = parts[0]
    except: pass
    if not info["codec"]:
        ext = os.path.splitext(fp)[1].lower()
        if ext in (".mp4",".mkv",".avi",".webm",".mov",".flv",".ts"): info["type"] = "video"
        info["codec"] = ext.lstrip(".")
    return info


def _ffmpeg_merge(file_list, output_path, metadata=None, mtype="audio", cover=None):
    lst = output_path + ".list.txt"
    try:
        with open(lst, "w", encoding="utf-8") as f:
            for fp in file_list:
                f.write(f"file '{fp.replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'\n")

        # Try lossless
        cmd = ["ffmpeg","-y","-threads","1","-f","concat","-safe","0","-i",lst,"-c","copy"]
        if cover and os.path.exists(cover) and mtype == "audio":
            cmd = ["ffmpeg","-y","-threads","1","-f","concat","-safe","0","-i",lst,
                   "-i",cover,"-map","0:a","-map","1:0","-c:a","copy",
                   "-id3v2_version","3","-metadata:s:v","title=Album cover",
                   "-metadata:s:v","comment=Cover (front)"]
        if metadata:
            for k, v in metadata.items():
                if v: cmd.extend(["-metadata", f"{k}={v}"])
        cmd.append(output_path)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if r.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return True, ""

        # Re-encode
        cmd2 = ["ffmpeg","-y","-threads","1","-f","concat","-safe","0","-i",lst]
        if cover and os.path.exists(cover): cmd2.extend(["-i", cover])
        if mtype == "video":
            cmd2.extend(["-c:v","libx264","-preset","fast","-crf","24",
                         "-c:a","aac","-b:a","128k","-movflags","+faststart"])
        else:
            cmd2.extend(["-c:a","libmp3lame","-b:a","192k","-ar","44100"])
            if cover and os.path.exists(cover):
                cmd2.extend(["-map","0:a","-map","1:0","-id3v2_version","3",
                             "-metadata:s:v","title=Album cover",
                             "-metadata:s:v","comment=Cover (front)"])
        if metadata:
            for k, v in metadata.items():
                if v: cmd2.extend(["-metadata", f"{k}={v}"])
        cmd2.append(output_path)
        r2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=14400)
        if r2.returncode != 0: return False, r2.stderr[-500:]
        return True, ""
    except subprocess.TimeoutExpired: return False, "FFmpeg timed out"
    except Exception as e: return False, str(e)
    finally:
        if os.path.exists(lst): os.remove(lst)


# ══════════════════════════════════════════════════════════════════════════════
# Core runner — with pause/resume
# ══════════════════════════════════════════════════════════════════════════════
async def _run_job(jid, uid, bot):
    job = await _db_get(jid)
    if not job: return

    ev = _mg_paused.get(jid)
    if not ev:
        ev = asyncio.Event(); ev.set()
        _mg_paused[jid] = ev

    client = None
    wdir = f"merge_tmp/{jid}"
    os.makedirs(wdir, exist_ok=True)

    try:
        acc = await db.get_bot(uid, job["account_id"])
        if not acc:
            await _db_up(jid, status="error", error="Account not found"); return
        client = await start_clone_bot(_CLIENT.client(acc))

        from_chat  = job["from_chat"]
        start_id   = job["start_id"]
        end_id     = job["end_id"]
        out_name   = job.get("output_name", "merged")
        metadata   = job.get("metadata", {})
        dest_chats = job.get("dest_chats", [])
        mtype      = job.get("merge_type", "audio")

        resume_id  = job.get("current_id", start_id)
        dl_count   = job.get("downloaded", 0)

        await _db_up(jid, status="queued", error="")

        # ── Global queue: only 1 merge running at a time ──
        async with _mg_global_lock:
            fresh = await _db_get(jid)
            if not fresh or fresh.get("status") in ("stopped", "paused"): return
            await _db_up(jid, status="downloading", error="")

        # Cover check
        cover = os.path.join(wdir, "cover.jpg")
        if not os.path.exists(cover): cover = None

        # Collect existing downloads for resume
        existing = sorted([os.path.join(wdir, f) for f in os.listdir(wdir)
                           if f[0].isdigit() and not f.endswith(".list.txt")])
        dl_files = existing[:]
        dl_bytes = sum(os.path.getsize(f) for f in existing)
        skipped = 0
        dl_start = time.time()
        status_msg = None
        last_edit = 0
        current = resume_id
        total_range = end_id - start_id + 1

        while current <= end_id:
            # Pause
            if not ev.is_set():
                await _db_up(jid, status="paused", current_id=current)
                return  # Exit to release global lock; resume will restart

            # Stop
            fresh = await _db_get(jid)
            if not fresh or fresh.get("status") == "stopped": return

            batch_end = min(current + 200 - 1, end_id)
            ids = list(range(current, batch_end + 1))

            try:
                msgs = await client.get_messages(from_chat, ids)
                if not isinstance(msgs, list): msgs = [msgs]
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2); continue
            except Exception as e:
                logger.warning(f"[MG {jid}] fetch: {e}")
                current += 200; continue

            valid = sorted([m for m in msgs if m and not m.empty and not m.service],
                           key=lambda m: m.id)

            for msg in valid:
                if not msg.media: skipped += 1; continue
                media_obj = None
                for attr in ('audio','video','document','voice','video_note'):
                    media_obj = getattr(msg, attr, None)
                    if media_obj: break
                if not media_obj: skipped += 1; continue

                # Extension
                ext = ""
                fn = getattr(media_obj, 'file_name', None)
                if fn: ext = os.path.splitext(fn)[1].lower()
                if not ext:
                    if getattr(msg,'audio',None):
                        mime = getattr(media_obj,'mime_type','') or ''
                        ext = ".m4a" if 'm4a' in mime or 'mp4' in mime else \
                              ".ogg" if 'ogg' in mime else \
                              ".flac" if 'flac' in mime else ".mp3"
                    elif getattr(msg,'voice',None): ext = ".ogg"
                    elif getattr(msg,'video',None) or getattr(msg,'video_note',None): ext = ".mp4"
                    elif getattr(msg,'document',None):
                        mime = getattr(media_obj,'mime_type','') or ''
                        ext = ".mp3" if 'audio' in mime else ".mp4" if 'video' in mime else ".bin"

                seq = f"{dl_count:06d}{ext}"
                dlp = os.path.join(wdir, seq)

                fp = None
                for att in range(5):
                    try:
                        fp = await client.download_media(msg, file_name=dlp)
                        if fp: break
                    except FloodWait as fw: await asyncio.sleep(fw.value+2)
                    except Exception:
                        if att < 4: await asyncio.sleep(3); continue
                        break

                if fp and os.path.exists(fp):
                    dl_bytes += os.path.getsize(fp)
                    dl_files.append(fp)
                    dl_count += 1
                    await _db_up(jid, downloaded=dl_count, current_id=msg.id,
                                 total_dl_bytes=dl_bytes)
                    
                    await asyncio.sleep(0.5)  # Smart delay to reduce memory/CPU load

                    now = time.time()
                    if now - last_edit >= 3:
                        last_edit = now
                        elapsed = now - dl_start
                        speed = dl_bytes / elapsed if elapsed > 0 else 0
                        left = total_range - dl_count - skipped
                        dl_eta = left * elapsed / max(dl_count, 1)
                        mg_eta = dl_bytes / 10485760  # Estimate: 10MB/s merge speed
                        up_eta = dl_bytes / max(speed * 0.8, 1) if speed else 0
                        total_eta = dl_eta + mg_eta + up_eta
                        
                        await _db_up(jid, dl_eta=dl_eta, mg_eta=mg_eta, up_eta=up_eta, total_eta=total_eta)

                        mtype_icon = "🎵" if mtype == "audio" else "🎬"
                        txt = (f"<b>{mtype_icon} {out_name} — Downloading</b>\n\n"
                               f"<code>{_bar(dl_count, total_range - skipped)}</code>\n\n"
                               f"📁 {dl_count}/{total_range - skipped} files\n"
                               f"💾 {_sz(dl_bytes)} • ⚡ {_spd(speed)}\n"
                               f"⏱ ETA: {_tm(dl_eta)}")
                        try:
                            if status_msg: await status_msg.edit_text(txt)
                            else: status_msg = await bot.send_message(uid, txt)
                        except: pass
                else:
                    skipped += 1

            current = batch_end + 1
            await _db_up(jid, current_id=current)

        dl_time = time.time() - dl_start

        if not dl_files:
            await _db_up(jid, status="error", error="No media files found")
            try:
                t = "<b>❌ No media files found in range.</b>"
                if status_msg: await status_msg.edit_text(t)
                else: await bot.send_message(uid, t)
            except: pass
            return

        try:
            t = f"<b>✅ Download Complete</b>\n📁 {dl_count} files • {_sz(dl_bytes)} • ⏱ {_tm(dl_time)}"
            if status_msg: await status_msg.edit_text(t)
            else: await bot.send_message(uid, t)
        except: pass

        # ── Phase 2: Merge ────────────────────────────────────────────────
        merge_start = time.time()
        await _db_up(jid, status="merging")

        out_ext = ".mp4" if mtype == "video" else ".mp3"
        out_path = os.path.join(wdir, f"{out_name}{out_ext}")

        try:
            await bot.send_message(uid,
                f"<b>🔀 Merging {dl_count} files → <code>{out_name}{out_ext}</code></b>\n"
                f"<i>Lossless if codecs match, otherwise high-quality re-encode.</i>")
        except: pass
        
        avg_dl_speed = dl_bytes / dl_time if dl_time > 0 else 1048576
        mg_eta_static = dl_bytes / 10485760
        up_eta_static = dl_bytes / avg_dl_speed
        await _db_up(jid, dl_eta=0, mg_eta=mg_eta_static, up_eta=up_eta_static, total_eta=(mg_eta_static + up_eta_static))

        loop = asyncio.get_event_loop()
        ok, err = await loop.run_in_executor(
            None, _ffmpeg_merge, dl_files, out_path, metadata, mtype, cover)
        merge_time = time.time() - merge_start

        if not ok:
            await _db_up(jid, status="error", error=err[:500])
            try: await bot.send_message(uid, f"<b>❌ Merge failed!</b>\n<code>{err[:400]}</code>")
            except: pass
            return

        fsize = os.path.getsize(out_path)
        if fsize > 2 * 1024**3:
            await _db_up(jid, status="error", error=f"Too large: {_sz(fsize)}")
            try: await bot.send_message(uid, f"<b>❌ {_sz(fsize)} exceeds 2GB limit.</b>")
            except: pass
            return

        # ── Phase 3: Upload ───────────────────────────────────────────────
        up_start = time.time()
        await _db_up(jid, status="uploading")

        caption = f"<b>🔀 {out_name}{out_ext}</b>\n📁 {dl_count} files • {_sz(fsize)}"
        if metadata.get("title"): caption += f"\n🎵 {metadata['title']}"
        if metadata.get("artist"): caption += f" — {metadata['artist']}"

        all_dests = [uid] + [d for d in dest_chats if d != uid]
        thumb = cover if cover and os.path.exists(cover) else None

        avg_dl_speed = dl_bytes / dl_time if dl_time > 0 else 1048576
        up_eta_static = fsize / avg_dl_speed
        await _db_up(jid, dl_eta=0, mg_eta=0, up_eta=up_eta_static, total_eta=up_eta_static)

        up_state = {"last": 0}
        async def _up_prog(current, total):
            now = time.time()
            if now - up_state["last"] >= 3:
                up_state["last"] = now
                ela = now - up_start
                eta = (total - current) * ela / max(current, 1)
                await _db_up(jid, dl_eta=0, mg_eta=0, up_eta=eta, total_eta=eta)

        for dest in all_dests:
            for att in range(3):
                try:
                    if mtype == "video":
                        await client.send_video(chat_id=dest, video=out_path,
                            caption=caption, file_name=f"{out_name}{out_ext}",
                            thumb=thumb, supports_streaming=True, progress=_up_prog)
                    else:
                        kw = {"chat_id":dest,"audio":out_path,"caption":caption,
                              "file_name":f"{out_name}{out_ext}","thumb":thumb, "progress":_up_prog}
                        if metadata.get("title"): kw["title"] = metadata["title"]
                        if metadata.get("artist"): kw["performer"] = metadata["artist"]
                        await client.send_audio(**kw)
                    break
                except FloodWait as fw: await asyncio.sleep(fw.value+2)
                except Exception as e:
                    if att < 2: await asyncio.sleep(5); continue
                    logger.warning(f"[MG {jid}] upload {dest}: {e}"); break

        up_time = time.time() - up_start
        total_time = dl_time + merge_time + up_time

        await _db_up(jid, status="done", dl_time=dl_time, merge_time=merge_time,
                     up_time=up_time, total_time=total_time, file_size=fsize)

        try:
            await bot.send_message(uid,
                f"<b>✅ Merge Complete!</b>\n\n"
                f"╭───── 📊 ─────╮\n"
                f"┃ 📁 Files: {dl_count}\n"
                f"┃ 📦 {out_name}{out_ext}\n"
                f"┃ 💾 {_sz(fsize)}\n"
                f"┃\n"
                f"┃ ⬇️ DL: {_tm(dl_time)}\n"
                f"┃ 🔀 Merge: {_tm(merge_time)}\n"
                f"┃ ⬆️ Upload: {_tm(up_time)}\n"
                f"┃ ⏱ Total: {_tm(total_time)}\n"
                f"╰─────────────╯")
        except: pass

    except asyncio.CancelledError:
        await _db_up(jid, status="stopped")
    except Exception as e:
        logger.error(f"[MG {jid}] {e}")
        await _db_up(jid, status="error", error=str(e)[:500])
        try: await bot.send_message(uid, f"<b>❌ Error:</b> <code>{e}</code>")
        except: pass
    finally:
        _mg_tasks.pop(jid, None)
        _mg_paused.pop(jid, None)
        try:
            if os.path.exists(wdir):
                fresh = await _db_get(jid)
                if not fresh or fresh.get("status") not in ("paused", "stopped", "queued"):
                    shutil.rmtree(wdir, ignore_errors=True)
        except: pass
        if client:
            try: await client.stop()
            except: pass


def _start_task(jid, uid, bot):
    old = _mg_tasks.get(jid)
    if old and not old.done(): return
    ev = asyncio.Event(); ev.set()
    _mg_paused[jid] = ev
    _mg_tasks[jid] = asyncio.create_task(_run_job(jid, uid, bot))


# ══════════════════════════════════════════════════════════════════════════════
# Multi-Job-style list UI  (callback prefix: mg#)
# ══════════════════════════════════════════════════════════════════════════════

async def _render_list(bot, uid, msg_or_q, mtype):
    """Render Audio or Video merge list — identical layout to Multi Job."""
    is_cb = hasattr(msg_or_q, 'message')
    jobs = await _db_list(uid, mtype)

    icon = "🎵" if mtype == "audio" else "🎬"
    label = "Audio" if mtype == "audio" else "Video"

    if not jobs:
        text = (
            f"<b>{icon} {label} Merge</b>\n\n"
            f"<i>No {label.lower()} merge jobs yet.\n\n"
            f"Merge multiple {label.lower()} files from a channel range into one file.\n"
            f"✅ Strict order • No format skipping\n"
            f"✅ Full metadata + cover art\n"
            f"✅ Pause / Resume support\n"
            f"✅ Multiple jobs simultaneously\n\n"
            f"👇 Create your first {label} Merge below!</i>"
        )
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"➕ Cʀᴇᴀᴛᴇ {label} Mᴇʀɢᴇ", callback_data=f"mg#new_{mtype}")],
            [InlineKeyboardButton("↩ Bᴀᴄᴋ", callback_data="settings#main")]
        ])
    else:
        lines = [f"<b>{icon} Your {label} Merges</b>\n"]
        for j in jobs:
            st   = _emoji(j.get("status", "stopped"))
            dl   = j.get("downloaded", 0)
            sid  = j.get("start_id", 0)
            eid  = j.get("end_id", 0)
            err  = f" <code>[{j.get('error','')}]</code>" if j.get("status") == "error" else ""
            name = j.get("name", j.get("output_name", j["job_id"][-6:]))
            lines.append(
                f"{st} <b>{name}</b>\n"
                f"  └ <i>Range: {sid} → {eid}</i>\n"
                f"  └ <code>[{j['job_id'][-6:]}]</code>  ⬇️{dl}  📍{j.get('current_id','?')}/{eid}{err}\n"
            )

        now_str = datetime.datetime.now().strftime("%I:%M:%S %p")
        text = "\n".join(lines) + f"\n\n<i>Last refreshed: {now_str}</i>"

        btns_list = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            short = jid[-6:]
            row = []
            if st in ("downloading", "merging", "uploading"):
                row.append(InlineKeyboardButton(f"⏸ Pᴀᴜsᴇ [{short}]", callback_data=f"mg#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mg#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ Rᴇsᴜᴍᴇ [{short}]", callback_data=f"mg#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mg#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Sᴛᴀʀᴛ [{short}]", callback_data=f"mg#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ Iɴғᴏ [{short}]", callback_data=f"mg#info#{jid}"))
            row.append(InlineKeyboardButton(f"✏️ Nᴀᴍᴇ [{short}]", callback_data=f"mg#rename#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 [{short}]", callback_data=f"mg#del#{jid}"))
            btns_list.append(row)

        btns_list.append([InlineKeyboardButton(f"➕ Cʀᴇᴀᴛᴇ {label} Mᴇʀɢᴇ", callback_data=f"mg#new_{mtype}")])
        btns_list.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ", callback_data=f"mg#{mtype}_list")])
        btns_list.append([InlineKeyboardButton("↩ Bᴀᴄᴋ", callback_data="settings#main")])
        btns = InlineKeyboardMarkup(btns_list)

    try:
        if is_cb:
            await msg_or_q.message.edit_text(text, reply_markup=btns)
        else:
            await msg_or_q.reply_text(text, reply_markup=btns)
    except: pass


# ══════════════════════════════════════════════════════════════════════════════
# /merge command — shortcut, picks type
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("merge") & filters.private)
async def merge_cmd(bot, message):
    if not _check_ffmpeg():
        return await message.reply("<b>❌ FFmpeg not installed.</b>\n<code>sudo apt install ffmpeg</code>")
    await message.reply(
        "<b>🔀 Merger</b>\n\n<i>Choose merge type:</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Aᴜᴅɪᴏ Mᴇʀɢᴇ", callback_data="mg#audio_list"),
             InlineKeyboardButton("🎬 Vɪᴅᴇᴏ Mᴇʀɢᴇ", callback_data="mg#video_list")],
            [InlineKeyboardButton("⫷ Cʟᴏsᴇ", callback_data="mg#close")]
        ]))


# ══════════════════════════════════════════════════════════════════════════════
# Callback handler  (prefix: mg#)
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^mg#'))
async def mg_cb(bot, query):
    uid = query.from_user.id
    parts = query.data.split("#", 2)
    action = parts[1] if len(parts) > 1 else ""
    param = parts[2] if len(parts) > 2 else ""

    # ── List views ────────────────────────────────────────────────────────
    if action == "audio_list":
        return await _render_list(bot, uid, query, "audio")
    if action == "video_list":
        return await _render_list(bot, uid, query, "video")
    if action == "close":
        return await query.message.delete()

    # ── Create ────────────────────────────────────────────────────────────
    if action.startswith("new_"):
        mtype = action.split("_", 1)[1]
        await query.message.delete()
        return await _create_flow(bot, uid, mtype)

    # ── Info ──────────────────────────────────────────────────────────────
    if action == "info":
        job = await _db_get(param)
        if not job: return await query.answer("Not found!", show_alert=True)
        mtype = job.get("merge_type", "audio")
        created = datetime.datetime.fromtimestamp(job.get("created_at",0)).strftime("%d %b %H:%M")

        meta = job.get("metadata", {})
        meta_txt = "\n".join(f"  {k}: {v}" for k,v in list(meta.items())[:8] if v) if meta else ""

        text = (
            f"<b>{_emoji(job['status'])} Merge Info</b>\n\n"
            f"<b>Name:</b> {job.get('name', job.get('output_name','?'))}\n"
            f"<b>Type:</b> {'🎵 Audio' if mtype=='audio' else '🎬 Video'}\n"
            f"<b>Range:</b> {job.get('start_id')} → {job.get('end_id')}\n"
            f"<b>Downloaded:</b> {job.get('downloaded',0)} files\n"
            f"<b>Cover:</b> {'✅' if job.get('has_cover') else '❌'}\n"
            f"<b>Status:</b> {job['status']}\n"
            f"<b>Created:</b> {created}\n"
        )

        dl_t = job.get("dl_time",0); mg_t = job.get("merge_time",0)
        up_t = job.get("up_time",0); tot = job.get("total_time",0)
        status = job['status']

        if status in ("downloading", "merging", "uploading"):
            dl_str = f"{_tm(job.get('dl_eta',0))}" if status == "downloading" else "Done ✅"
            mg_str = "Done ✅" if status == "uploading" else f"~{_tm(job.get('mg_eta',0))}"
            up_str = f"~{_tm(job.get('up_eta',0))}"
            tot_str = f"~{_tm(job.get('total_eta',0))}"
            
            text += (
                f"\n╭──── ⏳ Live ETA ────╮\n"
                f"┃ ⬇️ Download: {dl_str}\n"
                f"┃ 🔀 Merge: {mg_str}\n"
                f"┃ ⬆️ Upload: {up_str}\n"
                f"┃ 📊 Total ETA: {tot_str}\n"
                f"╰────────────────────╯\n"
            )
        elif dl_t or mg_t or up_t:
            text += (
                f"\n╭──── ⏱ Timings ────╮\n"
                f"┃ ⬇️ Download: {_tm(dl_t)}\n"
                f"┃ 🔀 Merge: {_tm(mg_t)}\n"
                f"┃ ⬆️ Upload: {_tm(up_t)}\n"
                f"┃ 📊 Total: {_tm(tot)}\n"
                f"╰────────────────────╯\n"
            )

        fsz = job.get("file_size",0)
        if fsz: text += f"\n<b>Size:</b> {_sz(fsz)}\n"
        if meta_txt: text += f"\n<b>Metadata:</b>\n{meta_txt}\n"
        if job.get("error"): text += f"\n<b>⚠️ Error:</b> <code>{job['error'][:200]}</code>"

        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data=f"mg#info#{param}")],
            [InlineKeyboardButton("↩ Bᴀᴄᴋ", callback_data=f"mg#{mtype}_list")]
        ]))

    # ── Rename ────────────────────────────────────────────────────────────
    elif action == "rename":
        await query.message.delete()
        try:
            r = await _mg_ask(bot, uid,
                "<b>✏️ Send new name for this merge job:</b>",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("/cancel")]],
                    resize_keyboard=True, one_time_keyboard=True))
            if "/cancel" not in r.text.lower():
                await _db_up(param, name=r.text.strip()[:100])
                await bot.send_message(uid,
                    f"✅ Renamed to <b>{r.text.strip()[:100]}</b>",
                    reply_markup=ReplyKeyboardRemove())
            else:
                await bot.send_message(uid, "<b>Cancelled.</b>",
                                       reply_markup=ReplyKeyboardRemove())
        except: pass
        job = await _db_get(param)
        mtype = job.get("merge_type","audio") if job else "audio"
        # We can't easily render list here without query, so just notify
        await bot.send_message(uid, f"Use /merge or /settings to see updated list.")

    # ── Pause ─────────────────────────────────────────────────────────────
    elif action == "pause":
        ev = _mg_paused.get(param)
        if ev: ev.clear()
        await _db_up(param, status="paused")
        await query.answer("⏸ Paused!", show_alert=True)
        job = await _db_get(param)
        mtype = job.get("merge_type","audio") if job else "audio"
        await _render_list(bot, uid, query, mtype)

    # ── Resume ────────────────────────────────────────────────────────────
    elif action == "resume":
        job = await _db_get(param)
        if not job: return await query.answer("Not found!", show_alert=True)
        mtype = job.get("merge_type","audio")
        ev = _mg_paused.get(param)
        if ev and param in _mg_tasks and not _mg_tasks[param].done():
            ev.set()
            await _db_up(param, status="downloading")
            await query.answer("▶️ Resumed!", show_alert=True)
        else:
            await _db_up(param, status="downloading")
            _start_task(param, uid, bot)
            await query.answer("▶️ Restarted from saved position!", show_alert=True)
        await _render_list(bot, uid, query, mtype)

    # ── Start ─────────────────────────────────────────────────────────────
    elif action == "start":
        job = await _db_get(param)
        if not job: return await query.answer("Not found!", show_alert=True)
        mtype = job.get("merge_type","audio")
        await _db_up(param, status="downloading")
        _start_task(param, uid, bot)
        await query.answer("▶️ Started!", show_alert=True)
        await _render_list(bot, uid, query, mtype)

    # ── Stop ──────────────────────────────────────────────────────────────
    elif action == "stop":
        task = _mg_tasks.pop(param, None)
        if task and not task.done(): task.cancel()
        ev = _mg_paused.pop(param, None)
        if ev: ev.set()
        await _db_up(param, status="stopped")
        await query.answer("⏹ Stopped!", show_alert=True)
        job = await _db_get(param)
        mtype = job.get("merge_type","audio") if job else "audio"
        await _render_list(bot, uid, query, mtype)

    # ── Delete ────────────────────────────────────────────────────────────
    elif action == "del":
        job = await _db_get(param)
        mtype = job.get("merge_type","audio") if job else "audio"
        task = _mg_tasks.pop(param, None)
        if task and not task.done(): task.cancel()
        _mg_paused.pop(param, None)
        await _db_del(param)
        wd = f"merge_tmp/{param}"
        if os.path.exists(wd): shutil.rmtree(wd, ignore_errors=True)
        await query.answer("🗑 Deleted!", show_alert=True)
        await _render_list(bot, uid, query, mtype)


# ══════════════════════════════════════════════════════════════════════════════
# Creation flow (7 steps)
# ══════════════════════════════════════════════════════════════════════════════

async def _create_flow(bot, uid, mtype="audio"):
    icon = "🎵" if mtype == "audio" else "🎬"
    label = "Audio" if mtype == "audio" else "Video"

    try:
        # Step 1: Account
        accounts = await db.get_bots(uid)
        if not accounts:
            return await bot.send_message(uid,
                "<b>❌ No accounts. Add in /settings → Accounts.</b>")

        kb = [[f"{'🤖' if a.get('is_bot',True) else '👤'} {a['name']}"] for a in accounts]
        kb.append(["❌ Cancel"])
        msg = await _mg_ask(bot, uid,
            f"<b>{icon} New {label} Merge</b>\n\n<b>Step 1/7:</b> Select account:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True))
        if not msg.text or "Cancel" in msg.text:
            return await bot.send_message(uid, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

        sel_name = msg.text.split(" ", 1)[1] if " " in msg.text else msg.text
        acc = next((a for a in accounts if a["name"] == sel_name), None)
        if not acc:
            return await bot.send_message(uid, "<b>❌ Account not found.</b>", reply_markup=ReplyKeyboardRemove())

        # Step 2: Start link
        msg = await _mg_ask(bot, uid,
            "<b>Step 2/7:</b> Send <b>start file link</b>\n\n"
            "<i>Example: https://t.me/c/123456/100</i>",
            reply_markup=ReplyKeyboardRemove())
        if not msg.text or msg.text.lower() == "/cancel":
            return await bot.send_message(uid, "<b>Cancelled.</b>")
        ref_s, sid = _parse_link(msg.text)
        if sid is None:
            return await bot.send_message(uid, "<b>❌ Invalid link.</b>")
        from_chat = (-1000000000000 - ref_s if isinstance(ref_s, int) else ref_s) if ref_s else None

        # Step 3: End link
        msg = await _mg_ask(bot, uid,
            "<b>Step 3/7:</b> Send <b>end file link</b>")
        if not msg.text or msg.text.lower() == "/cancel":
            return await bot.send_message(uid, "<b>Cancelled.</b>")
        ref_e, eid = _parse_link(msg.text)
        if eid is None:
            return await bot.send_message(uid, "<b>❌ Invalid link.</b>")
        if from_chat is None and ref_e:
            from_chat = (-1000000000000 - ref_e if isinstance(ref_e, int) else ref_e)
        if from_chat is None:
            return await bot.send_message(uid, "<b>❌ Could not detect channel.</b>")
        if sid > eid: sid, eid = eid, sid
        total = eid - sid + 1

        # Hard limit to prevent RAM exhaustion
        if total > 120:
            return await bot.send_message(uid, 
                f"<b>❌ Too many files ({total}).</b>\n\n"
                f"To prevent server crashes, maximum allowed is <b>120 files</b> per merge. "
                f"Please split your request into smaller ranges.",
                reply_markup=ReplyKeyboardRemove())

        # Step 4: Destination
        channels = await db.get_user_channels(uid)
        dest_chats = []
        if channels:
            ch_kb = [[f"📢 {ch['title']}"] for ch in channels]
            ch_kb.append(["⏭ Skip (DM only)"]); ch_kb.append(["❌ Cancel"])
            msg = await _mg_ask(bot, uid,
                f"<b>Step 4/7:</b> Destination channel\n<b>Range:</b> {sid}→{eid} ({total} msgs)",
                reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
            if not msg.text or "Cancel" in msg.text:
                return await bot.send_message(uid, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            if "Skip" not in msg.text:
                title = msg.text.replace("📢 ","").strip()
                ch = next((c for c in channels if c["title"] == title), None)
                if ch: dest_chats.append(int(ch["chat_id"]))
        else:
            await bot.send_message(uid, "<b>Step 4/7:</b> No channels. Sending to DM.",
                                   reply_markup=ReplyKeyboardRemove())
            await asyncio.sleep(0.5)

        # Step 5: Filename
        msg = await _mg_ask(bot, uid,
            "<b>Step 5/7:</b> Output <b>filename</b> (no extension)",
            reply_markup=ReplyKeyboardRemove())
        if not msg.text or msg.text.lower() == "/cancel":
            return await bot.send_message(uid, "<b>Cancelled.</b>")
        out_name = re.sub(r'[<>:"/\\|?*]', '_', msg.text.strip())

        # Step 6: Metadata
        msg = await _mg_ask(bot, uid,
            "<b>Step 6/7:</b> Send <b>metadata</b> (one per line)\n\n"
            "<code>title: My Title\n"
            "artist: Artist\n"
            "album: Album\n"
            "genre: Pop\n"
            "year: 2024\n"
            "track: 1\n"
            "composer: Name\n"
            "comment: Notes</code>\n\n"
            "Send <code>skip</code> for defaults.")

        metadata = {}
        if msg.text and msg.text.lower() not in ("skip","/cancel"):
            kmap = {"title":"title","artist":"artist","album":"album","genre":"genre",
                    "year":"date","date":"date","track":"track","composer":"composer",
                    "comment":"comment","album_artist":"album_artist","description":"description",
                    "language":"language","publisher":"publisher","performer":"performer",
                    "copyright":"copyright"}
            for line in msg.text.strip().split("\n"):
                if ":" in line:
                    k,v = line.split(":",1)
                    k = k.strip().lower(); v = v.strip()
                    if k and v: metadata[kmap.get(k,k)] = v

        # Step 6b: Cover
        cover_path = None
        msg = await _mg_ask(bot, uid,
            "<b>Step 6b:</b> Send <b>cover image</b> (photo/file)\n\n"
            "Send <code>skip</code> for no cover.")
        tmp_dir = f"merge_tmp/_cover_{uid}"
        os.makedirs(tmp_dir, exist_ok=True)
        if msg.photo:
            try: cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_dir, "cover.jpg"))
            except: pass
        elif msg.document and msg.document.mime_type and 'image' in msg.document.mime_type:
            try: cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_dir, "cover.jpg"))
            except: pass

        # Step 7: Confirm
        dest_preview = "DM only"
        if dest_chats:
            names = [next((c["title"] for c in channels if int(c["chat_id"])==d), str(d)) for d in dest_chats]
            dest_preview = ", ".join(names)

        meta_pre = "\n".join(f"  {k}: {v}" for k,v in list(metadata.items())[:5] if v) if metadata else ""

        msg = await _mg_ask(bot, uid,
            f"<b>Step 7/7: Confirm {label} Merge</b>\n\n"
            f"<b>Source:</b> <code>{from_chat}</code>\n"
            f"<b>Range:</b> {sid} → {eid} ({total} msgs)\n"
            f"<b>Output:</b> <code>{out_name}</code>\n"
            f"<b>Type:</b> {icon} {label}\n"
            f"<b>Cover:</b> {'✅' if cover_path else '❌'}\n"
            f"<b>Dest:</b> {dest_preview}\n"
            + (f"\n<b>Metadata:</b>\n{meta_pre}\n" if meta_pre else "") +
            f"\n<i>All media merged in exact order. No file skipped.</i>",
            reply_markup=ReplyKeyboardMarkup(
                [["✅ Start Merge"],["❌ Cancel"]],
                resize_keyboard=True, one_time_keyboard=True))

        if not msg.text or "Cancel" in msg.text:
            if os.path.exists(tmp_dir): shutil.rmtree(tmp_dir, ignore_errors=True)
            return await bot.send_message(uid, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

        # Create job
        jid = str(uuid.uuid4())
        real_dir = f"merge_tmp/{jid}"
        os.makedirs(real_dir, exist_ok=True)
        if cover_path and os.path.exists(cover_path):
            new_cover = os.path.join(real_dir, "cover.jpg")
            shutil.copy2(cover_path, new_cover)
        if os.path.exists(tmp_dir): shutil.rmtree(tmp_dir, ignore_errors=True)

        job = {
            "job_id": jid, "user_id": uid, "account_id": acc["id"],
            "from_chat": from_chat, "start_id": sid, "end_id": eid,
            "current_id": sid, "output_name": out_name, "merge_type": mtype,
            "metadata": metadata, "dest_chats": dest_chats,
            "has_cover": bool(cover_path), "name": out_name,
            "status": "downloading", "downloaded": 0,
            "total_dl_bytes": 0, "error": "", "created_at": time.time(),
        }
        await _db_save(job)
        _start_task(jid, uid, bot)

        await bot.send_message(uid,
            f"<b>✅ {icon} {label} Merge Started!</b>\n\n"
            f"<b>Range:</b> {sid} → {eid} ({total})\n"
            f"<b>Output:</b> <code>{out_name}</code>\n"
            f"<b>Job:</b> <code>{jid[-6:]}</code>\n\n"
            f"<i>Use /merge or /settings to monitor.</i>",
            reply_markup=ReplyKeyboardRemove())

    except asyncio.TimeoutError:
        await bot.send_message(uid, "<b>⏱ Timed out.</b>", reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        logger.error(f"[MG create] {e}")
        await bot.send_message(uid, f"<b>❌ Error:</b> <code>{e}</code>", reply_markup=ReplyKeyboardRemove())
