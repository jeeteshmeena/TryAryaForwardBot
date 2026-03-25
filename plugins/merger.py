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


def _ffprobe_duration(fp):
    """Return duration in seconds (float) using ffprobe, or 0.0 on failure."""
    try:
        r = subprocess.run(
            ["ffprobe","-v","quiet","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1", fp],
            capture_output=True, text=True, timeout=30)
        return float(r.stdout.strip())
    except:
        return 0.0


def _build_atempo_chain(speed):
    """Build FFmpeg atempo filter chain for speed (0.5x – 2.5x)."""
    filters = []
    rem = float(speed)
    # atempo range is [0.5, 2.0]; chain multiple for values outside
    while rem > 2.0:
        filters.append("atempo=2.0")
        rem /= 2.0
    while rem < 0.5:
        filters.append("atempo=0.5")
        rem /= 0.5
    if abs(rem - 1.0) > 0.001:
        filters.append(f"atempo={rem:.4f}")
    return ",".join(filters) if filters else ""


def _ffmpeg_merge(file_list, output_path, metadata=None, mtype="audio", cover=None, speed=1.0, make_video=False):
    """Merge file_list → output_path. Tries lossless copy first, falls back to re-encode.
    make_video: If True and cover is present, creates an MP4 video out of the merged audio and cover image.
    speed: 1.0 = normal, 2.5 = 2.5x faster.
    Returns (ok: bool, error: str).
    """
    lst = output_path + ".list.txt"
    try:
        with open(lst, "w", encoding="utf-8") as f:
            for fp in file_list:
                safe = fp.replace("'", "'\\''")
                f.write(f"file '{safe}'\n")

        atempo = _build_atempo_chain(speed) if abs(speed - 1.0) > 0.001 else ""
        needs_reencode = bool(atempo)

        if not needs_reencode:
            # Try lossless concat copy first
            cmd = ["ffmpeg","-y","-threads","1","-f","concat","-safe","0","-i",lst]
            if cover and os.path.exists(cover) and mtype == "audio":
                cmd += ["-i", cover, "-map","0:a","-map","1:0","-c:a","copy",
                        "-id3v2_version","3",
                        "-metadata:s:v","title=Album cover",
                        "-metadata:s:v","comment=Cover (front)"]
            else:
                cmd += ["-c", "copy"]
            if metadata:
                for k, v in (metadata or {}).items():
                    if v: cmd += ["-metadata", f"{k}={v}"]
            cmd.append(output_path)
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
            if r.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                return True, ""

        # Re-encode (needed for speed change OR if lossless failed)
        cmd2 = ["ffmpeg","-y","-threads","1"]
        if make_video and cover and os.path.exists(cover) and mtype == "audio":
            # Image + Audio merged to Video
            cmd2 += ["-loop", "1", "-framerate", "1", "-i", cover]
            cmd2 += ["-f","concat","-safe","0","-i",lst]
            if atempo: cmd2 += ["-af", atempo]
            cmd2 += ["-c:v", "libx264", "-tune", "stillimage", "-c:a", "aac", "-b:a", "192k", "-pix_fmt", "yuv420p", "-shortest"]
        else:
            cmd2 += ["-f","concat","-safe","0","-i",lst]
            if cover and os.path.exists(cover) and mtype == "audio":
                cmd2 += ["-i", cover]
            if mtype == "video":
                vf = f"setpts={1.0/speed:.4f}*PTS" if abs(speed - 1.0) > 0.001 else ""
                if vf: cmd2 += ["-vf", vf]
                if atempo: cmd2 += ["-af", atempo]
                cmd2 += ["-c:v","libx264","-preset","fast","-crf","24",
                         "-c:a","aac","-b:a","128k","-movflags","+faststart"]
            else:
                if atempo: cmd2 += ["-af", atempo]
                # 192k MP3 — transparent quality, ~60% smaller than many originals
                cmd2 += ["-c:a","libmp3lame","-b:a","192k","-ar","44100"]
                if cover and os.path.exists(cover):
                    cmd2 += ["-map","0:a","-map","1:0",
                             "-id3v2_version","3",
                             "-metadata:s:v","title=Album cover",
                             "-metadata:s:v","comment=Cover (front)"]
        if metadata:
            for k, v in (metadata or {}).items():
                if v: cmd2 += ["-metadata", f"{k}={v}"]
        cmd2.append(output_path)
        r2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=14400)
        if r2.returncode != 0:
            return False, r2.stderr[-600:]
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "FFmpeg timed out"
    except Exception as e:
        return False, str(e)
    finally:
        try:
            if os.path.exists(lst): os.remove(lst)
        except: pass


# ══════════════════════════════════════════════════════════════════════════════
# Pre-download size scanner
# ══════════════════════════════════════════════════════════════════════════════
async def _scan_total_size(client, from_chat, start_id, end_id):
    """Scan all messages in range and return (total_size_bytes, media_count) using
    Telegram metadata only — NO file downloads."""
    total = 0
    count = 0
    current = start_id
    while current <= end_id:
        batch_end = min(current + 200 - 1, end_id)
        ids = list(range(current, batch_end + 1))
        try:
            msgs = await client.get_messages(from_chat, ids)
            if not isinstance(msgs, list): msgs = [msgs]
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
            continue
        except:
            current += 200
            continue
        for m in msgs:
            if not m or m.empty or m.service or not m.media: continue
            for attr in ('audio','video','document','voice','video_note'):
                obj = getattr(m, attr, None)
                if obj:
                    total += getattr(obj, 'file_size', 0) or 0
                    count += 1
                    break
        current = batch_end + 1
        await asyncio.sleep(0.1)
    return total, count


# ══════════════════════════════════════════════════════════════════════════════
# Core runner — Chunked, Memory-safe
# ══════════════════════════════════════════════════════════════════════════════
CHUNK_SIZE   = 20          # Files downloaded at a time before partial merge
MAX_TOTAL_GB = 15.0        # Hard limit on total estimated size across all files
MAX_CHUNK_GB = 2.0         # Abort a single chunk if it somehow exceeds this

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
        metadata   = job.get("metadata", {}) or {}
        dest_chats = job.get("dest_chats", [])
        mtype      = job.get("merge_type", "audio")
        speed      = float(job.get("speed", 1.0))

        await _db_up(jid, status="queued", error="")

        # ── Global queue: only 1 merge running at a time ──
        async with _mg_global_lock:
            fresh = await _db_get(jid)
            if not fresh or fresh.get("status") in ("stopped", "paused"): return
            await _db_up(jid, status="scanning", error="")

        # ══════════════════════════════════════════════════════════════════
        # PHASE 0 — Pre-download size scan
        # ══════════════════════════════════════════════════════════════════
        try:
            scan_msg = await bot.send_message(uid,
                f"<b>🔍 Scanning file sizes before download...</b>\n"
                f"<i>Range: {start_id} → {end_id}</i>")
        except: scan_msg = None

        est_size, media_count = await _scan_total_size(client, from_chat, start_id, end_id)

        if est_size > MAX_TOTAL_GB * 1024**3:
            msg = (f"<b>❌ Pre-scan failed — total estimated size is {_sz(est_size)}, "
                   f"which exceeds the {MAX_TOTAL_GB:.0f}GB limit.</b>\n\n"
                   f"Please select a smaller range and try again.")
            await _db_up(jid, status="error", error=msg)
            try:
                if scan_msg: await scan_msg.edit_text(msg)
                else: await bot.send_message(uid, msg)
            except: pass
            return

        try:
            txt = (f"<b>✅ Pre-scan complete</b>\n"
                   f"📁 {media_count} media files found\n"
                   f"💾 Estimated total: <b>{_sz(est_size)}</b>\n"
                   f"🔀 Will process in chunks of {CHUNK_SIZE} files\n"
                   f"⚡ Speed: <b>{speed}x</b>")
            if scan_msg: await scan_msg.edit_text(txt)
            else: await bot.send_message(uid, txt)
        except: pass

        await asyncio.sleep(1)

        # Cover
        cover = os.path.join(wdir, "cover.jpg")
        if not os.path.exists(cover): cover = None

        # ══════════════════════════════════════════════════════════════════
        # PHASE 1 — Collect all media message IDs in strict order
        # ══════════════════════════════════════════════════════════════════
        await _db_up(jid, status="downloading", error="")
        all_msgs_ordered = []   # list of (msg_id, msg) in channel order
        current = start_id
        while current <= end_id:
            batch_end = min(current + 200 - 1, end_id)
            ids = list(range(current, batch_end + 1))
            try:
                msgs = await client.get_messages(from_chat, ids)
                if not isinstance(msgs, list): msgs = [msgs]
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2); continue
            except Exception as e:
                logger.warning(f"[MG {jid}] collect: {e}")
                current += 200; continue
            for m in sorted([x for x in msgs if x and not x.empty and not x.service], key=lambda x: x.id):
                if not m.media: continue
                for attr in ('audio','video','document','voice','video_note'):
                    if getattr(m, attr, None):
                        all_msgs_ordered.append(m)
                        break
            current = batch_end + 1
            await asyncio.sleep(0.2)

        if not all_msgs_ordered:
            await _db_up(jid, status="error", error="No media files found in range")
            await bot.send_message(uid, "<b>❌ No media files found in the selected range.</b>")
            return

        total_files = len(all_msgs_ordered)
        total_chunks = (total_files + CHUNK_SIZE - 1) // CHUNK_SIZE

        try:
            await bot.send_message(uid,
                f"<b>⬇️ Starting download</b>\n"
                f"📁 {total_files} files → {total_chunks} chunk(s) of max {CHUNK_SIZE}\n"
                f"<i>Each chunk is downloaded, merged, then deleted to save RAM.</i>")
        except: pass

        # ══════════════════════════════════════════════════════════════════
        # PHASE 2 — Chunked download + partial merge
        # ══════════════════════════════════════════════════════════════════
        part_files  = []     # merged part paths: part_001.mp3, part_002.mp3 ...
        log_entries = []     # [(original_filename, cumulative_start_ts)]
        cumulative_secs = 0.0
        dl_total_bytes = 0
        global_seq = 0       # zero-padded counter across ALL files

        for chunk_idx in range(total_chunks):
            # Check pause / stop
            if not ev.is_set():
                await _db_up(jid, status="paused", downloaded=global_seq)
                return
            fresh = await _db_get(jid)
            if not fresh or fresh.get("status") == "stopped": return

            chunk_msgs = all_msgs_ordered[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
            chunk_num  = chunk_idx + 1
            chunk_label = f"Part {chunk_num}/{total_chunks}"
            chunk_dir  = os.path.join(wdir, f"chunk_{chunk_num:04d}")
            os.makedirs(chunk_dir, exist_ok=True)

            chunk_files = []
            chunk_bytes = 0
            status_msg  = None

            try:
                status_msg = await bot.send_message(uid,
                    f"<b>⬇️ {chunk_label} — Downloading {len(chunk_msgs)} files</b>")
            except: pass

            for ci, msg in enumerate(chunk_msgs):
                media_obj = None
                for attr in ('audio','video','document','voice','video_note'):
                    media_obj = getattr(msg, attr, None)
                    if media_obj: break
                if not media_obj: continue

                ext = ""
                fn = getattr(media_obj, 'file_name', None)
                original_name = fn or f"file_{global_seq+1}"
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

                seq_name = f"{global_seq:06d}{ext}"
                dlp = os.path.join(chunk_dir, seq_name)

                fp = None
                for att in range(5):
                    try:
                        fp = await client.download_media(msg, file_name=dlp)
                        if fp: break
                    except FloodWait as fw: await asyncio.sleep(fw.value + 2)
                    except Exception:
                        if att < 4: await asyncio.sleep(3); continue
                        break

                if fp and os.path.exists(fp):
                    fsz = os.path.getsize(fp)
                    chunk_bytes += fsz
                    dl_total_bytes += fsz
                    chunk_files.append(fp)
                    global_seq += 1

                    # Log entry: timecode + original name (before speed adjustment)
                    h = int(cumulative_secs // 3600)
                    m_ = int((cumulative_secs % 3600) // 60)
                    s_ = int(cumulative_secs % 60)
                    tc = f"{h:02d}:{m_:02d}:{s_:02d}"
                    log_entries.append((tc, original_name, global_seq))

                    # Probe duration for log
                    dur = _ffprobe_duration(fp)
                    # Apply speed factor to cumulative time
                    cumulative_secs += dur / max(speed, 0.1)

                    await _db_up(jid, downloaded=global_seq, current_id=msg.id,
                                 total_dl_bytes=dl_total_bytes)
                    await asyncio.sleep(0.3)

                    # Update status every 3 files
                    if ci % 3 == 0:
                        pct = int((global_seq / total_files) * 100)
                        try:
                            txt = (f"<b>⬇️ {chunk_label} — Downloading</b>\n"
                                   f"<code>{_bar(global_seq, total_files)}</code>\n"
                                   f"📁 {global_seq}/{total_files} total • {_sz(dl_total_bytes)}")
                            if status_msg: await status_msg.edit_text(txt)
                        except: pass

            if not chunk_files:
                logger.warning(f"[MG {jid}] Chunk {chunk_num} had no downloadable files, skipping.")
                continue

            # Partial merge of this chunk
            await _db_up(jid, status="merging")
            part_ext  = ".mp4" if mtype == "video" else ".mp3"
            part_path = os.path.join(wdir, f"part_{chunk_num:04d}{part_ext}")

            try:
                if status_msg: await status_msg.edit_text(
                    f"<b>🔀 {chunk_label} — Merging {len(chunk_files)} files→ part {chunk_num}</b>")
            except: pass

            chunk_files_sorted = sorted(chunk_files, key=lambda p: os.path.basename(p))
            loop = asyncio.get_event_loop()
            # Chunk parts: always lossless (speed applied at final merge only)
            ok, err = await loop.run_in_executor(
                None, _ffmpeg_merge, chunk_files_sorted, part_path, None, mtype, None, 1.0, False)

            if not ok:
                await _db_up(jid, status="error", error=f"Chunk {chunk_num} merge failed: {err[:300]}")
                await bot.send_message(uid, f"<b>❌ Chunk {chunk_num} merge failed:</b>\n<code>{err[:300]}</code>")
                return

            part_files.append(part_path)

            # ✅ Delete chunk originals immediately to free disk
            for f in chunk_files:
                try: os.remove(f)
                except: pass
            try: os.rmdir(chunk_dir)
            except: pass

            await _db_up(jid, status="downloading")
            try:
                if status_msg: await status_msg.edit_text(
                    f"<b>✅ {chunk_label} done</b>\n"
                    f"💾 Part size: {_sz(os.path.getsize(part_path))} • "
                    f"Total so far: {_sz(dl_total_bytes)}")
            except: pass
            await asyncio.sleep(1)

        # ══════════════════════════════════════════════════════════════════
        # PHASE 3 — Final combine of all parts
        # ══════════════════════════════════════════════════════════════════
        await _db_up(jid, status="merging")
        make_video = job.get("make_video", False)
        
        # If make_video is true dynamically overwrite the out_ext for the final combination:
        out_ext  = ".mp4" if (mtype == "video" or make_video) else ".mp3"
        out_path = os.path.join(wdir, f"{out_name}{out_ext}")

        try:
            await bot.send_message(uid,
                f"<b>🔀 Final combine: {len(part_files)} parts → {out_name}{out_ext}</b>\n"
                f"{'⚡ Applying speed ' + str(speed) + 'x during final merge' if abs(speed-1.0)>0.001 else '🎯 Lossless combine (speed=1.0x)'}")
        except: pass

        part_files_sorted = sorted(part_files, key=lambda p: os.path.basename(p))
        loop = asyncio.get_event_loop()
        ok, err = await loop.run_in_executor(
            None, _ffmpeg_merge, part_files_sorted, out_path, metadata, mtype, cover, speed, make_video)

        if not ok:
            await _db_up(jid, status="error", error=err[:500])
            try: await bot.send_message(uid, f"<b>❌ Final merge failed!</b>\n<code>{err[:400]}</code>")
            except: pass
            return

        # Clean up part files
        for pf in part_files:
            try: os.remove(pf)
            except: pass

        # ══════════════════════════════════════════════════════════════════
        # PHASE 3b — Generate processing log
        # ══════════════════════════════════════════════════════════════════
        log_path = os.path.join(wdir, f"{out_name}_log.txt")
        try:
            with open(log_path, "w", encoding="utf-8") as lf:
                lf.write(f"Processing Log — {out_name}\n")
                lf.write(f"Generated: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
                lf.write(f"Files: {total_files} | Speed: {speed}x | Type: {mtype}\n")
                lf.write("=" * 60 + "\n\n")
                lf.write("TIMECODES (YouTube Chapter Format)\n")
                lf.write("-" * 40 + "\n")
                for tc, name, seq in log_entries:
                    lf.write(f"{tc} {seq:04d}. {name}\n")
        except Exception as e:
            logger.warning(f"Log write failed: {e}")
            log_path = None

        merge_time = 0  # kept for compat
        dl_time = 0

        # (ok/err check already handled above per-chunk and at final merge)

        fsize = os.path.getsize(out_path)
        if fsize > 2 * 1024**3:
            await _db_up(jid, status="error", error=f"Too large: {_sz(fsize)}")
            try: await bot.send_message(uid, f"<b>❌ {_sz(fsize)} exceeds 2GB limit.</b>")
            except: pass
            return

        # ── Phase 3: Upload ───────────────────────────────────────────────
        up_start = time.time()
        await _db_up(jid, status="uploading")

        caption = f"<b>🔀 {out_name}{out_ext}</b>\n📁 {global_seq} files • {_sz(fsize)}"
        if metadata.get("title"): caption += f"\n🎵 {metadata['title']}"
        if metadata.get("artist"): caption += f" — {metadata['artist']}"

        all_dests = [uid] + [d for d in dest_chats if d != uid]
        thumb = cover if cover and os.path.exists(cover) else None

        avg_dl_speed = max(dl_total_bytes, 1) / 60  # estimate: assume ~1min DL time for speed calc
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

        # Upload log file if generated
        if log_path and os.path.exists(log_path):
            try:
                await client.send_document(chat_id=uid, document=log_path,
                    caption=f"<b>📋 Processing Log</b>\n{out_name}")
            except: pass

        total_time = up_time
        await _db_up(jid, status="done", total_time=total_time, file_size=fsize)

        try:
            await bot.send_message(uid,
                f"<b>✅ Merge Complete!</b>\n\n"
                f"╭───── 📊 ─────╮\n"
                f"┃ 📁 Files: {global_seq}\n"
                f"┃ 📦 {out_name}{out_ext}\n"
                f"┃ 💾 {_sz(fsize)}\n"
                f"┃ ⚡ Speed: {speed}x\n"
                f"┃ ⬆️ Upload: {_tm(up_time)}\n"
                f"┃ ⏱ Total parts: {total_chunks}\n"
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

# /merge command — removed by user request


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

        # Step 6c: Make Video?
        make_video = False
        upload_to_yt = False
        if cover_path and mtype == "audio":
            msg = await _mg_ask(bot, uid,
                "<b>Step 6c:</b> Create an <b>MP4 Video</b> from this audio using the cover image? (Uses very little RAM)\n\n"
                "Send <code>yes</code> to build a video, or <code>skip</code> to just embed the image as MP3 cover art.")
            if "yes" in (msg.text or "").lower():
                make_video = True
                
            # Step 6d: YouTube Upload?
            if make_video:
                msg = await _mg_ask(bot, uid,
                    "<b>Step 6d:</b> Auto-Upload this video directly to <b>YouTube</b>?\n\n"
                    "<i>(Requires running /ytauth first to link your channel)</i>\n\n"
                    "Send <code>yes</code> to upload, or <code>skip</code> for Telegram only.")
                if "yes" in (msg.text or "").lower():
                    upload_to_yt = True

        # Step 7: Confirm
        dest_preview = "DM only"
        if dest_chats:
            names = [next((c["title"] for c in channels if int(c["chat_id"])==d), str(d)) for d in dest_chats]
            dest_preview = ", ".join(names)

        meta_pre = "\n".join(f"  {k}: {v}" for k,v in list(metadata.items())[:5] if v) if metadata else ""

        msg = await _mg_ask(bot, uid,
            f"<b>Step 7: Confirm {label} Merge</b>\n\n"
            f"<b>Source:</b> <code>{from_chat}</code>\n"
            f"<b>Range:</b> {sid} → {eid} ({total} msgs)\n"
            f"<b>Output:</b> <code>{out_name}</code>\n"
            f"<b>Type:</b> {icon} {label}\n"
            f"<b>Cover:</b> {'✅' if cover_path else '❌'}\n"
            f"<b>Make MP4 Video:</b> {'✅' if make_video else '❌'}\n"
            f"<b>Upload to YT:</b> {'✅' if upload_to_yt else '❌'}\n"
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
