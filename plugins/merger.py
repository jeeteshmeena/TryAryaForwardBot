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
from pyrogram import Client, filters, ContinuePropagation
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
_mg_dl_choices: dict[str, str | None] = {}   # key→ "skip"|"retry"|"abort"|None

# ─── Global concurrency queue ─────────────────────────────────────────────────
# All merger jobs share this semaphore.  Only MAX_CONCURRENT_MERGES jobs
# actually RUN at once; additional jobs are automatically QUEUED and started
# the moment a slot frees up.  Shared with Live/Multi Job via AryaJobQueue.
MAX_CONCURRENT_MERGES = 1   # keep at 1 — merges are extremely CPU+RAM heavy
_mg_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MERGES)
_mg_global_lock = asyncio.Lock()  # kept for backward compat


# ─── Future-based ask ────────────────────────────────────────────────────────
@Client.on_message(filters.private, group=-14)
async def _mg_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _mg_waiter:
        fut = _mg_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation


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
            "error":"⚠️","stopped":"🔴","paused":"⏸","queued":"⏳","scanning":"🔍"}.get(st, "❓")

IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

def _ist_now() -> datetime.datetime:
    """Return current time in IST (UTC+5:30)."""
    return datetime.datetime.now(IST_OFFSET)

def _ist_str(fmt='%d %b %Y %I:%M:%S %p IST') -> str:
    return _ist_now().strftime(fmt)

from typing import Optional


def _build_info_text(job: dict, now_ts: Optional[float] = None) -> str:
    """Build the premium info panel text for a merge job."""
    if now_ts is None:
        now_ts = time.time()

    mtype = job.get("merge_type", "audio")
    icon  = "🎵" if mtype == "audio" else "🎬"
    status = job.get("status", "stopped")
    name  = job.get("name", job.get("output_name", job.get("job_id", "")[-6:]))
    created_ts = job.get("created_at", 0)
    created_str = datetime.datetime.fromtimestamp(created_ts, tz=IST_OFFSET).strftime('%d %b %H:%M IST') if created_ts else "?"

    total_files = max(job.get("end_id", 1) - job.get("start_id", 0), 1)
    dl_done     = job.get("downloaded", 0)
    fsz         = job.get("file_size", 0)

    # ── Phase durations (persisted in DB) ─────────────────────────────────
    dl_time  = job.get("dl_time",  0) or 0
    mg_time  = job.get("merge_time", 0) or 0
    up_time  = job.get("up_time",  0) or 0
    yt_time  = job.get("yt_time",  0) or 0
    total_time_done = job.get("total_time", 0) or 0

    # ── Live elapsed for active phase ──────────────────────────────────────
    ph_start = job.get("phase_start_ts", 0) or 0
    if status in ("downloading", "merging", "uploading", "yt_uploading") and ph_start:
        live_elapsed = now_ts - ph_start
    else:
        live_elapsed = 0

    # ── Calculate dynamic ETAs ─────────────────────────────────────────────
    # Fetch base ETAs from DB if present
    dl_eta_db  = job.get("dl_eta",  0) or 0
    mg_eta_db  = job.get("mg_eta",  0) or 0
    up_eta_db  = job.get("up_eta",  0) or 0
    yt_eta_db  = job.get("yt_eta",  0) or 0

    # Project live ETA based on elapsed time if available and meaningful
    dl_eta = dl_eta_db
    if status == "downloading" and dl_done > 0 and live_elapsed > 5:
        speed_per_file = live_elapsed / dl_done
        dl_eta = speed_per_file * (total_files - dl_done)

    # Hardcoded or projected ETAs for merging if unavailable
    mg_eta = mg_eta_db
    if status == "merging" and live_elapsed > 0:
        # Rough estimate based on file count
        total_mg_eta = total_files * 3.5  # About 3.5s per file avg merge processing
        mg_eta = max(total_mg_eta - live_elapsed, 10)
    elif mg_eta == 0 and status in ("downloading", "queued"):
        mg_eta = total_files * 3.5

    up_eta = up_eta_db
    if up_eta == 0 and status in ("downloading", "merging", "queued"):
        up_eta = total_files * 1.5 # rough estimate

    yt_eta = yt_eta_db
    if yt_eta == 0 and job.get("upload_to_yt") and status in ("downloading", "merging", "uploading"):
        yt_eta = total_files * 2.0

    # ── Overall percentage ─────────────────────────────────────────────────
    # Weight phases: DL=40%, Merge=20%, TG Upload=25%, YT Upload=15%
    has_yt = bool(job.get("upload_to_yt"))
    if status == "done":
        pct = 100
    elif status in ("stopped", "error"):
        pct = 0
    else:
        dl_pct  = min(100, int(dl_done / max(total_files, 1) * 100))
        # Weights
        w_dl   = 40
        w_mg   = 20
        w_up   = 30 if not has_yt else 25
        w_yt   = 15 if has_yt else 0

        if status == "downloading":
            pct = int(dl_pct * w_dl / 100)
        elif status in ("merging", "scanning"):
            pct = w_dl
        elif status == "uploading":
            el = live_elapsed
            eta_total = up_eta + el if up_eta else el
            up_frac = min(1.0, el / max(eta_total, 1))
            pct = w_dl + w_mg + int(up_frac * w_up)
        elif status == "yt_uploading":
            el = live_elapsed
            eta_total = yt_eta + el if yt_eta else el
            yt_frac = min(1.0, el / max(eta_total, 1))
            pct = w_dl + w_mg + w_up + int(yt_frac * w_yt)
        else:
            pct = 0

    bar_w = 18
    filled = int(bar_w * pct / 100)
    prog_bar = f"[{'█' * filled}{'░' * (bar_w - filled)}] {pct}%"

    # ── Phase status rows ──────────────────────────────────────────────────
    def _phase_row(label, phase_status, done_time, live_eta, is_active):
        if phase_status == "done":
            return f"  ✅ {label}: Done ({_tm(done_time)})"
        elif is_active:
            eta_str = f"~{_tm(live_eta)}" if live_eta else "calculating…"
            return f"  ⏳ {label}: In progress — ETA {eta_str}"
        else:
            return f"  ⬜ {label}: Pending"

    dl_done_flag  = status not in ("downloading", "scanning", "queued", "stopped", "error")
    mg_done_flag  = status in ("uploading", "yt_uploading", "done")
    up_done_flag  = status in ("yt_uploading", "done")
    yt_done_flag  = status == "done" and has_yt

    dl_row  = _phase_row("📥 Download",        "done" if dl_done_flag else "todo", dl_time,  dl_eta,  status == "downloading")
    mg_row  = _phase_row("🔀 Merge",           "done" if mg_done_flag else "todo", mg_time,  mg_eta,  status == "merging")
    up_row  = _phase_row("📤 Telegram Upload", "done" if up_done_flag else "todo", up_time,  up_eta,  status == "uploading")
    yt_row  = _phase_row("🎥 YouTube Upload",  "done" if yt_done_flag else "todo", yt_time,  yt_eta,  status == "yt_uploading") if has_yt else None

    # ── Total ETA ──────────────────────────────────────────────────────────
    if status == "done":
        total_eta_str = f"✅ Completed in {_tm(total_time_done or (dl_time + mg_time + up_time + yt_time))}"
    else:
        remaining = 0
        if not dl_done_flag:  remaining += max(dl_eta - live_elapsed if status == "downloading" else dl_eta, 0)
        if not mg_done_flag:  remaining += mg_eta
        if not up_done_flag:  remaining += up_eta
        if has_yt and not yt_done_flag: remaining += yt_eta
        total_eta_str = f"~{_tm(remaining)}" if remaining else "Calculating…"

    # ── Assemble ───────────────────────────────────────────────────────────
    header = f"{_emoji(status)} <b>{icon} {name}</b>  [{job.get('job_id','')[-6:]}]"
    lines = [
        header,
        f"  Status: <b>{status.title()}</b>  •  Range: {job.get('start_id')}→{job.get('end_id')}",
        f"  <code>{prog_bar}</code>",
        "",
        "<b>Phase Progress:</b>",
        dl_row, mg_row, up_row,
    ]
    if yt_row: lines.append(yt_row)
    lines += [
        "",
        f"  ⏱ <b>Total ETA:</b> {total_eta_str}",
        f"  📁 Files: {dl_done}/{total_files}" + (f"   💾 {_sz(fsz)}" if fsz else ""),
        f"  🗓 Created: {created_str}",
    ]
    if job.get("error"):
        lines.append(f"\n  ⚠️ Error: <code>{job['error'][:180]}</code>")

    now_ist_str = _ist_now().strftime('%I:%M %p IST')
    lines.append(f"\n  <i>Last refreshed: {now_ist_str}</i>")
    return "\n".join(lines)

def _check_ffmpeg():
    return shutil.which("ffmpeg") is not None


def _strip_ffmpeg_banner(stderr_text: str) -> str:
    """Remove the FFmpeg version/configuration banner from stderr output.
    The banner lines start with 'ffmpeg version', 'built with', 'configuration:',
    or 'libXxx' version lines. Everything after those is the actual error.
    On Windows the gyan.dev banner is ~800 chars — showing raw stderr[:300] to
    the user only ever shows the banner, never the actual error.
    """
    lines = stderr_text.splitlines(keepends=True)
    result = []
    in_banner = True
    for line in lines:
        s = line.strip()
        if in_banner:
            # Standard FFmpeg banner patterns
            if (s.startswith("ffmpeg version") or
                    s.startswith("built with") or
                    s.startswith("configuration:") or
                    # lib version lines look like: "  libavutil      59. 39.100 / 59. 39.100"
                    (s.startswith("lib") and "/" in s)):
                continue
            else:
                in_banner = False
                if s:  # skip blank separator line right after banner
                    result.append(line)
        else:
            result.append(line)
    stripped = "".join(result).strip()
    # Fallback: if nothing left after stripping, return last 1500 chars as-is
    return stripped if stripped else stderr_text[-1500:]

def _parse_link(text):
    text = text.strip().rstrip('/')
    if text.isdigit(): return None, int(text)
    m = re.search(r'https?://t\.me/c/(\d+)(?:/\d+)?/(\d+)', text)
    if m: return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.search(r'https?://t\.me/([^/]+)(?:/\d+)?/(\d+)', text)
    if m: return m.group(1), int(m.group(2))
    return None, None

async def _safe_resolve_peer(client, chat_id):
    try:
        chat_id = int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
        try: await client.get_chat(chat_id)
        except: await client.get_users(chat_id)
    except Exception as e:
        err_str = str(e).upper()
        if "PEER_ID_INVALID" in err_str or "CHANNEL_INVALID" in err_str or "PEER_ID_NOT_HANDLED" in err_str or "USERNAME_NOT_OCCUPIED" in err_str:
            try:
                me = await client.get_me()
                if not getattr(me, 'is_bot', False):
                    async for _ in client.get_dialogs(): pass
                try: await client.get_chat(chat_id)
                except: await client.get_users(chat_id)
            except Exception as e2:
                logger.warning(f"Failed to resolve {chat_id}: {e2}")


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

def _get_duration(fp):
    """Alias for _ffprobe_duration for consistency."""
    return _ffprobe_duration(fp)


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

async def _ffmpeg_merge(file_list, output_path, metadata=None, mtype="audio", cover=None, speed=1.0, make_video=False, video_cover=None, outro_cover=None, total_duration=None, progress_cb=None, is_chunk=False):
    """Merge file_list → output_path. Tries lossless copy first, falls back to re-encode.
    make_video: If True and cover is present, creates an MP4 video out of the merged audio and cover image.
    speed: 1.0 = normal, 2.5 = 2.5x faster.
    Returns (ok: bool, error: str).
    """
    import asyncio, re, subprocess as _sp

    async def _run_cmd(cmd_list, timeout_sec):
        """Run an FFmpeg command and return (ok, error_message).
        Uses subprocess.run via run_in_executor for 100% reliable output
        capture on all platforms (especially Windows), with a separate
        asyncio task for real-time progress tracking when progress_cb is set.
        """
        loop = asyncio.get_event_loop()

        # ── Synchronous runner (captures ALL stdout+stderr reliably) ──────
        def _sync_run():
            try:
                result = _sp.run(
                    cmd_list,
                    stdout=_sp.PIPE,
                    stderr=_sp.PIPE,
                    timeout=timeout_sec
                )
                return result.returncode, result.stderr.decode('utf-8', errors='replace')
            except _sp.TimeoutExpired:
                return -1, "FFmpeg timed out"
            except FileNotFoundError:
                return -1, "ffmpeg executable not found — please install FFmpeg and ensure it is in PATH"
            except Exception as exc:
                return -1, str(exc)

        if progress_cb:
            # Run FFmpeg in thread; simultaneously poll ffprobe every 3 s for progress
            import time as _time
            _start = _time.time()
            fut = loop.run_in_executor(None, _sync_run)
            while not fut.done():
                await asyncio.sleep(3)
                elapsed = _time.time() - _start
                try:
                    await progress_cb(elapsed)   # rough progress by wall-clock
                except Exception:
                    pass
            returncode, stderr_text = await fut
        else:
            returncode, stderr_text = await loop.run_in_executor(None, _sync_run)

        if returncode == 0:
            return True, ""
        # Strip the verbose FFmpeg banner so the actual error is visible
        meaningful = _strip_ffmpeg_banner(stderr_text)
        # Log full command + error server-side for debugging
        logger.error(
            "[FFmpeg] Command failed (rc=%d):\n  %s\nError:\n%s",
            returncode,
            " ".join(str(x) for x in cmd_list),
            meaningful[:2000]
        )
        return False, meaningful

    lst = output_path + ".list.txt"
    vconcat_txt = output_path + ".vconcat.txt"
    try:
        # CRITICAL: use absolute paths so FFmpeg can find files regardless of CWD
        with open(lst, "w", encoding="utf-8") as f:
            for fp in file_list:
                abs_fp = os.path.abspath(fp).replace("\\", "/")
                safe = abs_fp.replace("'", "'\\''")
                f.write(f"file '{safe}'\n")

        atempo = _build_atempo_chain(speed) if abs(speed - 1.0) > 0.001 else ""
        
        # Audio concatenation with different formats (mp3 + m4a) must be re-encoded to prevent truncation.
        # We enforce re-encode if it's an audio merge with multiple files, or if make_video is True.
        # Video merges (mtype == "video") with make_video == False can safely use lossless concat demuxer.
        needs_reencode = bool(atempo) or make_video or (mtype == "audio" and len(file_list) > 1 and is_chunk)


        if not needs_reencode:
            # Try lossless concat copy first (only safe for audio output or pure video concat)
            cmd = ["ffmpeg","-y","-threads","2","-f","concat","-safe","0","-i",lst]
            if cover and os.path.exists(cover) and mtype == "audio":
                cmd += ["-i", cover, "-map","0:a","-map","1:0","-c:a","copy",
                        "-id3v2_version","3",
                        "-metadata:s:v","title=Album cover",
                        "-metadata:s:v","comment=Cover (front)",
                        "-max_muxing_queue_size", "4096"]
            elif mtype == "video":
                # Video concat: add faststart so YouTube can process it
                cmd += ["-c", "copy", "-movflags", "+faststart", "-max_muxing_queue_size", "4096"]
            else:
                cmd += ["-c", "copy", "-max_muxing_queue_size", "4096"]
            if metadata:
                for k, v in (metadata or {}).items():
                    if v: cmd += ["-metadata", f"{k}={v}"]
            cmd.append(output_path)
            abs_out = os.path.abspath(output_path)
            cmd[-1] = abs_out
            ok, err = await _run_cmd(cmd, 7200)
            if ok and os.path.exists(abs_out) and os.path.getsize(abs_out) > 1000:
                return True, ""

        # ══════════════════════════════════════════════════════════════════════
        # Re-encode path — TWO-STEP for video (RAM-safe), filter_complex for audio chunks
        # ══════════════════════════════════════════════════════════════════════
        eff_cover = video_cover or cover

        if make_video and eff_cover and os.path.exists(eff_cover) and mtype == "audio":
            # ─── Step A: Merge audio parts → single tmp_audio.m4a ──────────────
            # Use concat DEMUXER (not filter_complex) — streams files sequentially,
            # O(1) RAM regardless of how many parts there are. Parts are uniform
            # mp3/192k from Phase 2 so concat demuxer works perfectly.
            tmp_audio = output_path + ".tmp_audio.m4a"
            audio_join_cmd = ["ffmpeg", "-y", "-threads", "2",
                              "-f", "concat", "-safe", "0", "-i", lst, "-vn"]
            if atempo:
                audio_join_cmd += ["-af", atempo]
            audio_join_cmd += ["-c:a", "aac", "-b:a", "192k", tmp_audio]
            a_ok, a_err = await _run_cmd(audio_join_cmd, 86400)
            if not a_ok or not os.path.exists(tmp_audio) or os.path.getsize(tmp_audio) < 1000:
                try:
                    if os.path.exists(tmp_audio): os.remove(tmp_audio)
                except Exception: pass
                return False, f"Audio merge step A failed: {a_err}"

            real_dur = _get_duration(tmp_audio)
            if real_dur <= 0:
                real_dur = total_duration / max(speed, 0.1) if total_duration else 3600 * 5

            # ─── Step B: Build video from cover + single audio ──────────────────
            # Determine valid outros
            if isinstance(outro_cover, list):
                valid_outros = [o for o in outro_cover if isinstance(o, str) and os.path.exists(o)]
            elif isinstance(outro_cover, str) and os.path.exists(outro_cover):
                valid_outros = [outro_cover]
            else:
                valid_outros = []

            cmd_v = ["ffmpeg", "-y", "-threads", "2"]

            if valid_outros and len(valid_outros) >= 4:
                # 4-outro overlay mode: cover (input 0) + 4 outro images (inputs 1-4) + audio (input 5)
                outro_positions = [
                    max(0.0, real_dur * 0.25),
                    max(0.0, real_dur * 0.50),
                    max(0.0, real_dur * 0.75),
                    max(0.0, real_dur * 0.95 - 5),
                ]
                cmd_v += ["-loop", "1", "-framerate", "1", "-t", f"{real_dur:.2f}",
                           "-i", os.path.abspath(eff_cover)]
                for op in valid_outros[:4]:
                    cmd_v += ["-loop", "1", "-framerate", "1", "-t", "5",
                               "-i", os.path.abspath(op)]
                cmd_v += ["-i", tmp_audio]
                audio_idx = 5

                # filter_complex: scale cover → overlay each outro sequentially
                fc_parts = [
                    f"[0:v]scale=1280:720:force_original_aspect_ratio=decrease,"
                    f"pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p[base]"
                ]
                prev = "[base]"
                for i, pos in enumerate(outro_positions):
                    end_t = pos + 5.0
                    out_lbl = f"[ov{i}]" if i < 3 else "[finalv]"
                    fc_parts.append(
                        f"[{i+1}:v]scale=1280:720:force_original_aspect_ratio=decrease,"
                        f"pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p[os{i}];"
                        f"{prev}[os{i}]overlay=0:0:enable='between(t,{pos:.1f},{end_t:.1f})'{out_lbl}"
                    )
                    prev = out_lbl
                cmd_v += ["-filter_complex", ";".join(fc_parts)]
                cmd_v += ["-map", "[finalv]", "-map", f"{audio_idx}:a"]
            else:
                # Simple mode: cover image + merged audio (no outros, only 2 inputs)
                cmd_v += ["-loop", "1", "-framerate", "1", "-i", os.path.abspath(eff_cover)]
                cmd_v += ["-i", tmp_audio]
                cmd_v += [
                    "-filter_complex",
                    "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,"
                    "pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p[v1]",
                ]
                cmd_v += ["-map", "[v1]", "-map", "1:a"]

            # Common video encoding options
            cmd_v += [
                "-c:v", "libx264", "-preset", "superfast", "-tune", "stillimage",
                "-c:a", "copy",
                "-movflags", "+faststart", "-shortest",
                "-max_muxing_queue_size", "4096"
            ]
            if metadata:
                for k, v in (metadata or {}).items():
                    if v: cmd_v += ["-metadata", f"{k}={v}"]
            abs_output = os.path.abspath(output_path)
            cmd_v.append(abs_output)

            v_ok, v_err = await _run_cmd(cmd_v, 86400)
            try:
                if os.path.exists(tmp_audio): os.remove(tmp_audio)
            except Exception: pass
            if v_ok and os.path.exists(abs_output) and os.path.getsize(abs_output) > 100:
                return True, ""
            return False, v_err

        elif mtype == "video":
            # Pure video concat with optional speed adjustment (no image overlay)
            cmd2 = ["ffmpeg", "-y", "-threads", "2",
                    "-f", "concat", "-safe", "0", "-i", lst]
            vf = f"setpts={1.0/speed:.4f}*PTS" if abs(speed - 1.0) > 0.001 else ""
            if vf: cmd2 += ["-vf", vf]
            if atempo: cmd2 += ["-af", atempo]
            cmd2 += ["-c:v", "libx264", "-preset", "superfast", "-crf", "28",
                     "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart",
                     "-max_muxing_queue_size", "4096"]
            if metadata:
                for k, v in (metadata or {}).items():
                    if v: cmd2 += ["-metadata", f"{k}={v}"]
            abs_output = os.path.abspath(output_path)
            cmd2.append(abs_output)
            ok2, err2 = await _run_cmd(cmd2, 86400)
            if ok2 and os.path.exists(abs_output) and os.path.getsize(abs_output) > 100:
                return True, ""
            return False, err2

        else:
            # Pure audio re-encode: filter_complex concat (handles mixed codecs mp3+m4a+ogg).
            # NOTE: This path is only used for CHUNK merges (≤5 files).
            # Final combine of uniform parts is handled by the lossless path above.
            cmd2 = ["ffmpeg", "-y", "-threads", "2"]
            for p in file_list:
                cmd2 += ["-i", os.path.abspath(p)]
            fc = "".join(f"[{i}:a]" for i in range(len(file_list))) + f"concat=n={len(file_list)}:v=0:a=1[a1]"
            if atempo:
                fc += f";[a1]{atempo}[a2]"
            map_lbl = "[a2]" if atempo else "[a1]"

            if cover and os.path.exists(cover):
                cmd2 += ["-i", os.path.abspath(cover)]
                cov_idx = len(file_list)
                cmd2 += ["-filter_complex", fc, "-map", map_lbl, "-map", f"{cov_idx}:v",
                         "-id3v2_version", "3",
                         "-metadata:s:v", "title=Album cover",
                         "-metadata:s:v", "comment=Cover (front)",
                         "-c:a", "libmp3lame", "-b:a", "192k", "-ar", "48000",
                         "-max_muxing_queue_size", "4096"]
            else:
                cmd2 += ["-filter_complex", fc, "-map", map_lbl,
                         "-c:a", "libmp3lame", "-b:a", "192k", "-ar", "48000",
                         "-max_muxing_queue_size", "4096"]
            if metadata:
                for k, v in (metadata or {}).items():
                    if v: cmd2 += ["-metadata", f"{k}={v}"]
            abs_output = os.path.abspath(output_path)
            cmd2.append(abs_output)
            ok2, err2 = await _run_cmd(cmd2, 86400)
            if ok2 and os.path.exists(abs_output) and os.path.getsize(abs_output) > 100:
                return True, ""
            return False, err2

    except asyncio.TimeoutError:
        return False, "FFmpeg timed out"
    except Exception as e:
        return False, str(e)
    finally:
        try:
            if os.path.exists(lst): os.remove(lst)
            if os.path.exists(vconcat_txt): os.remove(vconcat_txt)
        except: pass


# ══════════════════════════════════════════════════════════════════════════════
# Pre-download size scanner
# ══════════════════════════════════════════════════════════════════════════════
async def _scan_total_size(client, from_chat, start_id, end_id):
    """Scan all messages in range and return (total_size_bytes, media_count) using
    Telegram metadata only — NO file downloads."""
    await _safe_resolve_peer(client, from_chat)
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
CHUNK_SIZE   = 5          # Reduced to 10 to keep RAM footprint low
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
    _sem_acquired = False  # Track semaphore for release in finally

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
        make_video = bool(job.get("make_video", False))
        upload_to_yt = bool(job.get("upload_to_yt", False))

        await _db_up(jid, status="queued", error="", created_at=time.time())

        # ── Semaphore queue: notify user of position, then wait ───────────────
        if _mg_semaphore._value == 0:   # all slots busy
            queue_pos = max(1, len(_mg_tasks))  # rough position estimate
            try:
                await bot.send_message(uid,
                    f"⏳ <b>Merger Queue</b>\n\n"
                    f"All merge slot(s) are busy (max {MAX_CONCURRENT_MERGES} at once).\n"
                    f"Your job <code>[{jid[-6:]}]</code> is <b>queued at position #{queue_pos}</b>.\n"
                    f"It will start automatically when a slot frees up. "
                    f"You can freely close Telegram; the job will run in the background.")
            except Exception: pass

        # Acquire semaphore — blocks until a slot is free
        await _mg_semaphore.acquire()
        _sem_acquired = True

        fresh = await _db_get(jid)
        if not fresh or fresh.get("status") in ("stopped", "paused"):
            return  # semaphore released in finally
        # Notify user their job is now starting (if it was queued)
        try:
            await bot.send_message(uid,
                f"▶️ <b>Merger Starting</b> — Job <code>[{jid[-6:]}]</code> now has a slot and is beginning.")
        except Exception: pass
        await _db_up(jid, status="scanning", error="")

        # ══════════════════════════════════════════════════════════════════
        # PHASE 0 — Pre-download size scan
        # ══════════════════════════════════════════════════════════════════
        try:
            me = await client.get_me()
            if from_chat == me.id or from_chat == me.username:
                from_chat = uid
        except:
            pass

        await _safe_resolve_peer(client, from_chat)

        try:
            scan_msg = await bot.send_message(uid,
                f"<b>🔍 Scanning file sizes before download...</b>\n"
                f"<i>Range: {start_id} → {end_id}</i>")
        except: scan_msg = None

        est_size, media_count = await _scan_total_size(client, from_chat, start_id, end_id)

        MAX_TOTAL_GB = 5.0
        MAX_FILES = 150

        if est_size > MAX_TOTAL_GB * 1024**3 or media_count > MAX_FILES:
            msg = (f"<b>❌ Pre-scan blocked your request:</b>\n"
                   f"Found {media_count} files ({_sz(est_size)}).\n\n"
                   f"Server limit is {MAX_FILES} files and {MAX_TOTAL_GB:.0f}GB per merge to prevent Out of Memory errors. "
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

        # Cover — load from job dir
        cover = None
        if job.get("has_cover"):
            _cp = os.path.abspath(os.path.join(wdir, "cover.jpg"))
            if os.path.exists(_cp):
                cover = _cp
        
        # Video-specific cover image (separate from MP3 cover art)
        video_cover = None
        if job.get("has_video_cover"):
            _vcp = os.path.abspath(os.path.join(wdir, "video_cover.jpg"))
            if os.path.exists(_vcp):
                video_cover = _vcp

        # Outro image (video padding)
        outro_cover = None
        if job.get("use_4auto_outros"):
            outro_cover = [
                os.path.abspath("assets/outro_1.jpg"),
                os.path.abspath("assets/outro_2.jpg"),
                os.path.abspath("assets/outro_3.jpg"),
                os.path.abspath("assets/outro_4.jpg"),
            ]
        elif job.get("has_outro_cover"):
            _ocp = os.path.abspath(os.path.join(wdir, "outro_cover.jpg"))
            if os.path.exists(_ocp):
                outro_cover = _ocp

        # ══════════════════════════════════════════════════════════════════
        # PHASE 1 — Collect all media message IDs in strict order
        # ══════════════════════════════════════════════════════════════════
        await _db_up(jid, status="downloading", error="", phase_start_ts=time.time())
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
                MAX_DL_RETRIES = 20  # extra retries for CDN-empty-file bug
                for att in range(MAX_DL_RETRIES):
                    # Use a fresh temp path every attempt to work around
                    # Telegram CDN serving a cached empty/corrupt file
                    temp_dlp = dlp if att == 0 else dlp.replace(ext, f"_r{att}{ext}")
                    try:
                        import time
                        target_msg = msg
                        # File references expire after 1 hour. Refresh if retrying or if old!
                        if att > 0 or (time.time() - job.get("phase_start_ts", time.time())) > 2400:
                            try:
                                fm = await client.get_messages(from_chat, msg.id)
                                if fm and not getattr(fm, "empty", True):
                                    target_msg = fm
                            except: pass

                        fp = await client.download_media(target_msg, file_name=temp_dlp)
                        if fp and os.path.exists(fp):
                            fsz_check = os.path.getsize(fp)
                            if fsz_check > 100:   # ≥100 bytes = real file
                                # Rename back to canonical path
                                if fp != dlp:
                                    try:
                                        os.replace(fp, dlp)
                                        fp = dlp
                                    except Exception: pass
                                break
                            else:
                                # CDN returned empty/tiny file — remove and retry
                                logger.warning(f"[MG {jid}] Attempt {att+1}: {os.path.basename(fp)} is {fsz_check}B, retrying (CDN cache miss)")
                                try: os.remove(fp)
                                except Exception: pass
                                fp = None
                                # Progressive back-off: 3s, 6s, 12s...
                                await asyncio.sleep(min(3 * (att + 1), 30))
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2)
                    except Exception as e:
                        estr = str(e)
                        if any(k in estr for k in ("Timeout", "Connection", "Read", "MessageNotModified", "reset")):
                            await asyncio.sleep(min(3 * (att + 1), 30))
                        else:
                            logger.error(f"[MG {jid}] Download error msg {msg.id}: {e}")
                            if att >= 5: break  # give up on non-transient errors sooner

                if not fp or not os.path.exists(fp) or os.path.getsize(fp) < 100:
                    # ── Download failed: ask user SKIP / RETRY / ABORT ─────────────
                    from pyrogram.types import InlineKeyboardButton as IKB, InlineKeyboardMarkup as IKM
                    _dl_key = f"mg_dl_choice_{jid}"
                    _mg_dl_choices.pop(_dl_key, None)

                    err_notice = await bot.send_message(
                        uid,
                        f"⚠️ <b>Download Failed — File #{global_seq+1}</b>\n\n"
                        f"Message ID: <code>{msg.id}</code>\n"
                        f"Tried <b>{MAX_DL_RETRIES} times</b> — the file may be temporarily unavailable or deleted.\n\n"
                        f"<b>What would you like to do?</b>\n"
                        f"• <b>Skip</b> — skip this file and continue the merge\n"
                        f"• <b>Retry</b> — try downloading again (20 more attempts)\n"
                        f"• <b>Abort</b> — stop the entire merge job",
                        reply_markup=IKM([[
                            IKB("⏭ Sᴋɪᴘ",  callback_data=f"mg_dl#skip#{jid}"),
                            IKB("🔄 Rᴇᴛʀʏ", callback_data=f"mg_dl#retry#{jid}"),
                            IKB("⛔ Aʙᴏʀᴛ", callback_data=f"mg_dl#abort#{jid}"),
                        ]])
                    )

                    # Wait up to 90s for user choice; default=skip
                    _mg_dl_choices[_dl_key] = None
                    for _wi in range(90):
                        await asyncio.sleep(1)
                        choice = _mg_dl_choices.get(_dl_key)
                        if choice is not None:
                            break
                    else:
                        choice = "skip"  # auto-skip after timeout
                    _mg_dl_choices.pop(_dl_key, None)

                    try: await err_notice.delete()
                    except: pass

                    if choice == "abort":
                        await _db_up(jid, status="error",
                            error=f"User aborted after download failure on file #{global_seq+1} (msg {msg.id}).")
                        await bot.send_message(uid, "⛔ <b>Merge Aborted</b> by your request.")
                        return

                    elif choice == "retry":
                        # Reset fp and retry the entire download loop again
                        fp = None
                        for att2 in range(MAX_DL_RETRIES):
                            temp_dlp2 = dlp.replace(ext, f"_r2_{att2}{ext}")
                            try:
                                fp = await client.download_media(msg, file_name=temp_dlp2)
                                if fp and os.path.exists(fp) and os.path.getsize(fp) > 100:
                                    if fp != dlp:
                                        try: os.replace(fp, dlp); fp = dlp
                                        except: pass
                                    break
                                else:
                                    if fp and os.path.exists(fp): os.remove(fp)
                                    fp = None
                                    await asyncio.sleep(min(3 * (att2 + 1), 30))
                            except Exception:
                                await asyncio.sleep(min(3 * (att2 + 1), 30))
                        if not fp or not os.path.exists(fp) or os.path.getsize(fp) < 100:
                            await bot.send_message(uid,
                                f"⚠️ <b>Retry also failed for file #{global_seq+1}</b>. Skipping.")
                            choice = "skip"

                    if choice == "skip" or (choice == "retry" and (not fp or os.path.getsize(fp) < 100)):
                        await bot.send_message(uid,
                            f"⏭ <b>Skipped file #{global_seq+1}</b> (msg {msg.id}).\n"
                            f"<i>The merge will continue without this file. A gap may exist in the output.</i>")
                        global_seq += 1
                        continue  # skip to next file in chunk
                
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

                # Probe duration for log, fallback to Telegram API duration if ffprobe fails
                dur = _ffprobe_duration(fp)
                if dur <= 0 and media_obj:
                    dur = float(getattr(media_obj, 'duration', 0) or 0)
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

            # End download phase — record dl_time
            _dl_end = time.time()
            _dl_time = _dl_end - (job.get("phase_start_ts") or _dl_end)

            # Partial merge of this chunk
            await _db_up(jid, status="merging", dl_time=_dl_time, phase_start_ts=time.time())
            part_ext  = ".mp4" if mtype == "video" else ".mp3"
            part_path = os.path.join(wdir, f"part_{chunk_num:04d}{part_ext}")

            try:
                if status_msg: await status_msg.edit_text(
                    f"<b>🔀 {chunk_label} — Merging {len(chunk_files)} files→ part {chunk_num}</b>")
            except: pass

            chunk_files_sorted = sorted(chunk_files, key=lambda p: os.path.basename(p))
            
            chunk_dur = sum(_ffprobe_duration(f) for f in chunk_files_sorted)
            last_edit = [time.time()]
            async def chunk_prog(cur_secs):
                now = time.time()
                if now - last_edit[0] > 5:
                    pct = min(100, int((cur_secs / max(chunk_dur, 0.1)) * 100))
                    try:
                        if status_msg: await status_msg.edit_text(
                            f"<b>🔀 {chunk_label} — Merging {len(chunk_files)} files→ part {chunk_num}</b>\n"
                            f"<code>{_bar(pct, 100)}</code>\n"
                            f"⏳ Progress: {pct}%"
                        )
                    except: pass
                    last_edit[0] = now

            # ── Pre-flight: verify every file exists and is non-empty ─────
            bad = [(p, "missing" if not os.path.exists(p) else "empty")
                   for p in chunk_files_sorted
                   if not os.path.exists(p) or os.path.getsize(p) == 0]
            if bad:
                desc = "; ".join(f"{os.path.basename(p)} ({r})" for p, r in bad)
                emsg = f"Pre-merge check warned — {len(bad)} file(s) not usable: {desc}"
                logger.warning("[MG %s] %s", jid, emsg)
                try:
                    await bot.send_message(uid, f"⚠️ <b>{emsg}</b>\n<i>Skipping these files to prevent crash...</i>")
                except: pass
                
                # Filter out bad files and continue so the 23-hour merge isn't killed
                chunk_files_sorted = [p for p in chunk_files_sorted if os.path.exists(p) and os.path.getsize(p) > 0]
                if not chunk_files_sorted:
                    logger.warning("[MG %s] Chunk %d is completely empty after filtering, skipping chunk.", jid, chunk_num)
                    continue

            # Chunk parts: apply speed chunk-by-chunk to save MASSIVE amounts of RAM
            ok, err = await _ffmpeg_merge(
                chunk_files_sorted, part_path, None, mtype, None, speed, False, progress_cb=chunk_prog, is_chunk=True)

            if not ok:
                await _db_up(jid, status="error", error=f"Chunk {chunk_num} merge failed: {err[:400]}")
                # Show up to 1200 chars so the actual error is readable (banner already stripped)
                await bot.send_message(uid,
                    f"<b>❌ Chunk {chunk_num} merge failed:</b>\n<code>{err[:1200]}</code>")
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
        
        # If make_video is true, output will be mp4 regardless of mtype
        out_ext  = ".mp4" if (mtype == "video" or make_video) else ".mp3"
        out_path = os.path.abspath(os.path.join(wdir, f"{out_name}{out_ext}"))

        # Use video_cover for video creation; fall back to audio cover if no dedicated one
        effective_cover_for_video = video_cover or cover

        _vid_text = '\n🎥 Building MP4 video...' if make_video else ''
        # Speed was already applied during chunk merging (Phase 2) — final combine is always 1.0x
        _spd_text = f'⚡ Speed {speed}x already applied during chunk merge' if abs(speed-1.0)>0.001 else '🎯 Lossless combine (speed=1.0x)'
        final_status_msg = None
        try:
            final_status_msg = await bot.send_message(uid,
                f"<b>🔀 Final combine: {len(part_files)} parts → {out_name}{out_ext}</b>\n"
                f"{_spd_text}{_vid_text}")
        except: pass

        part_files_sorted = sorted(part_files, key=lambda p: os.path.basename(p))
        final_dur = cumulative_secs
        last_edit2 = [time.time()]
        _fsm = final_status_msg  # captured ref — avoids stale closure on last chunk's status_msg
        async def final_prog(cur_secs):
            now = time.time()
            if now - last_edit2[0] > 5:
                pct = min(100, int((cur_secs / max(final_dur, 0.1)) * 100))
                try:
                    if _fsm: await _fsm.edit_text(
                        f"<b>🔀 Final combine: {len(part_files)} parts → {out_name}{out_ext}</b>\n"
                        f"<code>{_bar(pct, 100)}</code>\n"
                        f"⏳ Progress: {pct}%"
                    )
                except: pass
                last_edit2[0] = now

        # Speed already applied in Phase 2, so enforce 1.0x here
        ok, err = await _ffmpeg_merge(
            part_files_sorted, out_path, metadata, mtype,
            cover, 1.0, make_video, effective_cover_for_video, outro_cover, cumulative_secs, progress_cb=final_prog)

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
                lf.write(f"Generated: {_ist_str()} \n")
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
        if fsize > 3.9 * 1024**3:
            await _db_up(jid, status="error", error=f"Too large: {_sz(fsize)}")
            try: await bot.send_message(uid, f"<b>❌ {_sz(fsize)} exceeds 4GB limit.</b>")
            except: pass
            return

        # ── Phase 3: Upload ───────────────────────────────────────────────
        up_start = time.time()
        # Calculate merge_time from final combine start to now
        _merge_time = up_start - (job.get("phase_start_ts") or up_start)
        await _db_up(jid, status="uploading", merge_time=_merge_time, phase_start_ts=up_start)

        caption = f"<b>🔀 {out_name}{out_ext}</b>\n📁 {global_seq} files • {_sz(fsize)}"
        if metadata.get("title"): caption += f"\n🎵 {metadata['title']}"
        if metadata.get("artist"): caption += f" — {metadata['artist']}"

        all_dests = [uid] + [d for d in dest_chats if d != uid]
        thumb = cover if cover and os.path.exists(cover) else None

        avg_dl_speed = max(dl_total_bytes, 1) / 60  # estimate: assume ~1min DL time for speed calc
        up_eta_static = fsize / avg_dl_speed
        await _db_up(jid, dl_eta=0, mg_eta=0, up_eta=up_eta_static, total_eta=up_eta_static)

        up_state = {"last": 0, "last_bytes": 0, "last_ts": up_start, "speed_bps": 0}
        async def _up_prog(current, total):
            now = time.time()
            if now - up_state["last"] >= 3:
                elapsed = now - up_state["last_ts"]
                delta_bytes = current - up_state["last_bytes"]
                if elapsed > 0 and delta_bytes > 0:
                    up_state["speed_bps"] = delta_bytes / elapsed
                up_state["last_ts"] = now
                up_state["last_bytes"] = current
                up_state["last"] = now
                # ETA based on live speed
                speed = up_state["speed_bps"]
                eta = (total - current) / max(speed, 1) if speed > 0 else 0
                await _db_up(jid, dl_eta=0, mg_eta=0, up_eta=eta, total_eta=eta)

        for dest in all_dests:
            await _safe_resolve_peer(client, dest)
            
        replace_target = job.get("replace_target")
        if replace_target:
            dest = replace_target["chat_id"]
            mid = replace_target["msg_id"]
            await _safe_resolve_peer(client, dest)
            for att in range(3):
                try:
                    from pyrogram.types import InputMediaDocument, InputMediaAudio, InputMediaVideo
                    if mtype == "video":
                        media = InputMediaVideo(out_path, caption=caption, supports_streaming=True, thumb=thumb)
                    else:
                        kw = {"media": out_path, "caption": caption, "thumb": thumb}
                        if metadata.get("title"): kw["title"] = metadata["title"]
                        if metadata.get("artist"): kw["performer"] = metadata["artist"]
                        media = InputMediaAudio(**kw)
                        
                    await client.edit_message_media(chat_id=dest, message_id=mid, media=media)
                    # Notify DM
                    if dest != uid:
                        try: await bot.send_message(uid, f"<b>✅ Channel post successfully replaced!</b>")
                        except: pass
                    break
                except FloodWait as fw: await asyncio.sleep(fw.value+2)
                except Exception as e:
                    if att < 2: await asyncio.sleep(5); continue
                    logger.warning(f"[MG {jid}] replace_media {dest}:{mid} failed: {e}")
                    break
        else:
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
        await _db_up(jid, up_time=up_time)

        # Upload log file if generated
        if log_path and os.path.exists(log_path):
            try:
                await client.send_document(chat_id=uid, document=log_path,
                    caption=f"<b>📋 Processing Log</b>\n{out_name}")
            except: pass

        # ── Phase 4: YouTube Upload ───────────────────────────────────────
        yt_msg = ""
        _yt_start = time.time()
        if upload_to_yt:
            await _db_up(jid, status="yt_uploading", phase_start_ts=_yt_start)
            try:
                from plugins.youtube import upload_video_to_youtube
                yt_status = await bot.send_message(uid, f"<b>⬆️ Uploading to YouTube...</b>\n<i>Please wait, large files take time.</i>")
                
                yt_title_custom = job.get("yt_title")
                title = yt_title_custom or metadata.get("title") or getattr(metadata, "artist", "") or f"{out_name}"
                title = title[:100] # YouTube limit
                
                yt_thumb_custom = None
                if job.get("has_yt_thumb"):
                    _ytp = os.path.abspath(os.path.join(wdir, "yt_thumb.jpg"))
                    if os.path.exists(_ytp): yt_thumb_custom = _ytp
                
                start_epi = job.get("yt_start_epi")
                
                # Sequential timestamp generation — correct episode numbering
                # Each file in log_entries corresponds to one episode in strict order.
                # We do NOT extract numbers from filenames (that caused "Episode 3" everywhere).
                # Instead, we assign episode numbers sequentially: start_epi, start_epi+1, ...
                yt_timestamps = ""
                seq_epi = int(start_epi) if start_epi is not None else 1
                total_epi_count = seq_epi  # track last for description

                for tc, original_name, _ in log_entries:
                    yt_timestamps += f"{tc} Episode {seq_epi}\n"
                    total_epi_count = seq_epi
                    seq_epi += 1

                desc_hindi = f"हे अजनबियों, मैं आर्य बॉट [आपका दोस्त] हूँ। मैंने सफलतापूर्वक '{title}' को 'The Last Broadcast' पर मर्ज और अपलोड कर दिया है। मैंने इसे अपने टेलीग्राम डेटाबेस से एकत्र किया है और इसे [{start_epi or 1}-{total_epi_count}] के उसी क्रम में मर्ज/अपलोड किया है।\n\nचूंकि यह एक स्वचालित प्रक्रिया है, इसलिए आपको कुछ समस्याएं मिल सकती हैं—जैसे एपिसोड के क्रम में गड़बड़ी (जैसे कि एपिसोड 11, 10 से पहले), कुछ एपिसोड का छूटना, थोड़ी गुणवत्ता में कमी या अन्य असंगतताएं। यदि आपको कोई समस्या आती है, तो आप टिप्पणियों में रिपोर्ट कर सकते हैं। बेहतर सुविधा के लिए, टाइमस्टैम्प नीचे दिए गए हैं ताकि आप आसानी से एपिसोड के बीच नेविगेट कर सकें।"

                desc_english = f"Hey Strangers, I'm Arya Bot [Your Friend]. I successfully merged and uploaded '{title}' on The Last Broadcast. I collected this from my Telegram database and merged/uploaded it in the same order. Episodes {start_epi or 1}–{total_epi_count} are included.\n\nYou may notice some issues such as episode order mismatches, missing episodes, slight quality loss, or other inconsistencies. If you face issues, you can report them in the comments. Since this is an automated process, some limitations may exist. For better navigation, timestamps are provided below so you can jump between episodes easily."

                support_msg = "If my work has helped you in any way, you can support me as per your wish by visiting this link: https://razorpay.me/@SusJeetX and sending any amount (minimum 50 INR). This will help me continue providing more stories like this."
                support_msg_hi = "यदि मेरे कार्य से आपको किसी भी प्रकार की सहायता मिली है, तो आप इस लिंक पर जाकर अपनी इच्छानुसार मुझे समर्थन दे सकते हैं: https://razorpay.me/@SusJeetX और कोई भी राशि (न्यूनतम 50 INR) भेज सकते हैं। इससे मुझे इस तरह की और कहानियाँ प्रदान करने में मदद मिलेगी।"

                copyright_msg = "Warning: Copyright issues may occur at any time. Join my Telegram channel: https://t.me/StoriesByJeetXNew to get all stories and updates about the new YouTube channel, so you don't miss any content."
                copyright_msg_hi = "चेतावनी: किसी भी समय कॉपीराइट की समस्या आ सकती है। मेरे टेलीग्राम चैनल: https://t.me/StoriesByJeetXNew से जुड़ें ताकि आपको सभी कहानियाँ और नए YouTube चैनल के बारे में अपडेट मिलते रहें, और आप कोई भी सामग्री मिस न करें।"

                desc = (f"{desc_hindi}\n\n{support_msg_hi}\n\n{copyright_msg_hi}"
                        f"\n\n───────────────────────────\n\n"
                        f"{desc_english}\n\n{support_msg}\n\n{copyright_msg}"
                        f"\n\n───────────────────────────\n\n"
                        f"TIMESTAMPS / CHAPTERS\n\n{yt_timestamps}")
                success, yt_res = await upload_video_to_youtube(
                    video_path=out_path,
                    title=title,
                    description=desc,
                    privacy_status="private",
                    thumbnail_path=yt_thumb_custom
                )
                
                if success:
                    yt_vid_id = yt_res.split("/")[-1] if "/" in yt_res.replace("youtu.be/", "/") else None
                    if not yt_vid_id: yt_vid_id = yt_res
                    yt_msg = f"┃ 🟥 YouTube: <a href='{yt_res}'>Private Link</a>\n"
                    await yt_status.edit_text(f"<b>✅ YouTube Upload Successful!</b>\n{yt_res}")
                    try:
                        await _db_up(jid, yt_video_id=yt_vid_id, yt_title=title)
                    except Exception:
                        pass
                else:
                    yt_vid_id = None
                    yt_msg = f"┃ 🟥 YouTube: Failed\n"
                    await yt_status.edit_text(f"<b>❌ YouTube Upload Failed</b>\n<code>{yt_res}</code>")
            except Exception as e:
                logger.error(f"YouTube exception: {e}")
                yt_msg = f"┃ 🟥 YouTube: Error\n"
                yt_vid_id = None

        _yt_time = time.time() - _yt_start if upload_to_yt else 0
        total_time = dl_total_bytes and (up_time + _yt_time)  # compat field
        _total = (job.get("dl_time") or 0) + (job.get("merge_time") or 0) + up_time + _yt_time
        await _db_up(jid, status="done", total_time=_total, yt_time=_yt_time, file_size=fsize, log_entries=log_entries)

        markup = None
        if upload_to_yt and 'yt_vid_id' in locals() and yt_vid_id:
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Eᴅɪᴛ Yᴛ Vɪᴅᴇᴏ Dᴇᴛᴀɪʟs", callback_data=f"mg#yt_edit#{jid}")]
            ])

        try:
            await bot.send_message(uid,
                f"<b>✅ Merge Complete!</b>\n\n"
                f"╭───── 📊 ─────╮\n"
                f"┃ 📁 Files: {global_seq}\n"
                f"┃ 📦 {out_name}{out_ext}\n"
                f"┃ 💾 {_sz(fsize)}\n"
                f"┃ ⚡ Speed: {speed}x\n"
                f"┃ ⬆️ Upload: {_tm(up_time)}\n"
                f"┃ ⏱ Total chunks: {total_chunks}\n"
                f"{yt_msg}"
                f"╰─────────────╯", reply_markup=markup)
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
        if _sem_acquired:
            try: _mg_semaphore.release()
            except Exception: pass
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
            [InlineKeyboardButton(f"➕ Cʀᴇᴀᴛᴇ{label}Mᴇʀɢᴇ", callback_data=f"mg#new_{mtype}")],
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

        now_str = _ist_str('%I:%M:%S %p IST')
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

        btns_list.append([InlineKeyboardButton(f"➕ Cʀᴇᴀᴛᴇ{label}Mᴇʀɢᴇ", callback_data=f"mg#new_{mtype}")])
        btns_list.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ", callback_data=f"mg#{mtype}_list")])
        btns_list.append([InlineKeyboardButton("↩ Bᴀᴄᴋ", callback_data="mg#main")])
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

    # ── Main Menu ────────────────────────────────────────────────────────
    if action == "main":
        text = "<b>❪ Mᴇʀɢᴇʀ Sʏsᴛᴇᴍ ❫</b>\n\nChoose which type of merger you want to use:"
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("Mᴇʀɢᴇ Aᴜᴅɪᴏ", callback_data="mg#audio_list")],
            [InlineKeyboardButton("Mᴇʀɢᴇ Vɪᴅᴇᴏ", callback_data="mg#video_list")],
            [InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="back")]
        ])
        return await query.message.edit_text(text, reply_markup=btns)

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
        text = _build_info_text(job)

        info_btns = [
            [InlineKeyboardButton("🔄 Rᴇꜰʀᴇsʜ", callback_data=f"mg#info#{param}")],
        ]
        # Only show Edit YT button if the job is done and has a YouTube video ID stored
        if job.get("status") == "done" and job.get("yt_video_id"):
            info_btns.append([InlineKeyboardButton("✏️ Eᴅɪᴛ Yᴛ Tɪᴛʟᴇ/Dᴇsᴄ", callback_data=f"mg#yt_edit#{param}")])
        info_btns.append([InlineKeyboardButton("↩ Bᴀᴄᴋ", callback_data=f"mg#{mtype}_list")])
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(info_btns))

    # ── Edit YouTube Video Title/Description ──────────────────────────────
    elif action == "yt_edit":
        job = await _db_get(param)
        if not job: return await query.answer("Not found!", show_alert=True)
        if not job.get("yt_video_id"):
            return await query.answer("No YouTube video linked to this job.", show_alert=True)
        await query.message.delete()
        try:
            r = await _mg_ask(bot, uid,
                "<b>✏️ Edit YouTube Video</b>\n\n"
                f"Current video: <code>https://youtu.be/{job['yt_video_id']}</code>\n\n"
                "The bot will re-generate the description and update the YouTube video title and description "
                "from this job's stored data.\n\n"
                "Send <b>CONFIRM</b> to proceed, or /cancel to abort.",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("CONFIRM")], [KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]],
                    resize_keyboard=True, one_time_keyboard=True))
            if getattr(r, "text", None) and any(x in str(r.text).lower() for x in ["cancel", "cᴀɴᴄᴇʟ", "⛔", "/cancel"]):
                return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            await bot.send_message(uid, "<code>Updating YouTube video…</code>", reply_markup=ReplyKeyboardRemove())
            # Re-generate description from stored job data
            import re as _re
            log_entries = job.get("log_entries", [])
            start_epi = job.get("yt_start_epi")
            title = job.get("yt_title") or job.get("output_name") or "Untitled"
            # Rebuild timestamps sequentially
            yt_timestamps = ""
            seq_epi = int(start_epi) if start_epi is not None else 1
            total_epi_count = seq_epi
            for tc, orig_name, _ in log_entries:
                yt_timestamps += f"{tc} Episode {seq_epi}\n"
                total_epi_count = seq_epi
                seq_epi += 1
            if not yt_timestamps:
                yt_timestamps = "0:00 Episode 1\n"
            desc_hindi = (f"हे अजनबियों, मैं आर्य बॉट [आपका दोस्त] हूँ। मैंने सफलतापूर्वक '{title}' को 'The Last Broadcast' पर मर्ज और अपलोड कर दिया है। "
                          f"मैंने इसे अपने टेलीग्राम डेटाबेस से एकत्र किया है और इसे [{start_epi or 1}-{total_epi_count}] के उसी क्रम में मर्ज/अपलोड किया है।\n\n"
                          "चूंकि यह एक स्वचालित प्रक्रिया है, इसलिए आपको कुछ समस्याएं मिल सकती हैं। बेहतर सुविधा के लिए, टाइमस्टैम्प नीचे दिए गए हैं।")
            desc_english = (f"Hey Strangers, I'm Arya Bot [Your Friend]. I successfully merged and uploaded '{title}' on The Last Broadcast. "
                            f"Episodes {start_epi or 1}–{total_epi_count} are included.\n\nFor better navigation, timestamps are provided below.")
            support_msg = "If my work has helped you, support me: https://razorpay.me/@SusJeetX (minimum 50 INR)."
            support_msg_hi = "यदि मेरे कार्य से आपको सहायता मिली है, तो आप इस लिंक पर जाकर मुझे समर्थन दें: https://razorpay.me/@SusJeetX (न्यूनतम 50 INR)।"
            copyright_msg = "Warning: Copyright issues may occur. Join: https://t.me/StoriesByJeetXNew"
            copyright_msg_hi = "चेतावनी: कॉपीराइट समस्या आ सकती है। जुड़ें: https://t.me/StoriesByJeetXNew"
            new_desc = (f"{desc_hindi}\n\n{support_msg_hi}\n\n{copyright_msg_hi}"
                        f"\n\n───────────────────────────\n\n"
                        f"{desc_english}\n\n{support_msg}\n\n{copyright_msg}"
                        f"\n\n───────────────────────────\n\n"
                        f"TIMESTAMPS / CHAPTERS\n\n{yt_timestamps}")
            try:
                from plugins.youtube import update_youtube_video
                success, msg2 = await update_youtube_video(
                    video_id=job["yt_video_id"],
                    title=title[:100],
                    description=new_desc[:5000]
                )
                if success:
                    await bot.send_message(uid, f"<b>✅ YouTube video updated!</b>\n{msg2}")
                else:
                    await bot.send_message(uid, f"<b>❌ Update failed:</b> <code>{msg2}</code>")
            except Exception as yt_e:
                await bot.send_message(uid, f"<b>❌ Error updating YouTube video:</b> <code>{yt_e}</code>")
        except Exception as fe:
            logger.warning(f"yt_edit error: {fe}")
            await bot.send_message(uid, "<b>Cancelled or timed out.</b>", reply_markup=ReplyKeyboardRemove())
        await bot.send_message(uid, "Use /merge or /settings to see the job list.")

    elif action == "rename":
        await query.message.delete()
        try:
            r = await _mg_ask(bot, uid,
                "<b>✏️ Send new name for this merge job:</b>",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]],
                    resize_keyboard=True, one_time_keyboard=True))
            if "/cancel" not in r.text.lower():
                await _db_up(param, name=r.text.strip()[:100])
                await bot.send_message(uid,
                    f"✅ Renamed to <b>{r.text.strip()[:100]}</b>",
                    reply_markup=ReplyKeyboardRemove())
            else:
                await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>",
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
        if not msg.text or (getattr(msg, 'text', None) and any(x in msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔'])):
            return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

        sel_name = msg.text.split(" ", 1)[1] if " " in msg.text else msg.text
        acc = next((a for a in accounts if a["name"] == sel_name), None)
        if not acc:
            return await bot.send_message(uid, "<b>❌ Account not found.</b>", reply_markup=ReplyKeyboardRemove())

        # Step 2: Start link
        msg = await _mg_ask(bot, uid,
            "<b>Step 2/7:</b> Send <b>start file link</b>\n\n"
            "<i>Example: https://t.me/c/123456/100</i>",
            reply_markup=ReplyKeyboardRemove())
        if not msg.text or (('cancel' in msg.text.lower() or 'cᴀɴᴄᴇʟ' in msg.text.lower() or '⛔' in msg.text) or 'cᴀɴᴄᴇʟ' in msg.text.lower() or '⛔' in msg.text):
            return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        ref_s, sid = _parse_link(msg.text)
        if sid is None:
            return await bot.send_message(uid, "<b>❌ Invalid link.</b>")
        from_chat = ref_s if ref_s else None

        # Step 3: End link
        msg = await _mg_ask(bot, uid,
            "<b>Step 3/7:</b> Send <b>end file link</b>")
        if not msg.text or (('cancel' in msg.text.lower() or 'cᴀɴᴄᴇʟ' in msg.text.lower() or '⛔' in msg.text) or 'cᴀɴᴄᴇʟ' in msg.text.lower() or '⛔' in msg.text):
            return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        ref_e, eid = _parse_link(msg.text)
        if eid is None:
            return await bot.send_message(uid, "<b>❌ Invalid link.</b>")
        if from_chat is None and ref_e:
            from_chat = ref_e
        if from_chat is None:
            return await bot.send_message(uid, "<b>❌ Could not detect channel.</b>")
        if sid > eid: sid, eid = eid, sid
        total = eid - sid + 1

        # Size validation is now properly handled by _scan_total_size phase checking actual files

        # Step 4: Destination
        channels = await db.get_user_channels(uid)
        dest_chats = []
        replace_target = None
        if channels:
            ch_kb = [[f"📢 {ch['title']}"] for ch in channels]
            ch_kb.append(["🔄 Replace Existing Post"])
            ch_kb.append(["⏭ Skip (DM only)"]); ch_kb.append(["❌ Cancel"])
            msg = await _mg_ask(bot, uid,
                f"<b>Step 4/7:</b> Destination channel\n<b>Range:</b> {sid}→{eid} ({total} msgs)",
                reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
            if not msg.text or (getattr(msg, 'text', None) and any(x in msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔'])):
                return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            
            if "Replace" in msg.text:
                rep_msg = await _mg_ask(bot, uid,
                    "<b>Step 4b/7: Send the exact Telegram Link of the message you want to replace:</b>\n"
                    "<i>(e.g., https://t.me/c/12345/678)</i>",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True, one_time_keyboard=True))
                if not rep_msg.text or any(x in rep_msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
                    return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                link = rep_msg.text.strip()
                match = re.search(r't\.me/(?:c/)?([^/]+)/(\d+)', link)
                if not match:
                    return await bot.send_message(uid, "<b>❌ Invalid Telegram Link. Defaulting to DM.</b>", reply_markup=ReplyKeyboardRemove())
                else:
                    c_str, m_str = match.groups()
                    mid = int(m_str)
                    r_cid = int("-100" + c_str) if c_str.isdigit() else (c_str if c_str.startswith("@") else f"@{c_str}")
                    replace_target = {"chat_id": r_cid, "msg_id": mid}
            elif "Skip" not in msg.text:
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
        if not msg.text or (('cancel' in msg.text.lower() or 'cᴀɴᴄᴇʟ' in msg.text.lower() or '⛔' in msg.text) or 'cᴀɴᴄᴇʟ' in msg.text.lower() or '⛔' in msg.text):
            return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        out_name = re.sub(r'[<>:"/\\|?*]', '_', msg.text.strip())

        # Scan files for total size and duration
        scan_msg = None
        try: scan_msg = await bot.send_message(uid, "<i>Scanning source messages (calculating duration and size)...</i>")
        except: pass
        tot_bytes = 0
        tot_secs = 0
        valid_count = 0
        try:
            ch_id = int(from_chat) if str(from_chat).lstrip("-").isdigit() else from_chat
            msg_ids = list(range(sid, eid + 1))
            
            # Start UI clone bot for scan so we don't hit Pyrogram channel invalid error 
            ui_client = await start_clone_bot(_CLIENT.client(acc))
            try:
                await _safe_resolve_peer(ui_client, ch_id)
                
                for i in range(0, len(msg_ids), 200):
                    chunk = msg_ids[i:i + 200]
                    while True:
                        try:
                            msgs = await ui_client.get_messages(ch_id, chunk)
                            break
                        except Exception as e:
                            from plugins.utils import format_tg_error
                            err_msg = format_tg_error(e, "Scan Error")
                            try:
                                ask_res = await bot.ask(uid, f"{err_msg}\n\n<i>Fix the issue (e.g. ensure bot/clone is Admin), then click Retry!</i>", 
                                    reply_markup=ReplyKeyboardMarkup([["🔄 Retry Scan"], ["❌ Cancel Process"]], resize_keyboard=True), timeout=600)
                                if not ask_res.text or any(x in ask_res.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
                                    return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                                await ask_res.delete()
                                continue
                            except Exception:
                                return await bot.send_message(uid, "<b>‣ Scan Error:</b> Timed out waiting for retry.", reply_markup=ReplyKeyboardRemove())

                    if not isinstance(msgs, list): msgs = [msgs]
                    for m_ in msgs:
                        if not m_ or m_.empty: continue
                        media_obj = None
                        for attr in ('audio', 'video', 'document', 'voice', 'video_note'):
                            media_obj = getattr(m_, attr, None)
                            if media_obj: break
                        if media_obj:
                            tot_bytes += getattr(media_obj, 'file_size', 0) or 0
                            dur = getattr(media_obj, 'duration', 0) or 0
                            tot_secs += dur
                            valid_count += 1
                if scan_msg: await scan_msg.delete()
            finally:
                try: await ui_client.stop()
                except: pass
        except Exception as e:
            if scan_msg:
                try: await scan_msg.edit_text(f"<i>Scan partial/failed: {e}</i>")
                except: pass

        units = ["B", "KB", "MB", "GB", "TB"]
        size_f = float(tot_bytes)
        idx = 0
        while size_f >= 1024.0 and idx < len(units)-1:
            idx += 1; size_f /= 1024.0
        size_str = f"{size_f:.2f} {units[idx]}"
        
        mins, secs = divmod(int(tot_secs), 60)
        hrs, mins = divmod(mins, 60)
        dur_str = f"{hrs:02d}:{mins:02d}:{secs:02d}"

        # Step 5b: Speed
        speed_kb = [
            ["1.0x (Normal)", "1.25x", "1.5x"],
            ["1.75x", "2.0x", "2.5x"],
            ["0.75x (Slower)", "0.5x (Slowest)"]
        ]
        msg = await _mg_ask(bot, uid,
            f"<b>Step 5b/9:</b> Choose <b>playback speed</b>:\n\n"
            f"<b>Found:</b> {valid_count} valid media files\n"
            f"<b>Total Size:</b> {size_str}\n"
            f"<b>Total Duration:</b> {dur_str}\n\n"
            f"<i>Select a speed below:</i>",
            reply_markup=ReplyKeyboardMarkup(speed_kb, resize_keyboard=True, one_time_keyboard=True))
        speed = 1.0
        if msg.text:
            m = re.search(r'([0-9.]+)x', msg.text)
            if m:
                try: speed = float(m.group(1))
                except: speed = 1.0
        speed = max(0.5, min(speed, 2.5))
        
        fin_secs = int(tot_secs / speed) if speed > 0 else int(tot_secs)
        fmins, fsecs = divmod(fin_secs, 60)
        fhrs, fmins = divmod(fmins, 60)
        fin_dur_str = f"{fhrs:02d}:{fmins:02d}:{fsecs:02d}"

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

        # Step 6b: Audio Cover (for MP3 ID3 tag)
        cover_path = None
        msg = await _mg_ask(bot, uid,
            "<b>Step 6b/9:</b> Send <b>audio cover image</b> (embedded in MP3 ID3 tag)\n\n"
            "Send <code>skip</code> for no cover art.")
        tmp_dir = os.path.abspath(f"merge_tmp/_cover_{uid}")
        os.makedirs(tmp_dir, exist_ok=True)
        if msg.photo:
            try: cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_dir, "cover.jpg"))
            except: pass
        elif msg.document and msg.document.mime_type and 'image' in msg.document.mime_type:
            try: cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_dir, "cover.jpg"))
            except: pass
        if cover_path: cover_path = os.path.abspath(cover_path)

        # Step 6c: Make Video?
        make_video = False
        upload_to_yt = False
        yt_title = None
        yt_thumb_path = None
        yt_start_epi = None
        video_cover_path = None
        outro_cover_path = None
        if mtype == "audio":
            msg = await _mg_ask(bot, uid,
                "<b>Step 6c/9:</b> Create an <b>MP4 Video</b> (audio + 1080p image)?\n\n"
                "Send <code>yes</code> to build a video file, or <code>skip</code> for MP3 only.")
            if "yes" in (msg.text or "").lower():
                make_video = True
                
                # Step 6d: Video Cover Image (separate from MP3 cover)
                msg = await _mg_ask(bot, uid,
                    "<b>Step 6d/9:</b> Send the <b>1080p image</b> to use as the video background.\n\n"
                    "<i>(This is separate from the MP3 cover art — send a high-resolution image)</i>\n\n"
                    "Send <code>skip</code> to use the same image as MP3 cover.")
                tmp_vdir = os.path.abspath(f"merge_tmp/_vcover_{uid}")
                os.makedirs(tmp_vdir, exist_ok=True)
                if msg.photo:
                    try:
                        video_cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_vdir, "video_cover.jpg"))
                        video_cover_path = os.path.abspath(video_cover_path)
                    except: pass
                elif msg.document and msg.document.mime_type and 'image' in msg.document.mime_type:
                    try:
                        video_cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_vdir, "video_cover.jpg"))
                        video_cover_path = os.path.abspath(video_cover_path)
                    except: pass
                # Fall back to audio cover if no video cover sent
                if not video_cover_path:
                    video_cover_path = cover_path

        if mtype == "video" or make_video:
            # Outro Image
            msg = await _mg_ask(bot, uid,
                "<b>Step 6e/9:</b> Send the <b>Outro Image</b> to show at the end of the video for 5 seconds.\n\n"
                "Send <code>4auto</code> to use the 4 default outro images (appears 4 times during the video).\n"
                "Send <code>skip</code> to skip the outro.",
                reply_markup=ReplyKeyboardMarkup([["4auto"], ["skip"], ["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True))
            tmp_odir = os.path.abspath(f"merge_tmp/_ocover_{uid}")
            os.makedirs(tmp_odir, exist_ok=True)
            
            if msg.text and msg.text.lower() == "4auto":
                outro_cover_path = "4auto"
            else:
                if msg.photo:
                    try:
                        outro_cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_odir, "outro_cover.jpg"))
                        outro_cover_path = os.path.abspath(outro_cover_path)
                    except: pass
                elif msg.document and msg.document.mime_type and 'image' in msg.document.mime_type:
                    try:
                        outro_cover_path = await bot.download_media(msg, file_name=os.path.join(tmp_odir, "outro_cover.jpg"))
                        outro_cover_path = os.path.abspath(outro_cover_path)
                    except: pass

            # Step 6f: YouTube Upload?
            msg = await _mg_ask(bot, uid,
                "<b>Step 6f/9:</b> Auto-Upload to <b>YouTube (Private)</b> after rendering?\n\n"
                "<i>(Requires /ytauth setup first)</i>\n\n"
                "Send <code>yes</code> or <code>skip</code>.")
            if "yes" in (msg.text or "").lower():
                upload_to_yt = True

                # Step 6g: YouTube Title
                msg = await _mg_ask(bot, uid,
                    "<b>Step 6g/9:</b> Enter specific <b>YouTube Title</b>:\n\n"
                    "Send <code>skip</code> to use bot default.")
                yt_title = msg.text.strip() if msg.text.lower() != "skip" else None

                # Step 6h: YouTube Thumbnail
                msg = await _mg_ask(bot, uid,
                    "<b>Step 6h/9:</b> Send custom <b>YouTube Thumbnail</b> image:\n\n"
                    "Send <code>skip</code> for none.")
                tmp_tdir = os.path.abspath(f"merge_tmp/_ythumb_{uid}")
                os.makedirs(tmp_tdir, exist_ok=True)
                if msg.photo:
                    try:
                        yt_thumb_path = await bot.download_media(msg, file_name=os.path.join(tmp_tdir, "yt_thumb.jpg"))
                        yt_thumb_path = os.path.abspath(yt_thumb_path)
                    except: pass
                elif msg.document and msg.document.mime_type and 'image' in msg.document.mime_type:
                    try:
                        yt_thumb_path = await bot.download_media(msg, file_name=os.path.join(tmp_tdir, "yt_thumb.jpg"))
                        yt_thumb_path = os.path.abspath(yt_thumb_path)
                    except: pass

                # Step 6i: Starting Episode
                msg = await _mg_ask(bot, uid,
                    "<b>Step 6i/9:</b> Enter <b>Starting Episode Number</b> for Timestamps (e.g. 1 or 201).\n\n"
                    "Send <code>skip</code> to assume 1.")
                if msg.text.lower() != "skip" and msg.text.strip().isdigit():
                    yt_start_epi = int(msg.text.strip())

        # Step 7: Confirm
        dest_preview = "DM only"
        if dest_chats:
            names = [next((c["title"] for c in channels if int(c["chat_id"])==d), str(d)) for d in dest_chats]
            dest_preview = ", ".join(names)

        meta_pre = "\n".join(f"  {k}: {v}" for k,v in list(metadata.items())[:5] if v) if metadata else ""
        vc_label = ("✅ Separate 1080p image" if (video_cover_path and video_cover_path != cover_path)
                    else ("✅ Same as audio cover" if make_video and cover_path else "❌"))

        msg = await _mg_ask(bot, uid,
            f"<b>Step 7: Confirm {label} Merge</b>\n\n"
            f"<b>Source:</b> <code>{from_chat}</code>\n"
            f"<b>Range:</b> {sid} → {eid} ({total} msgs)\n"
            f"<b>Output:</b> <code>{out_name}</code>\n"
            f"<b>Type:</b> {icon} {label}\n"
            f"<b>Speed:</b> {speed}x\n"
            f"<b>Total Duration:</b> {dur_str} (Final: {fin_dur_str})\n"
            f"<b>Audio Cover:</b> {'✅' if cover_path else '❌'}\n"
            f"<b>Make MP4 Video:</b> {'✅' if make_video else '❌'}\n"
            f"<b>Video Image:</b> {vc_label}\n"
            f"<b>Outro Image:</b> {'✅' if outro_cover_path else '❌'}\n"
            f"<b>Upload to YT:</b> {'✅ Private' if upload_to_yt else '❌'}\n"
            + (f"<b>YT Title:</b> {yt_title[:20]+'...' if len(yt_title)>20 else yt_title}\n" if yt_title else "")
            + (f"<b>YT Thumb:</b> {'✅' if yt_thumb_path else '❌'}\n" if upload_to_yt else "")
            + f"<b>Dest:</b> {dest_preview}\n"
            + (f"\n<b>Metadata:</b>\n{meta_pre}\n" if meta_pre else "") +
            f"\n<i>All media merged in exact order. No file skipped.</i>",
            reply_markup=ReplyKeyboardMarkup(
                [["✅ Start Merge"],["❌ Cancel"]],
                resize_keyboard=True, one_time_keyboard=True))

        if not msg.text or (getattr(msg, 'text', None) and any(x in msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔'])):
            for td in (tmp_dir, f"merge_tmp/_vcover_{uid}", f"merge_tmp/_ocover_{uid}", f"merge_tmp/_ythumb_{uid}"):
                try: shutil.rmtree(os.path.abspath(td), ignore_errors=True)
                except: pass
            return await bot.send_message(uid, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

        # Create job
        jid = str(uuid.uuid4())
        real_dir = os.path.abspath(f"merge_tmp/{jid}")
        os.makedirs(real_dir, exist_ok=True)
        
        # Copy audio cover
        if cover_path and os.path.exists(str(cover_path)):
            shutil.copy2(str(cover_path), os.path.join(real_dir, "cover.jpg"))
        
        # Copy video cover (separate image for video rendering)
        has_video_cover = False
        if video_cover_path and os.path.exists(str(video_cover_path)):
            shutil.copy2(str(video_cover_path), os.path.join(real_dir, "video_cover.jpg"))
            has_video_cover = True
        
        if outro_cover_path == "4auto":
            pass
        elif outro_cover_path and os.path.exists(str(outro_cover_path)):
            shutil.copy2(str(outro_cover_path), os.path.join(real_dir, "outro_cover.jpg"))
            
        if yt_thumb_path and os.path.exists(str(yt_thumb_path)):
            shutil.copy2(str(yt_thumb_path), os.path.join(real_dir, "yt_thumb.jpg"))
        
        # Clean up temp dirs
        for td in (tmp_dir, f"merge_tmp/_vcover_{uid}", f"merge_tmp/_ocover_{uid}", f"merge_tmp/_ythumb_{uid}"):
            try: shutil.rmtree(os.path.abspath(td), ignore_errors=True)
            except: pass

        job = {
            "job_id": jid, "user_id": uid, "account_id": acc["id"],
            "from_chat": from_chat, "start_id": sid, "end_id": eid,
            "current_id": sid, "output_name": out_name, "merge_type": mtype,
            "metadata": metadata, "dest_chats": dest_chats, "replace_target": replace_target,
            "has_cover": bool(cover_path), "has_video_cover": has_video_cover,
            "has_outro_cover": True if outro_cover_path == "4auto" else bool(outro_cover_path),
            "use_4auto_outros": outro_cover_path == "4auto",
            "speed": speed, "make_video": make_video,
            "upload_to_yt": upload_to_yt, "yt_title": yt_title,
            "has_yt_thumb": bool(yt_thumb_path), "yt_start_epi": yt_start_epi,
            "name": out_name, "status": "downloading", "downloaded": 0,
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


# ── Download fail choice callback ─────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^mg_dl#(skip|retry|abort)#(.+)"))
async def mg_dl_choice_cb(bot, query):
    """Handle the Skip / Retry / Abort choice from the download failure prompt."""
    import re as _re
    m = _re.match(r"^mg_dl#(skip|retry|abort)#(.+)", query.data)
    if not m:
        return await query.answer()
    action = m.group(1)   # "skip" | "retry" | "abort"
    jid    = m.group(2)

    _dl_key = f"mg_dl_choice_{jid}"
    _mg_dl_choices[_dl_key] = action

    labels = {"skip": "⏭ Skipping file…", "retry": "🔄 Retrying download…", "abort": "⛔ Aborting merge…"}
    await query.answer(labels.get(action, "Ok"), show_alert=False)
    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass