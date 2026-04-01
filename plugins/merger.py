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
_mg_global_lock = asyncio.Lock()


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

    # ── ETAs from DB ───────────────────────────────────────────────────────
    dl_eta  = job.get("dl_eta",  0) or 0
    mg_eta  = job.get("mg_eta",  0) or 0
    up_eta  = job.get("up_eta",  0) or 0
    yt_eta  = job.get("yt_eta",  0) or 0

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
        await asyncio.wait_for(client.get_chat(chat_id), timeout=20)
    except asyncio.TimeoutError:
        logger.warning(f"[MG] _safe_resolve_peer: get_chat({chat_id}) timed out, continuing anyway")
    except Exception as e:
        err_str = str(e).upper()
        if "PEER_ID_INVALID" in err_str or "CHANNEL_INVALID" in err_str or "PEER_ID_NOT_HANDLED" in err_str:
            try:
                me = await asyncio.wait_for(client.get_me(), timeout=15)
                if not getattr(me, 'is_bot', False):
                    # Warm up dialogs cache with a strict timeout to prevent hanging
                    try:
                        async def _drain_dialogs():
                            async for _ in client.get_dialogs(limit=50): pass
                        await asyncio.wait_for(_drain_dialogs(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.warning(f"[MG] get_dialogs timed out for {chat_id}, proceeding anyway")
                await asyncio.wait_for(client.get_chat(chat_id), timeout=20)
            except asyncio.TimeoutError:
                logger.warning(f"[MG] _safe_resolve_peer second get_chat timed out for {chat_id}")
            except Exception as e2:
                logger.warning(f"Failed to resolve {chat_id}: {e2}")
        else:
            logger.warning(f"[MG] _safe_resolve_peer non-fatal error for {chat_id}: {e}")


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

async def _ffmpeg_merge(file_list, output_path, metadata=None, mtype="audio", cover=None, speed=1.0, make_video=False, video_cover=None, outro_cover=None, total_duration=None, progress_cb=None):
    """Merge file_list → output_path. Tries lossless copy first, falls back to re-encode.
    make_video: If True and cover is present, creates an MP4 video out of the merged audio and cover image.
    speed: 1.0 = normal, 2.5 = 2.5x faster.
    Returns (ok: bool, error: str).
    """
    import asyncio, re
    async def _run_cmd(cmd_list, timeout_sec):
        proc = await asyncio.create_subprocess_exec(
            *cmd_list, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stderr_lines = []
        async def _reader():
            while True:
                try:
                    chunk = await proc.stderr.read(4096)
                except Exception:
                    break
                if not chunk: break
                lstr = chunk.decode('utf-8', errors='replace')
                stderr_lines.append(lstr)
                # Keep memory minimal
                if len(stderr_lines) > 20: stderr_lines.pop(0)
                if progress_cb:
                    matches = re.findall(r'time=(\d+):(\d+):(\d+\.\d+)', lstr)
                    if matches:
                        # matches[-1] gives the latest time tuple (HH, MM, SS.ms) in this chunk
                        h, m_m, s = float(matches[-1][0]), float(matches[-1][1]), float(matches[-1][2])
                        await progress_cb(h*3600 + m_m*60 + s)
        try:
            await asyncio.wait_for(asyncio.gather(proc.wait(), _reader()), timeout=timeout_sec)
        except asyncio.TimeoutError:
            try: proc.kill()
            except: pass
            return False, "FFmpeg timed out"
        outerr = "".join(stderr_lines[-50:])
        if proc.returncode == 0: return True, ""
        return False, outerr

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
        needs_reencode = bool(atempo) or make_video or (mtype == "audio" and len(file_list) > 1)


        if not needs_reencode:
            cmd = ["ffmpeg","-y","-threads","2","-f","concat","-safe","0","-i",lst]
            if cover and os.path.exists(cover) and mtype == "audio":
                cmd += ["-i", cover, "-map","0:a","-map","1:0","-c:a","copy",
                        "-id3v2_version","3",
                        "-metadata:s:v","title=Album cover",
                        "-metadata:s:v","comment=Cover (front)",
                        "-max_muxing_queue_size", "4096"]
            elif mtype == "video":
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
        
        cmd2 = ["ffmpeg","-y","-threads","2"]
        eff_cover = video_cover or cover
        if make_video and eff_cover and os.path.exists(eff_cover) and mtype == "audio":
            if outro_cover and len([o for o in (outro_cover if isinstance(outro_cover, list) else [outro_cover]*4) if isinstance(o, str) and os.path.exists(o)]) == 4:
                outros = outro_cover if isinstance(outro_cover, list) else [outro_cover] * 4
                valid_outros = [o for o in outros if isinstance(o, str) and os.path.exists(o)]

                tmp_audio = output_path + ".tmp_audio.m4a"
                audio_cmd = ["ffmpeg","-y","-threads","2"]
                for p in file_list: audio_cmd += ["-i", os.path.abspath(p)]
                
                if len(file_list) == 1:
                    fc = f"[0:a]{atempo}[a2]" if atempo else ""
                    map_lbl = "[a2]" if atempo else "[0:a]"
                else:
                    fc = "".join(f"[{i}:a]" for i in range(len(file_list))) + f"concat=n={len(file_list)}:v=0:a=1[a1]"
                    if atempo: fc += f";[a1]{atempo}[a2]"
                    map_lbl = "[a2]" if atempo else "[a1]"
                
                if fc:
                    audio_cmd += ["-filter_complex", fc]
                audio_cmd += ["-map", map_lbl, "-vn","-c:a","aac","-b:a","192k", tmp_audio]
                
                a_ok, a_err = await _run_cmd(audio_cmd, 7200)
                if not a_ok or not os.path.exists(tmp_audio):
                    return False, "Audio merge failed: " + a_err

                real_dur = _get_duration(tmp_audio)
                if real_dur <= 0:
                    real_dur = total_duration / max(speed, 0.1) if total_duration else 3600 * 5

                outro_positions = [
                    max(0.0, real_dur * 0.25),
                    max(0.0, real_dur * 0.50),
                    max(0.0, real_dur * 0.75),
                    max(0.0, real_dur * 0.95 - 5),
                ]

                cmd2 = ["ffmpeg","-y", "-loop","1","-framerate","1","-t",str(real_dur),"-i", os.path.abspath(eff_cover)]
                for op in valid_outros:
                    cmd2 += ["-loop","1","-framerate","1","-t","5","-i", os.path.abspath(op)]
                cmd2 += ["-i", tmp_audio]

                n_outros = len(valid_outros)
                audio_input_idx = n_outros + 1
                fc_parts = [
                    "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,"
                    "pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p[base]"
                ]
                prev = "[base]"
                for i, pos in enumerate(outro_positions):
                    end_t = pos + 5.0
                    out_lbl = f"[ov{i}]" if i < n_outros - 1 else "[finalv]"
                    fc_parts.append(
                        f"[{i+1}:v]scale=1280:720:force_original_aspect_ratio=decrease,"
                        f"pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p[os{i}];"
                        f"{prev}[os{i}]overlay=0:0:enable='between(t,{pos:.1f},{end_t:.1f})'{out_lbl}"
                    )
                    prev = out_lbl

                cmd2 += ["-filter_complex", ";".join(fc_parts)]
                cmd2 += ["-map", "[finalv]", "-map", f"{audio_input_idx}:a"]
                cmd2 += [
                    "-c:v","libx264","-preset","superfast","-tune","stillimage",
                    "-c:a","aac","-b:a","192k",
                    "-movflags","+faststart",
                    "-max_muxing_queue_size","4096"
                ]
                if metadata:
                    for k, v in (metadata or {}).items():
                        if v: cmd2 += ["-metadata", f"{k}={v}"]
                abs_output = os.path.abspath(output_path)
                cmd2.append(abs_output)
                v_ok, v_err = await _run_cmd(cmd2, 86400)
                try:
                    if os.path.exists(tmp_audio): os.remove(tmp_audio)
                except Exception: pass
                if v_ok and os.path.exists(abs_output) and os.path.getsize(abs_output) > 100:
                    return True, ""
                else:
                    return False, v_err
            else:
                cmd2 += ["-loop", "1", "-framerate", "1", "-i", os.path.abspath(eff_cover)]
        else:
            if make_video and eff_cover and os.path.exists(eff_cover) and mtype == "audio":
                cmd2 += ["-loop", "1", "-framerate", "1", "-i", os.path.abspath(eff_cover)]

        if make_video and eff_cover and os.path.exists(eff_cover) and mtype == "audio" and not outro_cover:
            for p in file_list: cmd2 += ["-i", os.path.abspath(p)]
            
            if len(file_list) == 1:
                fc = f"[1:a]{atempo}[a2]" if atempo else ""
                map_albl = "[a2]" if atempo else "[1:a]"
            else:
                fc = "".join(f"[{i+1}:a]" for i in range(len(file_list))) + f"concat=n={len(file_list)}:v=0:a=1[a1]"
                if atempo: fc += f";[a1]{atempo}[a2]"
                map_albl = "[a2]" if atempo else "[a1]"
                
            fc = (fc + ";") if fc else ""
            fc += "[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p[v1]"
            cmd2 += ["-filter_complex", fc, "-map", "[v1]", "-map", map_albl]
            cmd2 += [
                "-c:v","libx264","-preset","superfast","-tune","stillimage",
                "-c:a","aac","-b:a","192k",
                "-movflags","+faststart",
                "-shortest","-max_muxing_queue_size","4096"
            ]
        else:
            if mtype == "video":
                cmd2 += ["-f","concat","-safe","0","-i",lst]
                vf = f"setpts={1.0/speed:.4f}*PTS" if abs(speed - 1.0) > 0.001 else ""
                if vf: cmd2 += ["-vf", vf]
                if atempo: cmd2 += ["-af", atempo]
                cmd2 += ["-c:v","libx264","-preset","superfast","-crf","28",
                         "-c:a","aac","-b:a","128k","-movflags","+faststart","-max_muxing_queue_size","4096"]
            else:
                for p in file_list: cmd2 += ["-i", os.path.abspath(p)]
                
                if len(file_list) == 1:
                    fc = f"[0:a]{atempo}[a2]" if atempo else ""
                    map_lbl = "[a2]" if atempo else "[0:a]"
                else:
                    fc = "".join(f"[{i}:a]" for i in range(len(file_list))) + f"concat=n={len(file_list)}:v=0:a=1[a1]"
                    if atempo: fc += f";[a1]{atempo}[a2]"
                    map_lbl = "[a2]" if atempo else "[a1]"
                
                if cover and os.path.exists(cover) and not make_video:
                    cmd2 += ["-i", os.path.abspath(cover)]
                    cov_idx = len(file_list)
                    if fc: fc += ";"
                    fc += f"[{cov_idx}:v]scale=trunc(iw/2)*2:trunc(ih/2)*2[cv]"
                    cmd2 += ["-filter_complex", fc, "-map", map_lbl, "-map", "[cv]",
                             "-id3v2_version","3",
                             "-metadata:s:v","title=Album cover",
                             "-metadata:s:v","comment=Cover (front)"]
                else:
                    if fc: cmd2 += ["-filter_complex", fc]
                    cmd2 += ["-map", map_lbl]
                    
                cmd2 += ["-c:a","libmp3lame","-b:a","192k","-ar","48000","-max_muxing_queue_size","4096"]
        
        if metadata:
            for k, v in (metadata or {}).items():
                if v: cmd2 += ["-metadata", f"{k}={v}"]
        abs_output = os.path.abspath(output_path)
        cmd2.append(abs_output)
        ok2, err2 = await _run_cmd(cmd2, 86400)
        if ok2 and os.path.exists(abs_output) and os.path.getsize(abs_output) > 100:
            return True, ""
        else:
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

async def _run_job(jid, uid, bot):
    job = await _db_get(jid)
    if not job: return

    sys_mode = await db.get_sys_mode()
    if sys_mode == "pc":
        # Ultra PC Mode: High power, high resources
        CHUNK_SIZE = 25
        MAX_TOTAL_GB = 150.0  
        MAX_CHUNK_GB = 15.0
        MAX_FILES = 999
    else:
        # Standard VPS Mode: Low RAM usage, strict limits
        CHUNK_SIZE = 5
        MAX_TOTAL_GB = 6.0
        MAX_CHUNK_GB = 2.0
        MAX_FILES = 150

    ev = _mg_paused.get(jid)
    if not ev:
        ev = asyncio.Event(); ev.set()
        _mg_paused[jid] = ev

    client = None
    wdir = f"merge_tmp/{jid}"
    os.makedirs(wdir, exist_ok=True)
    import html
    
    try:
        await _db_up(jid, status="queued", error="", created_at=time.time())

        # ── Global queue: only 1 merge running at a time ──
        async with _mg_global_lock:
            # Re-check status before proceeding
            fresh = await _db_get(jid)
            if not fresh or fresh.get("status") in ("stopped", "paused"): return
            await _db_up(jid, status="scanning", error="")
            
            # The entire rest of the function runs sequentially per VPS server to prevent out of memory!
            await _run_job_core(jid, uid, bot, job, sys_mode, client_task=None, ev=_mg_paused.get(jid))
            
    except asyncio.CancelledError:
        await _db_up(jid, status="stopped")
    except Exception as e:
        logger.error(f"[MG {jid}] Exception caught: {e}")
        await _db_up(jid, status="error", error=str(e)[:500])
        try: await bot.send_message(uid, f"<b>❌ Error:</b> <code>{html.escape(str(e)[:500])}</code>")
        except: pass
    finally:
        _mg_tasks.pop(jid, None)
        _mg_paused.pop(jid, None)


async def _run_job_core(jid, uid, bot, job, sys_mode, client_task, ev):
    import html
    client = None
    wdir = f"merge_tmp/{jid}"
    os.makedirs(wdir, exist_ok=True)
    
    try:
        acc = await db.get_bot(uid, job["account_id"])
        if not acc:
            err_msg = "❌ <b>Merge failed:</b> The selected account was not found. Please re-create the job with a valid account."
            await _db_up(jid, status="error", error="Account not found")
            try: await bot.send_message(uid, err_msg)
            except: pass
            return
        try:
            client = await asyncio.wait_for(
                start_clone_bot(_CLIENT.client(acc)),
                timeout=60
            )
        except asyncio.TimeoutError:
            err_msg = "❌ <b>Merge failed — account connection timed out (60s).</b>\n\nThe session may be expired or Telegram is unreachable. Please check your account in /settings → Accounts."
            await _db_up(jid, status="error", error="Account connection timed out")
            try: await bot.send_message(uid, err_msg)
            except: pass
            return
        except Exception as conn_err:
            err_msg = f"❌ <b>Merge failed — could not connect account:</b>\n<code>{html.escape(str(conn_err)[:300])}</code>\n\nPlease check your session string in /settings → Accounts."
            await _db_up(jid, status="error", error=str(conn_err)[:300])
            try: await bot.send_message(uid, err_msg)
            except: pass
            return

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

        if est_size > MAX_TOTAL_GB * 1024**3 or media_count > MAX_FILES:
            msg = (f"❌ Pre-scan blocked your request:\n"
                   f"Found {media_count} files ({_sz(est_size)}).\n\n"
                   f"Server limit is {MAX_FILES} files and {MAX_TOTAL_GB:.0f}GB per merge to prevent Out of Memory errors. "
                   f"Please select a smaller range and try again.")
            await _db_up(jid, status="error", error=msg)
            try:
                if scan_msg: await scan_msg.edit_text(msg)
                else: await bot.send_message(uid, msg)
            except Exception as e: logger.warning(f"[MG UI] pre-scan blocked error: {e}")
            return

        try:
            txt = (f"✅ Pre-scan complete\n"
                   f"📁 {media_count} media files found\n"
                   f"💾 Estimated total: {_sz(est_size)}\n"
                   f"🔀 Will process in chunks of {CHUNK_SIZE} files\n"
                   f"⚡ Speed: {speed}x")
            if scan_msg: await scan_msg.edit_text(txt)
            else: await bot.send_message(uid, txt)
        except Exception as e: logger.warning(f"[MG UI] pre-scan ok error: {e}")

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
                f"⬇️ Starting download\n"
                f"📁 {total_files} files → {total_chunks} chunk(s) of max {CHUNK_SIZE}\n"
                f"Each chunk is downloaded, merged, then deleted to save RAM.")
        except Exception as e: logger.warning(f"[MG UI] starting download notification error: {e}")

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
            chunk_dl_start = time.time()  # local timer per chunk

            try:
                status_msg = await bot.send_message(uid,
                    f"⬇️ {chunk_label} — Downloading {len(chunk_msgs)} files")
            except Exception as e:
                logger.warning(f"[MG UI] chunk downloading status error: {e}")
                status_msg = None

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
                for att in range(10):  # increased retries
                    try:
                        logger.info(f"[MG] Downloading chunk file {ci+1}/{len(chunk_msgs)}: {msg.id}")
                        fp = await asyncio.wait_for(client.download_media(msg, file_name=dlp), timeout=900)
                        if fp: break
                    except asyncio.TimeoutError:
                        logger.warning(f"[MG] Timeout downloading {msg.id} (attempt {att+1})")
                        await asyncio.sleep(3)
                        continue
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2)
                    except Exception as e:
                        if "TimeoutList" in str(e) or "Timeout" in str(e) or "Connection" in str(e):
                            if att < 9: await asyncio.sleep(3); continue
                        logger.error(f"[MG] Error downloading {msg.id}: {e}")
                        break
                
                if not fp or not os.path.exists(fp):
                    await _db_up(jid, status="error", error=f"Failed to download media for episode {global_seq+1} (msg {msg.id}).")
                    try: await bot.send_message(uid, f"<b>❌ Fatal Error:</b> Could not download message {msg.id} after 10 attempts! Aborting merge to prevent gaps.")
                    except: pass
                    return
                
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
                    try:
                        txt = (f"⬇️ {chunk_label} — Downloading\n"
                               f"<code>{_bar(global_seq, total_files)}</code>\n"
                               f"📁 {global_seq}/{total_files} total • {_sz(dl_total_bytes)}")
                        if status_msg: await status_msg.edit_text(txt)
                    except Exception as e: logger.warning(f"[MG UI] dl update error: {e}")

            if not chunk_files:
                logger.warning(f"[MG {jid}] Chunk {chunk_num} had no downloadable files, skipping.")
                continue

            # End download phase — record dl_time for this chunk
            _dl_end = time.time()
            _dl_time = _dl_end - chunk_dl_start

            # Partial merge of this chunk
            await _db_up(jid, status="merging", dl_time=_dl_time, phase_start_ts=time.time())
            part_ext  = ".mp4" if mtype == "video" else ".mp3"
            part_path = os.path.join(wdir, f"part_{chunk_num:04d}{part_ext}")

            try:
                if status_msg: await status_msg.edit_text(
                    f"🔀 {chunk_label} — Merging {len(chunk_files)} files → part {chunk_num}")
            except Exception as e: logger.warning(f"[MG UI] merge status update error: {e}")

            chunk_files_sorted = sorted(chunk_files, key=lambda p: os.path.basename(p))
            
            chunk_dur = sum(_ffprobe_duration(f) for f in chunk_files_sorted)
            last_edit = [time.time()]
            async def chunk_prog(cur_secs):
                now = time.time()
                if now - last_edit[0] > 5:
                    pct = min(100, int((cur_secs / max(chunk_dur, 0.1)) * 100))
                    try:
                        if status_msg: await status_msg.edit_text(
                            f"🔀 {chunk_label} — Merging {len(chunk_files)} files → part {chunk_num}\n"
                            f"<code>{_bar(pct, 100)}</code>\n"
                            f"⏳ Progress: {pct}%"
                        )
                    except Exception as e: logger.warning(f"[MG UI] chunk prog update error: {e}")
                    last_edit[0] = now

            # Chunk parts: apply speed chunk-by-chunk to save MASSIVE amounts of RAM
            ok, err = await _ffmpeg_merge(
                chunk_files_sorted, part_path, None, mtype, None, speed, False, progress_cb=chunk_prog)

            if not ok:
                await _db_up(jid, status="error", error=f"Chunk {chunk_num} merge failed: {err[:300]}")
                await bot.send_message(uid, f"<b>❌ Chunk {chunk_num} merge failed:</b>\n<code>{err[:300]}</code>")
                return

            part_files.append(part_path)

            # ✅ Delete chunk originals immediately to free disk/RAM
            for f in chunk_files:
                try: os.remove(f)
                except: pass
            try:
                os.rmdir(chunk_dir)
            except OSError:
                shutil.rmtree(chunk_dir, ignore_errors=True)

            await _db_up(jid, status="downloading")
            try:
                if status_msg: await status_msg.edit_text(
                    f"✅ {chunk_label} done\n"
                    f"💾 Part size: {_sz(os.path.getsize(part_path))} • "
                    f"Total so far: {_sz(dl_total_bytes)}")
            except Exception as e: logger.warning(f"[MG UI] chunk done text error: {e}")
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
        _spd_text = f'⚡ Applying speed {speed}x during final merge' if abs(speed-1.0)>0.001 else '🎯 Lossless combine (speed=1.0x)'
        try:
            await bot.send_message(uid,
                f"🔀 Final combine: {len(part_files)} parts → {out_name}{out_ext}\n"
                f"{_spd_text}{_vid_text}")
        except Exception as e: logger.warning(f"[MG UI] final combine initial msg error: {e}")

        part_files_sorted = sorted(part_files, key=lambda p: os.path.basename(p))
        final_dur = cumulative_secs

        # Dedicated progress message for the final combine
        try:
            final_status_msg = await bot.send_message(uid,
                f"🔀 Final combine starting…\n"
                f"📁 {len(part_files_sorted)} parts → {out_name}{out_ext}")
        except Exception as e:
            logger.warning(f"[MG UI] final status msg error: {e}")
            final_status_msg = None

        last_edit2 = [time.time()]
        async def final_prog(cur_secs):
            now = time.time()
            if now - last_edit2[0] > 5:
                pct = min(100, int((cur_secs / max(final_dur, 0.1)) * 100))
                try:
                    if final_status_msg:
                        await final_status_msg.edit_text(
                            f"🔀 Final combine: {len(part_files_sorted)} parts → {out_name}{out_ext}\n"
                            f"<code>{_bar(pct, 100)}</code>\n"
                            f"⏳ Progress: {pct}% • {_tm(int(cur_secs))}/{_tm(int(max(final_dur,0.1)))}"
                        )
                except Exception as e: logger.warning(f"[MG UI] final combine prog error: {e}")
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
        await _db_up(jid, status="done", total_time=_total, yt_time=_yt_time, file_size=fsize)

        markup = None
        if upload_to_yt and 'yt_vid_id' in locals() and yt_vid_id:
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Edit YT Video Details", callback_data=f"mg#yt_edit#{jid}")]
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

    finally:
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
    """Create a new background asyncio task for the merge job.
    Guards against duplicate starts — if a task is already running for this jid, it is a no-op."""
    old = _mg_tasks.get(jid)
    if old and not old.done():
        logger.warning(f"[MG] _start_task called for {jid} but task already running — ignoring duplicate.")
        return
    ev = asyncio.Event()
    ev.set()
    _mg_paused[jid] = ev
    task = asyncio.create_task(_run_job(jid, uid, bot))
    _mg_tasks[jid] = task
    logger.info(f"[MG] Task created for job {jid} (uid={uid})")


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
        text = _build_info_text(job)

        info_btns = [
            [InlineKeyboardButton("🔄 Refresh", callback_data=f"mg#info#{param}")],
        ]
        # Only show Edit YT button if the job is done and has a YouTube video ID stored
        if job.get("status") == "done" and job.get("yt_video_id"):
            info_btns.append([InlineKeyboardButton("✏️ Edit YT Title/Desc", callback_data=f"mg#yt_edit#{param}")])
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
                    [[KeyboardButton("CONFIRM")], [KeyboardButton("/cancel")]],
                    resize_keyboard=True, one_time_keyboard=True))
            if "/cancel" in r.text.lower() or "confirm" not in r.text.upper():
                return await bot.send_message(uid, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
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
        from_chat = ref_s if ref_s else None

        # Step 3: End link
        msg = await _mg_ask(bot, uid,
            "<b>Step 3/7:</b> Send <b>end file link</b>")
        if not msg.text or msg.text.lower() == "/cancel":
            return await bot.send_message(uid, "<b>Cancelled.</b>")
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
                    msgs = await ui_client.get_messages(ch_id, chunk)
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
                reply_markup=ReplyKeyboardMarkup([["4auto"], ["skip"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True))
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

        if not msg.text or "Cancel" in msg.text:
            for td in (tmp_dir, f"merge_tmp/_vcover_{uid}", f"merge_tmp/_ocover_{uid}", f"merge_tmp/_ythumb_{uid}"):
                try: shutil.rmtree(os.path.abspath(td), ignore_errors=True)
                except: pass
            return await bot.send_message(uid, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

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
            "metadata": metadata, "dest_chats": dest_chats,
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
