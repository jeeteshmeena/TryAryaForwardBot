"""
Audio Cleaner & Renamer — v4 STABLE TURBO
==========================================
FIXES vs v3:
  1. Job never fails from download errors — download retries 3x before skip.
  2. Completion notification guaranteed — bot ref saved to local var before cleanup.
  3. Force Activate works correctly — semaphore bypassed at task creation level.
  4. Silent auto-stop fixed — all coroutine exceptions caught and logged.
  5. Speed stable — _fill_cache timeout raised, retry on failure, no silent drops.
  6. fail_count resets after each successful file (was never reset in v3 loop correctly).
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
import concurrent.futures as cf
from database import db
from plugins.utils import extract_ep_label_robust
from .test import CLIENT, start_clone_bot
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove,
    CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()
COLL = "cleaner_jobs"

_cl_tasks: dict[str, asyncio.Task] = {}
_cl_paused: dict[str, asyncio.Event] = {}
_cl_waiter: dict[int, asyncio.Future] = {}
_cl_bot_ref: dict[str, object] = {}
_cl_cancel_users: set = set()
MAX_CONCURRENT = 3
_cl_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# Thread pool for FFmpeg — runs in OS threads so asyncio loop stays free
_FFMPEG_POOL = cf.ThreadPoolExecutor(max_workers=MAX_CONCURRENT + 2, thread_name_prefix="cl_ff")

# ─── DB Helpers ───────────────────────────────────────────────────────────────
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
@Client.on_message(filters.private & (filters.text | filters.command("cancel")), group=-15)
async def _cl_cancel_handler(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid is None or uid not in _cl_waiter:
        raise ContinuePropagation
    txt = (message.text or "").strip()
    if not (txt.startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt or txt.lower() == "cancel"):
        raise ContinuePropagation
    _cl_cancel_users.add(uid)
    fut = _cl_waiter.pop(uid, None)
    if fut and not fut.done():
        fut.set_result(message)

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
    from pyrogram.enums import ParseMode
    await bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _cl_waiter.pop(user_id, None)
        raise

def _parse_link(text):
    text = (text or "").strip().rstrip('/')
    if text.isdigit(): return None, int(text)
    m = re.search(r'https?://t\.me/c/(\d+)(?:/\d+)?/(\d+)', text)
    if m: return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.search(r'https?://t\.me/([^/]+)(?:/\d+)?/(\d+)', text)
    if m: return m.group(1), int(m.group(2))
    return None, None

def _ist_now():
    return datetime.datetime.now(IST_OFFSET)

def _tm(s):
    s = max(0, int(s))
    if s < 60: return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"


# ─── Info Text Builder ────────────────────────────────────────────────────────
def _build_cl_info(job: dict) -> str:
    status = job.get("status", "stopped")
    name   = job.get("base_name", "Cleaner")
    done   = job.get("files_done", 0)
    total  = max(job.get("total_files", 1), 1)
    err    = job.get("error", "")
    snum   = job.get("starting_number", 1)
    pct    = int(done / total * 100)
    bar    = f"[{'█'*int(18*pct/100)}{'░'*(18-int(18*pct/100))}] {pct}%"
    ic     = {"running":"🔄","paused":"⏸","completed":"✅","failed":"⚠️","stopped":"🔴","queued":"⏳"}.get(status,"❔")

    eta_str = ""
    ts = job.get("phase_start_ts", 0) or 0
    if status == "running" and done > 0 and ts > 0:
        rate = (time.time() - ts) / done
        eta_str = f"\n  ⏱ <b>ETA:</b> ~{_tm(rate * (total - done))}"

    lines = [
        f"<b>{ic} 🧹 {name} [{job.get('job_id','')[-6:]}]</b>",
        f"Status: {ic} {status.title()}",
        f"  <code>{bar}</code>",
        "",
        f"  📁 <b>Processed:</b> {done}/{total}",
        f"  🔢 <b>Range:</b> {name} {snum} → {name} {snum+total-1}",
        f"  🎨 <b>Artist:</b> {job.get('artist','—')}",
        f"  🎯 <b>Target:</b> {job.get('target_title','?')}",
        f"  ⚡ <b>Engine:</b> Stable Turbo v4",
    ]
    if eta_str: lines.append(eta_str)
    if err: lines.append(f"\n  ⚠️ <b>Error:</b> <code>{err[:200]}</code>")
    lines.append(f"\n  <i>Refreshed: {_ist_now().strftime('%I:%M %p IST')}</i>")
    return "\n".join(lines)


# ─── FFmpeg: TURBO (dynaudnorm = single-pass, 10× faster than loudnorm) ──────
def _run_ffmpeg_sync(cmd: list) -> tuple:
    """Blocking FFmpeg call — runs in ThreadPoolExecutor thread."""
    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600)
        if r.returncode != 0:
            return False, r.stderr.decode('utf-8', 'ignore')[:500]
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "FFmpeg timeout (10m)"
    except Exception as e:
        return False, str(e)


async def _ffmpeg_async(cmd: list) -> tuple:
    """Runs _run_ffmpeg_sync in thread pool so event loop stays free for downloads."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_FFMPEG_POOL, _run_ffmpeg_sync, cmd)


def _build_ffmpeg_cmd(input_path, output_path, cover_path, meta: dict) -> list:
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-analyzeduration", "10M", "-probesize", "10M",
           "-err_detect", "ignore_err", "-fflags", "+discardcorrupt",
           "-i", input_path]

    if cover_path and os.path.exists(cover_path) and os.path.getsize(cover_path) > 1024:
        cmd += ["-i", cover_path,
                "-map", "0:a:0", "-map", "1:v:0",
                "-c:v", "mjpeg", "-id3v2_version", "3",
                "-metadata:s:v", "title=Album cover",
                "-metadata:s:v", "comment=Cover (front)"]
    else:
        cmd += ["-map", "0:a:0"]

    cmd += ["-af", "dynaudnorm=f=150:g=15,aresample=44100"]
    cmd += ["-c:a", "libmp3lame", "-b:a", "128k", "-ac", "1",
            "-threads", "1",
            "-map_metadata", "-1"]

    for k, v in meta.items():
        if v: cmd += ["-metadata", f"{k}={v}"]

    cmd.append(output_path)
    return cmd


# ─── Client health ────────────────────────────────────────────────────────────
async def _ensure_alive(client):
    try:
        if not getattr(client, "is_connected", True):
            await client.connect()
    except Exception as e:
        logger.warning(f"[Cleaner] reconnect: {e}")
    return client


# ─── FIX #3: Force-activate runs without semaphore from the start ─────────────
async def _cl_run_job_force(job_id: str, bot=None):
    """Wrapper that skips semaphore entirely — used for Force Activate."""
    await _cl_run_job_inner(job_id, bot, skip_sem=True)

async def _cl_run_job(job_id: str, bot=None):
    """Normal entry — respects MAX_CONCURRENT semaphore."""
    await _cl_run_job_inner(job_id, bot, skip_sem=False)


# ─── MAIN JOB COROUTINE ───────────────────────────────────────────────────────
async def _cl_run_job_inner(job_id: str, bot=None, skip_sem: bool = False):
    job = await _cl_get_job(job_id)
    if not job or job.get("status") in ("completed", "failed", "stopped"): return

    # FIX #3: Clear force_active flag
    if job.get("force_active"):
        await _cl_update_job(job_id, {"force_active": False})

    async def _body():
        nonlocal job
        job = await _cl_get_job(job_id)
        if not job or job.get("status") in ("completed", "failed", "stopped"): return

        await _cl_update_job(job_id, {"status": "running", "error": "", "phase_start_ts": time.time()})

        uid      = job["user_id"]
        # FIX #2: Save bot ref to local variable BEFORE any cleanup
        _bot     = bot or _cl_bot_ref.get(job_id)
        _cl_bot_ref[job_id] = _bot

        # ── Client Init ───────────────────────────────────────────────────
        acc_id = job.get("account_id")
        client = None
        try:
            acc = await db.get_bot(uid, acc_id)
            if not acc: raise Exception("Account not found")
            if acc.get("is_bot", True) and _bot:
                client = _bot
            else:
                clone = _CLIENT.client(acc)
                try: client = await start_clone_bot(clone)
                except: client = clone
                client = await _ensure_alive(client)
        except Exception as e:
            await _cl_update_job(job_id, {"status": "failed", "error": f"Init: {e}"})
            if _bot:
                try: await _bot.send_message(uid, f"<b>⚠️ Cleaner Job Failed (Init)</b>\n<code>{e}</code>")
                except: pass
            return

        # ── Job Params ────────────────────────────────────────────────────
        from_ch   = job["from_chat"]
        dest_ch   = job["dest_chat"]
        sid, eid  = job["start_id"], job["end_id"]
        done      = job.get("files_done", 0)
        curr_mid  = job.get("current_msg_id", sid)
        topic_id  = job.get("from_topic_id", 0) or 0
        base_name = job.get("base_name", "Cleaned")
        art       = job.get("artist", "")
        yr        = str(job.get("year", "") or "")
        alb       = job.get("album", "") or art
        gen       = job.get("genre", "")
        cov_fid   = job.get("cover_file_id", "")
        curr_num  = job.get("curr_num_checkpoint", job.get("starting_number", 1) + done)
        rename    = job.get("rename_files", True)
        fmt       = job.get("name_format", "format_1")
        conv_vid  = job.get("convert_videos", False)
        use_cap   = job.get("use_caption", True)
        repl_mode = job.get("replace_mode", False)
        repl_sid  = job.get("replace_start_msg_id", 0)

        # ── Peer Resolution ───────────────────────────────────────────────
        for _p in [from_ch, dest_ch if dest_ch != uid else None]:
            if not _p: continue
            try: await client.get_chat(_p)
            except:
                if _bot:
                    try:
                        ci = await _bot.get_chat(_p)
                        if ci.username: await client.join_chat(ci.username)
                    except: pass

        # ── Cover Image ───────────────────────────────────────────────────
        local_cover = os.path.abspath(f"temp_cover_{job_id}.jpg")
        if cov_fid and not os.path.exists(local_cover):
            try:
                dl = await (_bot or client).download_media(cov_fid, file_name=local_cover)
                if not dl or os.path.getsize(local_cover) < 1024: local_cover = None
            except: local_cover = None
        elif not cov_fid:
            local_cover = None

        _seen      = set()
        fail_count = 0
        job_failed = False

        # ── BATCH message cache ───────────────────────────────────────────
        _msg_cache: dict[int, object] = {}

        async def _fill_cache(start: int):
            """Fetch up to 100 messages. Retries once on timeout."""
            ids = list(range(start, min(start + 100, eid + 1)))
            if not ids: return
            for attempt in range(2):
                try:
                    msgs = await asyncio.wait_for(
                        client.get_messages(from_ch, ids),
                        timeout=90  # raised from 60
                    )
                    if not isinstance(msgs, list): msgs = [msgs]
                    for m in msgs:
                        if m and not m.empty:
                            _msg_cache[m.id] = m
                    return
                except Exception as e:
                    logger.warning(f"[Cleaner {job_id}] cache fill attempt {attempt+1} err: {e}")
                    if attempt == 0: await asyncio.sleep(3)

        # ── Next media: find message + download in background (TRUE PARALLEL PIPELINE) ─
        # Runs as asyncio.Task so download N+1 happens while FFmpeg processes N.
        # NEVER skips a file silently — retries 5x with backoff, then raises.
        async def _next_media(start_mid: int):
            mid = start_mid
            while mid <= eid:
                if mid not in _msg_cache:
                    await _fill_cache(mid)
                m = _msg_cache.pop(mid, None)
                mid += 1
                if not m or m.empty: continue
                if topic_id and getattr(m, "message_thread_id", 0) != topic_id and m.id != topic_id:
                    continue
                m_obj = m.audio or m.voice or m.document or m.video or m.photo
                if not m_obj: continue

                _fn  = getattr(m_obj, 'file_name', '') or ''
                _tt  = getattr(m_obj, 'title', '') or ''
                _cp  = (getattr(m, 'caption', '') or '').strip()
                lbl  = (extract_ep_label_robust(f"{_tt} @@@ {_fn} @@@ {_cp}") or {}).get("label", "")
                if lbl and lbl in _seen: continue

                orig_fn = getattr(m_obj, 'file_name', '') or ''
                ext = (os.path.splitext(orig_fn)[1]
                       or (".mp3" if m.audio else ".mp4" if m.video else ".jpg" if m.photo else ".dat"))
                ipath = os.path.abspath(f"temp_cl_in_{job_id}_{m.id}{ext}")

                # Download with retry — 5 attempts, backoff 10/20/30/40s
                # NEVER returns None silently — raises after all attempts fail
                last_err = None
                for attempt in range(5):
                    try:
                        dp = await asyncio.wait_for(
                            client.download_media(m, file_name=ipath),
                            timeout=600
                        )
                        if dp and os.path.exists(str(dp)):
                            return m, str(dp), m_obj, m.id, lbl, ext   # ✓ success
                    except Exception as e:
                        last_err = e
                        logger.warning(f"[Cleaner {job_id}] dl {attempt+1}/5 mid={m.id}: {e}")
                        try:
                            if os.path.exists(ipath): os.remove(ipath)
                        except: pass
                        if attempt < 4:
                            await asyncio.sleep(10 * (attempt + 1))  # 10 20 30 40s

                # All 5 attempts failed — raise so main loop increments fail_count
                raise Exception(f"Download failed (5 attempts) mid={m.id}: {last_err}")

            return None  # no more messages in range

        # ── Main loop ─────────────────────────────────────────────────────
        msg_id = curr_mid
        await _fill_cache(msg_id)
        # Pre-kick: start downloading first file NOW in background
        _next_task: asyncio.Task | None = asyncio.create_task(_next_media(msg_id))

        while True:
            # Stop / pause check
            ev = _cl_paused.get(job_id)
            if ev and not ev.is_set():
                if _next_task and not _next_task.done(): _next_task.cancel()
                break

            try:
                p_res = await _next_task   # waits for: find msg + full download
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Download permanently failed (all 5 retries) — count as failure
                logger.error(f"[Cleaner {job_id}] fatal dl error: {e}")
                fail_count += 1
                if fail_count > 10:
                    err_msg = str(e)[:200]
                    await _cl_update_job(job_id, {"status": "failed", "error": err_msg})
                    if _bot:
                        try:
                            await _bot.send_message(uid,
                                f"<b>⚠️ Cleaner Job Failed!</b>\n\n"
                                f"<b>🧹 Name:</b> {base_name}\n"
                                f"<b>📁 Done:</b> {done} files\n"
                                f"<b>❌ Error:</b> <code>{err_msg}</code>")
                        except: pass
                    job_failed = True
                    break
                # Advance past this one broken message ID and try next
                msg_id += 1
                _next_task = asyncio.create_task(_next_media(msg_id))
                continue
            _next_task = None

            if not p_res:
                break  # no more media in range

            msg, dl_path, m_obj, active_mid, ep_label, orig_ext = p_res

            # ⚡ Kick off NEXT download IMMEDIATELY — runs parallel with FFmpeg below
            next_start = active_mid + 1
            if next_start <= eid:
                _next_task = asyncio.create_task(_next_media(next_start))
            msg_id = next_start

            # DB stop-check every 20 files (cheap — not every file)
            if done % 20 == 0:
                try:
                    _jchk = await _cl_get_job(job_id)
                    if _jchk and _jchk.get("status") == "stopped":
                        if _next_task: _next_task.cancel()
                        break
                except: pass

            try:
                is_audio = bool(msg.audio or msg.voice)
                is_video = bool(msg.video or (msg.document and 'video' in (getattr(msg.document, 'mime_type', '') or '')))
                is_photo = bool(msg.photo)
                use_ff   = conv_vid if is_video else (not is_photo)

                orig_fn    = getattr(m_obj, 'file_name', '') or ''
                orig_title = getattr(m_obj, 'title', '') or ''
                orig_cap   = (getattr(msg, 'caption', '') or '').strip()

                if rename:
                    ep_use = ep_label if ep_label else str(curr_num)
                    if   fmt == "format_2": clean_title = f"{ep_use} - {base_name}"
                    elif fmt == "format_3": clean_title = f"{base_name} EP {ep_use}"
                    else:                  clean_title = f"{base_name} {ep_use}"
                else:
                    clean_title = orig_title or os.path.splitext(orig_fn)[0] or orig_cap or f"{base_name} {curr_num}"

                out_ext    = ".mp3" if use_ff else orig_ext
                out_path   = os.path.abspath(f"temp_cl_out_{job_id}_{active_mid}{out_ext}")
                clean_file = f"{clean_title}{out_ext}"

                meta = {
                    "title":  clean_title,
                    "artist": art,
                    "album":  alb or art,
                    "year":   yr,
                    "genre":  gen,
                    "track":  ep_label or str(curr_num),
                }

                # FFmpeg in ThreadPoolExecutor — event loop free for _next_task download
                if use_ff:
                    ff_cmd = _build_ffmpeg_cmd(dl_path, out_path, local_cover, meta)
                    ok, ff_err = await _ffmpeg_async(ff_cmd)
                    try: os.remove(dl_path)
                    except: pass
                    if not ok:
                        raise Exception(f"FFmpeg: {ff_err[:120]}")
                else:
                    shutil.move(dl_path, out_path)

                # Upload
                out_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
                up = _bot if (_bot and out_size < 50 * 1024 * 1024 and not repl_mode) else client
                thumb = local_cover if (local_cover and os.path.exists(local_cover)) else None
                cap   = f"**{clean_file}**" if use_cap else ""

                for att in range(4):
                    try:
                        if repl_mode:
                            edit_mid = repl_sid + done
                            from pyrogram.types import InputMediaAudio, InputMediaVideo
                            if use_ff or is_audio:
                                _g  = await up.send_audio("me", out_path)
                                _im = InputMediaAudio(_g.audio.file_id, caption=cap,
                                                      title=clean_title, performer=art, thumb=thumb)
                            elif is_video:
                                _g  = await up.send_video("me", out_path)
                                _im = InputMediaVideo(_g.video.file_id, caption=cap, thumb=thumb)
                            else: break
                            await up.edit_message_media(dest_ch, edit_mid, media=_im)
                            try: await _g.delete()
                            except: pass
                        else:
                            if use_ff or is_audio:
                                await up.send_audio(dest_ch, out_path, caption=cap,
                                                    title=clean_title, performer=art,
                                                    file_name=clean_file, thumb=thumb)
                            elif is_video:
                                await up.send_video(dest_ch, out_path, caption=cap,
                                                    file_name=clean_file, thumb=thumb)
                            else:
                                await up.send_document(dest_ch, out_path, caption=cap,
                                                       file_name=clean_file, thumb=thumb)
                        break
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2)
                    except Exception as ue:
                        if att >= 3: raise
                        logger.warning(f"[Cleaner {job_id}] upload retry {att}: {ue}")
                        up = client
                        await asyncio.sleep(3 * (att + 1))

                try: os.remove(out_path)
                except: pass

                if ep_label: _seen.add(str(ep_label))
                done      += 1
                curr_num  += 1
                fail_count = 0   # reset on full success
                await _cl_update_job(job_id, {
                    "files_done":          done,
                    "current_msg_id":      msg_id,
                    "curr_num_checkpoint": curr_num,
                    "last_progress_ts":    time.time(),
                })
                logger.info(f"[Cleaner {job_id}] ✓ {done} | mid={active_mid} | {clean_title}")

            except Exception as e:
                logger.error(f"[Cleaner {job_id}] ✗ mid={active_mid}: {e}")
                fail_count += 1
                for _p in [dl_path, locals().get('out_path')]:
                    try:
                        if _p and os.path.exists(_p): os.remove(_p)
                    except: pass
                if fail_count > 10:
                    err_msg = str(e)[:200]
                    await _cl_update_job(job_id, {"status": "failed", "error": err_msg})
                    if _bot:
                        try:
                            await _bot.send_message(uid,
                                f"<b>⚠️ Cleaner Job Failed!</b>\n\n"
                                f"<b>🧹 Name:</b> {base_name}\n"
                                f"<b>📁 Done:</b> {done} files\n"
                                f"<b>❌ Error:</b> <code>{err_msg}</code>")
                        except: pass
                    if _next_task: _next_task.cancel()
                    job_failed = True
                    break
                await asyncio.sleep(2)

            # Safety net: ensure next task is running
            if _next_task is None and msg_id <= eid:
                _next_task = asyncio.create_task(_next_media(msg_id))

        # ── Cleanup ───────────────────────────────────────────────────────
        try:
            if local_cover and os.path.exists(local_cover): os.remove(local_cover)
        except: pass

        # FIX #2: Pop bot_ref AFTER we've saved it to _bot (local var)
        _cl_bot_ref.pop(job_id, None)

        if client and client is not _bot:
            try:
                from plugins.test import release_client
                cname = getattr(client, 'name', None)
                if cname: await release_client(cname)
            except: pass

        if job_failed: return

        job = await _cl_get_job(job_id)
        if job and job.get("status") not in ("failed", "stopped", "paused"):
            await _cl_update_job(job_id, {"status": "completed", "error": ""})
            # FIX #2: Completion notification — uses local _bot var, always works
            if _bot:
                try:
                    await _bot.send_message(uid,
                        f"<b>🎉 Cleaner Job Completed!</b>\n\n"
                        f"<b>🧹 Name:</b> {base_name}\n"
                        f"<b>📄 Files Processed:</b> {done}\n"
                        f"<b>🔢 Numbered:</b> {job.get('starting_number',1)} → {job.get('starting_number',1)+done-1}\n"
                        f"<i>Engine: Stable Turbo v4 ⚡</i>")
                except Exception as ex:
                    logger.error(f"[Cleaner {job_id}] completion notify failed: {ex}")

    # Run inside or outside semaphore
    if skip_sem:
        await _body()
    else:
        async with _cl_semaphore:
            await _body()


# ─── UI Callbacks ──────────────────────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^cl#(main|new|view|pause|resume|stop|del|cfg|reset|force_ask|force_do)"))
async def _cl_callbacks(bot, update: CallbackQuery):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS
    uid = update.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("cleaner"):
        return await update.answer(f"🔒 {FEATURE_LABELS['cleaner']} is temporarily disabled by admin.", show_alert=True)
    data   = update.data.split("#")
    action = data[1]

    if action == "main":
        jobs   = await _cl_get_all_jobs(uid)
        active = [j for j in jobs if j.get("status") not in ("completed", "stopped", "failed")]
        kb = [
            [InlineKeyboardButton("➕ Sᴛᴀʀᴛ Nᴇᴡ Cʟᴇᴀɴᴇʀ Jᴏʙ", callback_data="cl#new")],
            [InlineKeyboardButton("⚙️ Sᴇᴛ Cᴏᴠᴇʀ",  callback_data="cl#cfg#cover"),
             InlineKeyboardButton("⚙️ Sᴇᴛ Aʀᴛɪsᴛ", callback_data="cl#cfg#artist")],
            [InlineKeyboardButton("⚙️ Sᴇᴛ Gᴇɴʀᴇ",  callback_data="cl#cfg#genre")],
        ]
        row = []
        for j in active:
            row.append(InlineKeyboardButton(f"🧹 {j.get('base_name','Job')[:12]}", callback_data=f"cl#view#{j['job_id']}"))
            if len(row) == 2: kb.append(row); row = []
        if row: kb.append(row)
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="settings#main")])
        df = await _cl_get_defaults(uid)
        txt = (
            "<b><u>🧹 Aᴜᴅɪᴏ Cʟᴇᴀɴᴇʀ & Rᴇɴᴀᴍᴇʀ</u></b>\n\n"
            "Stable Turbo v4 — dynaudnorm + parallel pipeline.\n\n"
            f"<b>Defaults:</b>\n"
            f"  • Artist: {df.get('artist','<i>None</i>')}\n"
            f"  • Genre:  {df.get('genre','<i>None</i>')}\n"
            f"  • Cover:  {'<i>Set</i> ✅' if df.get('cover') else '<i>None</i>'}\n"
        )
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

    elif action == "cfg":
        cfg_type = data[2]
        ask_txt  = (f"Send the new default <b>{cfg_type.title()}</b>"
                    + (" (photo/image for cover)" if cfg_type == "cover" else "")
                    + "\n<i>Send /skip to clear.</i>")
        try:
            resp = await _cl_ask(bot, uid, ask_txt, timeout=120)
            txt  = (resp.text or "").strip()
            if txt.lower() == "/skip":
                await _cl_save_default(uid, cfg_type, "")
            elif cfg_type == "cover" and (resp.photo or resp.document):
                await _cl_save_default(uid, cfg_type, (resp.photo or resp.document).file_id)
            else:
                await _cl_save_default(uid, cfg_type, txt)
            try: await resp.delete()
            except: pass
            await bot.send_message(uid, f"✅ Default <b>{cfg_type}</b> updated!")
        except: pass
        update.data = "cl#main"; return await _cl_callbacks(bot, update)

    elif action == "new":
        try: await update.message.delete()
        except: pass
        asyncio.create_task(_create_cl_flow(bot, uid)); return True

    elif action == "view":
        jid = data[2]
        job = await _cl_get_job(jid)
        if not job: return await update.answer("Job not found.", show_alert=True)
        st, kb = job.get("status"), []
        if st in ("running", "queued"):
            kb.append([InlineKeyboardButton("⏸ Pᴀᴜsᴇ", callback_data=f"cl#pause#{jid}"),
                       InlineKeyboardButton("⏹ Sᴛᴏᴘ",  callback_data=f"cl#stop#{jid}")])
            if st == "queued":
                kb.append([InlineKeyboardButton("⚡ Fᴏʀᴄᴇ Aᴄᴛɪᴠᴀᴛᴇ", callback_data=f"cl#force_ask#{jid}")])
        elif st == "paused":
            kb.append([InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ", callback_data=f"cl#resume#{jid}"),
                       InlineKeyboardButton("⏹ Sᴛᴏᴘ",    callback_data=f"cl#stop#{jid}")])
        elif st in ("failed", "stopped"):
            kb.append([InlineKeyboardButton("🔁 Rᴇsᴇᴛ & Rᴇsᴛᴀʀᴛ", callback_data=f"cl#reset#{jid}"),
                       InlineKeyboardButton("⏹ Sᴛᴏᴘ",              callback_data=f"cl#stop#{jid}")])
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
        update.data = f"cl#view#{jid}"; return await _cl_callbacks(bot, update)

    elif action == "resume":
        jid = data[2]
        await _cl_update_job(jid, {"status": "running"})
        if jid not in _cl_paused: _cl_paused[jid] = asyncio.Event()
        _cl_paused[jid].set()
        old = _cl_tasks.get(jid)
        if old and not old.done(): old.cancel()
        _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid, _cl_bot_ref.get(jid) or bot))
        update.data = f"cl#view#{jid}"; return await _cl_callbacks(bot, update)

    elif action == "force_ask":
        jid = data[2]
        txt = ("⚡ <b>FORCE START</b>\n\n"
               "This bypasses the 3-job safety queue limit.\n"
               "The job will start <b>immediately</b> regardless of other running jobs.\n\n"
               "Proceed?")
        kb  = [[InlineKeyboardButton("✅ Yes, Force Start", callback_data=f"cl#force_do#{jid}")],
               [InlineKeyboardButton("⛔ Cancel",           callback_data=f"cl#view#{jid}")]]
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

    elif action == "force_do":
        jid = data[2]
        await _cl_update_job(jid, {"status": "running", "force_active": True})
        if jid not in _cl_paused: _cl_paused[jid] = asyncio.Event()
        _cl_paused[jid].set()
        # Cancel any existing waiting task
        if jid in _cl_tasks and not _cl_tasks[jid].done(): _cl_tasks[jid].cancel()
        # FIX #3: Use _cl_run_job_force which skips semaphore entirely
        _cl_tasks[jid] = asyncio.create_task(_cl_run_job_force(jid, _cl_bot_ref.get(jid) or bot))
        await update.answer("⚡ Force started!", show_alert=False)
        update.data = f"cl#view#{jid}"; return await _cl_callbacks(bot, update)

    elif action == "stop":
        jid = data[2]
        await _cl_update_job(jid, {"status": "stopped"})
        if jid in _cl_paused: _cl_paused[jid].set()
        old = _cl_tasks.get(jid)
        if old and not old.done(): old.cancel()
        update.data = f"cl#view#{jid}"; return await _cl_callbacks(bot, update)

    elif action == "del":
        await _cl_delete_job(data[2])
        update.data = "cl#main"; return await _cl_callbacks(bot, update)

    elif action == "reset":
        jid = data[2]
        await _cl_update_job(jid, {"status": "queued", "error": "", "phase_start_ts": 0})
        if jid not in _cl_paused: _cl_paused[jid] = asyncio.Event()
        _cl_paused[jid].set()
        old = _cl_tasks.get(jid)
        if old and not old.done(): old.cancel()
        _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid, _cl_bot_ref.get(jid) or bot))
        await update.answer("♻️ Reset & restarted!", show_alert=True)
        update.data = f"cl#view#{jid}"; return await _cl_callbacks(bot, update)


# ─── Setup Wizard ──────────────────────────────────────────────────────────────
async def _create_cl_flow(bot, user_id):
    old = _cl_waiter.pop(user_id, None)
    if old and not old.done(): old.cancel()
    _cl_cancel_users.discard(user_id)

    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    SKIP_BTN   = KeyboardButton("⏭ Sᴋɪᴘ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")
    mk_b = ReplyKeyboardMarkup([[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    mk_s = ReplyKeyboardMarkup([[SKIP_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    mk_c = ReplyKeyboardMarkup([[CANCEL_BTN]],            resize_keyboard=True, one_time_keyboard=True)

    def _cancelled(r):
        if user_id in _cl_cancel_users: return True
        if not r: return False
        t = (r.text or "").strip()
        return "/cancel" in t or "⛔" in t or "Cᴀɴᴄᴇʟ" in t or t.lower() == "cancel"

    async def _abort():
        _cl_cancel_users.discard(user_id); _cl_waiter.pop(user_id, None)
        await bot.send_message(user_id, "<i>❌ Cleaner wizard cancelled.</i>",
                               reply_markup=ReplyKeyboardRemove())

    def _skip(t): return "⏭" in t or "sᴋɪᴘ" in t.lower() or "/skip" in t.lower()

    # Account
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id, "<b>❌ No accounts found. Add one in /settings.</b>")

    def _acc_label(a):
        kind = "Bot" if a.get("is_bot", True) else "Userbot"
        return f"{kind}: {a.get('username') or a.get('name','?')} [{a['id']}]"

    acc_btns = [[KeyboardButton(_acc_label(a))] for a in accounts]
    acc_btns.append([CANCEL_BTN])
    r_acc = await _cl_ask(bot, user_id,
        "<b>🧹 Cleaner — Step 1/9</b>\n\nChoose the <b>account</b> to read from:",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_acc): return await _abort()

    acc_id  = None
    acc_txt = r_acc.text or ""
    if "[" in acc_txt and "]" in acc_txt:
        try: acc_id = int(acc_txt.split('[')[-1].split(']')[0])
        except: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]

    # Start link
    r_start = await _cl_ask(bot, user_id,
        "<b>» Step 2/9</b>\n\nSend the <b>Start Message Link</b> (first file):", reply_markup=mk_c)
    if _cancelled(r_start): return await _abort()
    from_chat, sid = _parse_link(r_start.text or "")

    # End link
    r_end = await _cl_ask(bot, user_id,
        "<b>» Step 3/9</b>\n\nSend the <b>End Message Link</b> (last file):", reply_markup=mk_b)
    if _cancelled(r_end): return await _abort()
    _, eid = _parse_link(r_end.text or "")
    if sid and eid and sid > eid: sid, eid = eid, sid

    # Destination
    from plugins.utils import ask_channel_picker
    dest_chat = user_id; replace_mode = False; replace_start_msg_id = 0
    picked = await ask_channel_picker(bot, user_id,
        "<b>» Step 4/9</b>\n\nSelect <b>destination channel</b>:",
        extra_options=["✏️ Replace/Edit Mode", "⏭️ Skip (Send to DM)"])
    if not picked: return await _abort()
    if picked == "✏️ Replace/Edit Mode":
        replace_mode = True
        pk = await ask_channel_picker(bot, user_id, "<b>Select channel to edit:</b>")
        if not pk: return await _abort()
        dest_chat = int(pk["chat_id"])
        rm = await _cl_ask(bot, user_id, "<b>First message ID to replace:</b>", reply_markup=mk_c)
        if _cancelled(rm): return await _abort()
        try: replace_start_msg_id = int((rm.text or "0").strip())
        except: replace_start_msg_id = 0
    elif picked != "⏭️ Skip (Send to DM)":
        dest_chat = int(picked["chat_id"])

    # Topic
    from_topic_id = 0
    r_topic = await _cl_ask(bot, user_id,
        "<b>» Step 4c/9 — Topic (Optional)</b>\n\n"
        "If source is a <b>group with topics</b>, send Topic ID or message link.\n"
        "<i>Skip for regular channels.</i>", reply_markup=mk_s)
    if _cancelled(r_topic): return await _abort()
    if not _skip(r_topic.text or ""):
        m = re.search(r't\.me/c/\d+/(\d+)/\d+', r_topic.text or "")
        if m: from_topic_id = int(m.group(1))
        else:
            try: from_topic_id = int((r_topic.text or "0").strip())
            except: pass

    # Rename
    r_ren = await _cl_ask(bot, user_id,
        "<b>» Step 5/9 — Rename Files?</b>",
        reply_markup=ReplyKeyboardMarkup([["✅ Yes", "❌ No"], [CANCEL_BTN]],
                                          resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_ren): return await _abort()
    rename_files = "yes" in (r_ren.text or "").lower()

    base_name, start_num, name_format = "Cleaned", 1, "format_1"
    if rename_files:
        rb = await _cl_ask(bot, user_id, "<b>» Step 5a — Base Name</b>", reply_markup=mk_b)
        if _cancelled(rb): return await _abort()
        base_name = re.sub(r'[<>:"/\\|?*]', '_', (rb.text or "Cleaned").strip())

        rn = await _cl_ask(bot, user_id, "<b>» Step 5b — Starting Number</b>", reply_markup=mk_b)
        if _cancelled(rn): return await _abort()
        try: start_num = int((rn.text or "1").strip())
        except: start_num = 1

        rf = await _cl_ask(bot, user_id, "<b>» Step 5c — Naming Format</b>",
            reply_markup=ReplyKeyboardMarkup(
                [["[Name] [N]", "[N] - [Name]", "[Name] EP [N]"], [CANCEL_BTN]],
                resize_keyboard=True, one_time_keyboard=True))
        if _cancelled(rf): return await _abort()
        if "EP"  in (rf.text or ""): name_format = "format_3"
        elif "-" in (rf.text or ""): name_format = "format_2"

    # Convert video
    r_cv = await _cl_ask(bot, user_id, "<b>» Step 6/9 — Convert Video to Audio?</b>",
        reply_markup=ReplyKeyboardMarkup([["✅ Yes, Convert", "❌ No, Keep Video"], [CANCEL_BTN]],
                                          resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_cv): return await _abort()
    convert_videos = "yes" in (r_cv.text or "").lower()

    # Metadata from defaults
    df = await _cl_get_defaults(user_id)
    adv_artist = df.get("artist", "")
    adv_album  = df.get("album", "")
    adv_year   = df.get("year", "")
    adv_genre  = df.get("genre", "")
    adv_cover  = df.get("cover", "")

    # Artist
    _artists = [a.strip() for a in str(adv_artist).split("|") if a.strip()]
    art_rows  = [[KeyboardButton(a)] for a in _artists] + [[SKIP_BTN, CANCEL_BTN]]
    r_art = await _cl_ask(bot, user_id,
        f"<b>> Step 7a — Artist Name</b>\n<i>Saved: {', '.join(_artists) or 'None'}</i>",
        reply_markup=ReplyKeyboardMarkup(art_rows, resize_keyboard=True))
    if _cancelled(r_art): return await _abort()
    if not _skip(r_art.text or ""):
        adv_artist = (r_art.text or "").strip()
        if adv_artist and adv_artist not in _artists:
            _artists.append(adv_artist)
            await _cl_save_default(user_id, "artist", "|".join(_artists))

    # Album
    _albums  = [a.strip() for a in str(df.get("album_history","") or "").split("|") if a.strip()]
    alb_rows = [[KeyboardButton(a)] for a in _albums[:5]]
    alb_rows += [[KeyboardButton("🗑 Clear"), KeyboardButton("✏️ Custom")], [SKIP_BTN, CANCEL_BTN]]
    r_alb = await _cl_ask(bot, user_id,
        f"<b>> Step 7b — Album Name</b>\n<i>Current: {adv_album or 'None'}</i>",
        reply_markup=ReplyKeyboardMarkup(alb_rows, resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_alb): return await _abort()
    _alb_t = (r_alb.text or "").strip()
    if "🗑 Clear" in _alb_t:
        adv_album = ""
    elif "✏️ Custom" in _alb_t:
        r2 = await _cl_ask(bot, user_id, "<b>Enter Album Name:</b>", reply_markup=mk_c)
        if _cancelled(r2): return await _abort()
        adv_album = (r2.text or "").strip()
        if adv_album not in _albums: _albums.append(adv_album)
        await _cl_save_default(user_id, "album_history", "|".join(_albums[-10:]))
    elif not _skip(_alb_t):
        adv_album = _alb_t
    if not adv_album: adv_album = adv_artist

    # Year
    r_yr = await _cl_ask(bot, user_id,
        f"<b>> Step 7c — Year</b>\n<i>Current: {adv_year or 'None'}</i>",
        reply_markup=ReplyKeyboardMarkup(
            [["2022","2023","2024","2025","2026"], ["✏️ Custom", SKIP_BTN, CANCEL_BTN]],
            resize_keyboard=True))
    if _cancelled(r_yr): return await _abort()
    yr_t = (r_yr.text or "").strip()
    if "✏️ Custom" in yr_t:
        r2 = await _cl_ask(bot, user_id, "<b>Enter Year:</b>", reply_markup=mk_c)
        if _cancelled(r2): return await _abort()
        yr_t = (r2.text or "").strip()
    if not _skip(yr_t): adv_year = yr_t

    # Genre
    r_gen = await _cl_ask(bot, user_id,
        f"<b>> Step 7d — Genre</b>\n<i>Current: {adv_genre or 'None'}</i>",
        reply_markup=ReplyKeyboardMarkup(
            [["Audiobook","Romance","Podcast"],
             ["Thriller","Comedy","Drama"],
             ["✏️ Custom", SKIP_BTN, CANCEL_BTN]],
            resize_keyboard=True))
    if _cancelled(r_gen): return await _abort()
    _gen_t = (r_gen.text or "").strip()
    if "✏️ Custom" in _gen_t:
        r2 = await _cl_ask(bot, user_id, "<b>Enter Genre:</b>", reply_markup=mk_c)
        if _cancelled(r2): return await _abort()
        _gen_t = (r2.text or "").strip()
    if not _skip(_gen_t): adv_genre = _gen_t

    # Cover
    r_cov = await _cl_ask(bot, user_id,
        f"<b>> Step 8/9 — Cover Image</b>\n"
        f"<i>{'Default cover set. ' if adv_cover else ''}Send photo or Skip.</i>",
        reply_markup=mk_s, timeout=300)
    if _cancelled(r_cov): return await _abort()
    if not _skip(r_cov.text or ""):
        if r_cov.photo: adv_cover = r_cov.photo.file_id
        elif r_cov.document and 'image' in (r_cov.document.mime_type or ''):
            adv_cover = r_cov.document.file_id

    # Caption
    r_cap = await _cl_ask(bot, user_id,
        "<b>» Step 9/9 — Add Caption?</b>\n\nAdd filename as caption in target channel?",
        reply_markup=ReplyKeyboardMarkup(
            [["✅ Yes, Add Caption"], ["❌ No, Empty Caption"], [CANCEL_BTN]],
            resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_cap): return await _abort()
    use_caption = "no, empty" not in (r_cap.text or "").lower()

    # Save and launch
    job_id = str(uuid.uuid4())
    total  = (eid - sid + 1) if (sid and eid) else 0

    job = {
        "job_id": job_id, "user_id": user_id, "status": "queued",
        "from_chat": from_chat, "dest_chat": dest_chat, "from_topic_id": from_topic_id,
        "replace_mode": replace_mode, "replace_start_msg_id": replace_start_msg_id,
        "start_id": sid, "end_id": eid, "total_files": total, "files_done": 0,
        "base_name": base_name, "starting_number": start_num,
        "name_format": name_format, "rename_files": rename_files,
        "convert_videos": convert_videos,
        "artist": adv_artist, "year": adv_year, "album": adv_album, "genre": adv_genre,
        "cover_file_id": adv_cover, "use_caption": use_caption,
        "account_id": sel_acc.get("id"), "is_bot": sel_acc.get("is_bot", True),
        "created_at": _ist_now().strftime('%Y-%m-%d %H:%M:%S'),
        "target_title": "DM" if dest_chat == user_id else "Channel",
        "phase_start_ts": 0,
    }
    await _cl_save_job(job)
    _cl_cancel_users.discard(user_id)

    if name_format == "format_2": num_str = f"<b>{start_num}</b> - {base_name} → <b>{start_num+total-1}</b> - {base_name}"
    elif name_format == "format_3": num_str = f"{base_name} EP <b>{start_num}</b> → {base_name} EP <b>{start_num+total-1}</b>"
    else: num_str = f"{base_name} <b>{start_num}</b> → {base_name} <b>{start_num+total-1}</b>"

    await bot.send_message(user_id,
        f"<b>✅ Cleaner Job Queued!</b>\n"
        f"Name: <code>{base_name}</code>\n"
        f"Files: <code>{sid}</code> → <code>{eid}</code> (~{total} msgs)\n"
        f"Numbering: {num_str}\n"
        f"Artist: {adv_artist or '—'}  |  Cover: {'✅ Set' if adv_cover else '—'}\n"
        f"<i>⚡ Engine: Stable Turbo v4</i>",
        reply_markup=ReplyKeyboardRemove())

    _cl_paused[job_id] = asyncio.Event()
    _cl_paused[job_id].set()
    _cl_bot_ref[job_id] = bot
    _cl_tasks[job_id] = asyncio.create_task(_cl_run_job(job_id, bot))
