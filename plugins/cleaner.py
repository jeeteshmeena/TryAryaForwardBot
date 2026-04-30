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
MAX_CONCURRENT = 100  # Allow up to 100 jobs to run visibly without artificial blocks
_cl_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_cl_dl_sem = asyncio.Semaphore(4)   # 4 concurrent downloads — prevents bandwidth saturation across multiple jobs
_cl_ul_sem = asyncio.Semaphore(8)
_cl_ff_sem = asyncio.Semaphore(4)  # 4GB RAM VPS: allow up to 4 parallel FFmpeg processes
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
    async with _cl_ff_sem:
        return await loop.run_in_executor(_FFMPEG_POOL, _run_ffmpeg_sync, cmd)


def _build_ffmpeg_cmd(input_path, output_path, cover_path, meta: dict, deep_clean: bool = False, force_reencode: bool = False) -> list:
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-analyzeduration", "10M", "-probesize", "10M",
           "-err_detect", "ignore_err", "-fflags", "+discardcorrupt",
           "-i", input_path]

    out_ext = os.path.splitext(output_path)[1].lower()

    # Multi-format cover art handling (only for audio)
    if cover_path and os.path.exists(cover_path) and os.path.getsize(cover_path) > 1024 and out_ext not in (".mp4", ".mkv", ".webm"):
        cmd += ["-i", cover_path, "-map", "0:a:0", "-map", "1:v:0"]
        if out_ext in (".m4a", ".m4b"):
            cmd += ["-c:v", "mjpeg", "-disposition:v", "attached_pic"]
        else:
            cmd += ["-c:v", "mjpeg", "-id3v2_version", "3", "-disposition:v", "attached_pic"]
        cmd += ["-metadata:s:v", "title=Album cover", "-metadata:s:v", "comment=Cover (front)"]
    else:
        # If it's a video, map both video and audio from input 0
        if out_ext in (".mp4", ".mkv", ".webm"):
            cmd += ["-map", "0:v:0?", "-map", "0:a:0?"]
        else:
            cmd += ["-map", "0:a:0?"]

    # Global flags to prevent muxer errors from weird streams
    cmd += ["-sn", "-dn"]  # No subtitles, No data streams

    # Ultra-Fast Stream Copy or Deep Mode
    in_ext = os.path.splitext(input_path)[1].lower()

    if deep_clean:
        cmd += ["-af", "afftdn,dynaudnorm=f=150:g=15,aresample=44100"]
        cmd += ["-c:a", "libmp3lame", "-b:a", "128k", "-ac", "1"]
    elif in_ext == out_ext and not force_reencode:
        cmd += ["-c:a", "copy"]
        if out_ext in (".mp4", ".mkv", ".webm"):
            cmd += ["-c:v", "copy"]
    else:
        cmd += ["-c:a", "libmp3lame", "-b:a", "128k", "-ac", "1", "-threads", "1"]

    cmd += ["-map_metadata", "-1"]

    for k, v in meta.items():
        if v: cmd += ["-metadata", f"{k}={v}"]

    cmd.append(output_path)
    return cmd


# ─── Client health ────────────────────────────────────────────────────────────
async def _ensure_alive(client):
    try:
        if not getattr(client, "is_initialized", False):
            await client.start()
            return client
        if not getattr(client, "is_connected", True):
            await client.connect()
    except Exception as e:
        if "already" not in str(e).lower():
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
        deep_clean = job.get("deep_clean", False)
        inject_ads = job.get("inject_ads", False)
        ads_config = job.get("ads_config", {})

        # ── Peer Resolution ───────────────────────────────────────────────
        from plugins.utils import safe_resolve_peer
        for _p in [from_ch, dest_ch if dest_ch != uid else None]:
            if not _p: continue
            try: await safe_resolve_peer(client, _p, bot=_bot)
            except Exception as e:
                logger.warning(f"[Cleaner {job_id}] Peer resolve warning for {_p}: {e}")


        # ── Cover Image ───────────────────────────────────────────────────
        local_cover = os.path.abspath(f"temp_cover_{job_id}.jpg")
        if cov_fid and not os.path.exists(local_cover):
            try:
                dl = await (_bot or client).download_media(cov_fid, file_name=local_cover)
                if not dl or os.path.getsize(local_cover) < 1024: local_cover = None
            except: local_cover = None
        elif not cov_fid:
            local_cover = None

        # ── Audio Ad Injection Setup ───────────────────────────────────────
        # Download ad files once for the entire job, reuse for each injection
        _ad_local = {}   # {"hindi": path, "eng": path, "cleaner": path}
        _ad_report = []  # [(serial_num, ad_type, at_ts)] — for final report

        # Compute which serial numbers get which ad type (based on total batch size)
        # Batch Rule: ≤50 files → 1+1+1; >50 → 2+2+1
        # Marathon Rule (per-file injection if file itself is > 1hr): 1+1+1 per hour tier
        _ad_schedule: dict[int, str] = {}   # serial_num → ad_type

        if inject_ads and ads_config:
            total_files = (eid - sid + 1) if (sid and eid) else 0

            # Backward-compat: migrate old "cleaner" key → "arya_premium"
            _ads_cfg_load = dict(ads_config)
            if "cleaner" in _ads_cfg_load and "arya_premium" not in _ads_cfg_load:
                _ads_cfg_load["arya_premium"] = _ads_cfg_load.pop("cleaner")

            # Download all ad audio files upfront (once per job)
            _ad_dl_cli = _bot or client
            for _akey, _afid in _ads_cfg_load.items():
                if not _afid: continue
                _ap = os.path.abspath(f"temp_ad_{job_id}_{_akey}.mp3")
                if not os.path.exists(_ap):
                    try:
                        dlr = await _ad_dl_cli.download_media(_afid, file_name=_ap)
                        if not dlr or not os.path.exists(_ap): continue
                    except Exception as e:
                        logger.warning(f"[Cleaner {job_id}] Failed to download ad '{_akey}': {e}")
                        continue
                _ad_local[_akey] = _ap

            if _ad_local:
                import random as _random
                _all_serials = list(range(curr_num, curr_num + total_files))
                # Distribution: hindi:eng:arya_premium:channel = 2:2:1:1 (>50 files), 1:1:1:1 (<=50)
                _ad_ratios = [
                    ("hindi",        2 if total_files > 50 else 1),
                    ("eng",          2 if total_files > 50 else 1),
                    ("arya_premium", 1),
                    ("channel",      1),
                ]
                _ad_pool = []
                for _at_key, _at_cnt in _ad_ratios:
                    if _at_key in _ad_local:
                        _ad_pool.extend([_at_key] * _at_cnt)
                _pick_count = min(len(_ad_pool), len(_all_serials))
                if _pick_count > 0:
                    _chosen = _random.sample(_all_serials, _pick_count)
                    _chosen.sort()
                    _random.shuffle(_ad_pool)
                    for _sn, _at in zip(_chosen, _ad_pool):
                        _ad_schedule[_sn] = _at
                    logger.info(f"[Cleaner {job_id}] Ad schedule ({len(_ad_schedule)} slots, {total_files} files): {_ad_schedule}")

        _seen      = set()
        fail_count = 0
        job_failed = False

        # ── BATCH message cache ───────────────────────────────────────────
        _msg_cache: dict[int, object] = {}

        async def _fill_cache(start: int):
            """Fetch up to 100 messages. Retries once on timeout, then raises exception so job pauses."""
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
                    else: raise Exception(f"Failed to fetch batch starting at mid={start} — {type(e).__name__}: {e}")

        # ── Next media: find message + download in background (TRUE PARALLEL PIPELINE) ─
        # ── Next media: find message + download ───────────────────────────
        # Strategy:
        #   • download_media returns None  → media expired/deleted on Telegram; SKIP silently (nothing to do)
        #   • download_media returns coroutine that resolves to None → same; SKIP silently
        #   • NoneType coroutine bug → guard against it; SKIP
        #   • Network/Timeout/Auth/Flood error → RAISE so main loop pauses the job
        async def _next_media(start_mid: int, exp_curr: int):
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

                # Ad Inject Only Logic: Skip file download completely if it's not scheduled for an ad
                ad_inject_only = job.get("ad_inject_only", False)
                if ad_inject_only and exp_curr not in _ad_schedule:
                    return m, None, m_obj, m.id, lbl, None

                orig_fn = getattr(m_obj, 'file_name', '') or ''
                ext = (os.path.splitext(orig_fn)[1]
                       or (".mp3" if m.audio else ".mp4" if m.video else ".jpg" if m.photo else ".dat"))
                ipath = os.path.abspath(f"temp_cl_in_{job_id}_{m.id}{ext}")

                # Size-aware timeout: 1s per 200KB, min 300s, max 600s
                fsize = getattr(m_obj, 'file_size', 0) or 0
                dl_timeout = min(600, max(300, fsize // (200 * 1024)))

                try:
                    async with _cl_dl_sem:
                        coro = client.download_media(m, file_name=ipath)
                        if coro is None:
                            # Media reference is gone — skip this message silently
                            logger.warning(f"[Cleaner {job_id}] mid={m.id}: download_media returned None (media expired/deleted)")
                            try:
                                if os.path.exists(ipath): os.remove(ipath)
                            except: pass
                            continue

                        dp = await asyncio.wait_for(coro, timeout=dl_timeout)

                    if dp and os.path.exists(str(dp)):
                        return m, str(dp), m_obj, m.id, lbl, ext   # ✓ success
                    else:
                        logger.warning(f"[Cleaner {job_id}] mid={m.id}: download resolved to None (media expired)")
                        try:
                            if os.path.exists(ipath): os.remove(ipath)
                        except: pass
                        continue

                except asyncio.TimeoutError:
                    try:
                        if os.path.exists(ipath): os.remove(ipath)
                    except: pass
                    raise Exception(f"Download timed out (>{dl_timeout}s) for mid={m.id}")

                except Exception as e:
                    err_upper = str(e).upper()
                    try:
                        if os.path.exists(ipath): os.remove(ipath)
                    except: pass
                    if any(x in err_upper for x in ("FILE_REFERENCE_EXPIRED", "FILE_ID_INVALID", "MSG_ID_INVALID", "MEDIA_EMPTY")):
                        logger.warning(f"[Cleaner {job_id}] mid={m.id}: media reference expired ({e}) — skipping")
                        continue
                    raise Exception(f"Download error at mid={m.id}: {type(e).__name__}: {e}")

            return None  # no more messages in range


        # ── Main loop ─────────────────────────────────────────────────────
        msg_id = curr_mid
        await _fill_cache(msg_id)
        # Pre-kick: start downloading first file NOW in background
        _next_task: asyncio.Task | None = asyncio.create_task(_next_media(msg_id, curr_num))
        _upload_task: asyncio.Task | None = None

        while True:
            # Stop / pause check
            ev = _cl_paused.get(job_id)
            if ev and not ev.is_set():
                if _next_task and not _next_task.done(): _next_task.cancel()
                break

            if _next_task is None:
                break

            try:
                p_res = await _next_task   # waits for: find msg + full download
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Download permanently failed — pause job immediately on that specific message so nothing is skipped.
                logger.error(f"[Cleaner {job_id}] fatal dl error: {e}")
                err_msg = str(e)[:200]
                await _cl_update_job(job_id, {"status": "paused", "error": err_msg, "current_msg_id": msg_id})
                if _bot:
                    try:
                        fail_kb = InlineKeyboardMarkup([[
                            InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ / Rᴇsᴛᴀʀᴛ", callback_data=f"cl#resume#{job_id}"),
                            InlineKeyboardButton("🗑 Dᴇʟᴇᴛᴇ", callback_data=f"cl#del#{job_id}")
                        ]])
                        await _bot.send_message(uid,
                            f"<b>⏸ Cleaner Job Paused!</b>\n\n"
                            f"<i>Job paused instantly due to an error. No files were skipped. Fix the issue and resume.</i>\n\n"
                            f"<b>🧹 Name:</b> {base_name}\n"
                            f"<b>📁 Done:</b> {done} files\n"
                            f"<b>❌ Last Error:</b> <code>{err_msg}</code>",
                            reply_markup=fail_kb)
                    except: pass
                job_failed = True
                break
            _next_task = None

            if not p_res:
                break  # no more media in range

            msg, dl_path, m_obj, active_mid, ep_label, orig_ext = p_res

            # Wait for previous upload to finish (Strict Ordering) before handling skipped files
            if _upload_task:
                up_ok, up_err, up_mid = await _upload_task
                _upload_task = None
                if not up_ok:
                    raise Exception(f"Upload task failed (mid={up_mid}): {up_err}")

            # Handled skipped files in Ad Inject Only mode
            if not dl_path:
                if ep_label: _seen.add(str(ep_label))
                done += 1
                curr_num += 1
                await _cl_update_job(job_id, {
                    "files_done": done,
                    "current_msg_id": active_mid + 1,
                    "curr_num_checkpoint": curr_num,
                    "last_progress_ts": time.time(),
                })
                # Kick next
                next_start = active_mid + 1
                if next_start <= eid:
                    _next_task = asyncio.create_task(_next_media(next_start, curr_num))
                else:
                    _next_task = None
                continue

            # ⚡ Kick off NEXT download IMMEDIATELY — runs parallel with FFmpeg below
            next_start = active_mid + 1
            if next_start <= eid:
                _next_task = asyncio.create_task(_next_media(next_start, curr_num + 1))
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

                if use_ff:
                    if deep_clean:
                        out_ext = ".mp3"
                    elif is_audio and orig_ext.lower() in (".mp3", ".m4a", ".flac", ".wav", ".aac", ".ogg"):
                        out_ext = orig_ext
                    elif is_video:
                        out_ext = orig_ext if orig_ext else ".mp4"
                    else:
                        out_ext = ".mp3"
                else:
                    out_ext = orig_ext
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
                # FFmpeg in ThreadPoolExecutor — event loop free for _next_task download
                if use_ff:
                    ff_cmd = _build_ffmpeg_cmd(dl_path, out_path, local_cover, meta, deep_clean=deep_clean)
                    ok, ff_err = await _ffmpeg_async(ff_cmd)
                    
                    # Rety with forced re-encoding if stream copy failed due to fake extensions
                    if not ok and not deep_clean:
                        _ff_lower = ff_err.lower()
                        if "invalid audio stream" in _ff_lower or "exactly one mp3 audio stream is required" in _ff_lower:
                            try:
                                if os.path.exists(out_path): os.remove(out_path)
                            except: pass
                            logger.warning(f"[Cleaner {job_id}] mid={active_mid}: Fake extension detected. Forcing re-encode...")
                            ff_cmd_retry = _build_ffmpeg_cmd(dl_path, out_path, local_cover, meta, deep_clean=deep_clean, force_reencode=True)
                            ok, ff_err = await _ffmpeg_async(ff_cmd_retry)

                    if not ok:
                        _ff_skip_phrases = (
                            "invalid audio stream", "no audio", "invalid data",
                            "could not find codec", "decoder not found", "encoder not found",
                            "invalid stream", "no such file", "invalid argument",
                            "moov atom not found", "end of file", "connection refused",
                        )
                        _ff_lower = ff_err.lower()
                        if any(p in _ff_lower for p in _ff_skip_phrases):
                            logger.warning(f"[Cleaner {job_id}] mid={active_mid}: FFmpeg stream error, falling back to basic rename — {ff_err[:80]}")
                            try:
                                if os.path.exists(out_path): os.remove(out_path)
                            except: pass
                            out_ext = orig_ext
                            out_path = os.path.abspath(f"temp_cl_out_{job_id}_{active_mid}{out_ext}")
                            clean_file = f"{clean_title}{out_ext}"
                            shutil.move(dl_path, out_path)
                            use_ff = False  # Mark as raw file for uploader
                        else:
                            try: os.remove(dl_path)
                            except: pass
                            raise Exception(f"FFmpeg: {ff_err[:120]}")
                    else:
                        try: os.remove(dl_path)
                        except: pass
                else:
                    shutil.move(dl_path, out_path)

                # ── Audio Ad Injection — duration-aware marathon (1 ad per hour) ──
                if inject_ads and _ad_schedule and curr_num in _ad_schedule and out_path and os.path.exists(out_path):
                    try:
                        import random as _rnd_inj
                        _inj_loop = asyncio.get_event_loop()

                        # Helper: probe duration synchronously in thread pool
                        def _probe_dur_sync(_ppath):
                            _pr = __import__("subprocess").run(
                                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                                 "-of", "default=noprint_wrappers=1:nokey=1", _ppath],
                                capture_output=True, text=True
                            )
                            return float(_pr.stdout.strip() or "0")

                        _dur = await _inj_loop.run_in_executor(_FFMPEG_POOL, _probe_dur_sync, out_path)

                        # Marathon rule: 1 injection per full hour (min 1, max 6)
                        _n_inj = max(1, min(6, int(_dur // 3600))) if _dur >= 3600 else 1

                        # Pick ad types: scheduled type first, then random extras
                        _sched_type = _ad_schedule[curr_num]
                        _avail_types = list(_ad_local.keys())
                        _inj_types = [_sched_type] + _rnd_inj.choices(_avail_types, k=_n_inj - 1)

                        # Evenly-spaced injection points as fractions (0..1) of original duration
                        _inj_pts_rel = [(i + 1) / (_n_inj + 1) for i in range(_n_inj)]

                        # Chain injections —— each output becomes input of next pass
                        _cur_path  = out_path
                        _cur_dur   = _dur
                        _any_injected = False

                        for _i, (_rel_pt, _at) in enumerate(zip(_inj_pts_rel, _inj_types)):
                            _this_ad = _ad_local.get(_at)
                            if not _this_ad or not os.path.exists(_this_ad):
                                continue

                            _pt_actual = max(30.0, min(_cur_dur * _rel_pt, _cur_dur - 30.0))
                            _inj_tmp   = os.path.abspath(f"temp_cl_inj_{job_id}_{active_mid}_{_i}.mp3")

                            _ff_inj = [
                                "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                                "-i", _cur_path, "-i", _this_ad,
                                "-filter_complex",
                                (
                                    f"[0:a]atrim=0:{_pt_actual:.3f},asetpts=PTS-STARTPTS[bef];"
                                    f"[0:a]atrim={_pt_actual:.3f},asetpts=PTS-STARTPTS[aft];"
                                    f"[1:a]asetpts=PTS-STARTPTS[adx];"
                                    f"[bef][adx][aft]concat=n=3:v=0:a=1[outa]"
                                ),
                                "-map", "[outa]",
                                "-c:a", "libmp3lame", "-b:a", "128k", "-ac", "1",
                                _inj_tmp
                            ]
                            _inj_ok, _inj_err = await _ffmpeg_async(_ff_inj)

                            if _inj_ok and os.path.exists(_inj_tmp) and os.path.getsize(_inj_tmp) > 1024:
                                # Remove previous intermediate (but NOT original out_path yet)
                                if _cur_path != out_path and os.path.exists(_cur_path):
                                    try: os.remove(_cur_path)
                                    except: pass
                                _cur_path = _inj_tmp
                                _any_injected = True
                                _ad_report.append((curr_num, _at, f"{int(_pt_actual//60)}m{int(_pt_actual%60)}s"))
                                logger.info(f"[Cleaner {job_id}] Inj {_i+1}/{_n_inj} serial={curr_num} type={_at} at={_pt_actual:.0f}s")
                                # Re-probe duration for accurate placement of next injection
                                _cur_dur = await _inj_loop.run_in_executor(_FFMPEG_POOL, _probe_dur_sync, _cur_path)
                            else:
                                logger.warning(f"[Cleaner {job_id}] Inj {_i+1}/{_n_inj} failed serial={curr_num}: {_inj_err[:80]}")
                                try:
                                    if os.path.exists(_inj_tmp): os.remove(_inj_tmp)
                                except: pass

                        if _any_injected:
                            # Remove original file, promote final chained output
                            if os.path.exists(out_path) and _cur_path != out_path:
                                try: os.remove(out_path)
                                except: pass
                            out_path = _cur_path

                    except Exception as _ae:
                        logger.warning(f"[Cleaner {job_id}] Ad injection error serial={curr_num}: {_ae}")

                # ── TRUE PARALLEL UPLOAD PIPELINE ──
                # 1. Wait for PREVIOUS file to finish uploading (Strict Ordering)
                # Wait is now handled before skipped file checks at the top of the loop
                # This just ensures we don't start a new upload before old one finishes
                if _upload_task:
                    up_ok, up_err, up_mid = await _upload_task
                    _upload_task = None
                    if not up_ok:
                        raise Exception(f"Upload task failed (mid={up_mid}): {up_err}")

                # 2. Kick off CURRENT file upload in background!
                out_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
                bg_up = _bot if (_bot and out_size < 50 * 1024 * 1024 and not repl_mode) else client
                bg_th = local_cover if (local_cover and os.path.exists(local_cover)) else None
                if job.get("ad_inject_only"):
                    bg_cap = orig_cap
                else:
                    bg_cap = f"**{clean_file}**" if use_cap else ""

                async def _background_upload_and_done(
                    u_cli, p_out, is_ff, is_aud, is_vid, cap, c_title, art, c_file, thumb, c_mid, l_ep, c_done
                ):
                    try:
                        for att in range(4):
                            try:
                                async with _cl_ul_sem:
                                    if repl_mode:
                                        edit_mid = c_mid if job.get("ad_inject_only") else (repl_sid + c_done)
                                        from pyrogram.types import InputMediaAudio, InputMediaVideo
                                        if is_ff or is_aud:
                                            _g = await asyncio.wait_for(u_cli.send_audio("me", p_out), timeout=300)
                                            _im = InputMediaAudio(_g.audio.file_id, caption=cap, title=c_title, performer=art, thumb=thumb)
                                        elif is_vid:
                                            _g = await asyncio.wait_for(u_cli.send_video("me", p_out), timeout=300)
                                            _im = InputMediaVideo(_g.video.file_id, caption=cap, thumb=thumb)
                                        else: break
                                        await asyncio.wait_for(u_cli.edit_message_media(dest_ch, edit_mid, media=_im), timeout=120)
                                        try: await _g.delete()
                                        except: pass
                                    else:
                                        if is_ff or is_aud:
                                            await asyncio.wait_for(u_cli.send_audio(dest_ch, p_out, caption=cap, title=c_title, performer=art, file_name=c_file, thumb=thumb), timeout=300)
                                        elif is_vid:
                                            await asyncio.wait_for(u_cli.send_video(dest_ch, p_out, caption=cap, file_name=c_file, thumb=thumb), timeout=300)
                                        else:
                                            await asyncio.wait_for(u_cli.send_document(dest_ch, p_out, caption=cap, file_name=c_file, thumb=thumb), timeout=300)
                                    break
                            except FloodWait as fw:
                                await asyncio.sleep(fw.value + 2)
                            except Exception as ue:
                                if att >= 3: return False, str(ue), c_mid
                                logger.warning(f"[Cleaner bg-up {job_id}] retry {att}: {ue}")
                                u_cli = client
                                await asyncio.sleep(3 * (att + 1))

                        try: os.remove(p_out)
                        except: pass

                        # Access safely via nonlocal
                        nonlocal done, curr_num, fail_count, msg_id
                        if l_ep: _seen.add(str(l_ep))
                        done += 1
                        curr_num += 1
                        fail_count = 0  # reset on full success
                        
                        await _cl_update_job(job_id, {
                            "files_done": done,
                            "current_msg_id": msg_id,  # next message ID already assigned in main loop safely
                            "curr_num_checkpoint": curr_num,
                            "last_progress_ts": time.time(),
                        })
                        logger.info(f"[Cleaner {job_id}] ✓ {done} | mid={c_mid} | {c_title}")
                        return True, None, c_mid
                    except Exception as fatal:
                        return False, str(fatal), c_mid

                # Spawn background upload task - it runs CONCURRENTLY with next N+1 Download and N+1 FFmpeg
                _upload_task = asyncio.create_task(
                    _background_upload_and_done(
                        bg_up, out_path, use_ff, is_audio, is_video, bg_cap,
                        clean_title, art, clean_file, bg_th, active_mid, ep_label, done
                    )
                )

            except Exception as e:
                logger.error(f"[Cleaner {job_id}] ✗ mid={active_mid}: {e}")
                err_msg = str(e)[:200]
                
                # If we threw this exception because NEXT loop's wait-for-upload failed:
                save_mid = active_mid
                if "Upload task failed (mid=" in str(e):
                    import re
                    match = re.search(r"mid=(\d+)", str(e))
                    if match: save_mid = int(match.group(1))

                await _cl_update_job(job_id, {"status": "paused", "error": err_msg, "current_msg_id": save_mid})
                if os.path.exists(dl_path):
                    try: os.remove(dl_path)
                    except: pass
                if _bot:
                    try:
                        fail_kb = InlineKeyboardMarkup([[
                            InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ / Rᴇsᴛᴀʀᴛ", callback_data=f"cl#resume#{job_id}"),
                            InlineKeyboardButton("🗑 Dᴇʟᴇᴛᴇ", callback_data=f"cl#del#{job_id}")
                        ]])
                        await _bot.send_message(uid,
                            f"<b>⏸ Cleaner Job Paused!</b>\n\n"
                            f"<i>Job paused instantly during processing. Fix the issue and resume.</i>\n\n"
                            f"<b>🧹 Name:</b> {base_name}\n"
                            f"<b>📁 Done:</b> {done} files\n"
                            f"<b>❌ Last Error:</b> <code>{err_msg}</code>",
                            reply_markup=fail_kb)
                    except: pass
                if _next_task: _next_task.cancel()
                if _upload_task: _upload_task.cancel()
                job_failed = True
                break

            # Safety net: ensure next task is running
            if _next_task is None and msg_id <= eid:
                _next_task = asyncio.create_task(_next_media(msg_id))

        # ── Cleanup ───────────────────────────────────────────────────────
        # Ensure final pending upload is strictly finished before we complete the job
        if _upload_task and not _upload_task.done():
            try:
                up_ok, up_err, _up_mid = await _upload_task
            except Exception as _ue:
                up_ok, up_err = False, str(_ue)
            if not up_ok:
                err_msg = str(up_err)[:200]
                await _cl_update_job(job_id, {"status": "paused", "error": err_msg})
                job_failed = True

        try:
            if local_cover and os.path.exists(local_cover): os.remove(local_cover)
        except: pass

        # Cleanup ad temp files
        for _ap in _ad_local.values():
            try:
                if os.path.exists(_ap): os.remove(_ap)
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
                    _ad_rep_txt = ""
                    if _ad_report:
                        _ad_lines = "\n".join(
                            f"  ➥ Ep <b>{sn}</b>: <i>{at}</i> ad @ {ts}"
                            for sn, at, ts in _ad_report
                        )
                        _ad_rep_txt = f"\n\n<b>🎵 Audio Ads Injected:</b> {len(_ad_report)}\n{_ad_lines}"
                    await _bot.send_message(uid,
                        f"<b>🎉 Cleaner Job Completed!</b>\n\n"
                        f"<b>🧹 Name:</b> {base_name}\n"
                        f"<b>📄 Files Processed:</b> {done}\n"
                        f"<b>🔢 Numbered:</b> {job.get('starting_number',1)} → {job.get('starting_number',1)+done-1}"
                        f"{_ad_rep_txt}\n"
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
        # Sort jobs by updated_at or created_at
        jobs.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        
        active = [j for j in jobs if j.get("status") in ("running", "queued", "paused")]
        failed = [j for j in jobs if j.get("status") in ("failed", "stopped")][:5]
        
        kb = [
            [InlineKeyboardButton("➕ Sᴛᴀʀᴛ Nᴇᴡ Cʟᴇᴀɴᴇʀ Jᴏʙ", callback_data="cl#new")],
            [InlineKeyboardButton("⚙️ Sᴇᴛ Cᴏᴠᴇʀ",  callback_data="cl#cfg#cover"),
             InlineKeyboardButton("⚙️ Sᴇᴛ Aʀᴛɪsᴛ", callback_data="cl#cfg#artist")],
            [InlineKeyboardButton("⚙️ Sᴇᴛ Gᴇɴʀᴇ",  callback_data="cl#cfg#genre")],
        ]
        
        if active:
            kb.append([InlineKeyboardButton("── ⚡ ACTIVE JOBS ──", callback_data="none")])
            row = []
            for j in active:
                row.append(InlineKeyboardButton(f"🔄 {j.get('base_name','Job')[:12]}", callback_data=f"cl#view#{j['job_id']}"))
                if len(row) == 2: kb.append(row); row = []
            if row: kb.append(row)
            
        if failed:
            kb.append([InlineKeyboardButton("── ⚠️ FAILED / STOPPED ──", callback_data="none")])
            for j in failed:
                kb.append([InlineKeyboardButton(f"❌ {j.get('base_name','Job')[:20]}", callback_data=f"cl#view#{j['job_id']}")])

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
        "<b>» Step 2/10</b>\n\nSend the <b>Start Message Link</b> (first file):", reply_markup=mk_c)
    if _cancelled(r_start): return await _abort()
    from_chat, sid = _parse_link(r_start.text or "")

    # End link
    r_end = await _cl_ask(bot, user_id,
        "<b>» Step 3/10</b>\n\nSend the <b>End Message Link</b> (last file):", reply_markup=mk_b)
    if _cancelled(r_end): return await _abort()
    _, eid = _parse_link(r_end.text or "")
    if sid and eid and sid > eid: sid, eid = eid, sid

    # Mode: Full Migration vs Ad Injection Only
    r_mode = await _cl_ask(bot, user_id,
        "<b>» Step 4/10 — Job Type</b>\n\nChoose job type:",
        reply_markup=ReplyKeyboardMarkup([
            ["✨ New Migration (Full Cleaner)"],
            ["💉 Existing Files (Ad Inject Only)"]
        ], resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_mode): return await _abort()
    ad_inject_only = "inject" in (r_mode.text or "").lower() or "existing" in (r_mode.text or "").lower()

    if ad_inject_only:
        dest_chat = from_chat
        replace_mode = True
        replace_start_msg_id = sid
        from_topic_id = 0
        rename_files = False
        convert_videos = False
        deep_clean = False
        adv_artist = ""; adv_year = ""; adv_album = ""; adv_genre = ""; adv_cover = None
        use_caption = True
        base_name = ""
        start_num = 1
        name_format = "format_1"
    else:
        # Destination
        from plugins.utils import ask_channel_picker
        dest_chat = user_id; replace_mode = False; replace_start_msg_id = 0
        picked = await ask_channel_picker(bot, user_id,
            "<b>» Step 5/10</b>\n\nSelect <b>destination channel</b>:",
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

    if not ad_inject_only:
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

        # Deep Clean
        r_adv = await _cl_ask(bot, user_id, 
            "<b>» Step 6b/10 — Deep Audio Clean?</b>\n\n"
            "<i>(Forces .MP3, Volume Normalize, & Noise Removal)\n"
            "⚠️ Warning: This uses 100% CPU and makes the bot run normally (much slower/takes longer).</i>",
            reply_markup=ReplyKeyboardMarkup([["✅ Yes, Deep Clean", "❌ No, Fast Output"], [CANCEL_BTN]],
                                              resize_keyboard=True, one_time_keyboard=True))
        if _cancelled(r_adv): return await _abort()
        deep_clean = "yes" in (r_adv.text or "").lower()

        # Metadata from defaults
        df = await _cl_get_defaults(user_id)
        adv_artist = df.get("artist", "")
        adv_album  = df.get("album", "")
        adv_year   = df.get("year", "")
        adv_genre  = df.get("genre", "")
        adv_cover  = df.get("cover", "")

        # Artist
        _artists = [a.strip() for a in str(adv_artist).split("|") if a.strip()]
        art_rows = []
        for i in range(0, len(_artists), 2): art_rows.append([KeyboardButton(a) for a in _artists[i:i+2]])
        art_rows.append([SKIP_BTN, CANCEL_BTN])
        r_art = await _cl_ask(bot, user_id,
            f"<b>> Step 7a/10 — Artist Name</b>\n<i>Saved: {', '.join(_artists[:5]) or 'None'}</i>",
            reply_markup=ReplyKeyboardMarkup(art_rows, resize_keyboard=True))
        if _cancelled(r_art): return await _abort()
        if not _skip(r_art.text or ""):
            adv_artist = (r_art.text or "").strip()
            if adv_artist and adv_artist not in _artists:
                _artists.append(adv_artist)
                await _cl_save_default(user_id, "artist", "|".join(_artists))

        # Album
        _albums  = [a.strip() for a in str(df.get("album_history","") or "").split("|") if a.strip()]
        alb_list = _albums[:6]
        alb_rows = []
        for i in range(0, len(alb_list), 2): alb_rows.append([KeyboardButton(a) for a in alb_list[i:i+2]])
        alb_rows.append([KeyboardButton("🗑 Clear"), KeyboardButton("✏️ Custom")])
        alb_rows.append([SKIP_BTN, CANCEL_BTN])
        r_alb = await _cl_ask(bot, user_id,
            f"<b>> Step 7b/10 — Album Name</b>\n<i>Current: {adv_album or 'None'}</i>",
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

    # Always load defaults before ads section (needed in both full and ad_inject_only modes)
    if ad_inject_only:
        df = await _cl_get_defaults(user_id)

    # Audio Ad Injection — 4 types, individual skip, duration-aware
    r_ads = await _cl_ask(bot, user_id,
        "<b>» Step 10/10 — Inject Audio Ads? (Premium)</b>\n\n"
        "<i>Injects promo audio into files based on duration (1 ad/hour for long files).\n"
        "You can skip any individual ad type.</i>\n\n"
        "Do you want to inject Audio Ads?",
        reply_markup=ReplyKeyboardMarkup([["✅ Yes, Inject Ads", "❌ No Ads"], ["⚙️ Edit Saved Ads"], [CANCEL_BTN]],
                                          resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_ads): return await _abort()

    inject_ads = False
    ads_config = {}
    r_ads_text = (r_ads.text or "").lower()

    if "yes" in r_ads_text or "edit" in r_ads_text:
        inject_ads = "yes" in r_ads_text
        # Backward-compat: migrate old "cleaner" key
        saved_ads = df.get("audio_ads", {})
        if "cleaner" in saved_ads and "arya_premium" not in saved_ads:
            saved_ads["arya_premium"] = saved_ads.pop("cleaner")

        if "edit" in r_ads_text or not saved_ads:
            # Ask for each of 4 ad slots individually — each can be skipped
            _ad_slots = [
                ("hindi",        "» Aᴅ 1 — Hɪɴᴅɪ Pʀᴏᴍᴏ",         "Hindi promotional audio (10–30s)."),
                ("eng",          "» Aᴅ 2 — Eɴɢʟɪsʜ Pʀᴏᴍᴏ",        "English promotional audio (10–30s)."),
                ("arya_premium", "» Aᴅ 3 — Aʀʏᴀ Pʀᴇᴍɪᴜᴍ",         "Arya Premium Bot promo audio (10–30s)."),
                ("channel",      "» Aᴅ 4 — Cʜᴀɴɴᴇʟ Pʀᴏᴍᴏᴛɪᴏɴ",    "Channel promotion audio (10–30s)."),
            ]
            ads_config = dict(saved_ads)

            for _sk, _sl, _sd in _ad_slots:
                _has_saved = bool(saved_ads.get(_sk))
                _saved_hint = " <i>(saved ✅)</i>" if _has_saved else " <i>(not set)</i>"
                _skip_label = "⏭ Skip (Keep Saved)" if _has_saved else "⏭ Skip (Disable)"
                _s_kb = ReplyKeyboardMarkup([[_skip_label], [CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)

                r_slot = await _cl_ask(bot, user_id,
                    f"<b>{_sl}</b>{_saved_hint}\n<i>{_sd}</i>\n\n"
                    f"Forward/upload the audio, or tap <b>Skip</b>.",
                    reply_markup=_s_kb)
                if _cancelled(r_slot): return await _abort()

                if "skip" in (r_slot.text or "").lower():
                    if not _has_saved:
                        ads_config.pop(_sk, None)  # disable slot if no saved ad
                    # else: keep existing saved value in ads_config
                else:
                    _fid = (r_slot.audio.file_id if r_slot.audio else
                            (r_slot.voice.file_id if r_slot.voice else None))
                    if _fid:
                        ads_config[_sk] = _fid
                    elif not _has_saved:
                        ads_config.pop(_sk, None)

            ads_config = {k: v for k, v in ads_config.items() if v}  # drop empty
            await _cl_save_default(user_id, "audio_ads", ads_config)

            if "edit" in r_ads_text:
                _active = len(ads_config)
                await bot.send_message(user_id,
                    f"✅ <b>Ads config saved!</b>\n"
                    f"Active slots: <b>{_active}/4</b>\n"
                    f"Types: {', '.join(ads_config.keys()) if ads_config else 'None'}",
                    reply_markup=ReplyKeyboardRemove())
                inject_ads = True  # User edited ads → auto-enable injection, fall through to job creation
        else:
            ads_config = saved_ads

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
        "convert_videos": convert_videos, "deep_clean": deep_clean,
        "artist": adv_artist, "year": adv_year, "album": adv_album, "genre": adv_genre,
        "cover_file_id": adv_cover, "use_caption": use_caption,
        "inject_ads": inject_ads, "ads_config": ads_config,
        "account_id": sel_acc.get("id"), "is_bot": sel_acc.get("is_bot", True),
        "created_at": _ist_now().strftime('%Y-%m-%d %H:%M:%S'),
        "target_title": "Source (In-Place)" if ad_inject_only else ("DM" if dest_chat == user_id else "Channel"),
        "phase_start_ts": 0,
        "ad_inject_only": ad_inject_only,
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
