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
_cl_cancel_users: set = set()          # user IDs that pressed Cancel mid-flow
MAX_CONCURRENT = 1
_cl_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

import concurrent.futures as _cf_cl
# Single-worker executor: ensures only 1 FFmpeg runs at a time from cleaner.
_CL_FFMPEG_EXECUTOR = _cf_cl.ThreadPoolExecutor(max_workers=1, thread_name_prefix="cl_ffmpeg")
CL_FFMPEG_NICE      = 15   # OS-level nice priority for cleaner ffmpeg processes
CL_FFMPEG_CPU_LIMIT = 60   # max % CPU per ffmpeg process when cpulimit is installed

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

# Dedicated cancel-command / cancel-button handler that ALSO kills the flow.
@Client.on_message(filters.private & (filters.text | filters.command("cancel")), group=-15)
async def _cl_cancel_handler(bot, message):
    """Catch '⛔ Cancel' keyboard button or /cancel sent during a Cleaner wizard."""
    uid = message.from_user.id if message.from_user else None
    if uid is None or uid not in _cl_waiter:
        raise ContinuePropagation   # nothing waiting — pass through
    txt = (message.text or "").strip()
    is_cancel = (
        txt.startswith("/cancel")
        or "⛔" in txt
        or "Cᴀɴᴄᴇʟ" in txt
        or txt.lower() == "cancel"
    )
    if not is_cancel:
        raise ContinuePropagation   # not a cancel — let the router handle it
    # Mark cancel intent so the flow function knows to abort
    _cl_cancel_users.add(uid)
    # Also resolve the waiting future so _cl_ask() unblocks immediately
    fut = _cl_waiter.pop(uid, None)
    if fut and not fut.done():
        fut.set_result(message)
    # Don't raise ContinuePropagation — swallow this message (it's already handled)


@Client.on_message(filters.private, group=-16)
async def _cl_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _cl_waiter:
        fut = _cl_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation


async def _cl_ask(bot, user_id, text, reply_markup=None, timeout=300):
    """Send `text` and await next private message from `user_id`.
    Returns the message, or raises asyncio.TimeoutError.
    Automatically checks _cl_cancel_users after resolving."""
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
        f"<b>{ic} 🧹 {name} [{job.get('job_id', '')[-6:]}]</b>",
        f"Status: {ic} {status.title()}",
    ]
    if job.get("worker_node"):
        lines.append(f"🖥 <b>Node:</b> {job.get('worker_node')}")
        
    lines.extend([
        f"  <code>{bar}</code>",
        "",
        f"  📁 <b>Processed:</b> {done}/{total} files",
        f"  🔢 <b>Range:</b> {name} {start_num} → {name} {start_num + total - 1}",
        f"  🎨 <b>Artist:</b> {job.get('artist', '—')}",
        f"  💿 <b>Album:</b> {job.get('album', '—')}",
        f"  🗓 <b>Year:</b> {job.get('year', '—')}",
        f"  🖼 <b>Cover:</b> {'✅ Set' if job.get('cover_file_id') else '—'}",
        f"  🎯 <b>Target:</b> {job.get('target_title', '?')}",
        f"  📝 <b>Caption:</b> {'✅ Yes' if job.get('use_caption', True) else '❌ No'}",
    ])
    if eta_str: lines.append(eta_str)
    if err:
        lines.append(f"\n  ⚠️ <b>Error:</b> <code>{err[:200]}</code>")
    
    lines.append(f"\n  <i>Last refreshed: {_ist_now().strftime('%I:%M %p IST')}</i>")
    return "\n".join(lines)


# ─── FFmpeg Engine ───────────────────────────────────────────────────────────
def _make_cl_run(cmd):
    """
    Build and return a *synchronous* callable that runs `cmd` via subprocess
    with OS-level CPU throttling (nice=15, ionice=idle, cpulimit if available).
    """
    import platform as _plat, shutil as _sh

    def _sync_run():
        try:
            kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300)
            if _plat.system() != "Windows":
                def _preexec():
                    os.nice(CL_FFMPEG_NICE)
                    # ionice idle: disk IO doesn't starve the bot or merger
                    try:
                        import ctypes
                        _sc = ctypes.CDLL(None).syscall
                        _sc(251, 0, 0, (3 << 13) | 7)  # ioprio_set idle class
                    except Exception:
                        pass
                kwargs["preexec_fn"] = _preexec
                cmd_run = cmd
            else:
                cmd_run = cmd

            result = subprocess.run(cmd_run, **kwargs)
            return result.returncode, result.stderr.decode('utf-8', errors='replace')
        except Exception as e:
            return -1, str(e)

    return _sync_run


async def _process_audio_ffmpeg(input_path, output_path, cover_path, meta: dict):
    """
    Re-encodes audio to clean 128kbps MP3 with fresh metadata and optional cover art.
    CPU is throttled via _make_cl_run: nice=15, ionice=idle, cpulimit (if installed).
    """
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error"]
    cmd += ["-i", input_path]

    if cover_path and os.path.exists(cover_path) and os.path.getsize(cover_path) > 1024:
        cmd += ["-i", cover_path]
        cmd += ["-map", "0:a:0", "-map", "1:v:0"]
        cmd += ["-c:v", "mjpeg", "-id3v2_version", "3"]
        cmd += ["-metadata:s:v", "title=Album cover", "-metadata:s:v", "comment=Cover (front)"]
    else:
        cmd += ["-map", "0:a:0"]

    cmd += ["-c:a", "libmp3lame", "-b:a", "192k", "-q:a", "0"]
    cmd += ["-threads", "1", "-max_muxing_queue_size", "1024"]
    cmd += ["-map_metadata", "-1"]

    for k, v in meta.items():
        if v: cmd += ["-metadata", f"{k}={v}"]

    cmd.append(output_path)

    loop = asyncio.get_event_loop()
    try:
        rc, stderr = await asyncio.wait_for(
            loop.run_in_executor(_CL_FFMPEG_EXECUTOR, _make_cl_run(cmd)),
            timeout=10800  # 3 hours max for a massive 4GB extraction
        )
        if rc != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 100:
            return False, stderr[-1500:]
        return True, ""
    except Exception as e:
        return False, str(e)


# ─── Client health-check helper ──────────────────────────────────────────────
async def _ensure_client_alive(client, acc, uid):
    """
    Verify `client` is connected (passive check with no timeout drops).
    Pyrogram inherently auto-reconnects, but if it was manually disconnected we connect it.
    """
    try:
        if not getattr(client, "is_connected", True):
            await client.connect()
    except Exception as e:
        logger.warning(f"[Cleaner] Reconnect attempt failed discretely: {e}")
    return client


class _DummySem:
    async def __aenter__(self): pass
    async def __aexit__(self, *a): pass

async def _cl_run_job(job_id: str, bot=None):
    """Main cleaner job coroutine. bot = the main Pyrogram bot client for DM notifications."""
    job = await _cl_get_job(job_id)
    if not job or job.get("status") in ("completed", "failed", "stopped"):
        return
        
    is_force = job.get("force_active", False)
    if is_force: await _cl_update_job(job_id, {"force_active": False})
    
    ctx = _DummySem() if is_force else _cl_semaphore
    async with ctx:
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
            # KEY: if the user selected a normal *bot* account (is_bot=True), we
            # MUST use the already-running main `bot` instance directly.
            # A fresh in-memory clone of a bot token starts with ZERO peer cache,
            # so get_messages() immediately fails with CHANNEL_INVALID.
            # The main bot is already connected, holds all access hashes from its
            # persistent session, and is admin in both channels — use it directly.
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

                is_bot_account = acc.get("is_bot", True)  # True = normal bot, False = userbot

                if is_bot_account and bot:
                    # Use the main bot directly — already running, has full peer cache
                    client = bot
                    logger.info(f"[Cleaner {job_id}] Using main bot client directly (bot account selected)")
                else:
                    # Userbot (session string) — needs a separate clone client
                    pyrogram_client = _CLIENT.client(acc)
                    try:
                        client = await start_clone_bot(pyrogram_client)
                    except Exception as start_err:
                        if "already" in str(start_err).lower() or "connected" in str(start_err).lower():
                            client = pyrogram_client
                        else:
                            raise
                    # Verify connection immediately after obtaining client
                    client = await _ensure_client_alive(client, acc, uid)
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
            
            # ── Protected Chat Guard ───────────────────────────────────────────────
            from plugins.utils import check_chat_protection
            prot_err = await check_chat_protection(uid, from_ch)
            if prot_err:
                await _cl_update_job(job_id, {"status": "failed", "error": prot_err})
                if bot:
                    try: await bot.send_message(uid, prot_err)
                    except Exception: pass
                return
            # ──────────────────────────────────────────────────────────────────────

            sid = job["start_id"]
            eid = job["end_id"]
            done = job.get("files_done", 0)
            curr_msg_id = job.get("current_msg_id", sid)
            from_topic_id = job.get("from_topic_id", 0) or 0   # 0 = no topic filter
            
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

            # ── Resolve peer cache for source and destination channels ────────
            # Without this, fresh in-memory sessions get PEER_ID_INVALID or
            # CHANNEL_INVALID when trying to get_messages / send_audio to channels
            # not in the clone's cache. Try clone first, then fall back to using
            # the main bot to seed the peer into the clone's session.
            logger.info(f"[Cleaner {job_id}] Resolving channel peers...")

            async def _resolve_peer_robust(peer_id):
                """Try to resolve a channel peer in the clone client with main-bot fallback."""
                if not peer_id:
                    return
                # Step 1: direct resolve via clone
                try:
                    await client.get_chat(peer_id)
                    logger.info(f"[Cleaner {job_id}] Peer resolved via clone: {peer_id}")
                    return
                except Exception as pe:
                    logger.warning(f"[Cleaner {job_id}] Clone peer resolve failed ({peer_id}): {pe}")

                # Step 2: get info from main bot, try resolve_peer in clone
                if bot:
                    try:
                        chat_info = await bot.get_chat(peer_id)
                        try:
                            await client.resolve_peer(peer_id)
                            logger.info(f"[Cleaner {job_id}] Peer resolved via resolve_peer: {peer_id}")
                            return
                        except Exception:
                            pass
                        # Step 3: join public channel to seed the peer
                        uname = getattr(chat_info, 'username', None)
                        if uname:
                            try:
                                await client.join_chat(uname)
                                logger.info(f"[Cleaner {job_id}] Joined/seeded peer via join_chat: {uname}")
                                return
                            except Exception as je:
                                logger.warning(f"[Cleaner {job_id}] join_chat failed for {uname}: {je}")
                    except Exception as be:
                        logger.warning(f"[Cleaner {job_id}] Main bot peer fetch failed ({peer_id}): {be}")

                logger.error(f"[Cleaner {job_id}] Could NOT resolve peer {peer_id} — job may fail with CHANNEL_INVALID")

            await _resolve_peer_robust(from_ch)
            if dest_ch and dest_ch != uid:
                await _resolve_peer_robust(dest_ch)


            fail_count = 0
            phase_start = time.time()
            job_failed = False

            def _extract_ep_label(fname: str) -> str:
                """
                Extract an episode number or range from a filename for output naming.

                PRIORITY ORDER:
                  1. Explicit range: '388-389', '567 to 677', '576–580'
                  2. Ep/Episode keyword: 'Episode 66- some suffix16' → '66'
                     (IMPORTANT: keyword match wins over any trailing numbers to avoid
                      picking up track counters like the '16' in 'Episode 66-...16.mp3')
                  3. Leading zero-padded number: '01 Title.mp3' → '1'
                  4. Fall back: empty string (cleaner uses sequential counter)
                """
                import re as _re
                base = _re.sub(r'\.\w{2,5}$', '', fname)             # strip extension
                base = _re.sub(r'\s*\(\d+\)\s*$', '', base).strip()  # strip (1),(2) copy markers
                base_norm = base.replace('\u2013', '-').replace('\u2014', '-')

                # Always locate keyword first (needed by both range guard and P2 below)
                kw_m = _re.search(
                    r'(?i)(?:episode|epi|ep)\b[\s\-\:\.\#\_\*]*',
                    base_norm)

                # ── Priority 1 (no-keyword only): pure numeric range file ───────────
                # e.g. 'Shadow 388-389.mp3' or 'Story 576-580.mp3'
                # When a keyword IS present the keyword's number wins, so we skip
                # this block entirely (prevents 'Episode 34 - 48 ghante' -> '34-48').
                if not kw_m:
                    r_m = _re.search(
                        r'\b(\d{1,4})\s*(?:-|to|and|&|~|,|/)\s*(\d{1,4})\b',
                        base_norm, _re.IGNORECASE)
                    if r_m:
                        a, b = int(r_m.group(1)), int(r_m.group(2))
                        if 0 < a < 5000 and a <= b + 1 < 5001:
                            orig_slice = base[r_m.start():r_m.end()].strip()
                            return orig_slice if orig_slice else (f"{a}-{b}" if a != b else str(a))

                # ── Priority 2: Ep/Episode keyword (most important fix) ─────────
                # This prevents trailing track-counter digits from being used
                # e.g. 'Episode 66- maa sab jaanti hai16' → '66' (not '16')
                if kw_m:
                    kw_ep = _re.search(
                        r'(?i)(?:episode|epi|ep)\b[\s\-\:\.\#\_\*]*(\d{1,4})(?!\d)',
                        base_norm)
                    if kw_ep:
                        n = int(kw_ep.group(1))
                        if 0 < n < 5000:
                            return str(n)

                # ── Priority 3: Leading zero-padded number ──────────────────────
                lead = _re.match(r'^0*(\d{1,4})(?:[^0-9]|$)', base_norm)
                if lead:
                    n = int(lead.group(1))
                    if 0 < n < 5000:
                        return str(n)

                # ── Priority 4: Single lone number (no keyword, no leading) ─────
                nums = [int(x) for x in _re.findall(r'(?<!\d)(\d{1,4})(?!\d)', base_norm)
                        if 0 < int(x) < 5000 and not (1900 <= int(x) <= 2100)]
                if len(nums) == 1:
                    return str(nums[0])

                return ''  # no unambiguous episode found — fall back to sequential

            # Loop through all message IDs
            msg_id = curr_msg_id
            while msg_id <= eid:
                # Save the loop var as progress checkpoint
                await _cl_update_job(job_id, {"current_msg_id": msg_id})
                ev = _cl_paused.get(job_id)
                if ev and not ev.is_set():
                    break  # pause triggered

                job = await _cl_get_job(job_id)
                if job.get("status") == "stopped":
                    break

                try:
                    msg = await client.get_messages(from_ch, msg_id)
                    # Skip empty messages or non-audio content
                    if not msg or msg.empty:
                        msg_id += 1
                        continue

                    # ── Topic filter: only process messages from the target thread ──
                    if from_topic_id:
                        msg_thread = getattr(msg, "message_thread_id", None)
                        # The very first topic-creation message has msg.id == thread_id
                        if msg_thread != from_topic_id and msg.id != from_topic_id:
                            msg_id += 1
                            continue

                    # ── Detect media type ───────────────────────────────────────
                    # Accept ALL media types (audio, video, document, photo, voice)
                    # Previously only audio/voice/audio-doc were processed,
                    # which caused the "0/1" result for large video/document files.
                    is_audio     = bool(msg.audio or msg.voice)
                    is_audio_doc = bool(msg.document and
                                        'audio' in (getattr(msg.document, 'mime_type', '') or ''))
                    is_video     = bool(msg.video or (
                                        msg.document and
                                        'video' in (getattr(msg.document, 'mime_type', '') or '')))
                    is_photo     = bool(msg.photo)
                    # Any downloadable media is acceptable
                    media_obj = (msg.audio or msg.voice or msg.document
                                 or msg.video or msg.photo)
                    if not media_obj:
                        msg_id += 1
                        continue   # truly no media (text-only / service)

                    # ── Evaluate FFmpeg usage ───────────────────────────────────
                    # Photos are never run through FFmpeg.
                    # Audio/Voice are ALWAYS run through FFmpeg to clean metadata/re-encode.
                    # Videos are only run through FFmpeg if `convert_videos` is True.
                    convert_videos = job.get("convert_videos", False)
                    if is_photo:
                        use_ffmpeg = False
                    elif is_video:
                        use_ffmpeg = convert_videos
                    else:
                        use_ffmpeg = True

                    # Original filename & extension
                    orig_fn  = getattr(media_obj, 'file_name', None) or ""
                    orig_ext = os.path.splitext(orig_fn)[1] if orig_fn else ''
                    if not orig_ext:
                        if msg.audio:    orig_ext = '.mp3'
                        elif msg.voice:  orig_ext = '.ogg'
                        elif msg.video:  orig_ext = '.mp4'
                        elif msg.photo:  orig_ext = '.jpg'
                        else:            orig_ext = '.mp3'

                    # ── Determine output title ──
                    change_meta = job.get("change_metadata", True)
                    rename_files = job.get("rename_files", True)  # True = use base_name, False = keep original
                    ep_label = _extract_ep_label(orig_fn) if orig_fn else ''

                    if rename_files:
                        # User wants renaming: use base_name + episode/number
                        if ep_label:
                            clean_title = f"{base_name} {ep_label}"
                        else:
                            clean_title = f"{base_name} {curr_num}"
                    else:
                        # User wants to keep original filename/title
                        orig_title = getattr(media_obj, 'title', None)
                        if orig_title:
                            clean_title = str(orig_title)
                        elif orig_fn:
                            clean_title = os.path.splitext(orig_fn)[0]
                        else:
                            clean_title = f"{base_name} {curr_num}"  # fallback if no orig filename

                        if orig_fn:
                            clean_file_override = orig_fn
                        else:
                            clean_file_override = None

                    # Track number (used for sorting in music apps)
                    if ep_label:
                        import re as _re_meta
                        _tm = _re_meta.search(r'\d+', ep_label)
                        track_num = _tm.group() if _tm else str(curr_num)
                    else:
                        track_num = str(curr_num)
                    
                    # ALWAYS increment curr_num for every media file processed.
                    curr_num += 1

                    # Output filename — always .mp3 (except photos)
                    if not rename_files and clean_file_override:
                        clean_file = clean_file_override
                    elif use_ffmpeg:
                        clean_file = f"{clean_title}.mp3"
                    else:
                        clean_file = f"{clean_title}{orig_ext}" if orig_ext else clean_title

                    # Always apply metadata (title + whatever user configured).
                    # rename_files controls the title; other fields are always set.
                    meta = {
                        "title":  clean_title,
                    }
                    note = job.get("metadata_note", "")
                    if note:       meta["comment"]     = note
                    if note:       meta["description"] = note
                    if art:        meta["artist"]      = art
                    if alb or art: meta["album"]       = alb or art
                    if yr:         meta["year"]        = yr
                    if gen:        meta["genre"]       = gen
                    if track_num:  meta["track"]       = track_num

                    # ── Health check before each download ──
                    try:
                        client = await _ensure_client_alive(client, None, uid)
                    except Exception as hc_err:
                        raise Exception(f"Client reconnect failed: {hc_err}")

                    # ── Download ────────────────────────────────────────────────
                    in_path = os.path.abspath(f"temp_cl_in_{job_id}_{msg_id}{orig_ext}")
                    if use_ffmpeg:
                        out_path = os.path.abspath(f"temp_cl_out_{job_id}_{msg_id}.mp3")
                    else:
                        out_path = os.path.abspath(f"temp_cl_out_{job_id}_{msg_id}{orig_ext}")

                    try:
                        dl_path = await asyncio.wait_for(
                            client.download_media(msg, file_name=in_path),
                            timeout=7200  # 2 hours max for 4GB files
                        )
                    except asyncio.TimeoutError:
                        raise Exception("Download timed out after 2 hours.")

                    if not dl_path or not os.path.exists(str(dl_path)):
                        msg_id += 1
                        continue

                    # Verify complete download
                    tg_size = getattr(media_obj, 'file_size', 0)
                    dl_size = os.path.getsize(str(dl_path))
                    if tg_size > 0 and dl_size < (tg_size * 0.95):
                        try: os.remove(str(dl_path))
                        except: pass
                        raise Exception(
                            f"Incomplete download: {dl_size} / {tg_size} bytes. Forcing retry.")

                    await db.update_global_stats(total_files_downloaded=1, total_data_usage_bytes=dl_size)

                    # ── Process ────────────────────────────────────────────────
                    if use_ffmpeg:
                        # Audio: full FFmpeg clean + re-encode
                        ok, err = await _process_audio_ffmpeg(
                            str(dl_path), out_path, local_cover, meta)
                        try: os.remove(str(dl_path))
                        except: pass
                        if not ok:
                            if "Invalid data found" in err:
                                logger.error(f"[Cleaner {job_id}] Fatal decode error on "
                                             f"{msg_id}: Invalid data. Skipping.")
                                msg_id += 1
                                continue
                            raise Exception(f"FFmpeg failed: {err[:500]}")
                    else:
                        # Non-audio (video / document / photo): rename-only, no re-encoding.
                        # Just move the downloaded file to the output path with the new name.
                        try:
                            import shutil as _shutil
                            _shutil.move(str(dl_path), out_path)
                        except Exception as mv_err:
                            try: os.remove(str(dl_path))
                            except: pass
                            raise Exception(f"Rename/move failed: {mv_err}")
                        logger.info(f"[Cleaner {job_id}] Rename-only for {orig_fn} "
                                    f"-> {clean_file} (no FFmpeg)")

                    # ── Upload with media-type awareness ────────────────────────
                    replace_mode   = job.get("replace_mode", False)
                    replace_msg_id = job.get("replace_start_msg_id", 0)
                    
                    # Prevent Bot API 50MB limit issues by forcing userbot for large files
                    out_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
                    if (dest_ch == uid and bot) and out_size < (45 * 1024 * 1024) and not replace_mode:
                        upload_client = bot
                    else:
                        upload_client = client
                        
                    thumb = local_cover if (local_cover and os.path.exists(local_cover)) else None
                    uploaded = False
                    upload_caption = f"**{clean_file}**" if job.get("use_caption", True) else ""

                    for attempt in range(5):
                        try:
                            if replace_mode and replace_msg_id:
                                # ── Replace/Edit existing message ─────────────────
                                edit_msg_id = replace_msg_id + done
                                # Upload to 'me' first to get a file_id, then edit
                                if use_ffmpeg or is_audio or is_audio_doc:
                                    from pyrogram.types import InputMediaAudio
                                    _ghost = await upload_client.send_audio(
                                        chat_id="me", audio=out_path,
                                        title=clean_title, performer=art,
                                        file_name=clean_file, thumb=thumb)
                                    _media = InputMediaAudio(
                                        media=_ghost.audio.file_id,
                                        caption=upload_caption,
                                        title=clean_title, performer=art, thumb=thumb)
                                    await upload_client.edit_message_media(
                                        chat_id=dest_ch, message_id=edit_msg_id, media=_media)
                                    try: await _ghost.delete()
                                    except: pass
                                elif is_video:
                                    from pyrogram.types import InputMediaVideo
                                    _ghost = await upload_client.send_video(
                                        chat_id="me", video=out_path,
                                        file_name=clean_file, thumb=thumb)
                                    _vobj  = _ghost.video or (_ghost.document if _ghost.document else None)
                                    _media = InputMediaVideo(
                                        media=_vobj.file_id,
                                        caption=upload_caption, thumb=thumb)
                                    await upload_client.edit_message_media(
                                        chat_id=dest_ch, message_id=edit_msg_id, media=_media)
                                    try: await _ghost.delete()
                                    except: pass
                                elif is_photo:
                                    from pyrogram.types import InputMediaPhoto
                                    _ghost = await upload_client.send_photo(
                                        chat_id="me", photo=out_path)
                                    _media = InputMediaPhoto(
                                        media=_ghost.photo.file_id,
                                        caption=upload_caption)
                                    await upload_client.edit_message_media(
                                        chat_id=dest_ch, message_id=edit_msg_id, media=_media)
                                    try: await _ghost.delete()
                                    except: pass
                                else:
                                    from pyrogram.types import InputMediaDocument
                                    _ghost = await upload_client.send_document(
                                        chat_id="me", document=out_path, file_name=clean_file)
                                    _media = InputMediaDocument(
                                        media=_ghost.document.file_id,
                                        caption=upload_caption)
                                    await upload_client.edit_message_media(
                                        chat_id=dest_ch, message_id=edit_msg_id, media=_media)
                                    try: await _ghost.delete()
                                    except: pass
                            else:
                                # ── Normal upload ───────────────────────────────
                                # use_ffmpeg is now True for everything except photos,
                                # so is_video / is_audio_doc files are also .mp3 now.
                                if use_ffmpeg:
                                    await upload_client.send_audio(
                                        chat_id=dest_ch, audio=out_path,
                                        caption=upload_caption,
                                        title=clean_title, performer=art,
                                        file_name=clean_file, thumb=thumb)
                                elif is_video:
                                    await upload_client.send_video(
                                        chat_id=dest_ch, video=out_path,
                                        caption=upload_caption,
                                        file_name=clean_file, thumb=thumb)
                                elif is_photo:
                                    await upload_client.send_photo(
                                        chat_id=dest_ch, photo=out_path,
                                        caption=upload_caption)
                                else:
                                    await upload_client.send_document(
                                        chat_id=dest_ch, document=out_path,
                                        caption=upload_caption,
                                        file_name=clean_file, thumb=thumb)
                            uploaded = True
                            await db.update_global_stats(total_files_uploaded=1, total_data_usage_bytes=out_size)
                            break
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2)
                        except Exception as ue:
                            ue_str = str(ue)
                            # Peer unknown — retry with main bot
                            if ("PEER_ID_INVALID" in ue_str or "CHANNEL_INVALID" in ue_str) \
                               and upload_client is not bot and bot:
                                logger.warning(f"[Cleaner {job_id}] Upload PEER_ID_INVALID, switching to main bot")
                                upload_client = bot
                                continue
                            if attempt >= 4:
                                raise Exception(f"Upload failed: {ue}")
                            await asyncio.sleep(3 * (attempt + 1))
                    
                    if not uploaded:
                        raise Exception("Upload: all 5 attempts exhausted")

                    try: os.remove(out_path)
                    except: pass

                    done += 1
                    fail_count = 0   # reset per successfully processed file
                    msg_id += 1      # Advance to next message safely
                    await _cl_update_job(job_id, {"files_done": done})
                    logger.info(f"[Cleaner {job_id}] Done {done}: {clean_title}")
                    # Brief cooldown after each file — lets the event loop run other tasks
                    # and prevents sustained 100% CPU while the download of the next file starts.
                    await asyncio.sleep(1.5)

                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2)
                    continue  # retry same msg
                except Exception as e:
                    err_str_lower = str(e).lower()
                    # Connection/network/transient errors
                    is_transient = any(k in err_str_lower for k in (
                        "not connected", "disconnected",
                        "connection", "timeout", "network", "flood_wait", "forcing retry",
                        "channel_invalid", "peer_id_invalid", "channel invalid", "peer id invalid"
                    ))
                    if is_transient:
                        logger.warning(f"[Cleaner {job_id}] Transient error at msg {msg_id}: {e} — retrying in 10s")
                        # For CHANNEL_INVALID: re-resolve the peer before retrying
                        if "channel_invalid" in err_str_lower or "peer_id_invalid" in err_str_lower:
                            for _repeer in [from_ch, dest_ch]:
                                if not _repeer or _repeer == uid:
                                    continue
                                try:
                                    await client.get_chat(_repeer)
                                    logger.info(f"[Cleaner {job_id}] Re-resolved peer: {_repeer}")
                                except Exception:
                                    if bot:
                                        try:
                                            uname = getattr(await bot.get_chat(_repeer), 'username', None)
                                            if uname:
                                                await client.join_chat(uname)
                                        except Exception:
                                            pass
                        try:
                            client = await _ensure_client_alive(client, None, uid)
                        except Exception:
                            pass
                        await asyncio.sleep(10)
                        continue  # retry the same msg without incrementing fail_count

                    fail_count += 1
                    logger.error(f"[Cleaner {job_id}] Error at msg {msg_id}: {e}")
                    if fail_count > 5:
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
                if client and client is not bot:
                    try:
                        from plugins.test import release_client
                        cname = getattr(client, 'name', None)
                        if cname: await release_client(cname)
                    except Exception: pass
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
                        if done == 0:
                            await bot.send_message(
                                uid,
                                f"<b>⚠️ Cleaner Job Finished!</b>\n\n"
                                f"No files were successfully processed. This may happen if the selected message(s) did not contain compatible media, or the client lacked access."
                            )
                        else:
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
            
            if client and client is not bot:
                try:
                    from plugins.test import release_client
                    cname = getattr(client, 'name', None)
                    if cname: await release_client(cname)
                except Exception: pass
            break


# ─── UI Callback Handlers ────────────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^cl#(main|new|view|pause|resume|stop|del|cfg|reset|force_ask|force_do)"))
async def _cl_callbacks(bot, update: CallbackQuery):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS
    uid = update.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("cleaner"):
        return await update.answer(f"🔒 {FEATURE_LABELS['cleaner']} is temporarily disabled by admin.", show_alert=True)
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
        kb.append([
            InlineKeyboardButton("⚙️ Sᴇᴛ Gᴇɴʀᴇ", callback_data="cl#cfg#genre"),
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
            "<b><u>\ud83e\uddf9 A\u1d1c\u1d04\u026a\u1d0f C\u029f\u1d07\u1d00\u0274\u1d07\u0280 & R\u1d07\u0274\u1d00\u1d0d\u1d07\u0280</u></b>\n\n"
            "This system strips background noise, cleans corrupted metadata, "
            "forces 128kbps standard formats, and strictly renames sequential files.\n\n"
            f"<b>Global Defaults:</b>\n"
            f"  • Artist: {df.get('artist', '<i>None</i>')}\n"
            f"  • Genre: {df.get('genre', '<i>None</i>')}\n"
            f"  • Cover Art: {'<i>Saved</i> ✅' if df.get('cover') else '<i>None</i>'}\n"
        )
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

    elif action == "cfg":
        cfg_type = data[2]
        if cfg_type == "artist":
            ask_txt = f"Send the new default <b>Artist</b>\n<i>Send multiple artists separated by | (e.g. Artist 1 | Artist 2)</i>\n<i>Send /skip to clear.</i>"
        else:
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
            if st == "queued":
                kb.append([InlineKeyboardButton("⚡ Fᴏʀᴄᴇ Aᴄᴛɪᴠᴀᴛᴇ", callback_data=f"cl#force_ask#{jid}")])
        elif st == "paused":
            kb.append([
                InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ", callback_data=f"cl#resume#{jid}"),
                InlineKeyboardButton("⏹ Sᴛᴏᴘ", callback_data=f"cl#stop#{jid}")
            ])
        elif st in ("failed", "stopped"):
            kb.append([
                InlineKeyboardButton("🔁 Rᴇsᴇᴛ & Rᴇsᴛᴀʀᴛ", callback_data=f"cl#reset#{jid}"),
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
            bot_ref = _cl_bot_ref.get(jid) or bot
            _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid, bot_ref))
        update.data = f"cl#view#{jid}"
        return await _cl_callbacks(bot, update)

    elif action == "force_ask":
        jid = data[2]
        txt = (
            "⚠️ <b>WARNING: FORCE START</b>\n\n"
            "You are about to bypass the safety queue and force this cleaner job to start concurrently.\n\n"
            "<b>Potential Issues:</b>\n"
            "• <b>Server Overload:</b> Multiple heavy jobs can exhaust server CPU, crashing the bot.\n"
            "• <b>FloodWaits/Bans:</b> Downloading/uploading too many files simultaneously drastically increases your risk of API limits or temporary bans.\n"
            "• <b>Slower Speed:</b> Running parallel instances slows down all ongoing jobs.\n\n"
            "Are you sure you want to force start this job immediately?"
        )
        kb = [
            [InlineKeyboardButton("✅ Yes, Force Start Anyway", callback_data=f"cl#force_do#{jid}")],
            [InlineKeyboardButton("⛔ Cancel (Keep in Queue)", callback_data=f"cl#view#{jid}")]
        ]
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

    elif action == "force_do":
        jid = data[2]
        await _cl_update_job(jid, {"status": "running", "force_active": True})
        if jid not in _cl_paused: _cl_paused[jid] = asyncio.Event()
        _cl_paused[jid].set()
        
        # If the task is already running (blocked in semaphore), we MUST cancel and restart it
        if jid in _cl_tasks and not _cl_tasks[jid].done():
            _cl_tasks[jid].cancel()
            await asyncio.sleep(0.5)
            
        bot_ref = _cl_bot_ref.get(jid) or bot
        _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid, bot_ref))
        
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

    elif action == "reset":
        jid = data[2]
        job = await _cl_get_job(jid)
        if not job:
            return await update.answer("Job not found.", show_alert=True)
        # Reset: clear error, set to queued, and restart from last checkpoint
        await _cl_update_job(jid, {
            "status": "queued",
            "error": "",
            "phase_start_ts": 0
        })
        if jid not in _cl_paused:
            _cl_paused[jid] = asyncio.Event()
        _cl_paused[jid].set()
        if jid in _cl_tasks and not _cl_tasks[jid].done():
            _cl_tasks[jid].cancel()
            await asyncio.sleep(0.3)
        bot_ref = _cl_bot_ref.get(jid) or bot
        _cl_tasks[jid] = asyncio.create_task(_cl_run_job(jid, bot_ref))
        await update.answer("♻️ Job reset and restarted!", show_alert=True)
        update.data = f"cl#view#{jid}"
        return await _cl_callbacks(bot, update)


async def _create_cl_flow(bot, user_id):
    old = _cl_waiter.pop(user_id, None)
    if old and not old.done(): old.cancel()
    # Clear any stale cancel flag from a previous flow
    _cl_cancel_users.discard(user_id)

    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    SKIP_BTN   = KeyboardButton("⏭ Sᴋɪᴘ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")
    markup_b   = ReplyKeyboardMarkup([[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    markup_s   = ReplyKeyboardMarkup([[SKIP_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)
    markup_c   = ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True)

    def _cancelled(r):
        """Return True if user cancelled — checks both the flag and the message text."""
        if user_id in _cl_cancel_users:
            return True
        if r is None:
            return False
        txt = (r.text or "").strip()
        return (
            txt.startswith("/cancel")
            or "⛔" in txt
            or "Cᴀɴᴄᴇʟ" in txt
            or txt.lower() == "cancel"
        )

    async def _abort():
        """Clean up and tell user the wizard was cancelled."""
        _cl_cancel_users.discard(user_id)
        _cl_waiter.pop(user_id, None)
        await bot.send_message(user_id, "<i>❌ Cleaner wizard cancelled.</i>",
                               reply_markup=ReplyKeyboardRemove())

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
        "<b>🧹 Create Cleaner Job — Step 1/9</b>\n\nChoose the <b>account</b> to read from:",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_acc): return await _abort()

    acc_id = None
    if "[" in (r_acc.text or "") and "]" in (r_acc.text or ""):
        try: acc_id = int(r_acc.text.split('[')[-1].split(']')[0])
        except: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]

    # ── Step 2: Start link ───────────────────────────────────────
    r_start = await _cl_ask(bot, user_id,
        "<b>»  Step 2/9</b>\n\nSend the <b>Start Message Link</b> (first file):",
        reply_markup=markup_c)
    if _cancelled(r_start): return await _abort()
    from_chat, sid = _parse_link(r_start.text or "")

    # ── Step 3: End link ─────────────────────────────────────────
    r_end = await _cl_ask(bot, user_id,
        "<b>»  Step 3/9</b>\n\nSend the <b>End Message Link</b> (last file):",
        reply_markup=markup_b)
    if _cancelled(r_end): return await _abort()
    _, eid = _parse_link(r_end.text or "")
    if sid and eid and sid > eid: sid, eid = eid, sid

    # ── Step 4: Destination ──────────────────────────────────────
    from plugins.utils import ask_channel_picker
    dest_chat = None
    ch = None
    replace_mode = False
    replace_start_msg_id = 0
    
    picked = await ask_channel_picker(bot, user_id, 
        "<b>»  Step 4/9</b>\n\nSelect <b>destination channel</b> for cleaned files:\n"
        "<i>Or choose <b>✏️ Replace/Edit Mode</b> to edit existing posts in-place.</i>",
        extra_options=["✏️ Replace/Edit Mode", "⏭️ Skip (Send to DM)"])
        
    if not picked: return await _abort()
    
    if picked == "✏️ Replace/Edit Mode":
        replace_mode = True
        picked_edit = await ask_channel_picker(bot, user_id,
            "<b>»  Step 4a/9 — Select Channel to Edit</b>\n\n"
            "Which channel contains the existing audio posts to replace?")
        if not picked_edit: return await _abort()
        
        dest_chat = int(picked_edit["chat_id"])

        r_mid = await _cl_ask(bot, user_id,
            "<b>»  Step 4b/9 — First Message ID to Replace</b>\n\n"
            "Send the <b>message ID</b> of the first existing audio post that should be replaced.\n"
            "<i>Each subsequent file will edit the next message ID automatically.</i>",
            reply_markup=markup_c)
        if _cancelled(r_mid): return await _abort()
        try:
            replace_start_msg_id = int((r_mid.text or "0").strip())
        except ValueError:
            replace_start_msg_id = 0

    elif picked != "⏭️ Skip (Send to DM)":
        dest_chat = int(picked["chat_id"])
        
    if not dest_chat:
        dest_chat = user_id

    # ── Step 4c: Optional Topic ID (for group topics) ────────────
    from_topic_id = 0
    r_topic = await _cl_ask(bot, user_id,
        "<b>»  Step 4c/9 — Source Topic (Optional)</b>\n\n"
        "If the source is a <b>group with topics</b>, send the <b>Topic ID</b> or a "
        "<b>message link</b> from that topic to clean only messages from that thread.\n\n"
        "<i>Example:</i> <code>https://t.me/c/1234567890/123/456</code>\n"
        "<i>The topic ID is the third number in the URL segment above.</i>\n\n"
        "<b>Skip</b> if the source is a regular channel or you want all messages.",
        reply_markup=markup_s)
    if _cancelled(r_topic): return await _abort()
    if not _skip(r_topic.text or ""):
        # Try to parse topic ID from a link like /c/CHATID/TOPICID/MSGID
        import re as _re_t
        _tm = _re_t.search(r'https?://t\.me/c/\d+/(\d+)/\d+', r_topic.text or "")
        if _tm:
            from_topic_id = int(_tm.group(1))
        else:
            try:
                from_topic_id = int((r_topic.text or "0").strip())
            except ValueError:
                from_topic_id = 0

    # ── Step 5: Rename Files? ───────────────────────────────────
    r_rename = await _cl_ask(bot, user_id,
        "<b>»  Step 5/10 — Rename Files?</b>\n\n"
        "Do you want to <b>rename</b> the files?\n\n"
        "• <b>Yes</b> → You'll set a base name and starting number. Files will be renamed "
        "like <code>Saaya 1.mp3</code>, <code>Saaya 2.mp3</code>, etc.\n"
        "• <b>No</b>  → Original file names and titles are kept exactly as-is. "
        "You can still update artist, year, genre, cover, etc.",
        reply_markup=ReplyKeyboardMarkup(
            [["✅ Yes, Rename Files", "❌ No, Keep Original Names"],
             [CANCEL_BTN]],
            resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_rename): return await _abort()
    rename_files = "yes" in (r_rename.text or "").lower()

    base_name = "Cleaned"
    start_num = 1
    if rename_files:
        # ── Step 5a: Base Name ──────────────────────────────────
        r_base = await _cl_ask(bot, user_id,
            "<b>»  Step 5a/10 — Base Name</b>\n\nSend the <b>Base Name</b> for the files.\n"
            "<i>Example: Send <code>Saaya</code> → outputs <code>Saaya 1.mp3</code>, <code>Saaya 2.mp3</code>...</i>",
            reply_markup=markup_b)
        if _cancelled(r_base): return await _abort()
        base_name = re.sub(r'[<>:"/\\|?*]', '_', (r_base.text or "Cleaned").strip())

        # ── Step 5b: Starting Number ──────────────────────────────
        r_num = await _cl_ask(bot, user_id,
            "<b>»  Step 5b/10 — Starting Number</b>\n\nSend the <b>Starting Number</b>.\n"
            "<i>Example: Send <code>1</code> for Saaya 1, or <code>201</code> for Saaya 201...</i>",
            reply_markup=markup_b)
        if _cancelled(r_num): return await _abort()
        start_num = int((r_num.text or "1").strip()) if (r_num.text or "").strip().isdigit() else 1

    # ── Step 6: Convert Videos to Audio? ──────────────────────────
    r_conv = await _cl_ask(bot, user_id,
        "<b>»  Step 6/10 — Convert Video to Audio?</b>\n\n"
        "Should large video files (up to 4GB) be converted to MP3?\n\n"
        "• <b>Yes</b> → Videos are extracted to Audio, metadata applied, and old files cleaned.\n"
        "• <b>No</b>  → Videos are cleanly processed AS videos without extraction.",
        reply_markup=ReplyKeyboardMarkup(
            [["✅ Yes, Convert to Audio", "❌ No, Keep as Video"],
             [CANCEL_BTN]],
            resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_conv): return await _abort()
    convert_videos = "yes" in (r_conv.text or "").lower()

    # ── Step 7: Metadata (individual prompts) ────────────────────
    df = await _cl_get_defaults(user_id)
    adv_artist_raw = df.get("artist", "")
    adv_artists = [a.strip() for a in str(adv_artist_raw).split("|") if a.strip()]
    adv_year   = df.get("year", "")
    adv_album  = df.get("album", "")
    adv_genre  = df.get("genre", "")
    adv_cover  = df.get("cover", "")

    # ── Steps 7-8: Metadata fields (filename/title is ALWAYS preserved) ──────
    # The original filename is kept as-is. Only embedded metadata tags
    # (artist, year, genre, cover, note) can be optionally changed.

    artist_kb = [[KeyboardButton(a)] for a in adv_artists]
    artist_kb.append([SKIP_BTN, CANCEL_BTN])

    r_art = await _cl_ask(bot, user_id,
        f"<b>>  Step 7.1/10 — Artist Name</b>\n\n"
        f"Select an Artist, or type a custom name to embed in all files.\n"
        f"<i>Current presets: {', '.join(adv_artists) if adv_artists else 'None'}</i>",
        reply_markup=ReplyKeyboardMarkup(artist_kb, resize_keyboard=True))
    if _cancelled(r_art): return await _abort()
    
    adv_artist = ""
    if not _skip(r_art.text or ""):
        adv_artist = (r_art.text or "").strip()
        if adv_artist and adv_artist not in adv_artists:
            adv_artists.append(adv_artist)
            await _cl_save_default(user_id, "artist", "|".join(adv_artists))

    r_alb = await _cl_ask(bot, user_id,
        f"<b>>  Step 7b/10 — Album Name</b>\n\n"
        f"Enter the <b>Album</b> name (shown in music apps).\n"
        f"<i>Current: {adv_album or 'None'}. Skip to use Artist name.</i>",
        reply_markup=markup_s)
    if _cancelled(r_alb): return await _abort()
    if not _skip(r_alb.text or ""): adv_album = (r_alb.text or "").strip()
    if not adv_album: adv_album = adv_artist

    r_yr = await _cl_ask(bot, user_id,
        f"<b>>  Step 7c/10 — Year</b>\n\n"
        f"Select a preset year or <b>type any custom year</b> (e.g. <code>2021</code>).\n"
        f"<i>Current: {adv_year or 'None'}. Skip to leave unchanged.</i>",
        reply_markup=ReplyKeyboardMarkup(
            [["2022", "2023", "2024", "2025", "2026"],
             [SKIP_BTN, CANCEL_BTN]],
            resize_keyboard=True))
    if _cancelled(r_yr): return await _abort()
    yr_text = (r_yr.text or "").strip()
    if not _skip(yr_text):
        adv_year = yr_text  # accept any user input (preset button or custom typed year)

    r_gen = await _cl_ask(bot, user_id,
        f"<b>>  Step 7d/10 — Genre</b>\n\n"
        f"Enter the <b>Genre</b> tag (or tap a preset).\n"
        f"<i>Current: {adv_genre or 'None'}. Skip to leave unchanged.</i>",
        reply_markup=ReplyKeyboardMarkup(
            [["Audiobook", "Romance", "Podcast"],
             ["Thriller", "Comedy", "Drama"],
             [SKIP_BTN, CANCEL_BTN]],
            resize_keyboard=True))
    if _cancelled(r_gen): return await _abort()
    if not _skip(r_gen.text or ""): adv_genre = (r_gen.text or "").strip()

    # ── Step 8: Cover Image ──────────────────────────────────────────────────
    r_cov = await _cl_ask(bot, user_id,
        f"<b>>  Step 8/10 — Cover Image</b>\n\n"
        f"Send a <b>photo/image</b> to embed as album cover art in all files.\n"
        f"<i>{'Current default cover is set. ' if adv_cover else ''}Skip to {'keep existing' if adv_cover else 'skip (no cover)'}.</i>",
        reply_markup=markup_s,
        timeout=300)
    if _cancelled(r_cov): return await _abort()
    if r_cov and not _skip(r_cov.text or ""):
        if r_cov.photo:
            adv_cover = r_cov.photo.file_id
        elif r_cov.document and 'image' in (r_cov.document.mime_type or ''):
            adv_cover = r_cov.document.file_id
            
    # ── Step 8.5: Metadata Note ────────────────────────────────────────────────
    me = await bot.get_me() if getattr(bot, "get_me", None) else None
    default_note = f"@{me.username}" if me and getattr(me, "username", None) else "@tryaryabot"
    
    r_note = await _cl_ask(bot, user_id,
        f"<b>>  Step 8.5/10 — Metadata Note</b>\n\n"
        f"Enter a <b>Note / Comment</b> to embed inside the file's metadata.\n"
        f"<i>By default, the bot's username ( {default_note} ) will be used.</i>\n"
        f"<i>Send /skip to use the default username.</i>",
        reply_markup=markup_s)
    if _cancelled(r_note): return await _abort()
    
    adv_note = default_note
    if not _skip(r_note.text or ""):
        adv_note = (r_note.text or "").strip()

    # Always apply metadata fields; title/filename is preserved in job execution
    change_metadata = True


    # ── Step 9: Caption Option ───────────────────────────────────
    r_cap = await _cl_ask(bot, user_id,
        f"<b>»  Step 9/9 — Add Caption?</b>\n\n"
        f"Do you want to add the file name as the caption in the target channel/DM?\n",
        reply_markup=ReplyKeyboardMarkup(
            [["✅ Yes, Add Caption"], ["❌ No, Empty Caption"], [CANCEL_BTN]],
            resize_keyboard=True, one_time_keyboard=True))
    if _cancelled(r_cap): return await _abort()
    use_caption = not ("no, empty caption" in (r_cap.text or "").lower())

    # ── Create Job ───────────────────────────────────────────────
    job_id = str(uuid.uuid4())
    total_range = (eid - sid) + 1 if (sid and eid) else 0
    
    routing = await db.get_task_routing()
    target_node = routing.get("cleaner")
    should_run_locally = (target_node == "main" or target_node is None)

    job = {
        "job_id": job_id, "user_id": user_id, "status": "queued",
        "from_chat": from_chat, "dest_chat": dest_chat,
        "from_topic_id": from_topic_id,
        "replace_mode": replace_mode,
        "replace_start_msg_id": replace_start_msg_id,
        "start_id": sid, "end_id": eid,
        "total_files": total_range, "files_done": 0,
        "base_name": base_name, "starting_number": start_num,
        "rename_files": rename_files,
        "convert_videos": convert_videos,
        "artist": adv_artist,
        "year": adv_year,
        "album": adv_album,
        "genre": adv_genre,
        "cover_file_id": adv_cover,
        "metadata_note": adv_note,
        "change_metadata": change_metadata,
        "use_caption": use_caption,
        "account_id": sel_acc.get("id") or acc_id,
        "is_bot": sel_acc.get("is_bot", True),
        "created_at": _ist_now().strftime('%Y-%m-%d %H:%M:%S'),
        "target_title": "DM" if dest_chat == user_id else (ch.get("title", "Channel") if ch else "Channel"),
        "phase_start_ts": 0,
    }
    await _cl_save_job(job)
    _cl_cancel_users.discard(user_id)   # ensure clean state after successful completion
    
    run_msg = "" if should_run_locally else f"\nQueued for worker: <b>{target_node}</b>"
    
    await bot.send_message(
        user_id,
        f"<b>✅ Cleaner Job Queued!</b>\n"
        f"Name: <code>{base_name}</code>\n"
        f"Files: <code>{sid}</code> → <code>{eid}</code> (~{total_range} msgs)\n"
        f"Numbering: {base_name} <b>{start_num}</b> → {base_name} <b>{start_num + total_range - 1}</b>\n"
        f"Convert Video: {'✅ Yes' if convert_videos else '❌ No'}\n"
        f"Artist: {adv_artist or '—'}  |  Cover: {'✅ Set' if adv_cover else '—'}{run_msg}",
        reply_markup=ReplyKeyboardRemove()
    )
    
    if should_run_locally:
        _cl_paused[job_id] = asyncio.Event()
        _cl_paused[job_id].set()
        _cl_bot_ref[job_id] = bot  # store so resume can notify too
        if not os.environ.get("MASTER_ONLY_QUEUE", "False").lower() in ("1", "true"):
            _cl_tasks[job_id] = asyncio.create_task(_cl_run_job(job_id, bot))
