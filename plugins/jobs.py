"""
Live Jobs Plugin — v3
======================
Each job runs as an independent asyncio.Task in the background.

New in v3:
  • Batch Phase (ON/OFF): copy old messages first, then transition seamlessly to live mode.
  • Dual Destinations: send every message to up to 2 target chats/topics simultaneously.
  • Per-job Size Limit: skip files above a configured MB or duration threshold.

Flow:
  /jobs → list → »  Create → Step1(account) → Step2(source)
       → Step3(dest1 + topic1) → Step4(dest2 optional + topic2)
       → Step5(batch ON/OFF) → Step6(size limit)
       → job starts
"""
import time
import asyncio
import logging
from database import db
from bot import BOT_INSTANCE
from .test import CLIENT, start_clone_bot, release_client
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

# In-memory: job_id → asyncio.Task
_job_tasks: dict[str, asyncio.Task] = {}

# In-memory: job_id → pending auto-resume asyncio.Task
# Tracked separately so stop/delete can cancel scheduled resumes
_auto_resume_tasks: dict[str, asyncio.Task] = {}

# In-memory per-job filename dedup for live phase.
# Key: job_id  ->  set of lowercased filenames already forwarded.
# This is ALWAYS-ON and prevents the same-named file from being forwarded twice,
# even if the file was re-uploaded (different file_unique_id).
_live_seen_names: dict[str, set] = {}

#  Future-based ask() — immune to pyrofork stale-listener bug 
_lj_waiting: dict[int, asyncio.Future] = {}


# ─── Per-account reconnect cooldown (prevents flood of GetFullUser on mass reconnect) ───
_lj_last_reconnect: dict = {}  # session_name -> last reconnect timestamp
_lj_me_cache: dict = {}        # session_name -> (me_obj, cached_at_timestamp)
_LJ_ME_CACHE_TTL = 300         # Reuse cached me for 5 min — avoids repeated GetFullUser calls

async def _lj_ping_client(client) -> bool:
    """
    Lightweight MTProto Ping to verify the transport is alive.
    Does NOT call users.GetFullUser (which causes FLOOD_WAIT_X).
    Falls back to is_connected() if ping isn't available.
    """
    try:
        # Use raw Ping — pure MTProto, zero API quota cost
        from pyrogram.raw.functions import Ping
        await asyncio.wait_for(
            client.invoke(Ping(ping_id=0)),
            timeout=10
        )
        return True
    except FloodWait as fw:
        logger.warning(f"[LiveJob] Ping FloodWait {fw.value}s — respecting and marking alive")
        await asyncio.sleep(fw.value + 1)
        return True   # We're alive, just rate-limited
    except asyncio.TimeoutError:
        return False
    except Exception as e:
        err = str(e).lower()
        if any(k in err for k in ("not been started", "not connected", "disconnected")):
            return False
        # Any other error (e.g. AUTH_KEY) — propagate
        raise


async def _lj_get_me_cached(client):
    """
    Get or reuse cached 'me' object. Avoids repeated GetFullUser RPC calls
    which directly cause the FLOOD_WAIT_X (users.GetFullUser) crash.
    """
    sname = getattr(client, 'name', None) or id(client)
    cached = _lj_me_cache.get(sname)
    if cached:
        me_obj, cached_at = cached
        if (asyncio.get_event_loop().time() - cached_at) < _LJ_ME_CACHE_TTL:
            return me_obj
    # Cache miss or expired — fetch fresh
    me_obj = await asyncio.wait_for(client.get_me(), timeout=20)
    _lj_me_cache[sname] = (me_obj, asyncio.get_event_loop().time())
    return me_obj


# ─── Client health-check / reconnect ────────────────────────────────────
async def _lj_ensure_client_alive(client):
    """
    Verify the Pyrogram client transport is alive using a cheap MTProto Ping.
    If dead, attempt a cold restart with exponential backoff (up to 3 attempts).

    KEY FIXES vs old version:
    - Uses Ping (not get_me/GetFullUser) → no FLOOD_WAIT_X from health checks
    - Per-session cooldown prevents hammering reconnect on multiple jobs
    - Exponential backoff: 5s, 15s, 30s between attempts
    - Clears me cache on restart so next get_me is fresh
    """
    sname = getattr(client, 'name', None) or str(id(client))

    # Per-session reconnect cooldown: don't try reconnecting more than once per 30s
    last_rc = _lj_last_reconnect.get(sname, 0)
    now = asyncio.get_event_loop().time()
    if (now - last_rc) < 30:
        # Recently tried reconnecting — assume alive to avoid hammering
        return client

    for attempt in range(3):
        is_alive = False
        try:
            is_alive = await _lj_ping_client(client)
        except Exception as ping_err:
            logger.warning(f"[LiveJob] Ping raised {ping_err} on attempt {attempt+1}")

        if is_alive:
            return client   # alive ✔️

        backoff = [5, 15, 30][attempt]
        logger.warning(f"[LiveJob] Client dead (attempt {attempt+1}/{3}) — reconnecting in {backoff}s…")
        _lj_last_reconnect[sname] = asyncio.get_event_loop().time()

        # Clean up the dead client
        try:
            if sname:
                await release_client(sname)
        except Exception: pass
        try:
            await client.stop()
        except Exception: pass

        # Evict stale me cache
        _lj_me_cache.pop(sname, None)

        await asyncio.sleep(backoff)

        try:
            client = await start_clone_bot(client)
            # Verify with ping (not get_me) after cold start
            if await _lj_ping_client(client):
                logger.info(f"[LiveJob] Client reconnected successfully on attempt {attempt+1}")
                return client
        except FloodWait as fw:
            logger.warning(f"[LiveJob] FloodWait {fw.value}s during reconnect attempt {attempt+1}")
            await asyncio.sleep(fw.value + 2)
        except Exception as re_err:
            logger.error(f"[LiveJob] Restart attempt {attempt+1} failed: {re_err}")

    raise RuntimeError("LIVEJOB_RECONNECT_FAILED: client failed to reconnect after 3 attempts")


@Client.on_message(filters.private, group=-12)
async def _lj_input_router(bot, message):
    """Route private messages to waiting _ask() futures for Live Job flow."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _lj_waiting:
        fut = _lj_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation


async def _ask(bot, user_id: int, text: str, reply_markup=None, timeout: int = 300):
    """Send text and wait for the next private message from user_id."""
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    old = _lj_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _lj_waiting[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _lj_waiting.pop(user_id, None)
        raise


async def _ask_topic(bot, user_id: int, dest_label: str) -> int | None:
    """Ask user for an optional topic thread ID (for group topics).
    Returns the thread ID as int, or None if not needed.
    NOTE: This was the cause of Live Job step 3/7 silently hanging —
    the function was called but never defined here.
    """
    from pyrogram.types import KeyboardButton, ReplyKeyboardMarkup
    r = await _ask(bot, user_id,
        f"<b>Topic Thread for {dest_label} (Optional)</b>\n\n"
        "• Send the <b>Thread ID</b> if you want to post inside a specific group topic\n"
        "• Send <b>0</b> if this is a regular channel or main group chat (no topic)\n\n"
        "<i>To find Thread ID: open the topic in Telegram Web → look at the number after "
        "<code>/topics/</code> in the URL.</i>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("0 (No Topic)")], [KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]],
            resize_keyboard=True, one_time_keyboard=True
        ))
    t = r.text.strip() if r and r.text else "0"
    if "/cancel" in t.lower() or "⛔" in t:
        return None
    if t.lstrip("-").isdigit() and int(t) > 0:
        return int(t)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════


async def _save_job(job: dict):
    await db.db.jobs.replace_one({"job_id": job["job_id"]}, job, upsert=True)

async def _get_job(job_id: str) -> dict | None:
    return await db.db.jobs.find_one({"job_id": job_id})

async def _list_jobs(user_id: int) -> list[dict]:
    return [j async for j in db.db.jobs.find({"user_id": user_id})]

async def _delete_job_db(job_id: str):
    await db.db.jobs.delete_one({"job_id": job_id})

async def _update_job(job_id: str, **kwargs):
    await db.db.jobs.update_one({"job_id": job_id}, {"$set": kwargs})

async def _inc_forwarded(job_id: str, n: int = 1, forward_type: str = 'batch'):
    await db.db.jobs.update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})
    import asyncio as _asyncio
    _asyncio.create_task(db.update_global_stats(**{f"{forward_type}_forward": n}))


# ══════════════════════════════════════════════════════════════════════════════
# Filter helpers
# ══════════════════════════════════════════════════════════════════════════════

def _get_unique_id(msg) -> str | None:
    """Extract file_unique_id from a message's media robustly."""
    if not msg: return None
    try:
        if getattr(msg, 'media', None):
            media_type = getattr(msg.media, 'value', str(msg.media))
            obj = getattr(msg, media_type, None)
            if obj:
                if hasattr(obj, 'file_unique_id'): return obj.file_unique_id
                if hasattr(obj, 'file_id'): return obj.file_id
                if isinstance(obj, list) and len(obj) > 0:
                    return getattr(obj[-1], 'file_unique_id', getattr(obj[-1], 'file_id', None))
    except Exception:
        pass
        
    for attr in ('document', 'video', 'audio', 'voice', 'animation', 'photo', 'sticker', 'video_note'):
        obj = getattr(msg, attr, None)
        if obj:
            if isinstance(obj, list) and len(obj) > 0: obj = obj[-1]
            return getattr(obj, 'file_unique_id', getattr(obj, 'file_id', None))
    return None

def _passes_filters(msg, disabled_types: list) -> bool:
    """Return True if message passes the user's content-type filters."""
    if msg.empty or msg.service:
        return False
    

    checks = [
        ('text',      lambda m: bool(m.text and (not m.media or getattr(m.media, 'value', str(m.media)) == 'web_page'))),
        ('audio',     lambda m: m.audio),
        ('voice',     lambda m: m.voice),
        ('video',     lambda m: m.video),
        ('photo',     lambda m: m.photo),
        ('document',  lambda m: m.document),
        ('animation', lambda m: m.animation),
        ('sticker',   lambda m: m.sticker),
        ('poll',      lambda m: m.poll),
    ]
    for typ, check in checks:
        if typ in disabled_types and check(msg):
            return False
    return True


def _passes_size_limit(msg, max_size_mb: int, max_duration_secs: int, min_dur_secs: int = 0) -> bool:
    """Return True if message is within the per-job size/duration limits.
    0 means no limit. min_dur_secs: skip files SHORTER than this value.
    """
    if max_size_mb > 0:
        max_bytes = max_size_mb * 1024 * 1024
        media_obj = None
        for attr in ('document', 'video', 'audio', 'voice', 'video_note', 'animation', 'photo'):
            media_obj = getattr(msg, attr, None)
            if media_obj:
                break
        if media_obj:
            size = getattr(media_obj, 'file_size', 0) or 0
            if size > max_bytes:
                return False

    if max_duration_secs > 0:
        for attr in ('video', 'audio', 'voice', 'video_note'):
            media_obj = getattr(msg, attr, None)
            if media_obj:
                dur = getattr(media_obj, 'duration', 0) or 0
                if dur > max_duration_secs:
                    return False
                break

    # Min-duration filter: skip files shorter than threshold (e.g. skip 10-sec clips)
    if min_dur_secs > 0:
        found_media_with_dur = False
        for attr in ('video', 'audio', 'voice', 'video_note'):
            media_obj = getattr(msg, attr, None)
            if media_obj:
                dur = getattr(media_obj, 'duration', 0) or 0
                found_media_with_dur = True
                if dur < min_dur_secs:
                    return False
                break
        # If the message has no duration-bearing media, don't apply min_dur filter

    return True


def _msg_in_topic(msg, from_thread_id: int) -> bool:
    """Return True if `msg` belongs to the given source topic (thread).

    Telegram rules:
    - Messages in a topic carry `message_thread_id` = the topic's root message ID.
    - The topic-creator message itself has msg.id == thread_id AND no message_thread_id.
    - In the 'General' topic (thread_id=1), messages may NOT carry message_thread_id at all.
    - Some Pyrogram builds expose `reply_to_top_id` which equals the topic root.
    - Forwarded messages keep the original message_thread_id.
    """
    tid = getattr(msg, "message_thread_id", None)
    if tid is not None and int(tid) == from_thread_id:
        return True
    # The topic-starter message itself
    if int(msg.id) == from_thread_id:
        return True
    # General topic (id=1): messages with no thread marker belong to General
    if from_thread_id == 1 and tid is None:
        return True
    # Fallback: reply_to_top_id (older pyrogram / pyrofork field)
    rtt = getattr(msg, "reply_to_top_id", None)
    if rtt is not None and int(rtt) == from_thread_id:
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Forward helper — supports dual destination + topic threads
# ══════════════════════════════════════════════════════════════════════════════

async def _forward_message(
    client, msg,
    to_chat: int, remove_caption: bool, cap_tpl: str | None, forward_tag: bool = False,
    thread_id: int = None,
    to_chat_2: int = None, thread_id_2: int = None,
    replacements: dict = None,
    remove_links_flag: bool = False
):
    """Copy message to 1 or 2 destinations. Falls back to download/re-upload if restricted."""
    from plugins.regix import custom_caption, remove_all_links
    import re
    import asyncio
    import os

    new_caption = None
    new_text = None
    is_text_replaced = False

    if msg.media:
        new_caption = custom_caption(msg, cap_tpl, apply_smart_clean=remove_caption, remove_links_flag=remove_links_flag)
        if replacements and new_caption:
            for old_txt, new_txt_str in replacements.items():
                if old_txt is None: continue
                new_str = "" if new_txt_str is None else str(new_txt_str)
                try: new_caption = re.sub(str(old_txt), new_str, str(new_caption), flags=re.IGNORECASE)
                except Exception: new_caption = str(new_caption).replace(str(old_txt), new_str)
    else:
        new_text = getattr(msg.text, "html", str(msg.text)) if msg.text else ""
        if remove_links_flag and new_text:
            new_text = remove_all_links(new_text)
            is_text_replaced = True
            
        if replacements and new_text:
            orig_text = new_text
            for old_txt, new_txt_str in replacements.items():
                if old_txt is None: continue
                new_str = "" if new_txt_str is None else str(new_txt_str)
                try: new_text = re.sub(str(old_txt), new_str, str(new_text), flags=re.IGNORECASE)
                except Exception: new_text = str(new_text).replace(str(old_txt), new_str)
            if orig_text != new_text:
                is_text_replaced = True

    async def _send_one(chat, thread):
        nonlocal forward_tag
        if new_caption is not None or is_text_replaced:
            # Telegram CANNOT modify text/captions of natively forwarded messages.
            # If the user wants to wipe captions, remove links, or replace text, we MUST use copy_message.
            forward_tag = False

        kw = {"message_thread_id": thread} if thread else {}
        if new_caption is not None:
            kw["caption"] = new_caption

        #  Attempt 1: copy_message 
        is_restricted = False
        for attempt in range(3):
            try:
                if forward_tag:
                    await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                else:
                    if is_text_replaced and not msg.media:
                        if not new_text or not new_text.strip():
                            return True # Silently skip since it's an empty text msg after stripping
                        await client.send_message(chat_id=chat, text=new_text, **kw)
                    else:
                        await client.copy_message(chat_id=chat, from_chat_id=msg.chat.id, message_id=msg.id, **kw)
                return True
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except Exception as e:
                err = str(e).upper()
                # Stop jobs completely if destination or source lacks permissions / invalid
                if any(x in err for x in ["PEER_ID_INVALID", "CHAT_WRITE_FORBIDDEN", "USER_BANNED", "CHANNEL_PRIVATE", "CHAT_ADMIN_REQUIRED"]):
                    raise ValueError(f"Fatal Chat Error: {e}")

                if "RESTRICTED" in err or "PROTECTED" in err or "FALLBACK" in err:
                    is_restricted = True
                    break
                if "TIMEOUT" in err or "CONNECTION" in err:
                    await asyncio.sleep(5)
                    continue 
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue
                return False

        if not is_restricted:
            return False

        #  Attempt 2: download + re-upload 
        for attempt in range(5):
            try:
                fp = None
                media_obj = getattr(msg, msg.media.value, None) if msg.media else None
                original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                if msg.media:
                    safe_name = f"downloads/{msg.id}_{original_name}" if original_name else f"downloads/{msg.id}"
                    
                    # Internal retry for download
                    for dl_attempt in range(5):
                        try:
                            fp = await client.download_media(msg, file_name=safe_name)
                            if fp: 
                                await db.update_global_stats(total_files_downloaded=1, total_data_usage_bytes=os.path.getsize(str(fp)))
                                break
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2)
                        except Exception as dl_e:
                            if "TIMEOUT" in str(dl_e).upper() or "CONNECTION" in str(dl_e).upper():
                                await asyncio.sleep(5)
                                continue
                            if dl_attempt < 4:
                                await asyncio.sleep(3)
                                continue
                            break
                    
                    if not fp: raise Exception("DownloadFailed")
                    
                    up_kw = {"chat_id": chat, "caption": new_caption if new_caption is not None else (msg.caption or "")}
                    if thread: up_kw["message_thread_id"] = thread
                    
                    if getattr(msg, 'photo', None): await client.send_photo(photo=fp, **up_kw)
                    elif getattr(msg, 'video', None): await client.send_video(video=fp, file_name=original_name, **up_kw)
                    elif getattr(msg, 'document', None): await client.send_document(document=fp, file_name=original_name, **up_kw)
                    elif getattr(msg, 'audio', None): await client.send_audio(audio=fp, file_name=original_name, **up_kw)
                    elif getattr(msg, 'voice', None): await client.send_voice(voice=fp, **up_kw)
                    elif getattr(msg, 'animation', None): await client.send_animation(animation=fp, **up_kw)
                    elif getattr(msg, 'sticker', None): await client.send_sticker(sticker=fp, **up_kw)
                    
                    await db.update_global_stats(total_files_uploaded=1, total_data_usage_bytes=os.path.getsize(str(fp)))
                    if os.path.exists(fp): os.remove(fp)
                    return True
                else:
                    if not new_text or not new_text.strip():
                        return True
                    await client.send_message(chat_id=chat, text=new_text if new_text is not None else getattr(msg.text, "html", str(msg.text)) if msg.text else "", **kw)
                    return True
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except Exception as e2:
                f_err = str(e2).upper()
                if "TIMEOUT" in f_err or "CONNECTION" in f_err:
                    await asyncio.sleep(5)
                    continue 
                if attempt < 4:
                    await asyncio.sleep(3)
                    continue
                return False
        return False

    success1 = await _send_one(to_chat, thread_id)
    success2 = False
    if to_chat_2:
        success2 = await _send_one(to_chat_2, thread_id_2)
    return success1 or success2


# ══════════════════════════════════════════════════════════════════════════════
async def _get_latest_id(client, chat_id, is_bot: bool) -> int:
    try:
        if not is_bot:
            async for msg in client.get_chat_history(chat_id, limit=1):
                return msg.id
        else:
            lo, hi = 1, 9_999_999
            BATCH = 50
            for _ in range(25):
                if hi - lo <= BATCH:
                    break
                mid = (lo + hi) // 2
                try:
                    probe = await client.get_messages(chat_id, [mid])
                    if not isinstance(probe, list): probe = [probe]
                    if any(m and not m.empty for m in probe):
                        lo = mid
                    else:
                        hi = mid
                except Exception:
                    hi = mid
            return hi
    except Exception:
        pass
    return 0


# ══════════════════════════════════════════════════════════════════════════════
# Core job runner
# ══════════════════════════════════════════════════════════════════════════════

BATCH_CHUNK = 200  # IDs per get_messages call in batch phase

async def _run_job(job_id: str, user_id: int):
    job = await _get_job(job_id)
    if not job:
        return

    acc = client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _update_job(job_id, status="error", error="Account not found")
            return

        client = await start_clone_bot(_CLIENT.client(acc))
        # Health-check immediately after starting
        client = await _lj_ensure_client_alive(client)
        is_bot = acc.get("is_bot", True)

        from_chat    = job["from_chat"]
        to_chat      = job["to_chat"]

        # ── Protected Chat Guard ───────────────────────────────────────────────
        from plugins.utils import check_chat_protection
        prot_err = await check_chat_protection(job["user_id"], from_chat)
        if prot_err:
            await _update_job(job_id, status="error", error=prot_err)
            try:
                await client.send_message(job["user_id"], prot_err)
            except Exception:
                pass
            return
        # ──────────────────────────────────────────────────────────────────────

        to_thread    = job.get("to_thread_id", None)
        to_chat_2    = job.get("to_chat_2", None)
        to_thread_2  = job.get("to_thread_id_2", None)
        max_size_mb  = int(job.get("max_size_mb", 0) or 0)
        max_dur_secs = int(job.get("max_duration_secs", 0) or 0)
        min_dur_secs = int(job.get("min_duration_secs", 0) or 0)
        notify_large_mb = int(job.get("notify_large_file_mb", 0) or 0)
        last_seen    = job.get("last_seen_id", 0)

        #  First-run init: get me (cached) for Bot-DM swap detection 
        for _att in range(3):
            try:
                me = await _lj_get_me_cached(client)
                break
            except FloodWait as fw:
                logger.warning(f"[Job {job_id}] get_me FloodWait {fw.value}s — waiting")
                await asyncio.sleep(fw.value + 2)
                if _att == 2: raise
            except Exception as e:
                if _att == 2: raise e
                await asyncio.sleep(5)
                
        if str(from_chat).lower() in [x.lower() for x in (str(me.id), me.username, "me", "saved") if x]:
            from_chat = user_id
            await _update_job(job_id, from_chat=from_chat)
            logger.info(f"[Job {job_id}] Swapped Bot's own ID with User ID ({user_id}) for Bot DM fetching")

        if str(to_chat).lower() in [x.lower() for x in (str(me.id), me.username, "me", "saved") if x]:
            to_chat = user_id
            await _update_job(job_id, to_chat=to_chat)

        if to_chat_2 and str(to_chat_2).lower() in [x.lower() for x in (str(me.id), me.username, "me", "saved") if x]:
            to_chat_2 = user_id
            await _update_job(job_id, to_chat_2=to_chat_2)

        # ── STEP 1: Resolve source chat type FIRST (before _get_latest_id!) ──────
        # This is CRITICAL: for DM/bot sources we MUST use get_chat_history (userbot only),
        # NOT get_messages — which fetches from the global inbox / saved messages (wrong!).
        # Resolving first also ensures _get_latest_id uses the correct method.
        is_dm_source = False
        from pyrogram.enums import ChatType
        from plugins.utils import safe_resolve_peer

        # Always resolve the source to a numeric ID and determine if it's a DM/bot
        _resolved_from_chat = from_chat
        try:
            # Try resolving via the forwarding client first, fall back to main bot
            try:
                peer_chat = await client.get_chat(from_chat)
            except Exception:
                peer_chat = await BOT_INSTANCE.get_chat(from_chat)

            if peer_chat.type in (ChatType.PRIVATE, ChatType.BOT):
                is_dm_source = True
            _resolved_from_chat = peer_chat.id
            from_chat = peer_chat.id
        except Exception as resolve_e:
            logger.warning(f"[Job {job_id}] Source resolve warning: {resolve_e}")
            # Fallback heuristic: positive int = user/bot DM, negative = channel/group
            if isinstance(from_chat, int) and from_chat > 0:
                is_dm_source = True
            elif isinstance(from_chat, str) and from_chat.lower() in ("me", "saved"):
                is_dm_source = True

        # ── STEP 2: Get latest message ID (with correct method for source type) ──
        # ⚠️ For DM sources, ALWAYS use get_chat_history — even for bot accounts.
        # The binary search via get_messages on a DM peer returns wrong (globally-scoped) IDs.
        # For bot + DM source: use the MAIN BOT's get_chat_history if the bot can't access it.
        if last_seen == 0:
            try:
                if is_dm_source:
                    # For ALL DM sources, get the true latest message via chat history
                    async for msg in client.get_chat_history(from_chat, limit=1):
                        last_seen = msg.id
                        break
                    if last_seen == 0:
                        # Fallback to main bot if client can't read history
                        try:
                            async for msg in BOT_INSTANCE.get_chat_history(from_chat, limit=1):
                                last_seen = msg.id
                                break
                        except Exception:
                            pass
                else:
                    last_seen = await _get_latest_id(client, from_chat, is_bot)
            except Exception as li_e:
                logger.warning(f"[Job {job_id}] _get_latest_id error: {li_e}")
                last_seen = 0
            await _update_job(job_id, last_seen_id=last_seen)
            logger.info(f"[Job {job_id}] Initialised at msg ID {last_seen}")

        # Safety: if source is a DM/bot and forwarding account is a normal BOT (not userbot):
        # Normal bots CANNOT read message history from DMs. get_messages() on a user/bot ID
        # from a bot client fetches from the bot's own global inbox = saved messages (WRONG).
        # In this case, disable batch mode and go straight to live monitoring.
        if is_dm_source and is_bot and job.get("batch_mode") and not job.get("batch_done"):
            logger.warning(
                f"[Job {job_id}] Source is a bot/user DM but forwarding account is a normal BOT. "
                f"Batch mode is unsupported for this combination — skipping batch phase. "
                f"Only new messages arriving to the bot will be forwarded in live mode."
            )
            await _update_job(job_id, batch_done=True,
                              error="[Info] Batch skipped: bot accounts cannot read DM history. "
                                    "Forwarding in live mode only.")
            try:
                await BOT_INSTANCE.send_message(user_id,
                    f"⚠️ <b>Live Job Notice</b>\n\n"
                    f"The source <b>{job.get('from_title', str(from_chat))}</b> is a Bot/User DM, "
                    f"but the selected forwarding account is a <b>Normal Bot</b>.\n\n"
                    f"Normal bots <b>cannot read message history</b> from DMs — batch mode was skipped.\n"
                    f"The job will now run in <b>live mode only</b>, forwarding new messages as they arrive.\n\n"
                    f"<i>To use batch mode, switch to a Userbot account.</i>")
            except Exception: pass

        try:
            dest_chats = [from_chat, to_chat] + ([to_chat_2] if to_chat_2 else [])
            for _chat in dest_chats:
                await safe_resolve_peer(client, _chat, bot=BOT_INSTANCE)
        except Exception:
            pass

        #  BATCH PHASE 
        if job.get("batch_mode") and not job.get("batch_done"):
            batch_cursor = int(job.get("batch_cursor") or job.get("batch_start_id") or 1)
            batch_end    = int(job.get("batch_end_id") or 0)

            # If no explicit end was set, use the snapshot we just captured
            if batch_end == 0:
                batch_end = last_seen if last_seen > 0 else 999999999
                await _update_job(job_id, batch_end_id=batch_end)

            logger.info(f"[Job {job_id}] Batch phase: msg {batch_cursor} → {batch_end}")
            # Progress bar helpers
            acc_name = acc.get('name', 'Userbot') if acc else 'Userbot'
            total_msgs_calc = max(0, batch_end - int(job.get("batch_start_id") or 1) + 1)
            
            def get_prog_text(current_count: int, status: str = "running") -> str:
                if status == "done":
                    return (
                        f"➤ <b>✓ ꜰᴏʀᴡᴀʀᴅɪɴɢ ᴄᴏᴍᴘʟᴇᴛᴇ!</b>\n"
                        f"➤ <b>ᴀᴄᴄᴏᴜɴᴛ:</b> <code>{acc_name}</code>\n\n"
                        f"➤ ᴀʟʟ <u>{current_count}</u> ꜰɪʟᴇꜱ ʜᴀᴠᴇ ʙᴇᴇɴ ᴍᴏᴠᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ!\n\n"
                        f"<i>ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀʀʏᴀ ꜰᴏʀᴡᴀʀᴅ ʙᴏᴛ</i>"
                    )
                else:
                    total_str = str(total_msgs_calc) if total_msgs_calc > 0 else '?'
                    return (
                        f"<b>➤ {acc_name}</b>\n"
                        f"➤ ᴛʀᴀɴꜱꜰᴇʀʀɪɴɢ ꜰɪʟᴇꜱ ᴘʟᴇᴀꜱᴇ ᴡᴀɪᴛ...\n\n"
                        f"➤ <b>ꜰɪʟᴇꜱ ꜱᴇɴᴛ:</b> <code>{current_count}</code> / <code>{total_str}</code>\n\n"
                        f"<i>ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀʀʏᴀ ꜰᴏʀᴡᴀʀᴅ ʙᴏᴛ</i>"
                    )

            if not job.get("prog_msg_created"):
                try:
                    sent = await client.send_message(to_chat, get_prog_text(0))
                    await _update_job(job_id, prog_msg_created=True, prog_msg_id=sent.id)
                    try: await client.pin_chat_message(to_chat, sent.id, disable_notification=True)
                    except Exception: pass
                except Exception:
                    await _update_job(job_id, prog_msg_created=True)

            job_last_prog_update = time.time()
            consecutive_empty = 0
            _channel_invalid_strikes = 0  # abort after 3 consecutive CHANNEL_INVALID heals

            # ── BOT DM BATCH (userbot + non-channel source) ──────────────────────
            # get_messages() without a channel peer queries the GLOBAL inbox (wrong).
            # For DM/username sources we collect ALL messages via get_chat_history
            # (which properly scopes to the specific conversation), sort chronologically,
            # and forward them all in one go — then skip the while loop entirely.
            if not is_bot and is_dm_source:
                logger.info(f"[Job {job_id}] DM batch: collecting via get_chat_history")
                dm_all = []
                batch_start_id = int(job.get('batch_start_id') or 1)
                try:
                    async for m in client.get_chat_history(from_chat):
                        if m.empty or m.service:
                            continue
                        if m.id < batch_start_id:
                            break  # gone past the start — stop
                        if batch_end > 0 and m.id > batch_end:
                            continue  # not yet in range
                        dm_all.append(m)
                except Exception as e:
                    logger.warning(f"[Job {job_id}] DM history collect error: {e}")

                dm_all.sort(key=lambda m: m.id)  # chronological order
                logger.info(f"[Job {job_id}] DM batch: {len(dm_all)} messages to forward")

                for msg in dm_all:
                    fresh = await _get_job(job_id)
                    if not fresh or fresh.get("status") != "running":
                        return

                    disabled_types = await db.get_filters(user_id)
                    configs        = await db.get_configs(user_id)
                    filters_dict   = configs.get('filters', {})
                    remove_caption = filters_dict.get('rm_caption', False)
                    remove_links   = 'links' in disabled_types
                    cap_tpl        = configs.get('caption')
                    forward_tag    = configs.get('forward_tag', False)
                    sleep_secs     = max(1, int(configs.get('duration', 1) or 1))
                    replacements   = configs.get('replacements', {})

                    if not _passes_filters(msg, disabled_types):
                        await _update_job(job_id, batch_cursor=msg.id + 1)
                        continue
                    if not _passes_size_limit(msg, max_size_mb, max_dur_secs):
                        await _update_job(job_id, batch_cursor=msg.id + 1)
                        continue

                    skip_dupes = fresh.get("skip_duplicates", False)
                    uniq_id = _get_unique_id(msg) if skip_dupes else None
                    if skip_dupes and uniq_id and uniq_id in (fresh.get("seen_file_ids") or []):
                        logger.debug(f"[Job {job_id}] DM Batch: skipping duplicate {uniq_id}")
                        await _update_job(job_id, batch_cursor=msg.id + 1)
                        continue

                    try:
                        success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        if success:
                            await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                        success = False
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug(f"[Job {job_id}] DM batch fwd error {msg.id}: {e}")
                        success = False

                    upd = {"batch_cursor": msg.id + 1}
                    if success and uniq_id:
                        seen = fresh.get("seen_file_ids") or []
                        if uniq_id not in seen:
                            seen.append(uniq_id)
                            if len(seen) > 5000: seen.pop(0)
                        upd["seen_file_ids"] = seen

                    await _update_job(job_id, **upd)


                    now_mj = time.time()
                    if (now_mj - job_last_prog_update) >= 10:
                        job_last_prog_update = now_mj
                        try:
                            prog_id = (await _get_job(job_id)).get("prog_msg_id")
                            if prog_id:
                                fresh_j = await _get_job(job_id)
                                _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                                from pyrogram.enums import ParseMode
                                await client.edit_message_text(to_chat, prog_id, get_prog_text(_fwd, "running"), parse_mode=ParseMode.HTML)
                        except Exception: pass

                    await asyncio.sleep(sleep_secs)

                # DM batch done — mark complete and fall through to live phase
                await _update_job(job_id, batch_done=True, batch_cursor=batch_end,
                                  last_seen_id=max(last_seen, batch_end))
                last_seen = max(last_seen, batch_end)
                logger.info(f"[Job {job_id}] DM batch complete ({len(dm_all)} msgs).")
                try:
                    fresh_j = await _get_job(job_id)
                    _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                    prog_id = fresh_j.get("prog_msg_id")
                    if prog_id:
                        from pyrogram.enums import ParseMode
                        await client.edit_message_text(to_chat, prog_id, get_prog_text(_fwd, "done"), parse_mode=ParseMode.HTML)
                except Exception:
                    pass

            else:
            # ── CHANNEL/GROUP BATCH (original get_messages path) ─────────────────
             while batch_cursor <= batch_end:
                fresh = await _get_job(job_id)
                if not fresh or fresh.get("status") != "running":
                    return

                disabled_types = await db.get_filters(user_id)
                configs        = await db.get_configs(user_id)
                filters_dict   = configs.get('filters', {})
                remove_caption = filters_dict.get('rm_caption', False)
                remove_links   = 'links' in disabled_types
                cap_tpl        = configs.get('caption')
                forward_tag    = configs.get('forward_tag', False)
                sleep_secs     = max(1, int(configs.get('duration', 1) or 1))

                replacements   = configs.get('replacements', {})

                chunk_end = min(batch_cursor + BATCH_CHUNK - 1, batch_end)
                batch_ids = list(range(batch_cursor, chunk_end + 1))

                # ── Fetch: for userbot + DM/username source, get_messages() uses
                # messages.GetMessages WITHOUT a peer → fetches from global inbox
                # (i.e. wrong chat). Always use get_chat_history for DM sources.
                try:
                    if not is_bot and is_dm_source:
                        # Userbot + DM/bot source → paginate via get_chat_history
                        batch_msgs = []
                        async for m in client.get_chat_history(from_chat, limit=BATCH_CHUNK, offset_id=batch_cursor):
                            if m.id < (int(job.get('batch_start_id') or 1)):
                                break
                            batch_msgs.append(m)
                        # get_chat_history returns newest→oldest; reverse to chronological
                        msgs = list(reversed(batch_msgs))
                        if not isinstance(msgs, list): msgs = [msgs]
                    else:
                        msgs = await client.get_messages(from_chat, batch_ids)
                        if not isinstance(msgs, list): msgs = [msgs]
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2)
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    err_fetch = str(e).upper()
                    if "CHANNEL_INVALID" in err_fetch or "PEER_ID_INVALID" in err_fetch:
                        _channel_invalid_strikes += 1
                        logger.error(
                            f"[Job {job_id}] FATAL Peer error in batch ({_channel_invalid_strikes}/3): {e}. "
                            f"{'Trying to heal...' if _channel_invalid_strikes < 3 else 'Giving up — source channel is permanently inaccessible.'}"
                        )
                        if _channel_invalid_strikes >= 3:
                            err_msg = (
                                f"⚠️ <b>Job Stopped — Source Channel Inaccessible</b>\n\n"
                                f"The source channel could not be accessed after 3 attempts.\n"
                                f"<b>Error:</b> <code>CHANNEL_INVALID</code>\n"
                                f"<b>Channel ID:</b> <code>{from_chat}</code>\n\n"
                                f"<i>Possible causes: the bot was removed, the channel was deleted, "
                                f"or the channel ID is incorrect. Please reconfigure the job.</i>"
                            )
                            await _update_job(job_id, status="error", error="CHANNEL_INVALID — source permanently inaccessible")
                            try: await BOT_INSTANCE.send_message(user_id, err_msg)
                            except Exception: pass
                            return
                        try: await safe_resolve_peer(client, from_chat, bot=BOT_INSTANCE)
                        except: pass
                        await asyncio.sleep(5)
                        continue


                    logger.warning(f"[Job {job_id}] Batch fetch error: {e}")
                    batch_cursor += BATCH_CHUNK
                    await _update_job(job_id, batch_cursor=batch_cursor)
                    continue

                valid = [m for m in msgs if m and not m.empty and not m.service]
                valid.sort(key=lambda m: m.id)
                
                # Cross-chat filter: verify every message belongs to the expected source chat.
                # For negative IDs (channels/groups): check m.chat.id == from_chat
                # For positive IDs (user/bot DMs): also check m.chat.id matches — this prevents
                # the bot's global inbox messages from leaking in when get_messages is misused.
                filtered = []
                for m in valid:
                    if isinstance(from_chat, int):
                        if m.chat is None:
                            continue
                        if m.chat.id != from_chat:
                            continue
                    # String usernames: accept (Pyrogram resolves the peer correctly)
                    filtered.append(m)
                valid = filtered

                
                if not valid:
                    consecutive_empty += 1
                    if consecutive_empty >= 200:  # 200 * 200 IDs = 40,000 IDs gap before giving up
                        logger.info(f"[Job {job_id}] Done — no more messages after {batch_cursor}")
                        break
                else:
                    consecutive_empty = 0

                # Filter by source topic if configured
                from_thread = job.get("from_thread")
                if from_thread:
                    from_thread = int(from_thread)
                    valid = [m for m in valid if _msg_in_topic(m, from_thread)]

                for msg in valid:
                    # Re-check stop between every message
                    fresh2 = await _get_job(job_id)
                    if not fresh2 or fresh2.get("status") != "running":
                        return

                    if not _passes_filters(msg, disabled_types):
                        await _update_job(job_id, batch_cursor=msg.id + 1)
                        continue
                    if not _passes_size_limit(msg, max_size_mb, max_dur_secs):
                        logger.debug(f"[Job {job_id}] Batch: skipping msg {msg.id} (size/duration limit)")
                        await _update_job(job_id, batch_cursor=msg.id + 1)
                        continue

                    skip_dupes = fresh2.get("skip_duplicates", False)
                    uniq_id = _get_unique_id(msg) if skip_dupes else None
                    if skip_dupes and uniq_id and uniq_id in (fresh2.get("seen_file_ids") or []):
                        logger.debug(f"[Job {job_id}] Batch: skipping duplicate {uniq_id}")
                        await _update_job(job_id, batch_cursor=msg.id + 1)
                        continue

                    try:
                        success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        if success:
                            await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                        success = False
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug(f"[Job {job_id}] Batch fwd error for {msg.id}: {e}")
                        success = False

                    upd = {"batch_cursor": msg.id + 1}
                    if success and uniq_id:
                        seen = fresh2.get("seen_file_ids") or []
                        if uniq_id not in seen:
                            seen.append(uniq_id)
                            if len(seen) > 5000: seen.pop(0)
                        upd["seen_file_ids"] = seen

                    await _update_job(job_id, **upd)


                    now_mj = time.time()
                    if (now_mj - job_last_prog_update) >= 10:
                        job_last_prog_update = now_mj
                        try:
                            prog_id = (await _get_job(job_id)).get("prog_msg_id")
                            if prog_id:
                                fresh_j = await _get_job(job_id)
                                _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                                from pyrogram.enums import ParseMode
                                await client.edit_message_text(to_chat, prog_id, get_prog_text(_fwd, "running"), parse_mode=ParseMode.HTML)
                        except Exception: pass

                    await asyncio.sleep(sleep_secs)

                batch_cursor = chunk_end + 1
                await _update_job(job_id, batch_cursor=batch_cursor)

            # Batch complete — mark done, advance last_seen past the batch
            await _update_job(job_id, batch_done=True, batch_cursor=batch_end,
                              last_seen_id=max(last_seen, batch_end))
            last_seen = max(last_seen, batch_end)
            logger.info(f"[Job {job_id}] Batch phase complete. Switching to live mode.")
            
            try:
                fresh_j = await _get_job(job_id)
                _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                prog_id = fresh_j.get("prog_msg_id")
                if prog_id:
                    from pyrogram.enums import ParseMode
                    await client.edit_message_text(to_chat, prog_id, get_prog_text(_fwd, "done"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

        #  LIVE PHASE 
        logger.info(f"[Job {job_id}] Live polling started. last_seen={last_seen}")

        # Send / restore live-phase destination progress message
        live_prog_id = (await _get_job(job_id)).get("live_prog_msg_id")
        if not live_prog_id:
            try:
                _cur_fwd = (await _get_job(job_id)).get("forwarded", 0)
                live_sent = await client.send_message(
                    to_chat,
                    f"📡 <b>Live Job Active — monitoring for new messages…</b>\n\n"
                    f"✅ Processed Files: <code>{_cur_fwd}</code>\n"
                    f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                    f"<i>This message updates every 60s. Arya Bot</i>"
                )
                live_prog_id = live_sent.id
                await _update_job(job_id, live_prog_msg_id=live_prog_id)
                try: await client.pin_chat_message(to_chat, live_prog_id, disable_notification=True)
                except Exception: pass
            except Exception:
                live_prog_id = None

        # ── Cache configs once before loop; refresh every 60s to pick up changes ──
        disabled_types: list = await db.get_filters(user_id)
        configs        = await db.get_configs(user_id)
        filters_dict   = configs.get('filters', {})
        remove_caption = filters_dict.get('rm_caption', False)
        remove_links   = 'links' in disabled_types
        cap_tpl        = configs.get('caption')
        forward_tag    = configs.get('forward_tag', False)
        replacements   = configs.get('replacements', {})
        _cfg_last_refresh = time.time()
        _last_ping_check  = time.time()

        live_last_update = 0.0

        # ── In-memory dedup: block same-named files from being forwarded twice ──
        # Initialise fresh for this session (cleared on restart only, which is fine
        # since last_seen_id ensures we never re-process old messages after a restart).
        if job_id not in _live_seen_names:
            _live_seen_names[job_id] = set()
        _live_fn_seen = _live_seen_names[job_id]  # local alias for speed
        _live_channel_invalid_strikes = 0  # abort after 3 consecutive CHANNEL_INVALID in live phase

        while True:
            fresh = await _get_job(job_id)
            if not fresh or fresh.get("status") != "running":
                break

            # Refresh configs every 60s so settings changes take effect
            _now = time.time()
            if (_now - _cfg_last_refresh) >= 60:
                try:
                    disabled_types = await db.get_filters(user_id)
                    configs        = await db.get_configs(user_id)
                    filters_dict   = configs.get('filters', {})
                    remove_caption = filters_dict.get('rm_caption', False)
                    remove_links   = 'links' in disabled_types
                    cap_tpl        = configs.get('caption')
                    forward_tag    = configs.get('forward_tag', False)
                    replacements   = configs.get('replacements', {})
                    _cfg_last_refresh = _now
                except Exception:
                    pass

            # Proactive cheap Ping health check every 120s (NOT get_me — avoids GetFullUser flood)
            if (_now - _last_ping_check) >= 120:
                _last_ping_check = _now
                try:
                    is_ok = await _lj_ping_client(client)
                    if not is_ok:
                        logger.warning(f"[Job {job_id}] Proactive ping failed — healing client")
                        client = await _lj_ensure_client_alive(client)
                except Exception as _ping_e:
                    logger.debug(f"[Job {job_id}] Proactive ping error: {_ping_e}")

            new_msgs: list = []

            try:
                # ── PATH 1: Userbot (any source) ────────────────────────────────────────────
                # Userbots can always use get_chat_history to fetch from any chat they're in.
                if not is_bot:
                    collected = []
                    offset_id = 0  # 0 = start from the very latest
                    while True:
                        page = []
                        async for msg in client.get_chat_history(
                            from_chat,
                            limit=100,
                            offset_id=offset_id
                        ):
                            if msg.id <= last_seen:
                                break
                            page.append(msg)
                        if not page:
                            break
                        collected.extend(page)
                        if len(page) < 100:
                            break
                        offset_id = page[-1].id
                    new_msgs = list(reversed(collected))

                elif is_dm_source:
                    # ── PATH 2: Bot account + DM source ────────────────────────────────────
                    # Normal bots CANNOT call get_chat_history on DMs (Bot API restriction).
                    # The correct approach: messages arrive via Pyrogram's update system.
                    # Here we use get_messages with explicit IDs (last_seen+1, +2, ...) from
                    # the bot's DM with the user — this DOES work because the bot is the peer.
                    # We only probe a small forward window (20 IDs) since it's a live DM.
                    probe = last_seen + 1
                    probe_ids = list(range(probe, probe + 20))
                    try:
                        msgs = await client.get_messages(from_chat, probe_ids)
                        if not isinstance(msgs, list): msgs = [msgs]
                        valid = [m for m in msgs if m and not m.empty and not m.service]
                        valid = [m for m in valid if m.chat is not None and m.chat.id == from_chat]
                        valid.sort(key=lambda m: m.id)
                        new_msgs = valid
                    except Exception as _dm_e:
                        logger.debug(f"[Job {job_id}] Bot DM probe error: {_dm_e}")
                        new_msgs = []

                else:
                    # ── Channel/Group source path (bot accounts only) ─────────────────────
                    # Bot accounts cannot use get_chat_history on channels/groups they're not
                    # subscribed to. Probe sequentially with get_messages instead.
                    # CRITICAL FIX: do NOT break early on a short batch — gaps in
                    # message IDs (deleted/service msgs) look like short batches but
                    # there may still be valid messages after the gap.
                    probe = last_seen + 1
                    consecutive_empty_batches = 0
                    while True:
                        batch_ids = list(range(probe, probe + 200))
                        try:
                            msgs = await client.get_messages(from_chat, batch_ids)
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 1)
                            continue
                        except Exception as e:
                            err_str = str(e).upper()
                            if "CHANNEL_INVALID" in err_str or "PEER_ID_INVALID" in err_str:
                                raise e
                            break
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        valid = [m for m in msgs if m and not m.empty and not m.service]
                        if not valid:
                            # Empty batch — could be a gap or true end
                            consecutive_empty_batches += 1
                            if consecutive_empty_batches >= 3:
                                # 3 consecutive empty batches of 200 = 600 empty IDs
                                # Highly unlikely to have more messages after this
                                break
                            probe += 200
                            continue
                        consecutive_empty_batches = 0
                        valid.sort(key=lambda m: m.id)

                        # Cross-chat filter: verify messages belong to the expected channel.
                        filtered = []
                        for m in valid:
                            if isinstance(from_chat, int):
                                if m.chat is None: continue
                                if m.chat.id != from_chat: continue
                            filtered.append(m)

                        new_msgs.extend(filtered)
                        probe = valid[-1].id + 1
                        if len(valid) < 10:
                            # Very sparse batch — likely at or near the live edge,
                            # stop probing to avoid unnecessary API calls
                            break

            except FloodWait as fw:
                # Respect Telegram flood wait fully — account safety
                logger.warning(f"[Job {job_id}] FloodWait {fw.value}s in live fetch — waiting")
                await asyncio.sleep(fw.value + 2)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                err_fetch = str(e)
                err_up = err_fetch.upper()

                # Account safety: these errors mean the account/session is invalid — stop the job
                _ACCOUNT_FATAL = ("USER_DEACTIVATED", "SESSION_REVOKED", "AUTH_KEY_INVALID", "AUTH_KEY_UNREGISTERED")
                if any(k in err_up for k in _ACCOUNT_FATAL):
                    raise  # Bubble up to outer handler which alerts owner

                is_conn_err = any(k in err_up for k in (
                    "TIMEOUT", "CONNECTION", "NOT BEEN STARTED", "NOT CONNECTED",
                    "DISCONNECTED", "RESET", "NETWORK", "SOCKET", "PING",
                    "FLOOD_WAIT"
                ))
                if is_conn_err:
                    logger.warning(f"[Job {job_id}] Connection error in live fetch: {err_fetch}. Healing client...")
                    try:
                        client = await _lj_ensure_client_alive(client)
                    except Exception as heal_e:
                        logger.error(f"[Job {job_id}] Client heal failed: {heal_e}")
                        # Report explicitly to admin if everything breaks
                        try:
                            from config import Config
                            for _owner in Config.BOT_OWNER_ID:
                                await BOT_INSTANCE.send_message(_owner,
                                    f"🚨 <b>Fatal Crash in Live Job {job_id}</b>\n"
                                    f"<code>{heal_e}</code>")
                        except:
                            pass
                    await asyncio.sleep(15)
                else:
                    if "CHANNEL_INVALID" in err_up or "PEER_ID_INVALID" in err_up:
                        _live_channel_invalid_strikes += 1
                        logger.error(
                            f"[Job {job_id}] FATAL Peer error in live fetch "
                            f"({_live_channel_invalid_strikes}/3): {err_fetch}. "
                            f"{'Trying to heal peer...' if _live_channel_invalid_strikes < 3 else 'Giving up.'}"
                        )
                        if _live_channel_invalid_strikes >= 3:
                            err_msg = (
                                f"⚠️ <b>Job Stopped — Source Channel Inaccessible</b>\n\n"
                                f"The source channel could not be reached after 3 consecutive attempts.\n"
                                f"<b>Error:</b> <code>CHANNEL_INVALID</code>\n"
                                f"<b>Channel ID:</b> <code>{from_chat}</code>\n\n"
                                f"<i>Possible causes: bot was removed from the channel, channel was deleted, "
                                f"or the channel ID is wrong. Please check and reconfigure the job.</i>"
                            )
                            await _update_job(job_id, status="error", error="CHANNEL_INVALID — source permanently inaccessible")
                            try: await BOT_INSTANCE.send_message(user_id, err_msg)
                            except Exception: pass
                            return
                        try: await safe_resolve_peer(client, from_chat, bot=BOT_INSTANCE)
                        except: pass
                        await asyncio.sleep(5)
                        continue
                    _live_channel_invalid_strikes = 0  # reset on any other error type

                    logger.warning(f"[Job {job_id}] Fetch error: {err_fetch}")
                    await asyncio.sleep(15)
                continue

            # Filter by source topic if configured — use fresh DB value (not stale startup snapshot)
            from_thread = fresh.get("from_thread") if fresh else job.get("from_thread")
            if from_thread:
                from_thread = int(from_thread)
                before_count = len(new_msgs)
                new_msgs = [m for m in new_msgs if _msg_in_topic(m, from_thread)]
                if before_count != len(new_msgs):
                    logger.debug(f"[Job {job_id}] Topic filter (thread={from_thread}): {before_count} → {len(new_msgs)} msgs")

            skip_dupes = fresh.get("skip_duplicates", False)

            for msg in new_msgs:
                if not _passes_filters(msg, disabled_types):
                    last_seen = max(last_seen, msg.id)
                    await _update_job(job_id, last_seen_id=last_seen)
                    continue
                if not _passes_size_limit(msg, max_size_mb, max_dur_secs, min_dur_secs):
                    logger.debug(f"[Job {job_id}] Live: skipping msg {msg.id} (size/duration limit)")
                    last_seen = max(last_seen, msg.id)
                    await _update_job(job_id, last_seen_id=last_seen)
                    continue

                # ── ALWAYS-ON: filename-based duplicate guard ─────────────────
                # Block forwarding if a file with the exact same name was already
                # forwarded in this live session, regardless of skip_duplicates.
                _fn_key = None
                if msg.media:
                    _media_attr = getattr(msg.media, 'value', str(msg.media))
                    _media_obj  = getattr(msg, _media_attr, None)
                    if _media_obj:
                        _fn_raw = getattr(_media_obj, 'file_name', None)
                        if not _fn_raw and isinstance(_media_obj, list) and _media_obj:
                            _fn_raw = getattr(_media_obj[-1], 'file_name', None)
                        if _fn_raw:
                            _fn_key = _fn_raw.strip().lower()
                if _fn_key and _fn_key in _live_fn_seen:
                    logger.info(
                        f"[Job {job_id}] Blocking duplicate filename '{_fn_key}' "
                        f"(msg {msg.id}) — already forwarded in this session."
                    )
                    last_seen = max(last_seen, msg.id)
                    await _update_job(job_id, last_seen_id=last_seen)
                    continue

                # ── Optional: file_unique_id-based dedup (exact binary match) ─
                uniq_id = _get_unique_id(msg) if skip_dupes else None
                if skip_dupes and uniq_id and uniq_id in (fresh.get("seen_file_ids") or []):
                    logger.debug(f"[Job {job_id}] Live: skipping duplicate file_unique_id {uniq_id}")
                    last_seen = max(last_seen, msg.id)
                    await _update_job(job_id, last_seen_id=last_seen)
                    continue

                # Large-file owner notification
                if notify_large_mb > 0 and msg.media:
                    media_obj = None
                    for _attr in ('document', 'video', 'audio', 'voice', 'animation'):
                        media_obj = getattr(msg, _attr, None)
                        if media_obj: break
                    if media_obj:
                        file_size_mb = (getattr(media_obj, 'file_size', 0) or 0) / 1024 / 1024
                        if file_size_mb >= notify_large_mb:
                            fname = getattr(media_obj, 'file_name', None) or 'file'
                            dur   = getattr(media_obj, 'duration', 0) or 0
                            dur_s = f" ({dur//60}m {dur%60}s)" if dur else ""
                            try:
                                from config import Config
                                for _owner in Config.BOT_OWNER_ID:
                                    await bot.send_message(_owner,
                                        f"📦 <b>Large File Detected</b> — Live Job <code>{job_id[-6:]}</code>\n\n"
                                        f"<b>File:</b> <code>{fname}</code>{dur_s}\n"
                                        f"<b>Size:</b> <code>{file_size_mb:.1f} MB</code> "
                                        f"(limit: {notify_large_mb} MB)\n"
                                        f"<b>Source:</b> {job.get('from_title','?')}\n"
                                        f"<b>Msg ID:</b> <code>{msg.id}</code>")
                            except Exception: pass
                
                success = False
                try:
                    success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                           to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                    if success:
                        await _inc_forwarded(job_id, 1, forward_type='live')
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fwd_err = str(e)
                    fwd_up = fwd_err.upper()
                    is_conn_err = any(k in fwd_up for k in (
                        "NOT BEEN STARTED", "NOT CONNECTED", "DISCONNECTED",
                        "CONNECTION", "TIMEOUT", "RESET"
                    ))
                    if is_conn_err:
                        logger.warning(f"[Job {job_id}] Connection error during forward: {fwd_err}. Healing...")
                        try:
                            client = await _lj_ensure_client_alive(client)
                        except Exception: pass
                    else:
                        logger.debug(f"[Job {job_id}] Forward error: {fwd_err}")

                last_seen = max(last_seen, msg.id)
                upd = {"last_seen_id": last_seen}

                if success:
                    # Mark filename as seen so duplicates are blocked going forward
                    if _fn_key:
                        _live_fn_seen.add(_fn_key)
                    # Track file_unique_id for binary-exact dedup
                    if uniq_id:
                        seen = fresh.get("seen_file_ids") or []
                        if uniq_id not in seen:
                            seen.append(uniq_id)
                            if len(seen) > 5000: seen.pop(0)
                        upd["seen_file_ids"] = seen
                        fresh["seen_file_ids"] = seen

                await _update_job(job_id, **upd)
                await asyncio.sleep(1)

            if new_msgs:
                await _update_job(job_id, last_seen_id=last_seen)

            # Update live phase destination progress bar every 60s
            now_live = time.time()
            if live_prog_id and (now_live - live_last_update) >= 60:
                live_last_update = now_live
                try:
                    _cur_fwd = (await _get_job(job_id)).get("forwarded", 0)
                    await client.edit_message_text(
                        to_chat, live_prog_id,
                        f"📡 <b>Live Job Active — monitoring for new messages…</b>\n\n"
                        f"✅ Processed Files: <code>{_cur_fwd}</code>\n"
                        f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                        f"<i>This message updates every 60s. Arya Bot</i>"
                    )
                except Exception:
                    pass

            sleep_secs = configs.get("duration", 5) or 5
            await asyncio.sleep(max(5, sleep_secs))

    except asyncio.CancelledError:
        logger.info(f"[Job {job_id}] Cancelled")
    except Exception as e:
        err_str = str(e)
        err_upper = err_str.upper()
        
        # Define all known transient/connection error signatures
        _TRANSIENT_KEYS = (
            "CONNECTION", "TIMEOUT", "NETWORK", "PING", "SOCKET", "RESET",
            "NOT BEEN STARTED", "NOT CONNECTED", "DISCONNECTED",
            "CONNECTION LOST", "FLOOD_WAIT",
            "LIVEJOB_RECONNECT_FAILED",   # raised by _lj_ensure_client_alive
        )
        
        if "AUTH_KEY_DUPLICATED" in err_str:
            # Session was used in 2 places — clear from cache so next restart is fresh
            logger.warning(f"[Job {job_id}] AUTH_KEY_DUPLICATED — clearing client cache and pausing job")
            if client:
                client_name = getattr(client, 'name', None)
                if client_name:
                    await release_client(client_name)
                    client = None   # prevent double-stop in finally
            # Don't mark job as error — just pause it so user can restart manually
            await _update_job(job_id, status="paused",
                              error="Session conflict (AUTH_KEY_DUPLICATED). Restart the job.")
        elif "USER_DEACTIVATED" in err_upper or "SESSION_REVOKED" in err_upper or "AUTH_KEY_INVALID" in err_upper:
            # Account was banned or session revoked — stop job permanently and alert owner
            logger.error(f"[Job {job_id}] ACCOUNT SAFETY: {err_str}")
            await _update_job(job_id, status="error", error=f"Account banned/session revoked: {err_str[:60]}")
            try:
                from config import Config
                for _owner in Config.BOT_OWNER_ID:
                    await BOT_INSTANCE.send_message(_owner,
                        f"🚨 <b>Account Safety Alert — Live Job {job_id}</b>\n\n"
                        f"⛔ <b>Account banned or session revoked!</b>\n"
                        f"<code>{err_str[:120]}</code>\n\n"
                        f"Please check your userbot account immediately.")
            except Exception: pass
        elif any(kw in err_upper for kw in _TRANSIENT_KEYS) or isinstance(e, FloodWait):
            # Transient network/connection/flood issue — DO NOT mark as error.
            # Auto-resume after waiting the required time.
            slp = 30
            if isinstance(e, FloodWait):
                slp = e.value + 5
            elif "FLOOD_WAIT" in err_upper:
                # Try to parse the actual wait time from the error string
                import re as _re
                _fw_match = _re.search(r'(\d+)', err_str)
                slp = int(_fw_match.group(1)) + 5 if _fw_match else 60
                # Account safety: warn for very long flood waits (>300s = account at risk)
                if slp > 300:
                    try:
                        from config import Config
                        for _owner in Config.BOT_OWNER_ID:
                            await BOT_INSTANCE.send_message(_owner,
                                f"⚠️ <b>Account Safety Warning — Live Job {job_id}</b>\n\n"
                                f"Long FLOOD_WAIT detected: <code>{slp}s</code>\n"
                                f"This may indicate the userbot account is at risk of restrictions.\n\n"
                                f"<i>Job will auto-resume after {slp}s.</i>")
                    except Exception: pass

            logger.warning(f"[Job {job_id}] Transient error: {err_str[:80]} — Auto-restarting in {slp}s")
            # Mark job as still running (not error) so UI stays green
            await _update_job(job_id, error=f"[Auto-reconnect] {err_str[:60]}")
            async def _auto_resume(_jid=job_id, _uid=user_id, _slp=slp, _client=client):
                await __import__('asyncio').sleep(_slp)
                # Clear reconnect cooldown so next run can actually ping
                sname_resume = getattr(_client, 'name', None) if _client else None
                if sname_resume:
                    _lj_last_reconnect.pop(sname_resume, None)
                # CRITICAL: verify the job still exists and is still "running" before re-launching
                # (prevents ghost tasks after job deletion or manual stop)
                _fresh = await _get_job(_jid)
                if not _fresh:
                    logger.info(f"[Job {_jid}] Auto-resume aborted: job was deleted")
                    _auto_resume_tasks.pop(_jid, None)
                    return
                if _fresh.get('status') not in ('running', None):
                    logger.info(f"[Job {_jid}] Auto-resume aborted: status={_fresh.get('status')}")
                    _auto_resume_tasks.pop(_jid, None)
                    return
                _auto_resume_tasks.pop(_jid, None)
                _start_job_task(_jid, _uid)
            _rt = __import__('asyncio').create_task(_auto_resume())
            _auto_resume_tasks[job_id] = _rt
        else:
            logger.error(f"[Job {job_id}] Fatal: {e}", exc_info=True)
            await _update_job(job_id, status="error", error=err_str[:80])
    finally:
        _job_tasks.pop(job_id, None)
        # Clean up in-memory filename dedup set to prevent memory leaks
        _live_seen_names.pop(job_id, None)
        if client:
            client_name = getattr(client, 'name', None)
            if client_name:
                # release_client stops AND removes from cache
                await release_client(client_name)
            else:
                try:
                    await client.stop()
                except Exception:
                    pass


def _start_job_task(job_id: str, user_id: int) -> asyncio.Task:
    # Cancel any existing running task first to prevent duplicate forwarding
    old_task = _job_tasks.get(job_id)
    if old_task and not old_task.done():
        old_task.cancel()
        logger.debug(f"[Job {job_id}] Cancelled existing task before starting new one")
    # Cancel any pending auto-resume too
    old_resume = _auto_resume_tasks.pop(job_id, None)
    if old_resume and not old_resume.done():
        old_resume.cancel()
    task = asyncio.create_task(_run_job(job_id, user_id))
    _job_tasks[job_id] = task
    return task


# ══════════════════════════════════════════════════════════════════════════════
# Resume all running jobs on bot restart
# ══════════════════════════════════════════════════════════════════════════════

async def resume_live_jobs(user_id: int = None, stagger_secs: float = 2.0):
    """
    Resume all 'running' Live Jobs after bot restart.
    Jobs are started with a `stagger_secs` delay between each to prevent
    simultaneous FloodWait and connection errors.
    """
    query: dict = {"status": "running"}
    if user_id:
        query["user_id"] = user_id
    jobs_to_resume = []
    async for job in db.db.jobs.find(query):
        jid = job["job_id"]
        uid = job["user_id"]
        if jid not in _job_tasks:
            jobs_to_resume.append((jid, uid))

    total = len(jobs_to_resume)
    if total:
        logger.info(f"[Jobs] Resuming {total} live job(s) with {stagger_secs}s stagger...")
        await asyncio.sleep(15)  # initial wait to unblock pyrogram core auth bounds
    for i, (jid, uid) in enumerate(jobs_to_resume):
        _start_job_task(jid, uid)
        logger.info(f"[Jobs] Resumed job {i+1}/{total}: {jid} (user {uid})")
        if i < total - 1:
            await asyncio.sleep(23.0)  # Heavy stagger to avoid Telegram flood on restart



# ══════════════════════════════════════════════════════════════════════════════
# UI helpers
# ══════════════════════════════════════════════════════════════════════════════

def _status_emoji(status: str) -> str:
    return {"running": "🟢", "stopped": "🔴", "error": "❌"}.get(status, " ")


def _batch_progress(job: dict) -> str:
    """Show batch progress line if batch mode is on and not yet finished."""
    if not job.get("batch_mode"):
        return ""
    if job.get("batch_done"):
        return "  ✅"
    cursor  = job.get("batch_cursor") or job.get("batch_start_id") or "?"
    end_id  = job.get("batch_end_id") or "?"
    return f"  {cursor}/{end_id}"


async def _render_jobs_list(bot, user_id: int, message_or_query):
    jobs = await _list_jobs(user_id)
    is_cb = hasattr(message_or_query, "message")

    if not jobs:
        text = (
            "<b>Live Jobs</b>\n\n"
            "<i>No jobs yet. A Live Job continuously watches a source chat\n"
            "and forwards new messages to your target — running in the background.\n\n"
            "✅ Batch mode: copy old messages first, then watch live\n"
            "✅ Dual destinations: send to 2 channels simultaneously\n"
            "✅ Per-job size limit\n\n"
            "👇 Create your first job below!</i>"
        )
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Nᴇᴡ Jᴏʙ", callback_data="job#new")],
            [InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="back")]
        ])
    else:
        lines = ["<b>Your Live Jobs</b>\n"]
        for j in jobs:
            st  = _status_emoji(j.get("status", "stopped"))
            fwd = j.get("forwarded", 0)
            err = f" <code>[{j.get('error','')}]</code>" if j.get("status") == "error" else ""
            bp  = _batch_progress(j)
            
            # Use last_seen_id compared to batch_start_id as fetched
            batch_start = j.get("batch_start_id", 0)
            last_seen = j.get("last_seen_id", 0)
            fetched = last_seen - batch_start if (batch_start and last_seen >= batch_start) else j.get("forwarded", 0)

            dest2 = f" + {j.get('to_title_2','?')}" if j.get("to_chat_2") else ""
            
            job_name = j.get("name", f"Live Job {j['job_id'][-6:]}")
            lines.append(
                f"{st} <b>{job_name}</b>\n"
                f"  └ <i>{j.get('from_title','?')} ➝ {j.get('to_title','?')}{dest2}</i>\n"
                f"  └ <code>[{j['job_id'][-6:]}]</code>  ✅{fwd}   {fetched}{bp}{err}\n"
            )
        import datetime
        now_str = datetime.datetime.now().strftime("%I:%M:%S %p")
        text = "\n".join(lines) + f"\n\n<i>Last refreshed: {now_str}</i>"

        btns_list = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            short = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"job#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"Sᴛᴀʀᴛ [{short}]", callback_data=f"job#start#{jid}"))
                row.append(InlineKeyboardButton(f"🔁 Rᴇsᴇᴛ [{short}]", callback_data=f"job#reset#{jid}"))
            row.append(InlineKeyboardButton(f"Iɴғᴏ [{short}]", callback_data=f"job#info#{jid}"))
            row.append(InlineKeyboardButton(f"⚙️ Sᴇᴛᴛɪɴɢs [{short}]", callback_data=f"job#settings#{jid}"))
            row.append(InlineKeyboardButton(f"Dᴇʟ [{short}]",  callback_data=f"job#del#{jid}"))
            btns_list.append(row)

        btns_list.append([InlineKeyboardButton("Cʀᴇᴀᴛᴇ Nᴇᴡ Jᴏʙ", callback_data="job#new")])
        btns_list.append([InlineKeyboardButton("Rᴇғʀᴇsʜ",        callback_data="job#list")])
        btns_list.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="back")])
        btns = InlineKeyboardMarkup(btns_list)

    try:
        if is_cb:
            await message_or_query.message.edit_text(text, reply_markup=btns)
        else:
            await message_or_query.reply_text(text, reply_markup=btns)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# /jobs command
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("jobs"))
async def jobs_cmd(bot, message):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS, _DISABLED_MSG
    uid = message.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("live_job"):
        return await message.reply_text(_DISABLED_MSG.format(feature=FEATURE_LABELS["live_job"]))
    await _render_jobs_list(bot, message.from_user.id, message)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^job#list$'))
async def job_list_cb(bot, query):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS
    uid = query.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("live_job"):
        return await query.answer(f"🔒 {FEATURE_LABELS['live_job']} is temporarily disabled by admin.", show_alert=True)
    await query.answer()
    await _render_jobs_list(bot, query.from_user.id, query)

@Client.on_callback_query(filters.regex(r'^job#rename#'))
async def job_rename_cb(bot, query):
    user_id = query.from_user.id
    job_id = query.data.split("#", 2)[2]
    await query.message.delete()
    
    r = await _ask(bot, user_id,
        "<b>✏️ Edit Live Job Name</b>\n\nSend a new name for this job:",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]], resize_keyboard=True, one_time_keyboard=True))
    if "/cancel" not in r.text.lower():
        await db.db[COLL].update_one({"job_id": job_id}, {"$set": {"name": r.text.strip()[:100]}})
        await bot.send_message(user_id, f"✅ Live Job renamed to <b>{r.text.strip()[:100]}</b>", reply_markup=ReplyKeyboardRemove())
    await _render_jobs_list(bot, user_id, r)


@Client.on_callback_query(filters.regex(r'^job#info#'))
async def job_info_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    job = await _get_job(job_id)
    if not job:
        return await query.answer("Job not found!", show_alert=True)

    import datetime
    created   = datetime.datetime.fromtimestamp(job.get("created", 0)).strftime("%d %b %Y %H:%M")
    st        = _status_emoji(job.get("status", "stopped"))
    thread_id = job.get("to_thread_id")
    topic_lbl = f"\n<b>Topic Thread:</b> <code>{thread_id}</code>" if thread_id else ""

    dest2_lbl = ""
    if job.get("to_chat_2"):
        t2 = job.get("to_thread_id_2")
        tp2 = f" [Thread {t2}]" if t2 else ""
        dest2_lbl = f"\n<b>Dest 2:</b> {job.get('to_title_2','?')}{tp2}"

    # Batch info
    batch_lbl = ""
    if job.get("batch_mode"):
        if job.get("batch_done"):
            batch_lbl = "\n<b>Batch:</b> ✅ Complete"
        else:
            cur = job.get("batch_cursor") or job.get("batch_start_id") or "?"
            end = job.get("batch_end_id") or "calculating..."
            batch_lbl = f"\n<b>Batch:</b> {cur} / {end}"

    # Size limit info
    size_lbl = ""
    if job.get("max_size_mb"):
        size_lbl += f"\n<b>Max file size:</b> {job['max_size_mb']} MB"
    if job.get("max_duration_secs"):
        mins = job['max_duration_secs'] // 60
        secs = job['max_duration_secs'] % 60
        size_lbl += f"\n<b>Max duration:</b> {mins}m {secs}s"
    if job.get("min_duration_secs"):
        mins = job['min_duration_secs'] // 60
        secs = job['min_duration_secs'] % 60
        size_lbl += f"\n<b>Min duration:</b> {mins}m {secs}s (shorter files skipped)"
    if job.get("notify_large_file_mb"):
        size_lbl += f"\n<b>Large file alert:</b> ≥ {job['notify_large_file_mb']} MB → DM to owner"

    text = (
        f"<b>Live Job Info</b>\n\n"
        f"<b>ID:</b> <code>{job_id[-6:]}</code>\n"
        f"<b>Name:</b> {job.get('name', 'Default')}\n"
        f"<b>Status:</b> {st} {job.get('status','?')}\n"
        f"<b>Source:</b> {job.get('from_title','?')}\n"
        f"<b>Dest 1:</b> {job.get('to_title','?')}{topic_lbl}"
        f"{dest2_lbl}{batch_lbl}{size_lbl}\n"
        f"<b>Forwarded:</b> {job.get('forwarded', 0)}\n"
        f"<b>Last Msg ID:</b> {job.get('last_seen_id', 0)}\n"
        f"<b>Created:</b> {created}\n"
    )
    if job.get("error"):
        text += f"\n<b>Error:</b>\n<blockquote><code>{job['error']}</code></blockquote>"

    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="job#list")
    ]]))


@Client.on_callback_query(filters.regex(r'^job#settings#'))
async def job_settings_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    job = await _get_job(job_id)
    if not job:
        return await query.answer("Job not found!", show_alert=True)

    text = (
        f"<b>⚙️ Job Settings</b>\n\n"
        f"<b>ID:</b> <code>{job_id[-6:]}</code>\n"
        f"<b>Name:</b> {job.get('name', 'Default')}\n"
        f"<b>Source:</b> {job.get('from_title','?')}\n"
        f"<b>Dest 1:</b> {job.get('to_title','?')}\n"
        f"<i>Configure limits, source changes, name manipulation, and duplicate prevention.</i>"
    )

    skip_lbl = "✅ ON" if job.get("skip_duplicates") else "❌ OFF"
    
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Eᴅɪᴛ Nᴀᴍᴇ", callback_data=f"job#rename#{job_id}")],
        [InlineKeyboardButton("🔄 Sᴏᴜʀᴄᴇ Cʜᴀɴɢᴇ Wɪᴢᴀʀᴅ", callback_data=f"job#src#{job_id}")],
        [InlineKeyboardButton("📏 Sɪᴢᴇ / Dᴜʀᴀᴛɪᴏɴ Lɪᴍɪᴛs", callback_data=f"job#limits#{job_id}")],
        [InlineKeyboardButton(f"📄 Sᴋɪᴘ Dᴜᴘʟɪᴄᴀᴛᴇs: {skip_lbl}", callback_data=f"job#togglededupl#{job_id}")],
        [InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="job#list")]
    ])
    await query.message.edit_text(text, reply_markup=btns)


@Client.on_callback_query(filters.regex(r'^job#togglededupl#'))
async def job_toggle_dedupl_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    job = await _get_job(job_id)
    if not job: return
    new_val = not job.get("skip_duplicates", False)
    await _update_job(job_id, skip_duplicates=new_val)
    # Refresh settings directly
    query.data = f"job#settings#{job_id}"
    await job_settings_cb(bot, query)


@Client.on_callback_query(filters.regex(r'^job#stop#'))
async def job_stop_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    task = _job_tasks.pop(job_id, None)
    if task and not task.done():
        task.cancel()
    rt = _auto_resume_tasks.pop(job_id, None)
    if rt and not rt.done():
        rt.cancel()
    await _update_job(job_id, status="stopped")
    await query.answer("⏹ Job stopped.", show_alert=False)
    await _render_jobs_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^job#reset#'))
async def job_reset_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    task = _job_tasks.pop(job_id, None)
    if task and not task.done():
        task.cancel()
    rt = _auto_resume_tasks.pop(job_id, None)
    if rt and not rt.done():
        rt.cancel()
    # Reset: clear batch_done, reset batch_cursor, last_seen_id, forwarded
    start_id = int(job.get("batch_start_id") or 1)
    await _update_job(job_id,
        status="stopped",
        batch_done=False,
        batch_cursor=start_id,
        last_seen_id=0,
        forwarded=0,
        error=""
    )
    await query.answer("🔁 Job reset to start!", show_alert=True)
    await _render_jobs_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^job#start#'))
async def job_start_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    if job_id in _job_tasks and not _job_tasks[job_id].done():
        return await query.answer("Already running!", show_alert=True)
    await _update_job(job_id, status="running")
    _start_job_task(job_id, user_id)
    await query.answer("▶️ Job started.", show_alert=False)
    await _render_jobs_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^job#del#'))
async def job_del_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    task = _job_tasks.pop(job_id, None)
    if task and not task.done():
        task.cancel()
    rt = _auto_resume_tasks.pop(job_id, None)
    if rt and not rt.done():
        rt.cancel()
    await _delete_job_db(job_id)
    await query.answer("»  Job deleted.", show_alert=False)
    await _render_jobs_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^job#limits#'))
async def job_limits_cb(bot, query):
    """Edit size/duration/notification limits for an existing live job."""
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    await query.message.delete()

    cur_max_sz  = job.get("max_size_mb", 0) or 0
    cur_max_dur = (job.get("max_duration_secs", 0) or 0) // 60
    cur_min_dur = (job.get("min_duration_secs", 0) or 0) // 60
    cur_notify  = job.get("notify_large_file_mb", 0) or 0

    r = await _ask(bot, user_id,
        f"<b>⚙️ Edit Limits — Job {job_id[-6:]}</b>\n\n"
        f"<b>Current settings:</b>\n"
        f"• Max size: <code>{cur_max_sz} MB</code>\n"
        f"• Max duration: <code>{cur_max_dur} min</code>\n"
        f"• Min duration: <code>{cur_min_dur} min</code> (skip shorter files)\n"
        f"• Large file alert: <code>{cur_notify} MB</code> (0 = off)\n\n"
        "<b>Send new limits in format:</b>\n"
        "<code>max_mb : max_min : min_min : alert_mb</code>\n\n"
        "Examples:\n"
        "• <code>0:0:0:0</code> — remove all limits\n"
        "• <code>200:0:1:0</code> — max 200MB, skip files under 1 min, no alert\n"
        "• <code>0:60:1:300</code> — max 60min, skip files under 1 min, alert at 300MB\n"
        "• <code>500:0:2:400</code> — max 500MB, min 2min, alert owner at 400MB+\n\n"
        "<i>Send 0 for any field to remove that limit.</i>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton(f"{cur_max_sz}:{cur_max_dur}:{cur_min_dur}:{cur_notify}")],
             [KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]],
            resize_keyboard=True, one_time_keyboard=True)
    )

    txt = r.text.strip() if r and r.text else ""
    if "⛔" in txt or "/cancel" in txt.lower():
        await bot.send_message(user_id, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        return await _render_jobs_list(bot, user_id, r)

    parts = [p.strip() for p in txt.split(":")]
    def _int(v, default=0):
        try: return max(0, int(v))
        except: return default

    new_max_sz  = _int(parts[0] if len(parts) > 0 else 0)
    new_max_dur = _int(parts[1] if len(parts) > 1 else 0) * 60
    new_min_dur = _int(parts[2] if len(parts) > 2 else 0) * 60
    new_notify  = _int(parts[3] if len(parts) > 3 else 0)

    await _update_job(job_id,
        max_size_mb=new_max_sz,
        max_duration_secs=new_max_dur,
        min_duration_secs=new_min_dur,
        notify_large_file_mb=new_notify
    )

    summary = (
        f"✅ <b>Limits updated for Job {job_id[-6:]}</b>\n\n"
        f"• Max size: <code>{'No limit' if not new_max_sz else str(new_max_sz)+' MB'}</code>\n"
        f"• Max duration: <code>{'No limit' if not new_max_dur else str(new_max_dur//60)+' min'}</code>\n"
        f"• Min duration: <code>{'Off' if not new_min_dur else str(new_min_dur//60)+' min (shorter files skipped)'}</code>\n"
        f"• Large file alert: <code>{'Off' if not new_notify else '≥ '+str(new_notify)+' MB → DM owner'}</code>\n\n"
        f"<i>Changes take effect on the next poll cycle.</i>"
    )
    await bot.send_message(user_id, summary, reply_markup=ReplyKeyboardRemove())
    await _render_jobs_list(bot, user_id, r)


# ══════════════════════════════════════════════════════════════════════════════
# Create Job — Interactive flow
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^job#new$'))
async def job_new_cb(bot, query):
    user_id = query.from_user.id
    await query.message.delete()
    await _create_job_flow(bot, user_id)


@Client.on_message(filters.private & filters.command("newjob"))
async def newjob_cmd(bot, message):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS, _DISABLED_MSG
    uid = message.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("live_job"):
        return await message.reply_text(_DISABLED_MSG.format(feature=FEATURE_LABELS["live_job"]))
    await _create_job_flow(bot, message.from_user.id)


async def _ask_dest(bot, user_id: int, channels: list, step_label: str, optional: bool = False, undo_btn: bool = False) -> tuple:
    """Helper: ask user to pick a channel from their saved list. Returns (chat_id, title, cancelled).
    cancelled='undo' means undo pressed."""
    from plugins.utils import ask_channel_picker
    
    extra = []
    if optional:
        extra.append("⏭ Sᴋɪᴘ (no second destination)")
    if undo_btn:
        extra.append("↩️ Uɴᴅᴏ")
        
    picked = await ask_channel_picker(bot, user_id, step_label, extra_options=extra)
    
    if not picked:
        return None, None, True
        
    if isinstance(picked, str):
        if picked == "↩️ Uɴᴅᴏ":
            return None, None, "undo"
        if picked == "⏭ Sᴋɪᴘ (no second destination)":
            return None, None, False
            
    return picked['chat_id'], picked['title'], False


async def _create_job_flow(bot, user_id: int):
    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")

    def _cancel(txt): return False if not txt else txt.strip().startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt
    def _undo(txt):   return False if not txt else txt.strip().startswith("/undo") or "↩️" in txt or "Uɴᴅᴏ" in txt

    # ── Step 1: Name ──────────────────────────────────────────────
    name_r = await _ask(bot, user_id,
        "<b>»  Create Live Job — Step 1/7</b>\n\n"
        "Send a <b>name</b> for this job, or press <b>Default</b>.",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Default")], [CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))
    if _cancel(name_r.text):
        return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

    job_name = name_r.text.strip()[:100]
    if job_name.lower() == "default":
        job_name = None

    # ── Step 2: Account ───────────────────────────────────────────
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id,
            "<b>❌ No accounts. Add one in /settings → Accounts first.</b>")

    def _acc_label(a):
        kind = "Bot" if a.get("is_bot", True) else "Userbot"
        name = a.get("username") or a.get("name", "Unknown")
        return f"{kind}: {name} [{a['id']}]"

    acc_btns = [[KeyboardButton(_acc_label(a))] for a in accounts]
    acc_btns.append([CANCEL_BTN])

    acc_r = await _ask(bot, user_id,
        "<b>»  Create Live Job — Step 2/7</b>\n\n"
        "Choose which <b>account</b> to use:\n\n"
        "<blockquote expandable>"
        "🤖 <b>Bot</b> — works for public channels and groups where the bot is admin.\n"
        "👤 <b>Userbot</b> — required for:\n"
        "  • Private/restricted channels\n"
        "  • Forwarding with copy (no forward tag)\n"
        "  • Saved Messages as source\n"
        "  • Groups where bots are blocked"
        "</blockquote>",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))

    if _cancel(acc_r.text):
        return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in acc_r.text and "]" in acc_r.text:
        try: acc_id = int(acc_r.text.split('[')[-1].split(']')[0])
        except Exception: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
    is_bot  = sel_acc.get("is_bot", True)

    # ── Step 3: Source ────────────────────────────────────────────
    while True:
        src_r = await _ask(bot, user_id,
            "<b>Step 3/7 — Source Chat</b>\n\n"
            "Send the <b>source channel, group, or chat</b> to watch for new messages.\n\n"
            "<blockquote expandable>"
            "Accepted formats:\n"
            "• <code>@username</code> — public channel/group username\n"
            "• <code>https://t.me/username</code> — public link\n"
            "• <code>https://t.me/c/1234567890/1</code> — private channel link\n"
            "• <code>-1001234567890</code> — numeric chat ID (negative for channels/groups)\n"
            "• <code>me</code> — your own Saved Messages (Userbot only)\n\n"
            "📌 For private channels: use a Userbot that is a member.\n"
            "📌 For public channels: Bot works if it can read messages.\n"
            "📌 Group Topics: supported — you will be asked for a thread ID next.\n"
            "📌 Bot DM: use the bot's username or numeric ID."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup([[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))

        if _cancel(src_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(src_r.text):
            # redo step 2
            acc_r2 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 2/7: Account</b>\n\nChoose the account again:",
                reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))
            if _cancel(acc_r2.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if "[" in acc_r2.text and "]" in acc_r2.text:
                try: acc_id = int(acc_r2.text.split('[')[-1].split(']')[0])
                except Exception: pass
            sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
            is_bot  = sel_acc.get("is_bot", True)
            continue
        break

    from_chat_raw = src_r.text.strip()
    if from_chat_raw.lower() in ("me", "saved"):
        if is_bot:
            return await bot.send_message(user_id, "<b>❌ Saved Messages require a Userbot account.</b>")
        from_chat  = "me"
        from_title = "Saved Messages"
    else:
        from_chat = from_chat_raw
        if from_chat.lstrip('-').isdigit():
            from_chat = int(from_chat)
            if from_chat > 0 and len(str(from_chat)) >= 13 and str(from_chat).startswith("100"):
                from_chat = -from_chat
        elif "t.me/c/" in from_chat:
            parts = from_chat.split("t.me/c/")[1].split("/")
            if parts[0].isdigit():
                if parts[0].startswith("100") and len(parts[0]) >= 13:
                    from_chat = int(f"-{parts[0]}")
                else:
                    from_chat = int(f"-100{parts[0]}")
        elif "t.me/" in from_chat:
            username = from_chat.split("t.me/")[1].split("/")[0].split("?")[0]
            if not username.startswith("+"):
                from_chat = username

        try:
            chat_obj   = await bot.get_chat(from_chat)
            from_title = (getattr(chat_obj, "title", None) or
                          getattr(chat_obj, "first_name", None) or str(from_chat))
        except Exception:
            from_title = str(from_chat)

    from_thread = await _ask_topic(bot, user_id, "Source")

    # ── Step 4: First Destination ─────────────────────────────────
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(user_id,
            "<b>❌ No target channels saved. Add via /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    while True:
        to_chat, to_title, cancelled = await _ask_dest(bot, user_id, channels,
            "<b>Step 4/7 — Primary Destination</b>\n\nWhere should new messages be forwarded?\n\n"
            "<blockquote expandable>"
            "Choose from your saved channels/groups.\n"
            "To add a channel, go to /settings → Channels.\n"
            "The account must be an admin with send permissions."
            "</blockquote>",
            undo_btn=True)
        if cancelled == "undo":
            # redo source step
            src_r2 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 3/7: Source Chat</b>\n\nSend source chat again:",
                reply_markup=ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))
            if _cancel(src_r2.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            from_chat_raw = src_r2.text.strip()
            from_chat = from_chat_raw
            if from_chat.lstrip('-').isdigit():
                from_chat = int(from_chat)
            try:
                chat_obj2   = await bot.get_chat(from_chat)
                from_title  = getattr(chat_obj2, "title", None) or str(from_chat)
            except Exception:
                from_title = str(from_chat)
            continue
        elif cancelled:
            return
        break

    to_thread = await _ask_topic(bot, user_id, "Primary Destination")

    # ── Step 5: Second Destination (Optional) ─────────────────────
    to_chat_2, to_title_2, cancelled2 = await _ask_dest(bot, user_id, channels,
        "<b>Step 5/7 — Second Destination (Optional)</b>\n\n"
        "Messages will be sent to <b>both</b> destinations when a new message arrives.\n"
        "Press Skip if you only need one destination.",
        optional=True)
    if cancelled2 is True:
        return

    to_thread_2 = None
    if to_chat_2:
        to_thread_2 = await _ask_topic(bot, user_id, "Second Destination")

    # ── Step 6: Batch Mode ────────────────────────────────────────
    while True:
        batch_r = await _ask(bot, user_id,
            "<b>Step 6/7 — Batch Mode (Copy Old Messages First)</b>\n\n"
            "Do you want to copy existing (old) messages before going live?\n\n"
            "<blockquote expandable>"
            "• <b>ON</b> — first copies old messages, then watches for new ones.\n"
            "• <b>OFF</b> — only watches for NEW messages from now on.\n\n"
            "If ON, you will choose a starting message ID next.\n"
            "Batch runs sequentially before live mode starts."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("✅ ON (Copy old messages first)")],
                 [KeyboardButton("❌ OFF (Live only)")],
                 [UNDO_BTN, CANCEL_BTN]],
                resize_keyboard=True, one_time_keyboard=True
            ))

        if _cancel(batch_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(batch_r.text):
            # redo destination
            to_chat, to_title, cancelled = await _ask_dest(bot, user_id, channels,
                "<b>↩️ Redo — Step 4/7: Primary Destination</b>\n\nChoose destination again:")
            if cancelled:
                return
            to_thread = await _ask_topic(bot, user_id, "Primary Destination")
            continue
        break

    batch_mode     = "on" in batch_r.text.lower()
    batch_start_id = 1
    batch_end_id   = 0

    if batch_mode:
        while True:
            range_r = await _ask(bot, user_id,
                "<b>Batch Range</b>\n\n"
                "Choose where to start the batch:\n\n"
                "<blockquote expandable>"
                "• <b>ALL</b> — start from the very first message\n"
                "• <code>500</code> — start from message ID 500\n"
                "• <code>500:2000</code> — copy only IDs 500 through 2000\n\n"
                "After the batch finishes, the job automatically switches to live mode."
                "</blockquote>",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("ALL")], [UNDO_BTN, CANCEL_BTN]],
                    resize_keyboard=True, one_time_keyboard=True))

            if _cancel(range_r.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if _undo(range_r.text):
                # redo batch on/off
                batch_r2 = await _ask(bot, user_id,
                    "<b>↩️ Redo — Step 6/7: Batch Mode</b>\n\nON or OFF?",
                    reply_markup=ReplyKeyboardMarkup(
                        [[KeyboardButton("✅ ON (Copy old messages first)")],
                         [KeyboardButton("❌ OFF (Live only)")], [CANCEL_BTN]],
                        resize_keyboard=True, one_time_keyboard=True))
                if _cancel(batch_r2.text):
                    return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                batch_mode = "on" in batch_r2.text.lower()
                if not batch_mode:
                    break
                continue
            break

        rtext = range_r.text.strip().lower()
        if rtext != "all":
            if ":" in rtext:
                parts = rtext.split(":", 1)
                try: batch_start_id = int(parts[0].strip())
                except Exception: pass
                try: batch_end_id   = int(parts[1].strip())
                except Exception: pass
            else:
                try: batch_start_id = int(rtext)
                except Exception: pass

    # ── Step 7: Size / Duration Limit ─────────────────────────────
    while True:
        limit_r = await _ask(bot, user_id,
            "<b>Step 7/7 — Size / Duration Limits</b>\n\n"
            "Set limits for this job. Files outside the limits will be <b>silently skipped</b>.\n\n"
            "<blockquote expandable>"
            "Format: <code>max_mb : max_min : min_min</code>\n\n"
            "• <code>0</code> — no limits (forward everything)\n"
            "• <code>200</code> — skip files larger than 200 MB\n"
            "• <code>200:60</code> — max 200MB, max 60 minutes\n"
            "• <code>200:60:1</code> — max 200MB, max 60min, <b>skip files under 1 minute</b>\n"
            "• <code>0:0:2</code> — no size/max-dur limit, but skip files shorter than 2 min\n\n"
            "<b>Tip:</b> Use min_min to skip short clips (e.g. 10-second files)"
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("0 (No limit)")], [UNDO_BTN, CANCEL_BTN]],
                resize_keyboard=True, one_time_keyboard=True
            ))

        if _cancel(limit_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(limit_r.text):
            # redo batch mode
            batch_r3 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 6/7: Batch Mode</b>\n\nON or OFF?",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("✅ ON")], [KeyboardButton("❌ OFF")], [CANCEL_BTN]],
                    resize_keyboard=True, one_time_keyboard=True))
            if _cancel(batch_r3.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            batch_mode = "on" in batch_r3.text.lower()
            continue
        break

    ltext = (limit_r.text or "").strip()
    max_size_mb, max_duration_s, min_duration_s = 0, 0, 0
    if ltext not in ("0", "0 (No limit)"):
        parts_l = [p.strip() for p in ltext.split(":")]
        def _lim_int(v):
            try: return max(0, int(v))
            except: return 0
        max_size_mb    = _lim_int(parts_l[0] if len(parts_l) > 0 else "0")
        max_duration_s = _lim_int(parts_l[1] if len(parts_l) > 1 else "0") * 60
        min_duration_s = _lim_int(parts_l[2] if len(parts_l) > 2 else "0") * 60

    # ── Step 8: Skip Duplicates ─────────────────────────────
    while True:
        dupe_r = await _ask(bot, user_id,
            "<b>Step 8/8 — Skip Duplicates?</b>\n\n"
            "If the source uploads a file that already exists in your target (based on exact file content / unique ID), "
            "should the bot silently skip it?\n\n"
            "<blockquote expandable>"
            "This checks Telegram's <code>file_unique_id</code>. Exact same files are skipped, but edited/different files (even if same name) are allowed."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("✅ YES (Skip duplicates)")],
                 [KeyboardButton("❌ NO (Allow duplicates)")],
                 [UNDO_BTN, CANCEL_BTN]],
                resize_keyboard=True, one_time_keyboard=True
            ))

        if _cancel(dupe_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(dupe_r.text):
            # redo limits
            limit_r2 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 7/8: Size / Duration Limits</b>\n\n"
                "Format: max_mb : max_min : min_min (or 0 for none):",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("0 (No limit)")], [CANCEL_BTN]],
                    resize_keyboard=True, one_time_keyboard=True))
            if _cancel(limit_r2.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            ltext = limit_r2.text.strip()
            max_size_mb, max_duration_s, min_duration_s = 0, 0, 0
            if ltext not in ("0", "0 (No limit)"):
                parts_l = [p.strip() for p in ltext.split(":")]
                def _lim_int(v):
                    try: return max(0, int(v))
                    except: return 0
                max_size_mb   = _lim_int(parts_l[0] if len(parts_l) > 0 else "0")
                max_duration_s = _lim_int(parts_l[1] if len(parts_l) > 1 else "0") * 60
                min_duration_s = _lim_int(parts_l[2] if len(parts_l) > 2 else "0") * 60
            continue
        break

    skip_dupelicates = "yes" in (dupe_r.text or "").lower() or "✅" in (dupe_r.text or "")

    # ── Save & Start ──────────────────────────────────────────────
    job_id = f"{user_id}-{int(time.time())}"
    job = {
        "job_id":             job_id,
        "user_id":            user_id,
        "name":               job_name if job_name else f"Live Job {job_id[-6:]}",
        "account_id":         sel_acc["id"],
        "from_chat":          from_chat,
        "from_title":         from_title,
        "from_thread":        from_thread,
        "to_chat":            to_chat,
        "to_title":           to_title,
        "to_thread_id":       to_thread,
        "to_chat_2":          to_chat_2,
        "to_title_2":         to_title_2,
        "to_thread_id_2":     to_thread_2,
        "batch_mode":         batch_mode,
        "batch_start_id":     batch_start_id,
        "batch_end_id":       batch_end_id,
        "batch_cursor":       batch_start_id,
        "batch_done":         False,
        "max_size_mb":        max_size_mb,
        "max_duration_secs":  max_duration_s,
        "min_duration_secs":  min_duration_s,
        "notify_large_file_mb": 0,
        "status":             "running",
        "created":            int(time.time()),
        "forwarded":          0,
        "last_seen_id":       0,
        "skip_duplicates":    skip_dupelicates,
        "seen_file_ids":      [],
    }
    await _save_job(job)
    _start_job_task(job_id, user_id)

    thread_lbl = f" → Topic <code>{to_thread}</code>" if to_thread else ""
    dest2_lbl  = f"\n<b>Dest 2:</b> {to_title_2}" + (f" → Topic <code>{to_thread_2}</code>" if to_thread_2 else "") if to_chat_2 else ""
    batch_lbl  = (f"\n<b>Batch:</b> ✅ ON — copying from ID {batch_start_id}"
                  + (f" to {batch_end_id}" if batch_end_id else " to latest")
                  + " first") if batch_mode else "\n<b>Batch:</b> ❌ OFF (live only)"
    size_lbl   = ""
    if max_size_mb:
        size_lbl += f"\n<b>Max size:</b> {max_size_mb} MB"
    if max_duration_s:
        size_lbl += f"\n<b>Max duration:</b> {max_duration_s // 60} min"
    if not size_lbl:
        size_lbl = "\n<b>Size limit:</b> None"
    dupe_lbl = "\n<b>Skip Dupes:</b> ✅ ON" if skip_dupelicates else "\n<b>Skip Dupes:</b> ❌ OFF"

    kind = "Bot" if is_bot else "Userbot"
    await bot.send_message(
        user_id,
        f"<b>✅ Live Job Created & Started!</b>\n\n"
        f"🟢 <b>{from_title}</b> → <b>{to_title}</b>{thread_lbl}"
        f"{dest2_lbl}\n"
        f"<b>Account:</b> {kind}: {sel_acc.get('name','?')}\n"
        f"{batch_lbl}{size_lbl}{dupe_lbl}\n"
        f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
        f"<i>Running in the background. Use /jobs to manage.</i>",
        reply_markup=ReplyKeyboardRemove()
    )

@Client.on_callback_query(filters.regex(r'^job#src#'))
async def job_src_cb(bot, query):
    user_id = query.from_user.id
    job_id = query.data.split('#')[2]
    await query.message.delete()
    asyncio.create_task(_do_change_source(bot, user_id, job_id))

async def _do_change_source(bot, uid: int, jid: str):
    from pyrogram.types import ReplyKeyboardRemove
    from plugins.utils import ask_channel_picker, check_chat_protection
    
    job = await _get_job(jid)
    if not job:
        await bot.send_message(uid, "<b>❌ Job not found.</b>")
        return

    was_running = job.get("status") == "running"
    if was_running:
        await _update_job(jid, status="paused")
        if jid in _job_tasks:
            _job_tasks[jid].cancel()

    await bot.send_message(
        uid,
        "<b>✏️ Change Live Job Source</b>\n\n"
        "Select a new source from your saved channels, or tap "
        "<b>✍️ Manual Input</b> to paste a chat ID / topic link directly.\n\n"
        "<i>The job will pause during selection and auto-resume once updated.</i>",
        reply_markup=__import__('pyrogram.types', fromlist=['ReplyKeyboardMarkup']).__class__
    )

    picked = await ask_channel_picker(
        bot, uid,
        prompt="Select the new source channel / group:",
        extra_options=["✍️ Manual Input"],
        timeout=300
    )

    new_source = None
    new_source_title = None

    if picked is None:
        pass
    elif picked == "✍️ Manual Input":
        try:
            ask_msg = await _ask(bot, uid,
                "✍️ <b>Enter the source:</b>\n\n"
                "• Numeric chat ID: <code>-100...</code>\n"
                "• @username: <code>@mychannel</code>\n"
                "• Topic URL: <code>https://t.me/c/...</code>\n"
                "<i>Send ⛔ to cancel.</i>",
                timeout=300,
                reply_markup=ReplyKeyboardRemove())
            txt = (ask_msg.text or "").strip()
            if not txt or "⛔" in txt or txt.lower() == "cancel":
                await bot.send_message(uid, "<i>Cancelled.</i>")
            else:
                import re as _re
                m = _re.match(r'https?://t\.me/c/(\d+)/(\d+)', txt)
                if m:
                    new_source = f"-100{m.group(1)}"
                    new_source_title = f"Topic /c/{m.group(1)}/{m.group(2)}"
                elif _re.match(r'https?://t\.me/([^/]+)/(\d+)', txt):
                    mm = _re.match(r'https?://t\.me/([^/]+)/(\d+)', txt)
                    new_source = f"@{mm.group(1)}"
                    new_source_title = f"@{mm.group(1)}"
                elif txt.lstrip('-').isdigit() or txt.startswith('@'):
                    new_source = txt
                    new_source_title = txt
                else:
                    await bot.send_message(uid, "<b>❌ Unrecognised format. Source not changed.</b>")
        except asyncio.TimeoutError:
            await bot.send_message(uid, "<i>⏱ Timed out. Source not changed.</i>")
    elif isinstance(picked, dict):
        new_source = str(picked.get("chat_id", ""))
        new_source_title = picked.get("title", new_source)

    if new_source:
        prot = await check_chat_protection(uid, new_source)
        if prot:
            await bot.send_message(uid, prot)
            if was_running:
                await _update_job(jid, status="running")
                _start_job_task(jid, uid)
            return

        await _update_job(jid, from_chat=new_source, from_title=new_source_title, last_seen_id=0, batch_cursor=0)
        await bot.send_message(
            uid,
            f"<b>✅ Source updated!</b>\n\n"
            f"<b>New Source:</b> <code>{new_source_title}</code>\n"
            f"<b>Scan Position:</b> Reset to 0\n"
            "<i>The job will continue monitoring the new source from the start.</i>"
        )
    else:
        if picked is not None:
            await bot.send_message(uid, "<i>Source unchanged.</i>")

    if was_running:
        await _update_job(jid, status="running")
        _start_job_task(jid, uid)
        await bot.send_message(uid, "▶️ <b>Job resumed and now monitoring the new source.</b>")



