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

#  Future-based ask() — immune to pyrofork stale-listener bug 
_lj_waiting: dict[int, asyncio.Future] = {}


# ─── Client health-check / reconnect ────────────────────────────────────
async def _lj_ensure_client_alive(client):
    """
    Verify the Pyrogram client is connected. If dead, attempt cold restart up to 3 times.
    Live jobs run 24/7 — the TCP connection silently dies after idle periods.
    """
    for attempt in range(3):
        try:
            await asyncio.wait_for(client.get_me(), timeout=15)
            return client   # alive ✔️
        except Exception as e:
            err_str = str(e).lower()
            is_conn = ("not been started" in err_str or "not connected" in err_str
                       or "disconnected" in err_str or isinstance(e, asyncio.TimeoutError))
            if is_conn:
                logger.warning(f"[LiveJob] Client dead (attempt {attempt+1}): {e} — reconnecting…")
                try:
                    cname = getattr(client, 'name', None)
                    if cname:
                        await release_client(cname)
                except Exception: pass
                try: await client.stop()
                except Exception: pass
                try:
                    client = await start_clone_bot(client)
                    await asyncio.sleep(1)
                    continue
                except Exception as re_err:
                    logger.error(f"[LiveJob] Restart attempt {attempt+1} failed: {re_err}")
                    await asyncio.sleep(3)
            else:
                raise   # not a connection error
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
    Telegram stores the topic ID in `message_thread_id`.
    The very first message that creates the topic has msg.id == thread_id
    and may not carry `message_thread_id`, so we check that too.
    """
    tid = getattr(msg, "message_thread_id", None)
    if tid is not None and int(tid) == from_thread_id:
        return True
    # The topic-starter message itself (msg.id == topic_id)
    if int(msg.id) == from_thread_id:
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
                            if fp: break
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

        #  First-run init: snapshot latest ID 
        for _att in range(3):
            try:
                me = await client.get_me()
                break
            except Exception as e:
                if _att == 2: raise e
                await __import__('asyncio').sleep(5)
                
        if from_chat == me.id or from_chat == me.username:
            from_chat = user_id
            await _update_job(job_id, from_chat=from_chat)
            logger.info(f"[Job {job_id}] Swapped Bot's own ID with User ID ({user_id}) for Bot DM fetching")

        if last_seen == 0:
            last_seen = await _get_latest_id(client, from_chat, is_bot)
            await _update_job(job_id, last_seen_id=last_seen)
            logger.info(f"[Job {job_id}] Initialised at msg ID {last_seen}")

        # Warm up peer cache and identify exact DM source type
        is_dm_source = False
        from pyrogram.enums import ChatType
        
        if str(from_chat).lower() in ("me", "saved"):
            is_dm_source = True
        else:
            try:
                peer_chat = await client.get_chat(from_chat)
                if peer_chat.type in (ChatType.PRIVATE, ChatType.BOT):
                    is_dm_source = True
                from_chat = peer_chat.id
            except Exception as warn_e:
                logger.warning(f"[Job {job_id}] Pre-fetch peer resolve warning: {warn_e}")
                if isinstance(from_chat, int) and from_chat >= 0:
                    is_dm_source = True
                    
        try:
            for _chat in [to_chat] + ([to_chat_2] if to_chat_2 else []):
                try:
                    await client.get_chat(_chat)
                except FloodWait as fw:
                    logger.warning(f"[Job {job_id}] FloodWait {fw.value}s on get_chat({_chat})")
                    await asyncio.sleep(fw.value + 2)
                except Exception:
                    pass
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
                    try:
                        success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        if success:
                            await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug(f"[Job {job_id}] DM batch fwd error {msg.id}: {e}")

                    await _update_job(job_id, batch_cursor=msg.id + 1)

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
                    logger.warning(f"[Job {job_id}] Batch fetch error: {e}")
                    batch_cursor += BATCH_CHUNK
                    await _update_job(job_id, batch_cursor=batch_cursor)
                    continue

                valid = [m for m in msgs if m and not m.empty and not m.service]
                valid.sort(key=lambda m: m.id)
                
                # Cross-chat filter: only apply for supergroups/channels (negative int IDs).
                # For positive int IDs (DMs/bots), string usernames (bot DMs, @channels), or "me":
                # Pyrogram's get_messages already fetches from the exact peer — no further
                # verification is needed, and attempting it would break Bot DM sources because
                # m.chat.id is a numeric ID that won't match a @username string.
                filtered = []
                for m in valid:
                    if isinstance(from_chat, int) and from_chat < 0:
                        # Private group/private channel: verify the message's chat matches
                        if m.chat is None: continue
                        if m.chat.id != from_chat: continue
                    # For string usernames, positive IDs (bots, DMs), and "me": accept all
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

                    try:
                        success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        if success:
                            await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug(f"[Job {job_id}] Batch fwd error for {msg.id}: {e}")

                    await _update_job(job_id, batch_cursor=msg.id + 1)

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
                    f"✅ Forwarded so far: <code>{_cur_fwd}</code>\n"
                    f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                    f"<i>This message updates every 60s. Powered by Arya Forward Bot</i>"
                )
                live_prog_id = live_sent.id
                await _update_job(job_id, live_prog_msg_id=live_prog_id)
                try: await client.pin_chat_message(to_chat, live_prog_id, disable_notification=True)
                except Exception: pass
            except Exception:
                live_prog_id = None

        live_last_update = 0.0

        while True:
            fresh = await _get_job(job_id)
            if not fresh or fresh.get("status") != "running":
                break


            disabled_types: list = await db.get_filters(user_id)
            configs        = await db.get_configs(user_id)
            filters_dict   = configs.get('filters', {})
            remove_caption = filters_dict.get('rm_caption', False)
            remove_links   = 'links' in disabled_types
            cap_tpl        = configs.get('caption')
            forward_tag    = configs.get('forward_tag', False)
            replacements   = configs.get('replacements', {})

            new_msgs: list = []

            try:
                if not is_bot:
                    # Userbot path: drain ALL messages newer than last_seen.
                    # CRITICAL FIX: must loop with increasing offset until we've
                    # collected every message newer than last_seen — not just the
                    # newest 50. Otherwise if 100+ messages arrive, we grab only
                    # the newest 50 and last_seen jumps past the older ones forever.
                    collected = []
                    # get_chat_history returns newest→oldest. We page through
                    # until we hit a message id <= last_seen.
                    offset_id = 0  # 0 = start from the very latest
                    while True:
                        page = []
                        async for msg in client.get_chat_history(
                            from_chat,
                            limit=100,
                            offset_id=offset_id
                        ):
                            if msg.id <= last_seen:
                                # We've reached the already-seen boundary — stop.
                                break
                            page.append(msg)
                        if not page:
                            break  # Nothing new on this page
                        collected.extend(page)
                        # If the page was a full 100 AND all were new, there may
                        # be more pages; continue from the oldest ID in this page.
                        if len(page) < 100:
                            break  # Partial page → no more new messages
                        offset_id = page[-1].id  # oldest in this page
                    # Reverse collected (oldest→newest) to get chronological order
                    new_msgs = list(reversed(collected))
                else:
                    # Bot path: probe IDs sequentially from last_seen+1 in batches
                    # of 200 (the API maximum). Continue until a full batch returns
                    # zero valid messages (true end of available messages).
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
                        except Exception:
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
                        
                        # Cross-chat filter: only apply for supergroups/channels (negative int IDs).
                        # For positive int IDs (DMs/bots), string usernames, or "me": accept all.
                        filtered = []
                        for m in valid:
                            if isinstance(from_chat, int) and from_chat < 0:
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
                await asyncio.sleep(fw.value + 1)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                err_fetch = str(e)
                err_up = err_fetch.upper()
                is_conn_err = any(k in err_up for k in (
                    "TIMEOUT", "CONNECTION", "NOT BEEN STARTED", "NOT CONNECTED",
                    "DISCONNECTED", "RESET", "NETWORK", "SOCKET", "PING"
                ))
                if is_conn_err:
                    logger.warning(f"[Job {job_id}] Connection error in live fetch: {err_fetch}. Healing client...")
                    try:
                        client = await _lj_ensure_client_alive(client)
                    except Exception as heal_e:
                        logger.error(f"[Job {job_id}] Client heal failed: {heal_e}")
                    await asyncio.sleep(15)
                else:
                    logger.warning(f"[Job {job_id}] Fetch error: {err_fetch}")
                    await asyncio.sleep(15)
                continue

            # Filter by source topic if configured
            from_thread = job.get("from_thread")
            if from_thread:
                from_thread = int(from_thread)
                new_msgs = [m for m in new_msgs if _msg_in_topic(m, from_thread)]

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
                await _update_job(job_id, last_seen_id=last_seen)
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
                        f"✅ Forwarded so far: <code>{_cur_fwd}</code>\n"
                        f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                        f"<i>This message updates every 60s. Powered by Arya Forward Bot</i>"
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
        elif any(kw in err_upper for kw in _TRANSIENT_KEYS) or isinstance(e, FloodWait):
            # Transient network/connection issue — DO NOT mark as error.
            # Auto-resume in 30s so the job stays green.
            slp = 30
            if isinstance(e, FloodWait):
                slp = e.value + 5
            elif "FLOOD_WAIT" in err_upper:
                slp = 60
                
            logger.warning(f"[Job {job_id}] Transient connection error: {err_str} - Auto-restarting in {slp}s")
            # Mark job as still running (not error) so UI stays green
            await _update_job(job_id, error=f"[Auto-reconnect] {err_str[:60]}")
            async def _auto_resume():
                await __import__('asyncio').sleep(slp)
                _start_job_task(job_id, user_id)
            __import__('asyncio').create_task(_auto_resume())
        else:
            logger.error(f"[Job {job_id}] Fatal: {e}", exc_info=True)
            await _update_job(job_id, status="error", error=err_str[:80])
    finally:
        _job_tasks.pop(job_id, None)
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
    task = asyncio.create_task(_run_job(job_id, user_id))
    _job_tasks[job_id] = task
    return task


# ══════════════════════════════════════════════════════════════════════════════
# Resume all running jobs on bot restart
# ══════════════════════════════════════════════════════════════════════════════

async def resume_live_jobs(user_id: int = None):
    query: dict = {"status": "running"}
    if user_id:
        query["user_id"] = user_id
    async for job in db.db.jobs.find(query):
        jid = job["job_id"]
        uid = job["user_id"]
        if jid not in _job_tasks:
            _start_job_task(jid, uid)
            logger.info(f"[Jobs] Resumed job {jid} for user {uid}")


# ══════════════════════════════════════════════════════════════════════════════
# UI helpers
# ══════════════════════════════════════════════════════════════════════════════

def _status_emoji(status: str) -> str:
    return {"running": "🟢", "stopped": "🔴", "error": "❌"}.get(status, "» ")


def _batch_progress(job: dict) -> str:
    """Show batch progress line if batch mode is on and not yet finished."""
    if not job.get("batch_mode"):
        return ""
    if job.get("batch_done"):
        return "  » ✅"
    cursor  = job.get("batch_cursor") or job.get("batch_start_id") or "?"
    end_id  = job.get("batch_end_id") or "?"
    return f"  » {cursor}/{end_id}"


async def _render_jobs_list(bot, user_id: int, message_or_query):
    jobs = await _list_jobs(user_id)
    is_cb = hasattr(message_or_query, "message")

    if not jobs:
        text = (
            "<b>»  Live Jobs</b>\n\n"
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
        lines = ["<b>»  Your Live Jobs</b>\n"]
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
                f"  └ <i>{j.get('from_title','?')} → {j.get('to_title','?')}{dest2}</i>\n"
                f"  └ <code>[{j['job_id'][-6:]}]</code>  ✅{fwd}  » {fetched}{bp}{err}\n"
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
            row.append(InlineKeyboardButton(f"⚙️ Lɪᴍɪᴛs [{short}]", callback_data=f"job#limits#{jid}"))
            row.append(InlineKeyboardButton(f"✏️ Nᴀᴍᴇ [{short}]", callback_data=f"job#rename#{jid}"))
            row.append(InlineKeyboardButton(f"Dᴇʟᴇᴛᴇ [{short}]",  callback_data=f"job#del#{jid}"))
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
            batch_lbl = f"\n<b>Batch:</b> »  {cur} / {end}"

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
        f"<b>»  Live Job Info</b>\n\n"
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
        text += f"\n<b>‣  Error:</b> <code>{job['error']}</code>"

    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="job#list")
    ]]))


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
    btns = [[KeyboardButton(ch['title'])] for ch in channels]
    if optional:
        btns.append([KeyboardButton("⏭ Sᴋɪᴘ (no second destination)")])
    extra = []
    if undo_btn:
        extra.append(KeyboardButton("↩️ Uɴᴅᴏ"))
    extra.append(KeyboardButton("⛔ Cᴀɴᴄᴇʟ"))
    btns.append(extra)

    resp = await _ask(bot, user_id, step_label,
                      reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True, one_time_keyboard=True))

    txt = resp.text.strip()
    if "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt or txt.startswith("/cancel"):
        await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        return None, None, True

    if undo_btn and ("↩️" in txt or "Uɴᴅᴏ" in txt or txt.startswith("/undo")):
        return None, None, "undo"

    if optional and "skip" in txt.lower():
        return None, None, False

    for ch in channels:
        if ch['title'] == txt:
            return ch['chat_id'], ch['title'], False

    return None, None, False


async def _create_job_flow(bot, user_id: int):
    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")

    def _cancel(txt): return txt.strip().startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt
    def _undo(txt):   return txt.strip().startswith("/undo") or "↩️" in txt or "Uɴᴅᴏ" in txt

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

    max_size_mb     = 0
    max_duration_s  = 0
    min_duration_s  = 0
    ltext = limit_r.text.strip()
    if ltext not in ("0", "0 (No limit)"):
        parts_l = [p.strip() for p in ltext.split(":")]
        def _lim_int(v):
            try: return max(0, int(v))
            except: return 0
        max_size_mb   = _lim_int(parts_l[0] if len(parts_l) > 0 else "0")
        max_duration_s = _lim_int(parts_l[1] if len(parts_l) > 1 else "0") * 60
        min_duration_s = _lim_int(parts_l[2] if len(parts_l) > 2 else "0") * 60

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

    kind = "Bot" if is_bot else "Userbot"
    await bot.send_message(
        user_id,
        f"<b>✅ Live Job Created & Started!</b>\n\n"
        f"🟢 <b>{from_title}</b> → <b>{to_title}</b>{thread_lbl}"
        f"{dest2_lbl}\n"
        f"<b>Account:</b> {kind}: {sel_acc.get('name','?')}\n"
        f"{batch_lbl}{size_lbl}\n"
        f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
        f"<i>Running in the background. Use /jobs to manage.</i>",
        reply_markup=ReplyKeyboardRemove()
    )