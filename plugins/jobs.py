"""
Live Jobs Plugin — v3
======================
Each job runs as an independent asyncio.Task in the background.

New in v3:
  • Batch Phase (ON/OFF): copy old messages first, then transition seamlessly to live mode.
  • Dual Destinations: send every message to up to 2 target chats/topics simultaneously.
  • Per-job Size Limit: skip files above a configured MB or duration threshold.

Flow:
  /jobs → list → ➕ Create → Step1(account) → Step2(source)
       → Step3(dest1 + topic1) → Step4(dest2 optional + topic2)
       → Step5(batch ON/OFF) → Step6(size limit)
       → job starts
"""
import time
import asyncio
import logging
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

# In-memory: job_id → asyncio.Task
_job_tasks: dict[str, asyncio.Task] = {}

# ─── Future-based ask() — immune to pyrofork stale-listener bug ──────────────
_lj_waiting: dict[int, asyncio.Future] = {}


@Client.on_message(filters.private, group=-12)
async def _lj_input_router(bot, message):
    """Route private messages to waiting _ask() futures for Live Job flow."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _lj_waiting:
        fut = _lj_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)


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
        
    if 'links' in disabled_types:
        import re
        text = msg.text or msg.caption or ""
        if text and re.search(r'(https?://\S+|www\.\S+|t\.me/\S+)', text, flags=re.IGNORECASE):
            return False

    checks = [
        ('text',      lambda m: m.text and not m.media),
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


def _passes_size_limit(msg, max_size_mb: int, max_duration_secs: int) -> bool:
    """Return True if message is within the per-job size/duration limits.
    0 means no limit.
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
        kw = {"message_thread_id": thread} if thread else {}
        if new_caption is not None:
            kw["caption"] = new_caption

        # ── Attempt 1: copy_message 
        is_restricted = False
        for attempt in range(3):
            try:
                if forward_tag:
                    await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                else:
                    if is_text_replaced and not msg.media:
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

        # ── Attempt 2: download + re-upload 
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

    await _send_one(to_chat, thread_id)
    if to_chat_2:
        await _send_one(to_chat_2, thread_id_2)


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
        is_bot = acc.get("is_bot", True)

        from_chat    = job["from_chat"]
        to_chat      = job["to_chat"]
        to_thread    = job.get("to_thread_id", None)
        to_chat_2    = job.get("to_chat_2", None)
        to_thread_2  = job.get("to_thread_id_2", None)
        max_size_mb  = int(job.get("max_size_mb", 0) or 0)
        max_dur_secs = int(job.get("max_duration_secs", 0) or 0)
        last_seen    = job.get("last_seen_id", 0)

        # ── First-run init: snapshot latest ID ────────────────────────────
        me = await client.get_me()
        if from_chat == me.id or from_chat == me.username:
            from_chat = user_id
            await _update_job(job_id, from_chat=from_chat)
            logger.info(f"[Job {job_id}] Swapped Bot's own ID with User ID ({user_id}) for Bot DM fetching")

        if last_seen == 0:
            last_seen = await _get_latest_id(client, from_chat, is_bot)
            await _update_job(job_id, last_seen_id=last_seen)
            logger.info(f"[Job {job_id}] Initialised at msg ID {last_seen}")

        # Warm up peer cache
        try:
            await client.get_chat(from_chat)
            await client.get_chat(to_chat)
            if to_chat_2:
                await client.get_chat(to_chat_2)
        except Exception as warn_e:
            logger.warning(f"[Job {job_id}] Pre-fetch peer resolve warning: {warn_e}")

        # ── BATCH PHASE ────────────────────────────────────────────────────
        if job.get("batch_mode") and not job.get("batch_done"):
            batch_cursor = int(job.get("batch_cursor") or job.get("batch_start_id") or 1)
            batch_end    = int(job.get("batch_end_id") or 0)

            # If no explicit end was set, use the snapshot we just captured
            if batch_end == 0:
                batch_end = last_seen if last_seen > 0 else 999999999
                await _update_job(job_id, batch_end_id=batch_end)

            logger.info(f"[Job {job_id}] Batch phase: msg {batch_cursor} → {batch_end}")
            
            # Progress bar helpers
            def make_progress_bar(percentage: int) -> str:
                return "█" * (percentage // 10) + "░" * (10 - (percentage // 10))
                
            def get_prog_text(percentage: int) -> str:
                return f"<b>🔄 Forwarding Process:</b>\n{make_progress_bar(percentage)} {percentage}%\n\n<i>Please wait...</i>"

            if not job.get("prog_msg_created"):
                try:
                    sent = await client.send_message(to_chat, get_prog_text(0))
                    await _update_job(job_id, prog_msg_created=True, prog_msg_id=sent.id)
                    try: await client.pin_chat_message(to_chat, sent.id, disable_notification=True)
                    except Exception: pass
                except Exception:
                    await _update_job(job_id, prog_msg_created=True)
            
            consecutive_empty = 0

            while batch_cursor <= batch_end:
                fresh = await _get_job(job_id)
                if not fresh or fresh.get("status") != "running":
                    return

                disabled_types = await db.get_filters(user_id)
                configs        = await db.get_configs(user_id)
                filters_dict   = configs.get('filters', {})
                remove_caption = filters_dict.get('rm_caption', False)
                remove_links   = filters_dict.get('links', False)
                cap_tpl        = configs.get('caption')
                forward_tag    = configs.get('forward_tag', False)
                sleep_secs     = max(1, int(configs.get('duration', 1) or 1))

                replacements   = configs.get('replacements', {})

                chunk_end = min(batch_cursor + BATCH_CHUNK - 1, batch_end)
                batch_ids = list(range(batch_cursor, chunk_end + 1))

                try:
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
                
                # Cross-chat filter: only needed for private groups (negative int IDs)
                # where Pyrogram may return messages from a different peer due to global ID overlaps.
                # For DM sources (positive int) or string usernames, skip this — get_messages
                # already fetches from the exact peer specified and m.chat may not match for DMs.
                filtered = []
                for m in valid:
                    if isinstance(from_chat, int) and from_chat < 0:
                        # Group/channel: verify the message belongs to the correct chat
                        if m.chat is None: continue
                        if m.chat.id != from_chat: continue
                    elif isinstance(from_chat, str) and from_chat != "me":
                        # Username: verify
                        if m.chat is None: continue
                        src = from_chat.replace("@", "").lower()
                        if str(m.chat.id) != src and (not m.chat.username or m.chat.username.lower() != src): continue
                    # For positive int IDs (DMs/bots) and "me": no filter needed
                    filtered.append(m)
                valid = filtered

                
                if not valid:
                    consecutive_empty += 1
                    if consecutive_empty >= 50:
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
                        await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug(f"[Job {job_id}] Batch fwd error for {msg.id}: {e}")

                    await _update_job(job_id, batch_cursor=msg.id + 1)
                    await asyncio.sleep(sleep_secs)

                batch_cursor = chunk_end + 1
                await _update_job(job_id, batch_cursor=batch_cursor)
                
                # Update progress bar occasionally
                try:
                    prog_id = (await _get_job(job_id)).get("prog_msg_id")
                    if prog_id:
                        total_msgs = max(1, batch_end - int(job.get("batch_start_id") or 1))
                        current_prog = max(0, batch_cursor - int(job.get("batch_start_id") or 1))
                        pct = min(100, int((current_prog / total_msgs) * 100))
                        await client.edit_message_text(to_chat, prog_id, get_prog_text(pct))
                except Exception:
                    pass

            # Batch complete — mark done, advance last_seen past the batch
            await _update_job(job_id, batch_done=True, batch_cursor=batch_end,
                              last_seen_id=max(last_seen, batch_end))
            last_seen = max(last_seen, batch_end)
            logger.info(f"[Job {job_id}] Batch phase complete. Switching to live mode.")
            
            try:
                prog_id = (await _get_job(job_id)).get("prog_msg_id")
                if prog_id:
                    await client.edit_message_text(to_chat, prog_id, "<b>✅ Forwarding Completed! All files have been successfully transferred.</b>")
            except Exception:
                pass

        # ── LIVE PHASE ─────────────────────────────────────────────────────
        logger.info(f"[Job {job_id}] Live polling started. last_seen={last_seen}")

        while True:
            fresh = await _get_job(job_id)
            if not fresh or fresh.get("status") != "running":
                break

            disabled_types: list = await db.get_filters(user_id)
            configs        = await db.get_configs(user_id)
            filters_dict   = configs.get('filters', {})
            remove_caption = filters_dict.get('rm_caption', False)
            remove_links   = filters_dict.get('links', False)
            cap_tpl        = configs.get('caption')
            forward_tag    = configs.get('forward_tag', False)
            replacements   = configs.get('replacements', {})

            new_msgs: list = []

            try:
                if not is_bot:
                    collected = []
                    async for msg in client.get_chat_history(from_chat, limit=50):
                        if msg.id <= last_seen:
                            break
                        collected.append(msg)
                    new_msgs = list(reversed(collected))
                else:
                    probe = last_seen + 1
                    while True:
                        batch_ids = list(range(probe, probe + 50))
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
                            break
                        valid.sort(key=lambda m: m.id)
                        
                        # Cross-chat filter: only for private groups (negative), not DMs (positive)
                        filtered = []
                        for m in valid:
                            if isinstance(from_chat, int) and from_chat < 0:
                                if m.chat is None: continue
                                if m.chat.id != from_chat: continue
                            elif isinstance(from_chat, str) and from_chat != "me":
                                if m.chat is None: continue
                                src = from_chat.replace("@", "").lower()
                                if str(m.chat.id) != src and (not m.chat.username or m.chat.username.lower() != src): continue
                            filtered.append(m)
                        
                        new_msgs.extend(filtered)
                        probe = valid[-1].id + 1
                        if len(valid) < 49:
                            break

            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[Job {job_id}] Fetch error: {e}")
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
                if not _passes_size_limit(msg, max_size_mb, max_dur_secs):
                    logger.debug(f"[Job {job_id}] Live: skipping msg {msg.id} (size/duration limit)")
                    last_seen = max(last_seen, msg.id)
                    await _update_job(job_id, last_seen_id=last_seen)
                    continue
                try:
                    await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                           to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                    await _inc_forwarded(job_id, 1, forward_type='live')
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.debug(f"[Job {job_id}] Forward error: {e}")
                last_seen = max(last_seen, msg.id)
                await _update_job(job_id, last_seen_id=last_seen)
                await asyncio.sleep(1)

            if new_msgs:
                await _update_job(job_id, last_seen_id=last_seen)

            sleep_secs = configs.get("duration", 5) or 5
            await asyncio.sleep(max(5, sleep_secs))

    except asyncio.CancelledError:
        logger.info(f"[Job {job_id}] Cancelled")
    except Exception as e:
        logger.error(f"[Job {job_id}] Fatal: {e}")
        await _update_job(job_id, status="error", error=str(e))
    finally:
        _job_tasks.pop(job_id, None)
        if client:
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
    return {"running": "🟢", "stopped": "🔴", "error": "⚠️"}.get(status, "❓")


def _batch_progress(job: dict) -> str:
    """Show batch progress line if batch mode is on and not yet finished."""
    if not job.get("batch_mode"):
        return ""
    if job.get("batch_done"):
        return "  📦✅"
    cursor  = job.get("batch_cursor") or job.get("batch_start_id") or "?"
    end_id  = job.get("batch_end_id") or "?"
    return f"  📦{cursor}/{end_id}"


async def _render_jobs_list(bot, user_id: int, message_or_query):
    jobs = await _list_jobs(user_id)
    is_cb = hasattr(message_or_query, "message")

    if not jobs:
        text = (
            "<b>📋 Live Jobs</b>\n\n"
            "<i>No jobs yet. A Live Job continuously watches a source chat\n"
            "and forwards new messages to your target — running in the background.\n\n"
            "✅ Batch mode: copy old messages first, then watch live\n"
            "✅ Dual destinations: send to 2 channels simultaneously\n"
            "✅ Per-job size limit\n\n"
            "👇 Create your first job below!</i>"
        )
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Nᴇᴡ Jᴏʙ", callback_data="job#new")],
            [InlineKeyboardButton("⫷ Bᴀᴄᴋ", callback_data="back")]
        ])
    else:
        lines = ["<b>📋 Your Live Jobs</b>\n"]
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
                f"  └ <code>[{j['job_id'][-6:]}]</code>  ✅{fwd}  ⬇️{fetched}{bp}{err}\n"
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
                row.append(InlineKeyboardButton(f"▶️ Sᴛᴀʀᴛ [{short}]", callback_data=f"job#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ Iɴғᴏ [{short}]", callback_data=f"job#info#{jid}"))
            row.append(InlineKeyboardButton(f"✏️ Nᴀᴍᴇ [{short}]", callback_data=f"job#rename#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 Dᴇʟᴇᴛᴇ [{short}]",  callback_data=f"job#del#{jid}"))
            btns_list.append(row)

        btns_list.append([InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Nᴇᴡ Jᴏʙ", callback_data="job#new")])
        btns_list.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ",        callback_data="job#list")])
        btns_list.append([InlineKeyboardButton("⫷ Bᴀᴄᴋ", callback_data="back")])
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
    await _render_jobs_list(bot, message.from_user.id, message)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^job#list$'))
async def job_list_cb(bot, query):
    await query.answer()
    await _render_jobs_list(bot, query.from_user.id, query)

@Client.on_callback_query(filters.regex(r'^job#rename#'))
async def job_rename_cb(bot, query):
    user_id = query.from_user.id
    job_id = query.data.split("#", 2)[2]
    await query.message.delete()
    
    r = await _ask(bot, user_id,
        "<b>✏️ Edit Live Job Name</b>\n\nSend a new name for this job:",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("/cancel")]], resize_keyboard=True, one_time_keyboard=True))
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
            batch_lbl = f"\n<b>Batch:</b> 📦 {cur} / {end}"

    # Size limit info
    size_lbl = ""
    if job.get("max_size_mb"):
        size_lbl += f"\n<b>Max file size:</b> {job['max_size_mb']} MB"
    if job.get("max_duration_secs"):
        mins = job['max_duration_secs'] // 60
        secs = job['max_duration_secs'] % 60
        size_lbl += f"\n<b>Max duration:</b> {mins}m {secs}s"

    text = (
        f"<b>📋 Live Job Info</b>\n\n"
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
        text += f"\n<b>⚠️ Error:</b> <code>{job['error']}</code>"

    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("↩ Bᴀᴄᴋ", callback_data="job#list")
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
    await query.answer("🗑 Job deleted.", show_alert=False)
    await _render_jobs_list(bot, user_id, query)


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
    await _create_job_flow(bot, message.from_user.id)


async def _ask_dest(bot, user_id: int, channels: list, step_label: str, optional: bool = False) -> tuple:
    """Helper: ask user to pick a channel from their saved list. Returns (chat_id, title, cancelled)."""
    btns = [[KeyboardButton(ch['title'])] for ch in channels]
    if optional:
        btns.append([KeyboardButton("⏭ Skip (no second destination)")])
    btns.append([KeyboardButton("/cancel")])

    resp = await _ask(bot, user_id, step_label,
                      reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True, one_time_keyboard=True))

    txt = resp.text.strip()
    if "/cancel" in txt:
        await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
        return None, None, True   # cancelled=True

    if optional and "skip" in txt.lower():
        return None, None, False  # skipped, not cancelled

    for ch in channels:
        if ch['title'] == txt:
            return ch['chat_id'], ch['title'], False

    return None, None, False


async def _ask_topic(bot, user_id: int, dest_label: str) -> int | None:
    """Ask for optional topic thread ID. Returns int or None."""
    r = await _ask(bot, user_id,
        f"<b>Topic Thread for {dest_label} (Optional)</b>\n\n"
        "• Send the <b>Thread ID</b> if you want to post inside a specific group topic\n"
        "• Send <b>0</b> or press 'No Topic' to post in the main chat\n\n"
        "<i>Find Thread ID: open the topic in Telegram Web → number after <code>/topics/</code> in URL</i>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("0 (No Topic)")], [KeyboardButton("/cancel")]],
            resize_keyboard=True, one_time_keyboard=True
        ))
    t = r.text.strip()
    if "/cancel" in t:
        return None
    if t.isdigit() and int(t) > 0:
        return int(t)
    return None


async def _create_job_flow(bot, user_id: int):
    # ── Step 1: Name ────────────────────────────────────────────────────────
    name_r = await _ask(bot, user_id,
        "<b>📋 Create Live Job — Step 1/7</b>\n\n"
        "Send a name for this job, or press 'Default' to use a random name.",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Default")], [KeyboardButton("/cancel")]], resize_keyboard=True, one_time_keyboard=True))
    if "/cancel" in name_r.text:
        return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
    
    job_name = name_r.text.strip()[:100]
    if job_name.lower() == "default":
        job_name = None

    # ── Step 2: Account ─────────────────────────────────────────────────────
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id,
            "<b>❌ No accounts. Add one in /settings → Accounts first.</b>")

    acc_btns = [[KeyboardButton(
        f"{'🤖 Bot' if a.get('is_bot', True) else '👤 Userbot'}: "
        f"{a.get('username') or a.get('name', 'Unknown')} [{a['id']}]"
    )] for a in accounts]
    acc_btns.append([KeyboardButton("/cancel")])

    acc_r = await bot.ask(user_id,
        "<b>📋 Create Live Job — Step 2/7</b>\n\n"
        "Choose which account to use:\n"
        "<i>(Userbot required for private chats)</i>",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in acc_r.text:
        return await acc_r.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in acc_r.text and "]" in acc_r.text:
        try: acc_id = int(acc_r.text.split('[')[-1].split(']')[0])
        except Exception: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
    is_bot  = sel_acc.get("is_bot", True)

    # ── Step 3: Source ───────────────────────────────────────────────────────
    src_r = await _ask(bot, user_id,
        "<b>Step 3/7 — Source Chat</b>\n\n"
        "Send one of:\n"
        "• <code>@username</code> or channel link\n"
        "• Numeric ID (e.g. <code>-1001234567890</code>)\n"
        "• <code>me</code> for Saved Messages (userbot only)\n\n"
        "/cancel to abort",
        reply_markup=ReplyKeyboardRemove())

    if src_r.text.strip().startswith("/cancel"):
        return await src_r.reply("<b>Cancelled.</b>")

    from_chat_raw = src_r.text.strip()
    if from_chat_raw.lower() in ("me", "saved"):
        if is_bot:
            return await src_r.reply("<b>❌ Saved Messages require a Userbot account.</b>")
        from_chat  = "me"
        from_title = "Saved Messages"
    else:
        from_chat = from_chat_raw
        if from_chat.lstrip('-').isdigit():
            from_chat = int(from_chat)
        elif "t.me/c/" in from_chat:
            parts = from_chat.split("t.me/c/")[1].split("/")
            if parts[0].isdigit():
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

    # ── Step 4: First Destination ────────────────────────────────────────────
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(user_id,
            "<b>❌ No target channels saved. Add via /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    to_chat, to_title, cancelled = await _ask_dest(bot, user_id, channels,
        "<b>Step 4/7 — Primary Destination</b>\n\nWhere should new messages be forwarded?")
    if cancelled or not to_chat:
        return

    to_thread = await _ask_topic(bot, user_id, "Primary Destination")

    # ── Step 5: Second Destination (Optional) ──────────────────────────────
    to_chat_2, to_title_2, cancelled2 = await _ask_dest(bot, user_id, channels,
        "<b>Step 5/7 — Second Destination (Optional)</b>\n\n"
        "Messages will be sent to <b>both</b> destinations when a new message arrives.\n"
        "Press 'Skip' if you only need one destination.",
        optional=True)
    if cancelled2:
        return

    to_thread_2 = None
    if to_chat_2:
        to_thread_2 = await _ask_topic(bot, user_id, "Second Destination")

    # ── Step 6: Batch Mode ───────────────────────────────────────────────────
    batch_r = await _ask(bot, user_id,
        "<b>Step 6/7 — Batch Mode (Copy Old Messages First)</b>\n\n"
        "Do you want to copy existing (old) messages before going live?\n\n"
        "• <b>ON</b> — first copies old messages, then watches for new ones\n"
        "• <b>OFF</b> — only watches for NEW messages from now on (current behavior)\n\n"
        "If ON, you will choose a starting message ID next.\n"
        "<i>Batch runs sequentially before live mode starts.</i>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("✅ ON (Copy old messages first)")],
             [KeyboardButton("❌ OFF (Live only)")],
             [KeyboardButton("/cancel")]],
            resize_keyboard=True, one_time_keyboard=True
        ))

    if "/cancel" in batch_r.text:
        return await batch_r.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

    batch_mode    = "on" in batch_r.text.lower()
    batch_start_id = 1
    batch_end_id   = 0  # 0 = up to current latest

    if batch_mode:
        range_r = await _ask(bot, user_id,
            "<b>Batch Range</b>\n\n"
            "Choose where to start the batch:\n\n"
            "• Send <b>ALL</b> to start from the very first message\n"
            "• Send a <b>start ID</b> (e.g. <code>500</code>) to start from that message\n"
            "• Send <b>start_id:end_id</b> (e.g. <code>500:2000</code>) for a specific range\n\n"
            "<i>After the batch finishes, the job automatically switches to live mode.</i>",
            reply_markup=ReplyKeyboardRemove())

        if "/cancel" in range_r.text:
            return await range_r.reply("<b>Cancelled.</b>")

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

    # ── Step 6: Size / Duration Limit ───────────────────────────────────────
    limit_r = await _ask(bot, user_id,
        "<b>Step 7/7 — Per-Job Size/Duration Limit</b>\n\n"
        "Set a maximum file size and/or duration for this job.\n"
        "Files above the limit will be <b>silently skipped</b>.\n\n"
        "<b>Format options:</b>\n"
        "• <code>0</code> — no limit (forward everything)\n"
        "• <code>50</code> — skip files larger than 50 MB\n"
        "• <code>50:10</code> — skip files larger than 50 MB <b>or</b> longer than 10 minutes\n"
        "• <code>0:5</code> — no size limit, but skip files longer than 5 minutes\n\n"
        "<i>Format: <b>max_mb:max_minutes</b>  (0 = no limit)</i>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("0 (No limit)")], [KeyboardButton("/cancel")]],
            resize_keyboard=True, one_time_keyboard=True
        ))

    if "/cancel" in limit_r.text:
        return await limit_r.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

    max_size_mb     = 0
    max_duration_s  = 0
    ltext = limit_r.text.strip()
    if ltext != "0":
        if ":" in ltext:
            parts = ltext.split(":", 1)
            try: max_size_mb    = int(parts[0].strip())
            except Exception: pass
            try: max_duration_s = int(parts[1].strip()) * 60
            except Exception: pass
        else:
            try: max_size_mb = int(ltext)
            except Exception: pass

    # ── Save & Start ─────────────────────────────────────────────────────────
    job_id = f"{user_id}-{int(time.time())}"
    job = {
        "job_id":             job_id,
        "user_id":            user_id,
        "name":               job_name if job_name else f"Live Job {job_id[-6:]}",
        "account_id":         sel_acc["id"],
        "from_chat":          from_chat,
        "from_title":         from_title,
        "from_thread":        from_thread,
        # Primary destination
        "to_chat":            to_chat,
        "to_title":           to_title,
        "to_thread_id":       to_thread,
        # Second destination (optional)
        "to_chat_2":          to_chat_2,
        "to_title_2":         to_title_2,
        "to_thread_id_2":     to_thread_2,
        # Batch settings
        "batch_mode":         batch_mode,
        "batch_start_id":     batch_start_id,
        "batch_end_id":       batch_end_id,
        "batch_cursor":       batch_start_id,
        "batch_done":         False,
        # Size limits
        "max_size_mb":        max_size_mb,
        "max_duration_secs":  max_duration_s,
        # Runtime
        "status":             "running",
        "created":            int(time.time()),
        "forwarded":          0,
        "last_seen_id":       0,
    }
    await _save_job(job)
    _start_job_task(job_id, user_id)

    # Build summary
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

    await bot.send_message(
        user_id,
        f"<b>✅ Live Job Created & Started!</b>\n\n"
        f"🟢 <b>{from_title}</b> → <b>{to_title}</b>{thread_lbl}"
        f"{dest2_lbl}\n"
        f"<b>Account:</b> {'🤖 Bot' if is_bot else '👤 Userbot'}: {sel_acc.get('name','?')}\n"
        f"{batch_lbl}{size_lbl}\n"
        f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
        f"<i>Running in the background. Use /jobs to manage.</i>",
        reply_markup=ReplyKeyboardRemove()
    )
