"""
Task Jobs Plugin
================
A "Task Job" is a persistent, long-running bulk-copy operation for private channels
(Forwarding OFF mode). Unlike a normal /fwd job which blocks the user, or a Live Job
that only watches for NEW messages, a Task Job:

  • Copies ALL existing messages from a source (old → new, sequentially)
  • Runs fully in the background as an asyncio.Task
  • Supports pause / resume at the exact message where it stopped
  • Multiple task jobs can run simultaneously
  • After finishing, normal forwarding (/fwd) still works

Commands:
  /taskjobs  — Open the Task Jobs manager UI
  /newtaskjob — Start create flow directly

Flow:
  /taskjobs → list → »  Create → Step1(account) → Step2(source + skip) → Step3(dest) → starts
"""

import re
import os
import time
import asyncio
import logging
from database import db
from .test import CLIENT, start_clone_bot
from pyrogram import Client, filters
from config import Config
from pyrogram.errors import FloodWait
from plugins.regix import custom_caption
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

#  In-memory task registry 
# task_job_id → asyncio.Task
_task_jobs: dict[str, asyncio.Task] = {}
# task_job_id → pause Event (set = running, clear = paused)
_pause_events: dict[str, asyncio.Event] = {}


# ══════════════════════════════════════════════════════════════════════════════
# Safe ask() helper — immune to pyrofork stale-listener bugs
# A dict maps user_id → asyncio.Future so only ONE handler is needed globally.
# ══════════════════════════════════════════════════════════════════════════════

# user_id → Future that resolves with the next Message from that user
_waiting: dict[int, asyncio.Future] = {}


from pyrogram import ContinuePropagation

@Client.on_message(filters.private, group=-10)
async def _taskjob_input_router(bot, message):
    """Catch all private messages and route them to any waiting _ask() futures."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _waiting:
        fut = _waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation


async def _ask(bot, user_id: int, text: str, reply_markup=None, timeout: int = 300):
    """
    Send `text` to `user_id`, then wait for their next private message.
    Uses a module-level Future dict — none of pyrofork's listener machinery involved.
    """
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    # Cancel any stale future that may be lingering from a previous run
    old = _waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _waiting[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _waiting.pop(user_id, None)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

COLL = "taskjobs"

async def _tj_save(job: dict):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)

async def _tj_get(job_id: str) -> dict | None:
    return await db.db[COLL].find_one({"job_id": job_id})

async def _tj_list(user_id: int) -> list[dict]:
    return [j async for j in db.db[COLL].find({"user_id": user_id})]

async def _tj_delete(job_id: str):
    await db.db[COLL].delete_one({"job_id": job_id})

async def _tj_update(job_id: str, **kwargs):
    await db.db[COLL].update_one({"job_id": job_id}, {"$set": kwargs})

async def _tj_inc(job_id: str, n: int = 1):
    """Atomically increment forwarded count."""
    await db.db[COLL].update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})


# ══════════════════════════════════════════════════════════════════════════════
# Status helpers
# ══════════════════════════════════════════════════════════════════════════════

def _st_emoji(status: str) -> str:
    return {
        "running": "🟢",
        "paused":  "⏸",
        "stopped": "🔴",
        "done":    "✅",
        "error":   "‣ ",
    }.get(status, "» ")


# ══════════════════════════════════════════════════════════════════════════════
# Filter helper (same as jobs.py)
# ══════════════════════════════════════════════════════════════════════════════

def _msg_in_topic(msg, from_thread_id: int) -> bool:
    """Return True if msg belongs to the given source topic."""
    tid = getattr(msg, "message_thread_id", None)
    if tid is not None and int(tid) == from_thread_id:
        return True
    if int(msg.id) == from_thread_id:
        return True
    return False


def _passes_filters(msg, disabled_types: list) -> bool:
    if msg.empty or msg.service:
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


# ══════════════════════════════════════════════════════════════════════════════
# Send one message sequentially (copy_message with fallback)
# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# Ordered Pipeline Logic
# ══════════════════════════════════════════════════════════════════════════════

async def _dl_worker(worker_id, dl_queue, up_queue, client, to_chat, thread_id):
    """Worker that safely handles parallel downloads while keeping exactly 2 limit."""
    while True:
        task = await dl_queue.get()
        if task is None: break
        seq_idx, msg, caption, new_text, is_text_replaced, forward_tag, remove_caption = task
        
        try:
            #  Attempt 1: copy_message (if restricted, raises exception)
            for attempt in range(3):
                try:
                    kw = {"message_thread_id": thread_id} if thread_id else {}
                    if caption is not None: kw["caption"] = caption
                    
                    if forward_tag:
                        await client.forward_messages(chat_id=to_chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                    else:
                        if is_text_replaced and not getattr(msg, 'media', None):
                            if not new_text or not new_text.strip():
                                await up_queue.put((seq_idx, 'skip', None, None))
                                break
                            await client.send_message(chat_id=to_chat, text=new_text, **kw)
                        else:
                            await client.copy_message(chat_id=to_chat, from_chat_id=msg.chat.id, message_id=msg.id, **kw)
                    # Success
                    await up_queue.put((seq_idx, 'done', None, None))
                    break
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2)
                    continue
                except Exception as e:
                    err = str(e).upper()
                    if "RESTRICTED" not in err and "PROTECTED" not in err and "FALLBACK" not in err:
                        if "TIMEOUT" in err or "CONNECTION" in err:
                            await asyncio.sleep(5)
                            continue 
                        # Immediate retry for unknown temporary error
                        if attempt < 2:
                            await asyncio.sleep(2)
                            continue
                        # If totally fails after 3 normal attempts, fail it
                        print(f"TaskJob copy_message hard fail: {e}")
                        await up_queue.put((seq_idx, 'skip', None, None))
                        break

                    #  Attempt 2: download + re-upload 
                    # Must be restricted/protected, do download:
                    fp = None
                    media_obj = getattr(msg, msg.media.value, None) if msg.media else None
                    original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                    if msg.media:
                        safe_name = f"downloads/{msg.id}_{original_name}" if original_name else f"downloads/{msg.id}"
                        # Retry system: 5 retries for large files downloading
                        for dl_attempt in range(5):
                            try:
                                fp = await client.download_media(msg, file_name=safe_name)
                                if fp: break
                            except FloodWait as fw:
                                await asyncio.sleep(fw.value + 2)
                            except Exception as e2:
                                if "TIMEOUT" in str(e2).upper() or "CONNECTION" in str(e2).upper():
                                    await asyncio.sleep(5)
                                    continue
                                if dl_attempt < 4:
                                    await asyncio.sleep(3)
                                    continue
                                print(f"TaskJob download hard fail for {msg.id}: {e2}")
                                break
                        
                        if not fp:
                            await up_queue.put((seq_idx, 'skip', None, None))
                            break
                        
                        await db.update_global_stats(total_files_downloaded=1)
                        up_kw = {"chat_id": to_chat, "caption": caption if caption is not None else (msg.caption or "")}
                        if thread_id: up_kw["message_thread_id"] = thread_id
                        
                        if getattr(msg, 'photo', None):
                            await up_queue.put((seq_idx, 'send_photo', {"photo": fp, **up_kw}, fp))
                        elif getattr(msg, 'video', None):
                            await up_queue.put((seq_idx, 'send_video', {"video": fp, "file_name": original_name or None, **up_kw}, fp))
                        elif getattr(msg, 'document', None):
                            await up_queue.put((seq_idx, 'send_document', {"document": fp, "file_name": original_name or None, **up_kw}, fp))
                        elif getattr(msg, 'audio', None):
                            await up_queue.put((seq_idx, 'send_audio', {"audio": fp, "file_name": original_name or None, **up_kw}, fp))
                        elif getattr(msg, 'voice', None):
                            await up_queue.put((seq_idx, 'send_voice', {"voice": fp, **up_kw}, fp))
                        elif getattr(msg, 'animation', None):
                            await up_queue.put((seq_idx, 'send_animation', {"animation": fp, **up_kw}, fp))
                        elif getattr(msg, 'sticker', None):
                            await up_queue.put((seq_idx, 'send_sticker', {"sticker": fp, **up_kw}, fp))
                        else:
                            await up_queue.put((seq_idx, 'skip', None, fp))
                        break # exit attempt loop
                    else:
                        # Re-send text message
                        if is_text_replaced and not getattr(msg, 'media', None):
                            if not new_text or not new_text.strip():
                                await up_queue.put((seq_idx, 'skip', None, None))
                                break
                        snd_kwargs = {"chat_id": to_chat, "text": new_text if new_text is not None else (msg.text or "")}
                        if thread_id: snd_kwargs["message_thread_id"] = thread_id
                        await up_queue.put((seq_idx, 'send_message', snd_kwargs, None))
                        break

        except Exception as general_err:
            print(f"TaskJob Worker general error: {general_err}")
            await up_queue.put((seq_idx, 'skip', None, None))
        finally:
            dl_queue.task_done()

BATCH_SIZE = 200  # IDs per get_messages call

async def _run_task_job(job_id: str, user_id: int):
    """
    Main coroutine for a Task Job.
    Iterates source messages from `start_id` → `end_id` (or until exhausted),
    sends each one sequentially, respects pause, and saves progress after every batch.
    """
    job = await _tj_get(job_id)
    if not job:
        return

    # Ensure a pause event exists
    if job_id not in _pause_events:
        ev = asyncio.Event()
        ev.set()  # running by default
        _pause_events[job_id] = ev
    pause_ev = _pause_events[job_id]

    acc = client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _tj_update(job_id, status="error", error="Account not found")
            return

        client = await start_clone_bot(_CLIENT.client(acc))
        is_bot = acc.get("is_bot", True)

        from_chat = job["from_chat"]
        to_chat   = job["to_chat"]
        end_id    = job.get("end_id", 0)   # 0 = no fixed end (all messages)
        current   = job.get("current_id", job.get("start_id", 1))

        await _tj_update(job_id, status="running", error="", start_time=time.time(), forwarded=job.get("forwarded", 0))
        logger.info(f"[TaskJob {job_id}] Started. current={current} end={end_id}")

        job_start_time = time.time()
        forwarded_at_start = job.get("forwarded", 0)

        #  Destination progress bar setup 
        to_chat_for_prog = job.get("to_chat")
        prog_msg_id = job.get("prog_msg_id", None)
        if not prog_msg_id:
            try:
                sent = await client.send_message(to_chat_for_prog,
                    "<b>»  Task Job Starting...</b>\n<code>[░░░░░░░░░░] 0%</code>\n\n<i>Please wait...</i>")
                prog_msg_id = sent.id
                await _tj_update(job_id, prog_msg_id=prog_msg_id)
                try: await client.pin_chat_message(to_chat_for_prog, prog_msg_id, disable_notification=True)
                except Exception: pass
            except Exception:
                pass

        def _make_prog_bar(pct: int) -> str:
            filled = pct // 10
            bar = "█" * filled + "░" * (10 - filled)
            return f"[{bar}] {pct}%"

        last_prog_update = 0.0

        while True:
            #  Pause check 
            await pause_ev.wait()  # blocks here if paused

            #  Stop check 
            fresh = await _tj_get(job_id)
            if not fresh or fresh.get("status") in ("stopped", "error"):
                break

            #  End check 
            if end_id > 0 and current > end_id:
                await _tj_update(job_id, status="done", current_id=current)
                logger.info(f"[TaskJob {job_id}] Completed — reached end_id {end_id}")
                break

            #  Load settings 
            disabled_types = await db.get_filters(user_id)
            configs        = await db.get_configs(user_id)
            filters_dict   = configs.get('filters', {})
            remove_caption = filters_dict.get('rm_caption', False)
            remove_links   = 'links' in disabled_types
            cap_tpl        = configs.get('caption')
            forward_tag    = configs.get('forward_tag', False)
            sleep_secs     = max(1, configs.get('duration', 1) or 1)

            #  Build batch of IDs 
            batch_end = current + BATCH_SIZE - 1
            if end_id > 0:
                batch_end = min(batch_end, end_id)
            batch_ids = list(range(current, batch_end + 1))

            #  Fetch messages 
            try:
                if is_bot:
                    msgs = await client.get_messages(from_chat, batch_ids)
                    if not isinstance(msgs, list): msgs = [msgs]
                else:
                    # Userbot: get_messages by ID also works
                    msgs = await client.get_messages(from_chat, batch_ids)
                    if not isinstance(msgs, list): msgs = [msgs]
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[TaskJob {job_id}] Fetch error at {current}: {e}")
                await asyncio.sleep(10)
                current += BATCH_SIZE   # skip bad batch
                await _tj_update(job_id, current_id=current)
                continue

            #  Sort & filter 
            valid = [m for m in msgs if m and not m.empty]
            valid.sort(key=lambda m: m.id)  # guarantee ascending order

            if not valid:
                # No messages in this ID range — channel may have ended
                if is_bot:
                    # Try to detect if we've gone past the last message
                    max_probe = batch_end
                    found_any = any(m and not m.empty for m in msgs)
                    if not found_any:
                        # 50 consecutive empty batches = done (10,000 max skip ids)
                        consecutive_empty = fresh.get("consecutive_empty", 0) + 1
                        if consecutive_empty >= 50:
                            await _tj_update(job_id, status="done", current_id=current)
                            logger.info(f"[TaskJob {job_id}] Done — no more messages after {current}")
                            break
                        await _tj_update(job_id, consecutive_empty=consecutive_empty, current_id=current + BATCH_SIZE)
                        current += BATCH_SIZE
                        await asyncio.sleep(2)
                        continue
                    await _tj_update(job_id, consecutive_empty=0)
                current += BATCH_SIZE
                await _tj_update(job_id, current_id=current)
                continue

            await _tj_update(job_id, consecutive_empty=0)

            #  Pipeline Execution (Strict Order) 
            MAX_WORKERS = 2
            dl_queue = asyncio.Queue(maxsize=100)
            up_queue = asyncio.Queue(maxsize=100)
            
            workers = [asyncio.create_task(_dl_worker(i, dl_queue, up_queue, client, to_chat, None)) for i in range(MAX_WORKERS)]
            
            # Feed messages to the dl_queue
            fwd_count = 0
            seq_counter = 0
            
            # Filter by source topic if configured
            from_thread = job.get("from_thread")
            if from_thread:
                from_thread = int(from_thread)
                valid = [m for m in valid if _msg_in_topic(m, from_thread)]

            for msg in valid:
                await pause_ev.wait()
                fresh2 = await _tj_get(job_id)
                if not fresh2 or fresh2.get("status") in ("stopped",):
                    for _ in workers: await dl_queue.put(None)
                    return

                if not _passes_filters(msg, disabled_types):
                    continue

                caption = None
                new_text = None
                is_text_replaced = False
                if getattr(msg, 'media', None):
                    caption = custom_caption(msg, cap_tpl, apply_smart_clean=remove_caption, remove_links_flag=remove_links)
                else:
                    from plugins.regix import remove_all_links
                    new_text = getattr(msg.text, "html", str(msg.text)) if msg.text else ""
                    if remove_links and new_text:
                        new_text = remove_all_links(new_text)
                        is_text_replaced = True
                        
                    if configs.get('replacements') and new_text:
                        orig_text = new_text
                        for old_txt, new_txt_str in configs.get('replacements').items():
                            if old_txt is None: continue
                            new_str = "" if new_txt_str is None else str(new_txt_str)
                            try: new_text = re.sub(str(old_txt), new_str, str(new_text), flags=re.IGNORECASE)
                            except Exception: new_text = str(new_text).replace(str(old_txt), new_str)
                        if orig_text != new_text: is_text_replaced = True

                await dl_queue.put((seq_counter, msg, caption, new_text, is_text_replaced, forward_tag, remove_caption))
                seq_counter += 1

            # Tell workers to stop cleanly
            for _ in workers: await dl_queue.put(None)

            # Sequential Uploader logic (awaits ordered completion from up_queue buffer)
            expected_seq = 0
            buffer = {}
            running_uploads = seq_counter
            
            # Add small delay (0.1s minimum) for stability
            effective_sleep = max(0.2, sleep_secs)
            
            while expected_seq < running_uploads:
                # Get completed payloads seamlessly
                item = await up_queue.get()
                seq, act, prm, fpath = item
                buffer[seq] = (act, prm, fpath)
                
                while expected_seq in buffer:
                    act, prm, fpath = buffer.pop(expected_seq)
                    
                    if act not in ('skip', 'done'):
                        for up_attempt in range(4):
                            try:
                                if act == 'send_photo': await client.send_photo(**prm)
                                elif act == 'send_video': await client.send_video(**prm)
                                elif act == 'send_document': await client.send_document(**prm)
                                elif act == 'send_audio': await client.send_audio(**prm)
                                elif act == 'send_voice': await client.send_voice(**prm)
                                elif act == 'send_animation': await client.send_animation(**prm)
                                elif act == 'send_sticker': await client.send_sticker(**prm)
                                elif act == 'send_message': await client.send_message(**prm)
                                fwd_count += 1
                                await _tj_inc(job_id)
                                if act != 'send_message': await db.update_global_stats(total_files_uploaded=1)
                                break
                            except FloodWait as fw:
                                await asyncio.sleep(fw.value + 2)
                            except Exception as eup:
                                eup_err = str(eup).upper()
                                if "TIMEOUT" in eup_err or "CONNECTION" in eup_err:
                                    await asyncio.sleep(5)
                                    continue
                                print(f"Upload fail for {expected_seq}: {eup}")
                                break
                    elif act == 'done':
                        # the copy_message in worker succeeded initially
                        fwd_count += 1
                        await _tj_inc(job_id)

                    # Cleanup file correctly
                    if fpath:
                        try:
                            import os
                            if os.path.exists(fpath): os.remove(fpath)
                        except: pass
                    
                    expected_seq += 1
                    up_queue.task_done()
                    # Apply minimal stabilizing sleep exactly after upload attempt
                    await asyncio.sleep(effective_sleep)

            await asyncio.gather(*workers)

            #  Advance cursor 
            if valid:
                current = valid[-1].id + 1
            else:
                current += BATCH_SIZE

            await _tj_update(job_id, current_id=current)

            #  Update destination progress bar with live ETA 
            now_t = time.time()
            if prog_msg_id and to_chat_for_prog and (now_t - last_prog_update) >= 30:
                last_prog_update = now_t
                try:
                    fresh_j   = await _tj_get(job_id)
                    total_fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                    start_id  = job.get("start_id", 1)
                    _end_id   = job.get("end_id", 0)

                    # Percentage (only if end_id is known)
                    pct = 0
                    if _end_id > 0 and current > start_id:
                        pct = min(99, int(((current - start_id) / max(1, _end_id - start_id)) * 100))

                    # ETA based on msgs/sec since job start
                    elapsed = max(1, now_t - job_start_time)
                    delta_fwd = total_fwd - forwarded_at_start
                    speed_mps = delta_fwd / elapsed  # messages per second
                    if speed_mps > 0 and _end_id > 0:
                        remaining_msgs = max(0, _end_id - current)
                        eta_secs = remaining_msgs / speed_mps
                        eta_h = int(eta_secs // 3600)
                        eta_m = int((eta_secs % 3600) // 60)
                        eta_str = f"{eta_h}h {eta_m}m" if eta_h else f"{eta_m}m"
                    elif speed_mps > 0:
                        eta_str = "Live (no end)"
                    else:
                        eta_str = "Starting..."

                    bar = _make_prog_bar(pct)
                    prog_text = (
                        f"<b>»  Task Job Running</b>\n"
                        f"<code>{bar}</code>\n\n"
                        f"✅ Forwarded: {total_fwd}\n"
                        f"»  At msg: {current}\n"
                        f"»  ETA: {eta_str}"
                    )
                    await client.edit_message_text(to_chat_for_prog, prog_msg_id, prog_text)
                except Exception:
                    pass

    except asyncio.CancelledError:
        logger.info(f"[TaskJob {job_id}] Cancelled")
        await _tj_update(job_id, status="stopped")
        # Mark progress bar stopped
        if client and prog_msg_id and to_chat_for_prog:
            try:
                fresh_j = await _tj_get(job_id)
                total_fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                await client.edit_message_text(to_chat_for_prog, prog_msg_id,
                    f"<b>⏹ Task Job Stopped</b>\n<code>[░░░░░░░░░░] — Paused</code>\n\n✅ Forwarded: {total_fwd}")
            except Exception: pass
    except Exception as e:
        logger.error(f"[TaskJob {job_id}] Fatal: {e}")
        await _tj_update(job_id, status="error", error=str(e))
    finally:
        _task_jobs.pop(job_id, None)
        _pause_events.pop(job_id, None)
        if client:
            try: await client.stop()
            except Exception: pass


def _start_task(job_id: str, user_id: int):
    ev = asyncio.Event()
    ev.set()
    _pause_events[job_id] = ev
    task = asyncio.create_task(_run_task_job(job_id, user_id))
    _task_jobs[job_id] = task
    return task


# ══════════════════════════════════════════════════════════════════════════════
# Resume on bot restart
# ══════════════════════════════════════════════════════════════════════════════

async def resume_task_jobs(user_id: int = None):
    query = {"status": "running"}
    if user_id:
        query["user_id"] = user_id
    async for job in db.db[COLL].find(query):
        jid = job["job_id"]
        uid = job["user_id"]
        if jid not in _task_jobs:
            _start_task(jid, uid)
            logger.info(f"[TaskJobs] Resumed {jid} for user {uid}")


# ══════════════════════════════════════════════════════════════════════════════
# UI: render job list
# ══════════════════════════════════════════════════════════════════════════════

async def _render_taskjob_list(bot, user_id: int, message_or_query):
    jobs = await _tj_list(user_id)
    is_cb = hasattr(message_or_query, 'message')

    if not jobs:
        text = (
            "<b>»  Task Jobs</b>\n\n"
            "<i>No task jobs yet.\n\n"
            "A <b>Task Job</b> copies all existing messages from a source channel\n"
            "to your target — running fully in the background.\n\n"
            "✅ Supports pause / resume\n"
            "✅ Multiple jobs simultaneously\n"
            "✅ Real-time status\n"
            "✅ Continues from where it left off\n\n"
            "👇 Create your first task job below!</i>"
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Tᴀsᴋ Jᴏʙ", callback_data="tj#new")
        ]])
    else:
        lines = ["<b>»  Your Task Jobs</b>\n"]
        for j in jobs:
            st  = _st_emoji(j.get("status", "stopped"))
            fwd = j.get("forwarded", 0)
            cur = j.get("current_id", "?")
            err = f" <code>[{j.get('error','')}]</code>" if j.get("status") == "error" else ""
            lines.append(
                f"{st} <b>{j.get('from_title','?')} → {j.get('to_title','?')}</b>"
                f"  <code>[{j['job_id'][-6:]}]</code>  ✅{fwd}  » {cur}{err}"
            )
        text = "\n".join(lines)

        btns_list = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            short = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏸ Pᴀᴜsᴇ [{short}]",  callback_data=f"tj#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]",   callback_data=f"tj#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ Rᴇsᴜᴍᴇ [{short}]", callback_data=f"tj#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]",   callback_data=f"tj#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Sᴛᴀʀᴛ [{short}]",  callback_data=f"tj#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ Iɴғᴏ [{short}]", callback_data=f"tj#info#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 Dᴇʟᴇᴛᴇ [{short}]",  callback_data=f"tj#del#{jid}"))
            btns_list.append(row)

        btns_list.append([InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Tᴀsᴋ Jᴏʙ", callback_data="tj#new")])
        btns_list.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ",         callback_data="tj#list")])
        btns = InlineKeyboardMarkup(btns_list)

    try:
        if is_cb:
            await message_or_query.message.edit_text(text, reply_markup=btns)
        else:
            await message_or_query.reply_text(text, reply_markup=btns)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Commands
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command(["taskjobs", "taskjob"]))
async def taskjobs_cmd(bot, message):
    await _render_taskjob_list(bot, message.from_user.id, message)


@Client.on_message(filters.private & filters.command("newtaskjob"))
async def newtaskjob_cmd(bot, message):
    await _create_taskjob_flow(bot, message.from_user.id)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^tj#list$'))
async def tj_list_cb(bot, query):
    await _render_taskjob_list(bot, query.from_user.id, query)


@Client.on_callback_query(filters.regex(r'^tj#new$'))
async def tj_new_cb(bot, query):
    user_id = query.from_user.id
    await query.message.delete()
    await _create_taskjob_flow(bot, user_id)


@Client.on_callback_query(filters.regex(r'^tj#info#'))
async def tj_info_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    job = await _tj_get(job_id)
    if not job:
        return await query.answer("Job not found!", show_alert=True)

    import datetime
    created = datetime.datetime.fromtimestamp(job.get("created", 0)).strftime("%d %b %Y %H:%M")
    st = _st_emoji(job.get("status", "stopped"))
    current = job.get("current_id", "N/A")
    end_id  = job.get("end_id", 0)
    end_lbl = f"ID {end_id}" if end_id else "∞ (all messages)"

    text = (
        f"<b>»  Task Job Info</b>\n\n"
        f"<b>ID:</b> <code>{job_id[-6:]}</code>\n"
        f"<b>Status:</b> {st} {job.get('status', '?')}\n"
        f"<b>Source:</b> {job.get('from_title', '?')}\n"
        f"<b>Target:</b> {job.get('to_title', '?')}\n"
        f"<b>Started at:</b> ID <code>{job.get('start_id', 1)}</code>\n"
        f"<b>Current ID:</b> <code>{current}</code>\n"
        f"<b>End ID:</b> {end_lbl}\n"
        f"<b>Forwarded:</b> {job.get('forwarded', 0)}\n"
        f"<b>Created:</b> {created}\n"
    )
    if job.get("error"):
        text += f"\n<b>‣  Error:</b> <code>{job['error']}</code>"

    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="tj#list")
    ]]))


@Client.on_callback_query(filters.regex(r'^tj#pause#'))
async def tj_pause_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id

    job = await _tj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)

    # Clear the pause event so the running task blocks
    ev = _pause_events.get(job_id)
    if ev:
        ev.clear()

    await _tj_update(job_id, status="paused")
    await query.answer("⏸ Job paused. It will stop after the current message.", show_alert=False)
    await _render_taskjob_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^tj#resume#'))
async def tj_resume_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id

    job = await _tj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)

    # If task is still alive (just paused), simply set the event
    ev = _pause_events.get(job_id)
    if ev and job_id in _task_jobs and not _task_jobs[job_id].done():
        ev.set()
        await _tj_update(job_id, status="running")
        await query.answer("▶️ Job resumed!", show_alert=False)
    else:
        # Task died while paused — restart it fresh from saved cursor
        await _tj_update(job_id, status="running")
        _start_task(job_id, user_id)
        await query.answer("▶️ Job restarted from saved position!", show_alert=False)

    await _render_taskjob_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^tj#stop#'))
async def tj_stop_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id

    job = await _tj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)

    task = _task_jobs.pop(job_id, None)
    if task and not task.done():
        task.cancel()

    ev = _pause_events.pop(job_id, None)
    if ev: ev.set()  # unblock so cancel propagates

    await _tj_update(job_id, status="stopped")
    await query.answer("⏹ Job stopped.", show_alert=False)
    await _render_taskjob_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^tj#start#'))
async def tj_start_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id

    job = await _tj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)

    if job_id in _task_jobs and not _task_jobs[job_id].done():
        return await query.answer("Already running!", show_alert=True)

    await _tj_update(job_id, status="running")
    _start_task(job_id, user_id)
    await query.answer("▶️ Task Job started!", show_alert=False)
    await _render_taskjob_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^tj#del#'))
async def tj_del_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id

    job = await _tj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)

    task = _task_jobs.pop(job_id, None)
    if task and not task.done():
        task.cancel()

    ev = _pause_events.pop(job_id, None)
    if ev: ev.set()

    await _tj_delete(job_id)
    await query.answer("🗑 Task Job deleted.", show_alert=False)
    await _render_taskjob_list(bot, user_id, query)


# ══════════════════════════════════════════════════════════════════════════════
# Create Task Job — Interactive flow
# ══════════════════════════════════════════════════════════════════════════════

async def _create_taskjob_flow(bot, user_id: int):
    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")

    def _cancel(txt): return txt.strip().startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt
    def _undo(txt):   return txt.strip().startswith("/undo")   or "↩️" in txt or "Uɴᴅᴏ"   in txt

    # ── Step 1: Account ───────────────────────────────────────────
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id,
            "<b>❌ No accounts found. Add one in /settings → Accounts first.</b>")

    def _acc_label(a):
        kind = "Bot" if a.get("is_bot", True) else "Userbot"
        name = a.get("username") or a.get("name", "Unknown")
        return f"{kind}: {name} [{a['id']}]"

    acc_btns = [[KeyboardButton(_acc_label(a))] for a in accounts]
    acc_btns.append([CANCEL_BTN])

    acc_r = await _ask(bot, user_id,
        "<b>»  Create Task Job — Step 1/4</b>\n\n"
        "Choose the <b>account</b> to use for this task:\n\n"
        "<blockquote expandable>"
        "🤖 <b>Bot</b> — works for public channels where the bot is admin.\n"
        "👤 <b>Userbot</b> — required for:\n"
        "  • Private/restricted channels\n"
        "  • Forwarding without forward tags\n"
        "  • Groups where bots cannot read history"
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

    # ── Step 2: Source Chat ───────────────────────────────────────
    while True:
        src_r = await _ask(bot, user_id,
            "<b>Step 2/4 — Source Channel</b>\n\n"
            "Send the <b>source channel or group</b> to copy files from.\n\n"
            "<blockquote expandable>"
            "Accepted formats:\n"
            "• <code>@username</code> — public channel/group username\n"
            "• <code>https://t.me/username</code> — public link\n"
            "• <code>https://t.me/c/1234567890/1</code> — private channel link\n"
            "• <code>-1001234567890</code> — numeric chat ID (negative for channels/groups)\n\n"
            "📌 Private channel: use a Userbot that is already a member.\n"
            "📌 Public channel: Bot account works if it can read messages.\n"
            "📌 This is the channel you want to copy FROM — not the destination."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup([[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))

        if _cancel(src_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(src_r.text):
            # redo account selection
            acc_r2 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 1/4: Account</b>\n\nChoose the account again:",
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

    link_match = re.search(r't\.me/c/(\d+)', from_chat_raw)
    if link_match:
        from_chat = int(f"-100{link_match.group(1)}")
    elif from_chat_raw.lstrip('-').isdigit():
        from_chat = int(from_chat_raw)
    else:
        from_chat = from_chat_raw

    try:
        chat_obj   = await bot.get_chat(from_chat)
        from_title = getattr(chat_obj, "title", None) or str(from_chat)
    except Exception:
        from_title = str(from_chat)

    # ── Step 3: Message Range ──────────────────────────────────────
    while True:
        range_r = await _ask(bot, user_id,
            "<b>Step 3/4 — Message Range</b>\n\n"
            "Choose which messages to copy:\n\n"
            "<blockquote expandable>"
            "• <b>ALL</b> — copy from message ID 1 (very beginning)\n"
            "• <code>100</code> — start from message ID 100 onward\n"
            "• <code>100:500</code> — copy only message IDs 100 through 500\n\n"
            "The job runs until all messages in the range are copied.\n"
            "You can pause and resume it from the Task Jobs menu."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("ALL")], [UNDO_BTN, CANCEL_BTN]],
                resize_keyboard=True, one_time_keyboard=True))

        if _cancel(range_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(range_r.text):
            # redo source chat
            src_r2 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 2/4: Source Channel</b>\n\nSend source channel again:",
                reply_markup=ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))
            if _cancel(src_r2.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            from_chat_raw = src_r2.text.strip()
            lm = re.search(r't\.me/c/(\d+)', from_chat_raw)
            if lm:
                from_chat = int(f"-100{lm.group(1)}")
            elif from_chat_raw.lstrip('-').isdigit():
                from_chat = int(from_chat_raw)
            else:
                from_chat = from_chat_raw
            try:
                co2 = await bot.get_chat(from_chat)
                from_title = getattr(co2, "title", None) or str(from_chat)
            except Exception:
                from_title = str(from_chat)
            continue
        break

    start_id = 1
    end_id   = 0
    rtext = range_r.text.strip().lower()

    if rtext != "all":
        if ":" in rtext:
            parts = rtext.split(":", 1)
            try: start_id = int(parts[0].strip())
            except Exception: pass
            try: end_id = int(parts[1].strip())
            except Exception: pass
        else:
            try: start_id = int(rtext)
            except Exception: pass

    # ── Step 4: Destination ───────────────────────────────────────
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(user_id,
            "<b>❌ No target channels saved. Add via /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    ch_btns = [[KeyboardButton(ch['title'])] for ch in channels]
    ch_btns.append([UNDO_BTN, CANCEL_BTN])

    while True:
        ch_r = await _ask(bot, user_id,
            "<b>Step 4/4 — Target Channel</b>\n\nChoose where to forward messages:\n\n"
            "<blockquote expandable>"
            "Choose from your saved channels/groups.\n"
            "To add a channel, go to /settings → Channels.\n"
            "The selected account must be an admin with send permissions."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(ch_btns, resize_keyboard=True, one_time_keyboard=True))

        if _cancel(ch_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(ch_r.text):
            # redo message range
            range_r2 = await _ask(bot, user_id,
                "<b>↩️ Redo — Step 3/4: Message Range</b>\n\nSend range again (ALL / ID / start:end):",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("ALL")], [CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))
            if _cancel(range_r2.text):
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            rtext2 = range_r2.text.strip().lower()
            start_id, end_id = 1, 0
            if rtext2 != "all":
                if ":" in rtext2:
                    p2 = rtext2.split(":", 1)
                    try: start_id = int(p2[0].strip())
                    except Exception: pass
                    try: end_id   = int(p2[1].strip())
                    except Exception: pass
                else:
                    try: start_id = int(rtext2)
                    except Exception: pass
            continue
        break

    to_chat, to_title = None, ch_r.text.strip()
    for ch in channels:
        if ch['title'] == to_title:
            to_chat  = ch['chat_id']
            to_title = ch['title']
            break

    if not to_chat:
        return await bot.send_message(user_id, "<b>Invalid selection. Cancelled.</b>",
                                      reply_markup=ReplyKeyboardRemove())

    # ── Save & Start ──────────────────────────────────────────────
    job_id = f"tj-{user_id}-{int(time.time())}"
    job = {
        "job_id":      job_id,
        "user_id":     user_id,
        "account_id":  sel_acc["id"],
        "from_chat":   from_chat,
        "from_title":  from_title,
        "to_chat":     to_chat,
        "to_title":    to_title,
        "start_id":    start_id,
        "end_id":      end_id,
        "current_id":  start_id,
        "status":      "running",
        "created":     int(time.time()),
        "forwarded":   0,
        "consecutive_empty": 0,
        "error":       "",
    }
    await _tj_save(job)
    _start_task(job_id, user_id)

    end_lbl = f"up to ID <code>{end_id}</code>" if end_id else "all messages"
    kind    = "Bot" if is_bot else "Userbot"
    await bot.send_message(
        user_id,
        f"<b>✅ Task Job Created & Started!</b>\n\n"
        f"🟢 Copying <b>{from_title}</b> → <b>{to_title}</b>\n"
        f"<b>Account:</b> {kind}: {sel_acc.get('name','?')}\n"
        f"<b>Range:</b> From ID <code>{start_id}</code> · {end_lbl}\n"
        f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
        f"<i>Running in the background.\n"
        f"Use /taskjobs to pause, resume, or stop.\n"
        f"Normal /fwd forwarding still works independently.</i>",
        reply_markup=ReplyKeyboardRemove()
    )