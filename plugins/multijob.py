"""
Multi Job Plugin
================
A "Multi Job" is a batch-only background copy operation.
Unlike Live Job (which watches for NEW messages), Multi Job copies a specific
range of old messages from a source to a target and then stops (done).

Key features:
  • All source types: public/private channels, groups, DMs, topics
  • Dual destinations (same as Live Job)
  • Simultaneous jobs running in parallel
  • Full global filter support
  • Pause / Resume / Stop / Delete per-job
  • Survives bot restart (resumes running jobs)

Commands:
  /multijob  — Open the Multi Job manager
"""
import re
import os
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

#  In-memory task registry 
_mj_tasks:  dict[str, asyncio.Task]  = {}
_mj_paused: dict[str, asyncio.Event] = {}   # set=running, clear=paused

#  Future-based ask() — immune to pyrofork stale-listener bug 
_mj_waiting: dict[int, asyncio.Future] = {}


from pyrogram import ContinuePropagation

@Client.on_message(filters.private, group=-11)
async def _mj_input_router(bot, message):
    """Route all private messages to any waiting _mj_ask() futures."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _mj_waiting:
        fut = _mj_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation


async def _mj_ask(bot, user_id: int, text: str, reply_markup=None, timeout: int = 300):
    """Send text and wait for the next private message from user_id."""
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    old = _mj_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _mj_waiting[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _mj_waiting.pop(user_id, None)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

COLL = "multijobs"


async def _mj_save(job: dict):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)


async def _mj_get(job_id: str) -> dict | None:
    return await db.db[COLL].find_one({"job_id": job_id})


async def _mj_list(user_id: int) -> list[dict]:
    return [j async for j in db.db[COLL].find({"user_id": user_id})]


async def _mj_delete(job_id: str):
    await db.db[COLL].delete_one({"job_id": job_id})


async def _mj_update(job_id: str, **kwargs):
    await db.db[COLL].update_one({"job_id": job_id}, {"$set": kwargs})


async def _mj_inc(job_id: str, n: int = 1):
    await db.db[COLL].update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})
    import asyncio
    asyncio.create_task(db.update_global_stats(batch_forward=n))


# ══════════════════════════════════════════════════════════════════════════════
# Filter helpers (global user filters)
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
# Forward helper — supports dual destination + topic threads
# ══════════════════════════════════════════════════════════════════════════════

async def _mj_forward(
    client, msg,
    to_chat: int, remove_caption: bool, cap_tpl: str | None, forward_tag: bool = False,
    thread_id: int = None,
    to_chat_2: int = None, thread_id_2: int = None,
    replacements: dict = None,
    remove_links_flag: bool = False
):
    from plugins.regix import custom_caption, remove_all_links
    import re

    # Compute caption
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

        try:
            if forward_tag:
                await client.forward_messages(
                    chat_id=chat, from_chat_id=msg.chat.id,
                    message_ids=msg.id, **kw
                )
            else:
                if is_text_replaced and not msg.media:
                    if not new_text or not new_text.strip():
                        return True # silently skip empty text
                    await client.send_message(chat_id=chat, text=new_text, **kw)
                else:
                    await client.copy_message(
                        chat_id=chat, from_chat_id=msg.chat.id,
                        message_id=msg.id, **kw
                    )
        except Exception as exc:
            err = str(exc).upper()
            if any(x in err for x in ["PEER_ID_INVALID", "CHAT_WRITE_FORBIDDEN", "USER_BANNED", "CHANNEL_PRIVATE", "CHAT_ADMIN_REQUIRED"]):
                raise ValueError(f"Fatal Chat Error: {exc}")

            if "RESTRICTED" not in err and "PROTECTED" not in err:
                try:
                    if not forward_tag:
                        await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                    else:
                        if is_text_replaced and not msg.media:
                            if not new_text or not new_text.strip():
                                return True # silently skip empty text
                            await client.send_message(chat_id=chat, text=new_text, **kw)
                        else:
                            await client.copy_message(chat_id=chat, from_chat_id=msg.chat.id, message_id=msg.id, **kw)
                    return True
                except Exception as inner_e:
                    inner_err = str(inner_e).upper()
                    if any(x in inner_err for x in ["PEER_ID_INVALID", "CHAT_WRITE_FORBIDDEN", "USER_BANNED", "CHANNEL_PRIVATE", "CHAT_ADMIN_REQUIRED"]):
                        raise ValueError(f"Fatal Chat Error: {inner_e}")
                    pass
            
            # --- Fallback to Download/Re-upload for restricted sources ---
            try:
                media_obj = getattr(msg, msg.media.value, None) if msg.media else None
                original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                if msg.media:
                    safe_name = f"downloads/{msg.id}_{original_name}" if original_name else f"downloads/{msg.id}"
                    fp = None
                    for _dl_try in range(3):
                        try:
                            fp = await client.download_media(msg, file_name=safe_name)
                            if fp: break
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2)
                        except Exception as dl_e:
                            err_dl = str(dl_e).upper()
                            if "TIMEOUT" in err_dl or "CONNECTION" in err_dl:
                                await asyncio.sleep(5)
                                continue
                            break
                    if not fp: raise Exception("DownloadFailed")
                    
                    up_kw = {"chat_id": chat, "caption": kw.get("caption", msg.caption or "")}
                    if thread: up_kw["message_thread_id"] = thread
                    
                    if msg.photo:      await client.send_photo(photo=fp, **up_kw)
                    elif msg.video:    await client.send_video(video=fp, file_name=original_name, **up_kw)
                    elif msg.document: await client.send_document(document=fp, file_name=original_name, **up_kw)
                    elif msg.audio:    await client.send_audio(audio=fp, file_name=original_name, **up_kw)
                    elif msg.voice:    await client.send_voice(voice=fp, **up_kw)
                    elif msg.animation: await client.send_animation(animation=fp, **up_kw)
                    elif msg.sticker:  await client.send_sticker(sticker=fp, **up_kw)
                    
                    import os
                    if os.path.exists(fp): os.remove(fp)
                else:
                    await client.send_message(chat_id=chat, text=new_text if new_text is not None else getattr(msg.text, "html", str(msg.text)) if msg.text else "", **kw)
            except Exception as fallback_e:
                logger.debug(f"[MultiJob _send_one] Fallback failed to {chat}: {fallback_e}")

    await _send_one(to_chat, thread_id)
    if to_chat_2:
        await _send_one(to_chat_2, thread_id_2)


# ══════════════════════════════════════════════════════════════════════════════
# Core batch runner
# ══════════════════════════════════════════════════════════════════════════════

BATCH_SIZE = 200


async def _run_multijob(job_id: str, user_id: int, bot=None):
    job = await _mj_get(job_id)
    if not job:
        return

    if job_id not in _mj_paused:
        ev = asyncio.Event()
        ev.set()
        _mj_paused[job_id] = ev
    pause_ev = _mj_paused[job_id]

    client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _mj_update(job_id, status="error", error="Account not found")
            return

        client  = await start_clone_bot(_CLIENT.client(acc))
        is_bot  = acc.get("is_bot", True)

        from_chat   = job["from_chat"]
        to_chat     = job["to_chat"]
        to_thread   = job.get("to_thread_id")
        to_chat_2   = job.get("to_chat_2")
        to_thread_2 = job.get("to_thread_id_2")
        end_id      = int(job.get("end_id") or 0)
        current     = int(job.get("current_id") or job.get("start_id") or 1)

        await _mj_update(job_id, status="running", error="")
        logger.info(f"[MultiJob {job_id}] Started. current={current} end={end_id}")

        # Warm up peer cache
        try:
            await client.get_chat(from_chat)
            await client.get_chat(to_chat)
            if to_chat_2:
                await client.get_chat(to_chat_2)
        except Exception as warn_e:
            logger.warning(f"[MultiJob {job_id}] Pre-fetch peer resolve warning: {warn_e}")

        consecutive_empty = 0
        batch_cycle = 0  # counter to refresh configs periodically
        
        # Load user settings once (refresh every 20 batches to pick up changes)
        disabled_types = await db.get_filters(user_id)
        configs        = await db.get_configs(user_id)
        filters_dict   = configs.get('filters', {})
        remove_caption = filters_dict.get('rm_caption', False)
        cap_tpl        = configs.get('caption')
        forward_tag    = configs.get('forward_tag', False)
        sleep_secs     = max(1, int(configs.get('duration', 1) or 1))
        replacements   = configs.get('replacements', {})

        #  Destination progress bar 
        def _mj_prog_text(fwd: int, total: int, status: str = "running") -> str:
            pct = min(int(fwd * 100 / total), 100) if total > 0 else 0
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            if status == "done":
                head = "✅ <b>Multi Job Complete!</b>"
                body = f"<b>All {fwd} files forwarded successfully.</b>"
            elif status == "stopped":
                head = "⏹ <b>Multi Job Stopped</b>"
                body = f"<b>Stopped at {fwd} / {total if total else '?'} files.</b>"
            elif status == "error":
                head = "‣  <b>Multi Job Error</b>"
                body = f"<b>Failed after forwarding {fwd} files.</b>"
            else:
                head = "📤 <b>Multi Job Running — please wait…</b>"
                body = f"<b>Files:</b> <code>{fwd}</code> / <code>{total if total else '?'}</code>"
            return (
                f"{head}\n\n"
                f"<code>[{bar}]</code>  <b>{pct}%</b>\n"
                f"{body}\n\n"
                f"<i>Powered by Arya Forward Bot</i>"
            )

        mj_total = max(0, end_id - int(job.get("start_id") or 1)) if end_id > 0 else 0
        mj_prog_msg_id = job.get("prog_msg_id", None)
        if not mj_prog_msg_id:
            try:
                sent = await client.send_message(to_chat, _mj_prog_text(0, mj_total), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
                mj_prog_msg_id = sent.id
                await _mj_update(job_id, prog_msg_id=mj_prog_msg_id)
                try: await client.pin_chat_message(to_chat, mj_prog_msg_id, disable_notification=True)
                except Exception: pass
            except Exception:
                mj_prog_msg_id = None

        mj_last_prog_update = 0.0
        mj_start_time = time.time()
        mj_fwd_at_start = int(job.get("forwarded", 0))
        # 


        while True:
            # Pause check
            await pause_ev.wait()

            # Stop check
            fresh = await _mj_get(job_id)
            if not fresh or fresh.get("status") in ("stopped", "error"):
                # Finalize progress bar on external stop
                if client and mj_prog_msg_id:
                    try:
                        fj = await _mj_get(job_id)
                        _fwd = fj.get("forwarded", 0) if fj else 0
                        await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "stopped"), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
                    except Exception: pass
                break

            # End check
            if end_id > 0 and current > end_id:
                await _mj_update(job_id, status="done", current_id=current)
                logger.info(f"[MultiJob {job_id}] Done — reached end_id {end_id}")
                # Finalize destination progress bar
                if client and mj_prog_msg_id:
                    try:
                        fj = await _mj_get(job_id)
                        _fwd = fj.get("forwarded", 0) if fj else 0
                        await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "done"), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
                        await client.unpin_chat_message(to_chat, mj_prog_msg_id)
                        async def _del_done_prog():
                            await asyncio.sleep(300)
                            try: await client.delete_messages(to_chat, mj_prog_msg_id)
                            except Exception: pass
                        asyncio.create_task(_del_done_prog())
                    except Exception: pass
                # Send completion report
                done_job = await _mj_get(job_id)
                if done_job:
                    try:
                        await bot.send_message(
                            user_id,
                            f"<b>✅ Multi Job Complete!</b>\n\n"
                            f"<b>Name:</b> {done_job.get('name', job_id[-6:])}\n"
                            f"<b>Source:</b> {done_job.get('from_title','?')}\n"
                            f"<b>Dest:</b> {done_job.get('to_title','?')}\n"
                            f"<b>Forwarded:</b> {done_job.get('forwarded', 0)} messages\n"
                            f"<b>Range:</b> {done_job.get('start_id',1)} → {end_id}\n\n"
                            f"<i>Use /multijob to manage jobs.</i>"
                        )
                    except Exception:
                        pass
                break

            # Refresh configs every 20 batches
            batch_cycle += 1
            if batch_cycle % 20 == 1:
                disabled_types = await db.get_filters(user_id)
                configs        = await db.get_configs(user_id)
                filters_dict   = configs.get('filters', {})
                remove_caption = filters_dict.get('rm_caption', False)
                cap_tpl        = configs.get('caption')
                forward_tag    = configs.get('forward_tag', False)
                sleep_secs     = max(1, int(configs.get('duration', 1) or 1))
                replacements   = configs.get('replacements', {})

            # Build batch
            batch_end = current + BATCH_SIZE - 1
            if end_id > 0:
                batch_end = min(batch_end, end_id)
            batch_ids = list(range(current, batch_end + 1))

            # Fetch messages
            try:
                msgs = await client.get_messages(from_chat, batch_ids)
                if not isinstance(msgs, list):
                    msgs = [msgs]
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[MultiJob {job_id}] Fetch error at {current}: {e}")
                await asyncio.sleep(10)
                current += BATCH_SIZE
                await _mj_update(job_id, current_id=current)
                continue

            valid = [m for m in msgs if m and not m.empty]
            valid.sort(key=lambda m: m.id)
            
            # Cross-chat filter: only needed for private groups (negative int IDs)
            # where Pyrogram may return messages from a different peer due to global ID overlaps.
            # For DM sources (positive int) or "me", skip this — get_messages already fetches
            # from the exact peer, and m.chat.id comparisons fail for DM messages.
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
                if consecutive_empty >= 200:  # 200 * 200 IDs = 40,000 gaps before giving up
                    await _mj_update(job_id, status="done", current_id=current)
                    logger.info(f"[MultiJob {job_id}] Done — no more messages after {current}")
                    # Finalize destination progress bar
                    if client and mj_prog_msg_id:
                        try:
                            fj = await _mj_get(job_id)
                            _fwd = fj.get("forwarded", 0) if fj else 0
                            await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "done"), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
                            await client.unpin_chat_message(to_chat, mj_prog_msg_id)
                            async def _del_empty_prog():
                                await asyncio.sleep(300)
                                try: await client.delete_messages(to_chat, mj_prog_msg_id)
                                except Exception: pass
                            asyncio.create_task(_del_empty_prog())
                        except Exception: pass
                    # Send completion report
                    done_job2 = await _mj_get(job_id)
                    if done_job2:
                        try:
                            await bot.send_message(
                                user_id,
                                f"<b>✅ Multi Job Complete!</b>\n\n"
                                f"<b>Name:</b> {done_job2.get('name', job_id[-6:])}\n"
                                f"<b>Source:</b> {done_job2.get('from_title','?')}\n"
                                f"<b>Dest:</b> {done_job2.get('to_title','?')}\n"
                                f"<b>Forwarded:</b> {done_job2.get('forwarded', 0)} messages\n\n"
                                f"<i>No more messages found after ID {current}.</i>"
                            )
                        except Exception:
                            pass
                    break
                current += BATCH_SIZE
                await _mj_update(job_id, current_id=current, consecutive_empty=consecutive_empty)
                await asyncio.sleep(2)
                continue

            consecutive_empty = 0

            # Filter by source topic if configured
            from_thread = job.get("from_thread")
            if from_thread:
                from_thread = int(from_thread)
                valid = [m for m in valid if _msg_in_topic(m, from_thread)]

            # Forward each valid message
            for msg in valid:
                await pause_ev.wait()

                fresh2 = await _mj_get(job_id)
                if not fresh2 or fresh2.get("status") in ("stopped",):
                    return

                if not _passes_filters(msg, disabled_types):
                    current = msg.id + 1
                    await _mj_update(job_id, current_id=current)
                    continue

                # Get links-filter flag for caption stripping
                _remove_links = 'links' in disabled_types
                await _mj_forward(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                   to_thread, to_chat_2, to_thread_2, replacements, _remove_links)
                
                current = msg.id + 1
                await _mj_update(job_id, current_id=current)
                await _mj_inc(job_id, 1)
                
                await asyncio.sleep(sleep_secs)

            # Advance cursor
            current = valid[-1].id + 1
            await _mj_update(job_id, current_id=current)

            #  Update destination progress bar (every 30s) 
            now_mj = time.time()
            if mj_prog_msg_id and (now_mj - mj_last_prog_update) >= 30:
                mj_last_prog_update = now_mj
                try:
                    fresh_j = await _mj_get(job_id)
                    _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                    # Dynamic ETA
                    _elapsed = max(1, now_mj - mj_start_time)
                    _delta   = _fwd - mj_fwd_at_start
                    _spd     = _delta / _elapsed
                    if _spd > 0 and end_id > 0:
                        _rem = max(0, end_id - current)
                        _eta_s = int(_rem / _spd)
                        _h, _r = divmod(_eta_s, 3600)
                        _m2 = _r // 60
                        _eta_str = f"{_h}h {_m2}m" if _h else f"{_m2}m"
                    else:
                        _eta_str = "Calculating..."
                    _pct = 0
                    if end_id > 0 and current > int(job.get("start_id") or 1):
                        _pct = min(99, int((current - int(job.get("start_id") or 1)) / max(1, end_id - int(job.get("start_id") or 1)) * 100))
                    _filled = _pct // 5
                    _bar = "█" * _filled + "░" * (20 - _filled)
                    _prog_txt = (
                        f"📤 <b>Multi Job Running — please wait…</b>\n\n"
                        f"<code>[{_bar}]</code>  <b>{_pct}%</b>\n"
                        f"<b>Files:</b> <code>{_fwd}</code> / <code>{end_id if end_id > 0 else '?'}</code>\n"
                        f"»  <b>ETA:</b> {_eta_str}\n\n"
                        f"<i>Powered by Arya Forward Bot</i>"
                    )
                    await client.edit_message_text(to_chat, mj_prog_msg_id, _prog_txt, parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
                except Exception:
                    pass

    except asyncio.CancelledError:
        logger.info(f"[MultiJob {job_id}] Cancelled")
        await _mj_update(job_id, status="stopped")
        if client and mj_prog_msg_id:
            try:
                fj = await _mj_get(job_id)
                _fwd = fj.get("forwarded", 0) if fj else 0
                await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "stopped"), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
                await client.unpin_chat_message(to_chat, mj_prog_msg_id)
                async def _del_cancelled_prog():
                    await asyncio.sleep(180)
                    try: await client.delete_messages(to_chat, mj_prog_msg_id)
                    except Exception: pass
                asyncio.create_task(_del_cancelled_prog())
            except Exception: pass
    except Exception as e:
        logger.error(f"[MultiJob {job_id}] Fatal: {e}")
        await _mj_update(job_id, status="error", error=str(e))
        if client and mj_prog_msg_id:
            try:
                fj = await _mj_get(job_id)
                _fwd = fj.get("forwarded", 0) if fj else 0
                await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "error"), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
            except Exception: pass
    finally:
        _mj_tasks.pop(job_id, None)
        _mj_paused.pop(job_id, None)
        if client:
            try: await client.stop()
            except Exception: pass


def _mj_start_task(job_id: str, user_id: int, bot=None) -> asyncio.Task:
    ev = asyncio.Event()
    ev.set()
    _mj_paused[job_id] = ev
    task = asyncio.create_task(_run_multijob(job_id, user_id, bot=bot))
    _mj_tasks[job_id] = task
    return task


# ══════════════════════════════════════════════════════════════════════════════
# Resume on bot restart
# ══════════════════════════════════════════════════════════════════════════════

async def resume_multi_jobs(user_id: int = None, bot=None):
    query = {"status": "running"}
    if user_id:
        query["user_id"] = user_id
    async for job in db.db[COLL].find(query):
        jid = job["job_id"]
        uid = job["user_id"]
        if jid not in _mj_tasks:
            _mj_start_task(jid, uid, bot=bot)
            logger.info(f"[MultiJob] Resumed {jid} for user {uid}")


# ══════════════════════════════════════════════════════════════════════════════
# UI helpers
# ══════════════════════════════════════════════════════════════════════════════

def _mj_emoji(status: str) -> str:
    return {
        "running": "🟢", "paused": "⏸",
        "stopped": "🔴", "done": "✅", "error": "❌"
    }.get(status, "⭘")


async def _render_mj_list(bot, user_id: int, msg_or_query):
    jobs  = await _mj_list(user_id)
    is_cb = hasattr(msg_or_query, "message")

    if not jobs:
        text = (
            "<b>»  Multi Jobs</b>\n\n"
            "<i>No jobs yet.\n\n"
            "A <b>Multi Job</b> copies a specific range of messages from any "
            "source channel/group to your target — fully in the background.\n\n"
            "✅ All source types (public, private, DMs, topics)\n"
            "✅ Dual destinations\n"
            "✅ Multiple jobs run simultaneously\n"
            "✅ Pause / Resume support\n"
            "✅ Survives bot restarts\n\n"
            "👇 Create your first Multi Job below!</i>"
        )
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Mᴜʟᴛɪ Jᴏʙ", callback_data="mj#new")],
            [InlineKeyboardButton("🔙 Bᴀᴄᴋ", callback_data="back")]
        ])
    else:
        lines = ["<b>»  Your Multi Jobs</b>\n"]
        for j in jobs:
            st   = _mj_emoji(j.get("status", "stopped"))
            fwd  = j.get("forwarded", 0)
            cur  = j.get("current_id", "?")
            end  = j.get("end_id", 0) or "∞"
            start_id = j.get("start_id", 1)
            fetched = cur - start_id if isinstance(cur, int) and isinstance(start_id, int) and cur >= start_id else 0
            err  = f" <code>[{j.get('error','')}]</code>" if j.get("status") == "error" else ""
            d2   = f" + {j.get('to_title_2','?')}" if j.get("to_chat_2") else ""
            default_name = f"Multi Job {j['job_id'][-6:]}"
            name = j.get("name", default_name)
            lines.append(
                f"{st} <b>{name}</b>\n"
                f"  └ <i>{j.get('from_title','?')} → {j.get('to_title','?')}{d2}</i>\n"
                f"  └ <code>[{j['job_id'][-6:]}]</code>  ✅{fwd}  » {fetched}  » {cur}/{end}{err}\n"
            )
        import datetime
        now_str = datetime.datetime.now().strftime("%I:%M:%S %p")
        text = "\n".join(lines) + f"\n\n<i>Last refreshed: {now_str}</i>"

        btns_list = []
        for j in jobs:
            st   = j.get("status", "stopped")
            jid  = j["job_id"]
            short = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏸ Pᴀᴜsᴇ [{short}]", callback_data=f"mj#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mj#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ Rᴇsᴜᴍᴇ [{short}]", callback_data=f"mj#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mj#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Sᴛᴀʀᴛ [{short}]", callback_data=f"mj#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ Iɴғᴏ [{short}]", callback_data=f"mj#info#{jid}"))
            row.append(InlineKeyboardButton(f"✏️ Nᴀᴍᴇ [{short}]", callback_data=f"mj#rename#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 Dᴇʟᴇᴛᴇ [{short}]",  callback_data=f"mj#del#{jid}"))
            btns_list.append(row)

        btns_list.append([InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Mᴜʟᴛɪ Jᴏʙ", callback_data="mj#new")])
        btns_list.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ",           callback_data="mj#list")])
        btns_list.append([InlineKeyboardButton("🔙 Bᴀᴄᴋ", callback_data="back")])
        btns = InlineKeyboardMarkup(btns_list)

    try:
        if is_cb:
            await msg_or_query.message.edit_text(text, reply_markup=btns)
        else:
            await msg_or_query.reply_text(text, reply_markup=btns)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Commands
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command(["multijob", "multijobs", "mj"]))
async def multijob_cmd(bot, message):
    await _render_mj_list(bot, message.from_user.id, message)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^mj#list$'))
async def mj_list_cb(bot, query):
    await query.answer()
    await _render_mj_list(bot, query.from_user.id, query)
@Client.on_callback_query(filters.regex(r'^mj#rename#'))
async def mj_rename_cb(bot, query):
    user_id = query.from_user.id
    job_id = query.data.split("#", 2)[2]
    await query.message.delete()
    
    r = await _mj_ask(bot, user_id,
        "<b>✏️ Edit Multi Job Name</b>\n\nSend a new name for this job:",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("/cancel")]], resize_keyboard=True, one_time_keyboard=True))
    if "/cancel" not in r.text.lower():
        await db.db[COLL].update_one({"job_id": job_id}, {"$set": {"name": r.text.strip()[:100]}})
        await bot.send_message(user_id, f"✅ Multi Job renamed to <b>{r.text.strip()[:100]}</b>", reply_markup=ReplyKeyboardRemove())
    await _render_mj_list(bot, user_id, r)


@Client.on_callback_query(filters.regex(r'^mj#new$'))
async def mj_new_cb(bot, query):
    user_id = query.from_user.id
    await query.message.delete()
    await _create_mj_flow(bot, user_id)


@Client.on_callback_query(filters.regex(r'^mj#info#'))
async def mj_info_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    job = await _mj_get(job_id)
    if not job:
        return await query.answer("Job not found!", show_alert=True)

    import datetime
    created   = datetime.datetime.fromtimestamp(job.get("created", 0)).strftime("%d %b %Y %H:%M")
    st        = _mj_emoji(job.get("status", "stopped"))
    thread_id = job.get("to_thread_id")
    topic_lbl = f"\n<b>Topic Thread:</b> <code>{thread_id}</code>" if thread_id else ""
    
    start_id = job.get("start_id", 1)
    cur = job.get("current_id", "?")
    fetched = cur - start_id if isinstance(cur, int) and isinstance(start_id, int) and cur >= start_id else 0

    dest2_lbl = ""
    if job.get("to_chat_2"):
        t2 = job.get("to_thread_id_2")
        tp2 = f" [Thread {t2}]" if t2 else ""
        dest2_lbl = f"\n<b>Dest 2:</b> {job.get('to_title_2','?')}{tp2}"

    text = (
        f"<b>»  Multi Job Info</b>\n\n"
        f"<b>ID:</b> <code>{job_id[-6:]}</code>\n"
        f"<b>Name:</b> {job.get('name', 'Default')}\n"
        f"<b>Status:</b> {st} {job.get('status','?')}\n"
        f"<b>Source:</b> {job.get('from_title','?')}\n"
        f"<b>Dest 1:</b> {job.get('to_title','?')}{topic_lbl}"
        f"{dest2_lbl}\n"
        f"<b>Fetched messages:</b> {fetched}\n"
        f"<b>Forwarded:</b> {job.get('forwarded', 0)}\n"
        f"<b>Current ID progress:</b> {job.get('current_id', '?')} / {job.get('end_id', 0) or '∞'}\n"
        f"<b>Created:</b> {created}\n"
    )
    if job.get("error"):
        text += f"\n<b>‣  Error:</b> <code>{job['error']}</code>"

    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Bᴀᴄᴋ", callback_data="mj#list")
    ]]))


@Client.on_callback_query(filters.regex(r'^mj#pause#'))
async def mj_pause_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _mj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    ev = _mj_paused.get(job_id)
    if ev:
        ev.clear()
    await _mj_update(job_id, status="paused")
    await query.answer("⏸ Job paused.", show_alert=False)
    await _render_mj_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^mj#resume#'))
async def mj_resume_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _mj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    ev = _mj_paused.get(job_id)
    if ev and job_id in _mj_tasks and not _mj_tasks[job_id].done():
        ev.set()
        await _mj_update(job_id, status="running")
        await query.answer("▶️ Resumed!", show_alert=False)
    else:
        await _mj_update(job_id, status="running")
        _mj_start_task(job_id, user_id)
        await query.answer("▶️ Restarted from saved position!", show_alert=False)
    await _render_mj_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^mj#stop#'))
async def mj_stop_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _mj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    task = _mj_tasks.pop(job_id, None)
    if task and not task.done():
        task.cancel()
    ev = _mj_paused.pop(job_id, None)
    if ev: ev.set()
    await _mj_update(job_id, status="stopped")
    await query.answer("⏹ Job stopped.", show_alert=False)
    await _render_mj_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^mj#start#'))
async def mj_start_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _mj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    if job_id in _mj_tasks and not _mj_tasks[job_id].done():
        return await query.answer("Already running!", show_alert=True)
    await _mj_update(job_id, status="running")
    _mj_start_task(job_id, user_id)
    await query.answer("▶️ Job started!", show_alert=False)
    await _render_mj_list(bot, user_id, query)


@Client.on_callback_query(filters.regex(r'^mj#del#'))
async def mj_del_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _mj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    task = _mj_tasks.pop(job_id, None)
    if task and not task.done():
        task.cancel()
    ev = _mj_paused.pop(job_id, None)
    if ev: ev.set()
    await _mj_delete(job_id)
    await query.answer("🗑 Job deleted.", show_alert=False)
    await _render_mj_list(bot, user_id, query)


# ══════════════════════════════════════════════════════════════════════════════
# Create Multi Job — Interactive flow
# ══════════════════════════════════════════════════════════════════════════════

async def _mj_ask_dest(bot, user_id: int, channels: list, step_label: str, optional: bool = False) -> tuple:
    """Ask user to pick a saved channel. Returns (chat_id, title, cancelled)."""
    btns = [[KeyboardButton(ch['title'])] for ch in channels]
    if optional:
        btns.append([KeyboardButton("⏭ Skip (no second destination)")])
    btns.append([KeyboardButton("/cancel")])

    resp = await _mj_ask(bot, user_id, step_label,
                          reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True, one_time_keyboard=True))
    txt = resp.text.strip()
    if "/cancel" in txt:
        await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
        return None, None, True
    if optional and "skip" in txt.lower():
        return None, None, False
    for ch in channels:
        if ch['title'] == txt:
            return ch['chat_id'], ch['title'], False
    return None, None, False


async def _mj_ask_topic(bot, user_id: int, dest_label: str) -> int | None:
    """Ask for optional topic thread ID."""
    r = await _mj_ask(bot, user_id,
        f"<b>Topic Thread for {dest_label} (Optional)</b>\n\n"
        "• Send the <b>Thread ID</b> if you want to post inside a specific group topic\n"
        "• Send <b>0</b> to post in the main chat\n\n"
        "<i>Find Thread ID: open topic in Telegram Web → number after <code>/topics/</code> in URL</i>",
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


async def _create_mj_flow(bot, user_id: int):
    # Clear any stale future
    old = _mj_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()

    #  Step 1: Name 
    name_r = await _mj_ask(bot, user_id,
        "<b>»  Create Multi Job — Step 1/6</b>\n\n"
        "Send a name for this job, or press 'Default' to use a random name.",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Default")], [KeyboardButton("/cancel")]], resize_keyboard=True, one_time_keyboard=True))
    if "/cancel" in name_r.text:
        return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
    
    job_name = name_r.text.strip()[:100]
    if job_name.lower() == "default":
        job_name = None

    #  Step 2: Account 
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id,
            "<b>❌ No accounts found. Add one in /settings → Accounts first.</b>")

    acc_btns = [[KeyboardButton(
        f"{'»  Bot' if a.get('is_bot', True) else '»  Userbot'}: "
        f"{a.get('username') or a.get('name', 'Unknown')} [{a['id']}]"
    )] for a in accounts]
    acc_btns.append([KeyboardButton("/cancel")])

    acc_r = await _mj_ask(bot, user_id,
        "<b>»  Create Multi Job — Step 2/5</b>\n\n"
        "Choose which account to use:\n"
        "<i>(Userbot required for private/restricted channels)</i>",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in acc_r.text:
        return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in acc_r.text and "]" in acc_r.text:
        try: acc_id = int(acc_r.text.split('[')[-1].split(']')[0])
        except Exception: pass
    sel_acc = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
    is_bot  = sel_acc.get("is_bot", True)

    #  Step 3: Source 
    src_r = await _mj_ask(bot, user_id,
        "<b>Step 3/5 — Source Chat</b>\n\n"
        "Send one of:\n"
        "• <code>@username</code> or channel link\n"
        "• Numeric ID (e.g. <code>-1001234567890</code>)\n"
        "• <code>me</code> for Saved Messages (Userbot only)\n\n"
        "/cancel to abort",
        reply_markup=ReplyKeyboardRemove())

    if src_r.text.strip().startswith("/cancel"):
        return await bot.send_message(user_id, "<b>Cancelled.</b>")

    from_chat_raw = src_r.text.strip()
    if from_chat_raw.lower() in ("me", "saved"):
        if is_bot:
            return await bot.send_message(user_id,
                "<b>❌ Saved Messages require a Userbot account.</b>",
                reply_markup=ReplyKeyboardRemove())
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

    from_thread = await _mj_ask_topic(bot, user_id, "Source")

    #  Step 4: Primary Destination 
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(user_id,
            "<b>❌ No target channels saved. Add via /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    to_chat, to_title, cancelled = await _mj_ask_dest(bot, user_id, channels,
        "<b>Step 4/5 — Destination</b>\n\nWhere should messages be sent?")
    if cancelled or not to_chat:
        return

    to_thread = await _mj_ask_topic(bot, user_id, "Destination")

    #  Step 5: Message Range 
    range_r = await _mj_ask(bot, user_id,
        "<b>Step 5/5 — Message Range</b>\n\n"
        "Choose which messages to copy:\n\n"
        "• Send <b>ALL</b> to copy from the very first message\n"
        "• Send a <b>start ID</b> (e.g. <code>100</code>) to start from that message\n"
        "• Send <b>start:end</b> (e.g. <code>100:5000</code>) for a specific range\n\n"
        "<i>The job stops automatically when all messages in the range are copied.</i>",
        reply_markup=ReplyKeyboardRemove())

    if "/cancel" in range_r.text:
        return await bot.send_message(user_id, "<b>Cancelled.</b>")

    start_id = from_thread if from_thread else 1
    end_id   = 0
    rtext    = range_r.text.strip().lower()
    if rtext != "all":
        if ":" in rtext:
            parts = rtext.split(":", 1)
            try: start_id = int(parts[0].strip())
            except Exception: pass
            try: end_id   = int(parts[1].strip())
            except Exception: pass
        else:
            try: start_id = int(rtext)
            except Exception: pass

    #  Save & Start 
    job_id = f"mj-{user_id}-{int(time.time())}"
    job = {
        "job_id":         job_id,
        "user_id":        user_id,
        "name":           job_name if job_name else f"Multi Job {job_id[-6:]}",
        "account_id":     sel_acc["id"],
        "from_chat":      from_chat,
        "from_title":     from_title,
        "from_thread":    from_thread,
        "to_chat":        to_chat,
        "to_title":       to_title,
        "to_thread_id":   to_thread,
        "start_id":       start_id,
        "end_id":         end_id,
        "current_id":     start_id,
        "status":         "running",
        "created":        int(time.time()),
        "forwarded":      0,
        "consecutive_empty": 0,
        "error":          "",
    }
    await _mj_save(job)
    _mj_start_task(job_id, user_id, bot=bot)

    end_lbl   = f"to ID <code>{end_id}</code>" if end_id else "all messages"
    thread_lbl = f" → Topic <code>{to_thread}</code>" if to_thread else ""

    await bot.send_message(
        user_id,
        f"<b>✅ Multi Job Created & Started!</b>\n\n"
        f"»  <b>{from_title}</b> → <b>{to_title}</b>{thread_lbl}\n"
        f"<b>Account:</b> {'»  Bot' if is_bot else '»  Userbot'}: {sel_acc.get('name','?')}\n"
        f"<b>Range:</b> From ID <code>{start_id}</code> · {end_lbl}\n"
        f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
        f"<i>Running in background.\nUse /multijob to manage.</i>",
        reply_markup=ReplyKeyboardRemove()
    )
