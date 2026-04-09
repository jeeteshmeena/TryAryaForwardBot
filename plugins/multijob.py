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
from plugins.job_queue import AryaJobQueue

#  In-memory task registry 
_mj_tasks:  dict[str, asyncio.Task]  = {}
_mj_paused: dict[str, asyncio.Event] = {}   # set=running, clear=paused

#  Future-based ask() — immune to pyrofork stale-listener bug 
_mj_waiting: dict[int, asyncio.Future] = {}


# ─── Client health-check / reconnect ────────────────────────────────────
async def _mj_ensure_client_alive(client):
    """
    Verify the Pyrogram client is connected. If dead, attempts cold restart up to 3 times.
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
                logger.warning(f"[MultiJob] Client dead (attempt {attempt+1}): {e} — reconnecting…")
                try:
                    cname = getattr(client, 'name', None)
                    if cname:
                        from plugins.test import release_client as _rc_mj
                        await _rc_mj(cname)
                except Exception: pass
                try: await client.stop()
                except Exception: pass
                
                try:
                    client = await start_clone_bot(client)
                    await asyncio.sleep(1)
                    continue
                except Exception as re_err:
                    logger.error(f"[MultiJob] Restart attempt {attempt+1} failed: {re_err}")
                    await asyncio.sleep(3)
            else:
                raise   # not a connection error — let caller handle it
    raise RuntimeError("MULTIJOB_RECONNECT_FAILED: client failed to reconnect after 3 attempts")


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
        # Use local flag — do NOT mutate nonlocal forward_tag as it would
        # contaminate the second destination call and all future messages.
        use_forward_tag = forward_tag
        if new_caption is not None or is_text_replaced:
            # Telegram CANNOT modify text/captions of natively forwarded messages.
            # If the user wants to wipe captions, remove links, or replace text, we MUST use copy_message.
            use_forward_tag = False

        kw = {"message_thread_id": thread} if thread else {}
        if new_caption is not None:
            kw["caption"] = new_caption

        for _send_attempt in range(4):
            try:
                if use_forward_tag:
                    await client.forward_messages(
                        chat_id=chat, from_chat_id=msg.chat.id,
                        message_ids=msg.id, **kw
                    )
                else:
                    if is_text_replaced and not msg.media:
                        if not new_text or not new_text.strip():
                            return True  # silently skip empty text
                        await client.send_message(chat_id=chat, text=new_text, **kw)
                    else:
                        await client.copy_message(
                            chat_id=chat, from_chat_id=msg.chat.id,
                            message_id=msg.id, **kw
                        )
                return True  # success
            except FloodWait as fw:
                # Respect Telegram's rate limit — wait and retry
                logger.warning(f"[MultiJob _send_one] FloodWait {fw.value}s to {chat}")
                await asyncio.sleep(fw.value + 2)
                continue
            except Exception as exc:
                err = str(exc).upper()
                if any(x in err for x in ["PEER_ID_INVALID", "CHAT_WRITE_FORBIDDEN", "USER_BANNED", "CHANNEL_PRIVATE", "CHAT_ADMIN_REQUIRED"]):
                    raise ValueError(f"Fatal Chat Error: {exc}")
                if "RESTRICTED" in err or "PROTECTED" in err:
                    # Try copy → forward fallback once for protected content
                    try:
                        await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                        return True
                    except Exception:
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
                                    if fp: 
                                        await db.update_global_stats(total_files_downloaded=1)
                                        break
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
                            
                            await db.update_global_stats(total_files_uploaded=1)
                            import os
                            if os.path.exists(fp): os.remove(fp)
                        else:
                            await client.send_message(chat_id=chat, text=new_text if new_text is not None else getattr(msg.text, "html", str(msg.text)) if msg.text else "", **kw)
                        return True
                    except Exception as fallback_e:
                        logger.debug(f"[MultiJob _send_one] Fallback failed to {chat}: {fallback_e}")
                        return False

                # If transient, try to heal before retrying
                is_transient = any(k in err for k in ("TIMEOUT", "CONNECTION", "READ", "RESET", "NOT BEEN STARTED", "DISCONNECTED", "NOT CONNECTED", "PING", "FLOOD"))
                if is_transient:
                    try:
                        pass 
                    except Exception:
                        pass
                
                # For transient errors, retry up to 4 attempts
                if _send_attempt >= 3:
                    logger.warning(f"[MultiJob _send_one] All retries exhausted for msg {msg.id} to {chat}: {exc}")
                    if is_transient:
                        raise ConnectionError(f"Transient error persisted after 4 retries: {exc}")
                    return False
                await asyncio.sleep(5 * (_send_attempt + 1))
                continue

    success1 = await _send_one(to_chat, thread_id)
    success2 = False
    if to_chat_2:
        success2 = await _send_one(to_chat_2, thread_id_2)
    return success1 or success2


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
    _mj_queue_acquired = False
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _mj_update(job_id, status="error", error="Account not found")
            return

        is_bot  = acc.get("is_bot", True)

        is_force = job.get("force_active", False)
        if is_force:
            await _mj_update(job_id, force_active=False)
            pos = 0
        else:
            # ── Queue system: limit concurrent Multi Jobs ─────────────────────────
            pos = await AryaJobQueue.acquire(job_id, "multijob")
            _mj_queue_acquired = True
            
        if pos > 0 and bot:
            try:
                await bot.send_message(user_id,
                    f"⏳ <b>Multi Job Queued</b>\n\n"
                    f"All {AryaJobQueue.max_slots('multijob')} Multi Job slots are busy.\n"
                    f"Job <code>[{job_id[-6:]}]</code> will start automatically at position #{pos}.\n"
                    f"You can close Telegram; it runs in the background.")
            except Exception: pass
            # Now we hold the slot — notify start
            try:
                await bot.send_message(user_id,
                    f"▶️ Multi Job <code>[{job_id[-6:]}]</code> now has a slot and is starting.")
            except Exception: pass

        # ─── Initial client check ─────────────────────────
        client = await start_clone_bot(_CLIENT.client(acc))
        client = await _mj_ensure_client_alive(client)

        from_chat   = job["from_chat"]
        to_chat     = job["to_chat"]
        to_thread   = job.get("to_thread_id")
        to_chat_2   = job.get("to_chat_2")
        to_thread_2 = job.get("to_thread_id_2")
        
        # ── Protected Chat Guard ───────────────────────────────────────────────
        from plugins.utils import check_chat_protection
        prot_err = await check_chat_protection(job["user_id"], from_chat)
        if prot_err:
            await _mj_update(job_id, status="error", error=prot_err)
            try:
                await bot.send_message(job["user_id"], prot_err)
            except Exception:
                pass
            return
        # ──────────────────────────────────────────────────────────────────────

        end_id      = int(job.get("end_id") or 0)
        current     = int(job.get("current_id") or job.get("start_id") or 1)

        await _mj_update(job_id, status="running", error="")
        logger.info(f"[MultiJob {job_id}] Started. current={current} end={end_id}")

        # Warm up peer cache and strictly correctly identify DM vs Group
        is_dm_source = False
        from pyrogram.enums import ChatType
        
        if str(from_chat).lower() in ("me", "saved"):
            is_dm_source = True
        else:
            try:
                peer_chat = await client.get_chat(from_chat)
                if peer_chat.type in (ChatType.PRIVATE, ChatType.BOT):
                    is_dm_source = True
                from_chat = peer_chat.id  # Lock in numeric ID to prevent pyrogram confusion
            except Exception as warn_e:
                logger.warning(f"[MultiJob {job_id}] Pre-fetch peer resolve warning: {warn_e}")
                if isinstance(from_chat, int) and from_chat >= 0:
                    is_dm_source = True
        
        try:
            for _wchat in [to_chat] + ([to_chat_2] if to_chat_2 else []):
                try:
                    await client.get_chat(_wchat)
                except FloodWait as fw:
                    logger.warning(f"[MultiJob {job_id}] FloodWait {fw.value}s on get_chat({_wchat})")
                    await asyncio.sleep(fw.value + 2)
                except Exception:
                    pass
        except Exception:
            pass

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
        acc_name = acc.get('name', 'Userbot')

        def _mj_prog_text(fwd: int, total: int, status: str = "running") -> str:
            if status == "done":
                return (
                    f"➤ <b>✓ ɩᴜʟᴛɪ ᴊᴏʙ ᴄᴏᴍᴘʟᴇᴛᴇ!</b>\n"
                    f"➤ <b>ᴀᴄᴄᴏᴜɴᴛ:</b> <code>{acc_name}</code>\n\n"
                    f"➤ ᴀʟʟ <u>{fwd}</u> ғɪʟᴇѕ ʜᴀᴠᴇ ʙᴇᴇɴ ᴍᴏᴠᴇᴅ ѕᴜᴄᴄᴇѕѕғᴜʟʟʟʸ!\n\n"
                    f"<i>ᴘᴏᴡᴇʀᴇᴅ ʙʸ ᴀʀʸᴀ ғᴏʀᴡᴀʀᴅ ʙᴏᴛ</i>"
                )
            elif status == "stopped":
                return (
                    f"➤ <b>⏹ ᴊᴏʙ ѕᴛᴏᴘᴘᴇᴅ</b>\n"
                    f"➤ <b>ᴀᴄᴄᴏᴜɴᴛ:</b> <code>{acc_name}</code>\n\n"
                    f"➤ ғɪʟᴇѕ ѕᴇɴᴛ: <code>{fwd}</code> / <code>{total if total else '?'}</code>\n\n"
                    f"<i>ᴘᴏᴡᴇʀᴇᴅ ʙʸ ᴀʀʸᴀ ғᴏʀᴡᴀʀᴅ ʙᴏᴛ</i>"
                )
            elif status == "error":
                return (
                    f"➤ <b>⚠️ ᴊᴏʙ ᴇʀʀᴏʀ</b>\n"
                    f"➤ <b>ᴀᴄᴄᴏᴜɴᴛ:</b> <code>{acc_name}</code>\n\n"
                    f"➤ ғɪʟᴇѕ ѕᴇɴᴛ ʙᴇғᴏʀᴇ ᴇʀʀᴏʀ: <code>{fwd}</code>\n\n"
                    f"<i>ᴘᴏᴡᴇʀᴇᴅ ʙʸ ᴀʀʸᴀ ғᴏʀᴡᴀʀᴅ ʙᴏᴛ</i>"
                )
            else:
                total_str = str(total) if total else '?'
                return (
                    f"<b>➤ {acc_name}</b>\n"
                    f"➤ ᴛʀаɴѕғᴇʀʀɪɴɢ ғɪʟᴇѕ ᴘʟᴇᴀѕᴇ ᴡᴀɪᴛ...\n\n"
                    f"➤ <b>ғɪʟᴇѕ ѕᴇɴᴛ:</b> <code>{fwd}</code> / <code>{total_str}</code>\n\n"
                    f"<i>ᴘᴏᴡᴇʀᴇᴅ ʙʸ ᴀʀʸᴀ ғᴏʀᴡᴀʀᴅ ʙᴏᴛ</i>"
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

        # \u2500\u2500 BOT DM BATCH (userbot + non-channel source) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        # get_messages(id_list) without a channel peer queries the GLOBAL inbox.
        if not is_bot and is_dm_source:
            logger.info(f"[MultiJob {job_id}] DM source — collecting via get_chat_history")
            # CRITICAL Resume FIX: Use current, not start_id, otherwise we duplicate or fail offset!
            start_id_val = current
            dm_msgs = []
            try:
                # Ensure client is alive before huge history fetch
                client = await _mj_ensure_client_alive(client)
                async for m in client.get_chat_history(from_chat):
                    if m.empty or m.service:
                        continue
                    if m.id < start_id_val:
                        break
                    if end_id > 0 and m.id > end_id:
                        continue
                    dm_msgs.append(m)
            except Exception as e:
                logger.warning(f"[MultiJob {job_id}] DM collect error: {e}")

            dm_msgs.sort(key=lambda m: m.id)
            logger.info(f"[MultiJob {job_id}] DM batch: {len(dm_msgs)} msgs to forward")

            for msg in dm_msgs:
                await pause_ev.wait()
                fresh2 = await _mj_get(job_id)
                if not fresh2 or fresh2.get("status") in ("stopped",):
                    return
                if not _passes_filters(msg, disabled_types):
                    current = msg.id + 1
                    await _mj_update(job_id, current_id=current)
                    continue
                _remove_links = 'links' in disabled_types

                # CHECKPOINT: record we're AT this message before forwarding
                await _mj_update(job_id, current_id=msg.id)

                client = await _mj_ensure_client_alive(client)
                success = await _mj_forward(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                   to_thread, to_chat_2, to_thread_2, replacements, _remove_links)
                current = msg.id + 1
                await _mj_update(job_id, current_id=current)
                if success:
                    await _mj_inc(job_id, 1)
                else:
                    logger.warning(f"[MultiJob {job_id}] DM: Forward of msg {msg.id} failed — advancing past")

                now_mj = time.time()
                if mj_prog_msg_id and (now_mj - mj_last_prog_update) >= 10:
                    mj_last_prog_update = now_mj
                    try:
                        fresh_j = await _mj_get(job_id)
                        _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                        from pyrogram.enums import ParseMode
                        await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "running"), parse_mode=ParseMode.HTML)
                    except Exception: pass

                await asyncio.sleep(sleep_secs)

            await _mj_update(job_id, status="done", current_id=current)
            fj = await _mj_get(job_id)
            _fwd = fj.get("forwarded", 0) if fj else len(dm_msgs)
            if client and mj_prog_msg_id:
                try:
                    from pyrogram.enums import ParseMode
                    await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "done"), parse_mode=ParseMode.HTML)
                    await client.unpin_chat_message(to_chat, mj_prog_msg_id)
                except Exception: pass
            if bot and fj:
                try:
                    await bot.send_message(user_id,
                        f"<b>\u2705 Multi Job Complete!</b>\n\n"
                        f"<b>Name:</b> {fj.get('name', job_id[-6:])}\n"
                        f"<b>Source:</b> {fj.get('from_title','?')}\n"
                        f"<b>Dest:</b> {fj.get('to_title','?')}\n"
                        f"<b>Forwarded:</b> {_fwd} messages")
                except Exception: pass
            return

        # \u2500\u2500 CHANNEL/GROUP: original ID-range loop \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
            # For userbot + DM/username source: get_messages() uses
            # messages.GetMessages WITHOUT a peer → looks up IDs in global inbox
            # (returns wrong messages from saved msgs or other chats).
            # Always use get_chat_history for non-channel DM sources.
            try:
                if not is_bot and is_dm_source:
                    # get_chat_history paginates newest→oldest; reverse to get chronological
                    batch_hist = []
                    async for m in client.get_chat_history(from_chat, limit=BATCH_SIZE, offset_id=current):
                        batch_hist.append(m)
                    msgs = list(reversed(batch_hist))
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                else:
                    msgs = await client.get_messages(from_chat, batch_ids)
                    if not isinstance(msgs, list):
                        msgs = [msgs]
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                err_str = str(e).upper()
                if any(x in err_str for x in ["PEER_ID_INVALID", "CHANNEL_INVALID", "USERNAME_INVALID", "CHAT_ID_INVALID"]):
                    logger.error(f"[MultiJob {job_id}] Fatal Source Error: {e}")
                    await _mj_update(job_id, status="error", error=f"Source Invalid: {e}")
                    break
                    
                is_transient = any(k in err_str for k in (
                    "TIMEOUT", "CONNECTION", "READ", "RESET", "DISCONNECT",
                    "NOT BEEN STARTED", "NOT CONNECTED", "CLOSED DATABASE",
                    "NETWORK", "SOCKET", "PING", "MULTIJOB_RECONNECT_FAILED"
                ))
                if is_transient:
                    logger.warning(f"[MultiJob {job_id}] Transient fetch error at {current}: {e}. Healing client...")
                    try:
                        client = await _mj_ensure_client_alive(client)
                    except Exception: pass
                    # CRITICAL: Do NOT advance current — retry the same batch.
                    await asyncio.sleep(10)
                    continue

                # Unknown / non-transient API error — do NOT skip hundreds of IDs.
                # Sleep and retry; if it keeps happening the outer handler will catch it.
                logger.warning(f"[MultiJob {job_id}] Unknown fetch error at {current}: {e} — retrying batch in 30s")
                await asyncio.sleep(30)
                continue

            valid = [m for m in msgs if m and not m.empty]
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

                # ── CHECKPOINT before forwarding ──────────────────────────────────
                # Write current_id = msg.id BEFORE attempting the forward.
                # If the bot crashes or the connection dies mid-forward, the DB
                # still points AT this message so restart will retry it — not skip it.
                # old code wrote msg.id+1 AFTER forward; if forward failed and the
                # job then crashed, the cursor was already past the failed message.
                await _mj_update(job_id, current_id=msg.id)
                # ─────────────────────────────────────────────────────────────────

                # Heal client connection before forward
                client = await _mj_ensure_client_alive(client)

                _remove_links = 'links' in disabled_types
                success = await _mj_forward(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                   to_thread, to_chat_2, to_thread_2, replacements, _remove_links)

                # Advance cursor past this message in all cases.
                # If success=False it means _mj_forward exhausted its 4 retries
                # (permanently skippable content — protected/restricted/wrong type).
                # We still advance so the job doesn't stall on unforwardable content.
                current = msg.id + 1
                await _mj_update(job_id, current_id=current)
                if success:
                    await _mj_inc(job_id, 1)
                else:
                    logger.warning(f"[MultiJob {job_id}] Forward of msg {msg.id} failed after retries — advancing past it")

                await asyncio.sleep(sleep_secs)

            # Advance cursor — guard against valid being empty after topic-filter
            if valid:
                current = valid[-1].id + 1
            else:
                current += BATCH_SIZE  # skip the batch that had no topic-matching msgs
            await _mj_update(job_id, current_id=current)

            #  Update destination progress bar (every 10s) 
            now_mj = time.time()
            if mj_prog_msg_id and (now_mj - mj_last_prog_update) >= 10:
                mj_last_prog_update = now_mj
                try:
                    fresh_j = await _mj_get(job_id)
                    _fwd = fresh_j.get("forwarded", 0) if fresh_j else 0
                    from pyrogram.enums import ParseMode
                    await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "running"), parse_mode=ParseMode.HTML)
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
        err_str = str(e)
        err_upper = err_str.upper()
        _MJ_TRANSIENT = (
            "CONNECTION", "TIMEOUT", "NETWORK", "PING", "SOCKET", "RESET",
            "NOT BEEN STARTED", "NOT CONNECTED", "DISCONNECTED",
            "CONNECTION LOST", "CLOSED DATABASE",
            "MULTIJOB_RECONNECT_FAILED",   # raised by _mj_ensure_client_alive
        )
        if any(kw in err_upper for kw in _MJ_TRANSIENT):
            # Transient network / connection error — auto-restart instead of
            # permanently marking the job as error so no files are missed.
            logger.warning(f"[MultiJob {job_id}] Transient outer error: {err_str} — auto-restarting in 30s")
            await _mj_update(job_id, error=f"[Auto-reconnect] {err_str[:80]}")
            # Keep status=running so the UI stays green and the job resumes.
            async def _mj_auto_resume():
                await asyncio.sleep(30)
                _mj_start_task(job_id, user_id, bot=bot)
            asyncio.create_task(_mj_auto_resume())
        elif "AUTH_KEY_DUPLICATED" in err_str:
            logger.warning(f"[MultiJob {job_id}] AUTH_KEY_DUPLICATED — pausing")
            await _mj_update(job_id, status="paused",
                             error="Session conflict (AUTH_KEY_DUPLICATED). Restart the job.")
        else:
            logger.error(f"[MultiJob {job_id}] Fatal: {e}", exc_info=True)
            await _mj_update(job_id, status="error", error=err_str[:120])
        # Only mark progress bar as error for truly fatal failures.
        # For transient/AUTH errors, the job is auto-restarting — keep current bar state.
        is_transient_outer = any(kw in err_upper for kw in _MJ_TRANSIENT)
        if client and mj_prog_msg_id and not is_transient_outer and "AUTH_KEY_DUPLICATED" not in err_str:
            try:
                fj = await _mj_get(job_id)
                _fwd = fj.get("forwarded", 0) if fj else 0
                await client.edit_message_text(to_chat, mj_prog_msg_id, _mj_prog_text(_fwd, mj_total, "error"), parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML)
            except Exception: pass

    finally:
        _mj_tasks.pop(job_id, None)
        _mj_paused.pop(job_id, None)
        if _mj_queue_acquired:
            AryaJobQueue.release(job_id, "multijob")
        if client:
            from plugins.test import release_client
            client_name = getattr(client, 'name', None)
            if client_name:
                await release_client(client_name)
            else:
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
            [InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="back")]
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
            is_queued = False
            if st == "running":
                # MultiJob uses "running" even when waiting in AryaJobQueue
                from plugins.job_queue import AryaJobQueue
                try: 
                    if AryaJobQueue.queue_position(jid, "multijob") > 0:
                        is_queued = True
                except: pass

            if is_queued:
                row.append(InlineKeyboardButton(f"⚡ Fᴏʀᴄᴇ [{short}]", callback_data=f"mj#force_ask#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mj#stop#{jid}"))
            elif st == "running":
                row.append(InlineKeyboardButton(f"⏸ Pᴀᴜsᴇ [{short}]", callback_data=f"mj#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mj#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ Rᴇsᴜᴍᴇ [{short}]", callback_data=f"mj#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Sᴛᴏᴘ [{short}]", callback_data=f"mj#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Sᴛᴀʀᴛ [{short}]", callback_data=f"mj#start#{jid}"))
                row.append(InlineKeyboardButton(f"🔁 Rᴇsᴇᴛ [{short}]", callback_data=f"mj#reset#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ Iɴғᴏ [{short}]", callback_data=f"mj#info#{jid}"))
            row.append(InlineKeyboardButton(f"✏️ Nᴀᴍᴇ [{short}]", callback_data=f"mj#rename#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 Dᴇʟᴇᴛᴇ [{short}]",  callback_data=f"mj#del#{jid}"))
            btns_list.append(row)

        btns_list.append([InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Mᴜʟᴛɪ Jᴏʙ", callback_data="mj#new")])
        btns_list.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ",           callback_data="mj#list")])
        btns_list.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="back")])
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
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS, _DISABLED_MSG
    uid = message.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("multi_job"):
        return await message.reply_text(_DISABLED_MSG.format(feature=FEATURE_LABELS["multi_job"]))
    await _render_mj_list(bot, message.from_user.id, message)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^mj#list$'))
async def mj_list_cb(bot, query):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS
    uid = query.from_user.id
    if not await is_any_owner(uid) and not await is_feature_enabled("multi_job"):
        return await query.answer(f"🖒 {FEATURE_LABELS['multi_job']} is disabled by admin.", show_alert=True)
    await query.answer()
    await _render_mj_list(bot, query.from_user.id, query)
@Client.on_callback_query(filters.regex(r'^mj#rename#'))
async def mj_rename_cb(bot, query):
    user_id = query.from_user.id
    job_id = query.data.split("#", 2)[2]
    await query.message.delete()
    
    r = await _mj_ask(bot, user_id,
        "<b>✏️ Edit Multi Job Name</b>\n\nSend a new name for this job:",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]], resize_keyboard=True, one_time_keyboard=True))
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
        InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="mj#list")
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
        routing = await db.get_task_routing()
        target_node = routing.get("multijob")
        should_run_locally = (target_node == "main" or target_node is None)

        if should_run_locally:
            await _mj_update(job_id, status="running")
            _mj_start_task(job_id, user_id)
            await query.answer("▶️ Restarted from saved position!", show_alert=False)
        else:
            await _mj_update(job_id, status="queued")
            await query.answer(f"▶️ Queued for worker: {target_node}", show_alert=False)
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


@Client.on_callback_query(filters.regex(r'^mj#reset#'))
async def mj_reset_cb(bot, query):
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
    start_id = int(job.get("start_id") or 1)
    await _mj_update(job_id,
        status="stopped",
        current_id=start_id,
        forwarded=0,
        consecutive_empty=0,
        error=""
    )
    await query.answer("🔁 Job reset to start!", show_alert=True)
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


@Client.on_callback_query(filters.regex(r'^mj#force_ask#'))
async def mj_force_ask_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    txt = (
        "⚠️ <b>WARNING: FORCE START</b>\n\n"
        "You are about to bypass the safety queue and force this Multi Job to start concurrently.\n\n"
        "<b>Potential Issues:</b>\n"
        "• <b>API Limits:</b> Forwarding too many messages simultaneously drastically increases your risk of FloodWaits or temporary bans.\n"
        "• <b>Server CPU:</b> Slower overall speed as tasks compete.\n\n"
        "Are you sure you want to force start this job immediately?"
    )
    kb = [
        [InlineKeyboardButton("✅ Yes, Force Start Anyway", callback_data=f"mj#force_do#{job_id}")],
        [InlineKeyboardButton("⛔ Cancel (Keep in Queue)", callback_data="mj#list")]
    ]
    return await query.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

@Client.on_callback_query(filters.regex(r'^mj#force_do#'))
async def mj_force_do_cb(bot, query):
    job_id  = query.data.split("#", 2)[2]
    user_id = query.from_user.id
    job = await _mj_get(job_id)
    if not job or job.get("user_id") != user_id:
        return await query.answer("⛔ Unauthorized.", show_alert=True)
        
    await _mj_update(job_id, status="running", force_active=True)
    
    # Release from AryaJobQueue explicitly to prevent stale lists
    try:
        from plugins.job_queue import AryaJobQueue
        if job_id in AryaJobQueue._waiting_order.get("multijob", []):
            AryaJobQueue._waiting_order["multijob"].remove(job_id)
    except Exception: pass
    
    task = _mj_tasks.get(job_id)
    if task and not task.done():
        task.cancel()
        await asyncio.sleep(0.5)
        
    _mj_start_task(job_id, user_id, bot=bot)
    await query.answer("🚀 Job forcefully activated!", show_alert=False)
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

async def _mj_ask_dest(bot, user_id: int, channels: list, step_label: str, optional: bool = False, undo_btn: bool = False) -> tuple:
    """Ask user to pick a saved channel. Returns (chat_id, title, cancelled).
    cancelled=True means cancelled, cancelled='undo' means undo was pressed."""
    btns = [[KeyboardButton(ch['title'])] for ch in channels]
    if optional:
        btns.append([KeyboardButton("⏭ Sᴋɪᴘ (no second destination)")])
    extra = []
    if undo_btn:
        extra.append(KeyboardButton("↩️ Uɴᴅᴏ"))
    extra.append(KeyboardButton("⛔ Cᴀɴᴄᴇʟ"))
    btns.append(extra)

    resp = await _mj_ask(bot, user_id, step_label,
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


async def _mj_ask_topic(bot, user_id: int, dest_label: str) -> int | None:
    """Ask for optional topic thread ID."""
    r = await _mj_ask(bot, user_id,
        f"<b>Topic Thread for {dest_label} (Optional)</b>\n\n"
        "• Send the <b>Thread ID</b> if you want to post inside a specific group topic\n"
        "• Send <b>0</b> to post in the main chat\n\n"
        "<i>Find Thread ID: open topic in Telegram Web → number after <code>/topics/</code> in URL</i>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("0 (No Topic)")], [KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]],
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

    CANCEL_BTN = KeyboardButton("⛔ Cᴀɴᴄᴇʟ")
    UNDO_BTN   = KeyboardButton("↩️ Uɴᴅᴏ")

    def _cancel(txt): return txt.strip().startswith("/cancel") or "⛔" in txt or "Cᴀɴᴄᴇʟ" in txt
    def _undo(txt):   return txt.strip().startswith("/undo") or "↩️" in txt or "Uɴᴅᴏ" in txt

    # ── Step 1: Name ──────────────────────────────────────────────
    name_r = await _mj_ask(bot, user_id,
        "<b>»  Create Multi Job — Step 1/6</b>\n\n"
        "Send a <b>name</b> for this job, or press <b>Default</b>.",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("Default")], [CANCEL_BTN]],
            resize_keyboard=True, one_time_keyboard=True))
    if _cancel(name_r.text):
        return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

    job_name = name_r.text.strip()[:100]
    if job_name.lower() == "default":
        job_name = None

    # ── Step 2: Account ───────────────────────────────────────────
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

    acc_r = await _mj_ask(bot, user_id,
        "<b>»  Create Multi Job — Step 2/6</b>\n\n"
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
        src_r = await _mj_ask(bot, user_id,
            "<b>Step 3/6 — Source Chat</b>\n\n"
            "Send the <b>source channel, group, or chat</b> to copy messages from.\n\n"
            "<blockquote expandable>"
            "Accepted formats:\n"
            "• <code>@username</code> — public channel/group username\n"
            "• <code>https://t.me/username</code> — public link\n"
            "• <code>https://t.me/c/1234567890/1</code> — private channel link\n"
            "• <code>-1001234567890</code> — numeric chat ID (negative for channels/groups)\n"
            "• <code>me</code> — your own Saved Messages (Userbot only)\n\n"
            "📌 For private channels: use a Userbot that is already a member.\n"
            "📌 For public channels: Bot account works if it can read messages.\n"
            "📌 Group Topics: supported — you will be asked for a thread ID next.\n"
            "📌 Bot DM: use the bot's username or numeric ID."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(
                [[UNDO_BTN, CANCEL_BTN]], resize_keyboard=True, one_time_keyboard=True))

        if _cancel(src_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(src_r.text):
            # Redo step 2
            acc_r2 = await _mj_ask(bot, user_id,
                "<b>↩️ Redo — Step 2/6: Account</b>\n\nChoose the account again:",
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
            return await bot.send_message(user_id,
                "<b>❌ Saved Messages require a Userbot account.</b>",
                reply_markup=ReplyKeyboardRemove())
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

    from_thread = await _mj_ask_topic(bot, user_id, "Source")

    # ── Step 4: Primary Destination ───────────────────────────────
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(user_id,
            "<b>❌ No target channels saved. Add via /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    while True:
        to_chat, to_title, cancelled = await _mj_ask_dest(bot, user_id, channels,
            "<b>Step 4/6 — Destination</b>\n\nWhere should messages be sent?\n\n"
            "<blockquote expandable>"
            "Choose from your saved channels/groups.\n"
            "To add a channel, go to /settings → Channels.\n"
            "The account you chose must be an admin with send permission."
            "</blockquote>",
            undo_btn=True)
        if cancelled == "undo":
            # Redo source
            continue  # will fall through — in practice they'd re-enter src step, but we keep it simple here
        elif cancelled:
            return
        break

    to_thread = await _mj_ask_topic(bot, user_id, "Destination")

    # ── Step 5: Message Range ─────────────────────────────────────
    while True:
        range_r = await _mj_ask(bot, user_id,
            "<b>Step 5/6 — Message Range</b>\n\n"
            "Which messages should be copied?\n\n"
            "<blockquote expandable>"
            "Options:\n"
            "• <b>ALL</b> — copy from the very first message (ID 1)\n"
            "• <code>500</code> — start from message ID 500 onwards\n"
            "• <code>100:5000</code> — copy only messages from ID 100 to 5000\n\n"
            "The job stops automatically when the range is complete.\n"
            "For continuous/unlimited copying, leave end ID as 0 or send ALL."
            "</blockquote>",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("ALL")], [UNDO_BTN, CANCEL_BTN]],
                resize_keyboard=True, one_time_keyboard=True))

        if _cancel(range_r.text):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if _undo(range_r.text):
            # Redo destination
            to_chat, to_title, cancelled = await _mj_ask_dest(bot, user_id, channels,
                "<b>↩️ Redo — Step 4/6: Destination</b>\n\nChoose destination again:")
            if cancelled:
                return
            to_thread = await _mj_ask_topic(bot, user_id, "Destination")
            continue
        break

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

    # ── Step 6: Second Destination (optional) ─────────────────────
    to_chat_2, to_title_2, cancelled2 = await _mj_ask_dest(bot, user_id, channels,
        "<b>Step 6/6 — Second Destination (Optional)</b>\n\n"
        "Send to a <b>second</b> channel simultaneously? Press Skip if not needed.",
        optional=True)
    if cancelled2 == True:
        return
    to_thread_2 = None
    if to_chat_2:
        to_thread_2 = await _mj_ask_topic(bot, user_id, "Second Destination")

    # ── Save & Start ──────────────────────────────────────────────
    job_id = f"mj-{user_id}-{int(time.time())}"
    
    routing = await db.get_task_routing()
    target_node = routing.get("multijob")
    # If not explicitly routed, default to main
    should_run_locally = (target_node == "main" or target_node is None)

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
        "to_chat_2":      to_chat_2,
        "to_title_2":     to_title_2,
        "to_thread_id_2": to_thread_2,
        "start_id":       start_id,
        "end_id":         end_id,
        "current_id":     start_id,
        "status":         "running" if should_run_locally else "queued",
        "created":        int(time.time()),
        "forwarded":      0,
        "consecutive_empty": 0,
        "error":          "",
    }
    await _mj_save(job)
    
    if should_run_locally:
        _mj_start_task(job_id, user_id, bot=bot)

    end_lbl   = f"to ID <code>{end_id}</code>" if end_id else "all messages"
    thread_lbl = f" → Topic <code>{to_thread}</code>" if to_thread else ""
    kind = "Bot" if is_bot else "Userbot"
    
    run_msg = "<i>Running in background.\nUse /multijob to manage.</i>" if should_run_locally else f"<i>Queued for worker: <b>{target_node}</b>.\nUse /multijob to manage.</i>"

    await bot.send_message(
        user_id,
        f"<b>✅ Multi Job Created!</b>\n\n"
        f"»  <b>{from_title}</b> → <b>{to_title}</b>{thread_lbl}\n"
        f"<b>Account:</b> {kind}: {sel_acc.get('name','?')}\n"
        f"<b>Range:</b> From ID <code>{start_id}</code> · {end_lbl}\n"
        f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
        f"{run_msg}",
        reply_markup=ReplyKeyboardRemove()
    )