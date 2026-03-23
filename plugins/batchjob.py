"""
Batch Jobs Plugin — Unicode-styled
===================================
Persistent background bulk-copy jobs with pause/resume.
Smart Schedule: queues jobs per destination channel, 1-min gap notification.
"""
import re
import os
import time
import asyncio
import logging
import datetime
from database import db
from .test import CLIENT, start_clone_bot
from plugins.jobs import _has_links
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

_task_jobs:   dict[str, asyncio.Task] = {}
_pause_events: dict[str, asyncio.Event] = {}

# Smart Schedule: per destination channel queues & active tracker
# _ch_active[to_chat]  -> job_id currently running for that channel
# _ch_queue[to_chat]   -> list of (job_id, user_id) waiting
_ch_active: dict = {}
_ch_queue:  dict = {}

COLL = "batchjobs"

# Batch download semaphore
_DOWNLOAD_SEM = asyncio.Semaphore(2)

# ── Unicode helpers ────────────────────────────────────────────────────────────
def _st(status: str) -> str:
    return {"running": "🟢", "paused": "⏸", "stopped": "🔴", "done": "✅",
            "error": "⚠️", "queued": "🕐"}.get(status, "❓")


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _tj_save(job: dict):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)

async def _tj_get(job_id: str) -> dict | None:
    return await db.db[COLL].find_one({"job_id": job_id})

async def _tj_list(user_id: int) -> list[dict]:
    return [j async for j in db.db[COLL].find({"user_id": user_id})]

async def _tj_delete(job_id: str):
    await db.db[COLL].delete_one({"job_id": job_id})

async def _tj_update(job_id: str, **kw):
    await db.db[COLL].update_one({"job_id": job_id}, {"$set": kw})

async def _tj_inc(job_id: str, n: int = 1):
    await db.db[COLL].update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})


_tj_status_msgs: dict = {}

async def _tj_notify(bot, job: dict, phase: str = ""):
    """Send/edit a live batch job status message to the user."""
    if not bot:
        return
    uid    = job["user_id"]
    job_id = job["job_id"]
    st     = _st(job.get("status", "running"))
    fwd    = job.get("forwarded", 0)
    cur    = job.get("current_id", "?")
    end    = job.get("end_id", 0)
    cname  = job.get("custom_name", "")
    name_p = f" <b>{cname}</b>" if cname else ""
    rng_p  = f"<code>{job.get('start_id',1)}</code> → <code>{end}</code>" if end else f"<code>{job.get('start_id',1)}</code> → ∞"
    err_p  = f"\n┣⊸ ⚠️ <code>{job['error']}</code>" if job.get("error") else ""
    phase_p = f"\n  • <b>Phase:</b> <code>{phase}</code>" if phase else ""
    text = (
        f"<b>Batch Job Progress</b>\n\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_p}\n"
        f"  • <b>Status:</b> {st} {job.get('status','running')}\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}\n\n"
        f"  • <b>Range:</b> {rng_p}\n"
        f"  • <b>Current:</b> <code>{cur}</code>\n"
        f"  • <b>Forwarded:</b> <code>{fwd}</code>"
        f"{phase_p}{err_p}"
    )
    key = (uid, job_id)
    try:
        existing_mid = _tj_status_msgs.get(key)
        if existing_mid:
            try:
                await bot.edit_message_text(uid, existing_mid, text)
                return
            except Exception:
                pass
        sent = await bot.send_message(uid, text)
        _tj_status_msgs[key] = sent.id
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Filter helper
# ══════════════════════════════════════════════════════════════════════════════

def _passes_filters(msg, dis: list) -> bool:
    if msg.empty or msg.service: return False
    for typ, chk in [
        ('text',      lambda m: m.text and not m.media),
        ('audio',     lambda m: m.audio), ('voice',     lambda m: m.voice),
        ('video',     lambda m: m.video), ('photo',     lambda m: m.photo),
        ('document',  lambda m: m.document), ('animation', lambda m: m.animation),
        ('sticker',   lambda m: m.sticker), ('poll',      lambda m: m.poll),
    ]:
        if typ in dis and chk(msg): return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Send helper
# ══════════════════════════════════════════════════════════════════════════════

async def _send_one(client, msg, to_chat: int, remove_caption: bool, caption_tpl,
                    forward_tag=False, from_chat=None, block_links=False, to_topic=None):
    caption = None
    is_modified = False

    if caption_tpl and msg.media:
        caption = caption_tpl
        is_modified = True
    elif remove_caption and msg.media:
        caption = ""
        is_modified = True
    elif block_links and _has_links(msg):
        content = getattr(msg, 'caption' if msg.media else 'text', None)
        if content:
            raw = getattr(content, 'html', str(content))
            import re as _lre
            _LRE = _lre.compile(
                r'(https?://\S+|t\.me/\S+|@[A-Za-z0-9_]{4,}'
                r'|\b(?:www\.|bit\.ly/|youtu\.be/)\S+'
                r'|\b[\w.-]+\.(?:com|net|org|io|co|me|tv|gg|app|xyz|info|news|link|site)(?:/\S*)?\b)',
                _lre.IGNORECASE)
            caption = _LRE.sub('', raw).strip()
            is_modified = True

    if is_modified and forward_tag:
        forward_tag = False

    from_id = from_chat or msg.chat.id

    try:
        kw_fwd = {}
        if to_topic: kw_fwd["message_thread_id"] = kw_fwd["reply_to_message_id"] = to_topic
        if forward_tag:
            try:
                await client.forward_messages(chat_id=to_chat, from_chat_id=from_id,
                                              message_ids=msg.id, **kw_fwd)
                return True
            except FloodWait as fw:
                raise fw
            except Exception:
                pass

        try:
            if msg.media:
                mo = getattr(msg, msg.media.value, None)
                if mo and hasattr(mo, "file_id"):
                    kw = {}
                    if to_topic: kw["message_thread_id"] = kw["reply_to_message_id"] = to_topic
                    if caption is not None: kw["caption"] = caption
                    elif msg.caption: kw["caption"] = msg.caption
                    await client.send_cached_media(chat_id=to_chat, file_id=mo.file_id, **kw)
                    return True
        except Exception:
            pass

        kw_msg = {}
        if to_topic: kw_msg["message_thread_id"] = kw_msg["reply_to_message_id"] = to_topic

        if not msg.media and is_modified:
            await client.send_message(chat_id=to_chat, text=caption or "", **kw_msg)
            return True

        if caption is not None and msg.media:
            await client.copy_message(chat_id=to_chat, from_chat_id=from_id,
                                      message_id=msg.id, caption=caption, **kw_msg)
        else:
            await client.copy_message(chat_id=to_chat, from_chat_id=from_id,
                                      message_id=msg.id, **kw_msg)
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
        return await _send_one(client, msg, to_chat, remove_caption, caption_tpl,
                               forward_tag, from_chat, block_links, to_topic)
    except Exception as e:
        try:
            if msg.media:
                mo = getattr(msg, msg.media.value, None)
                display_name = getattr(mo, 'file_name', None) if mo else None
                if display_name:
                    import re as _re4
                    display_name = _re4.sub(r'[\\/*?"<>|]', '', display_name).strip() or None
                import shutil as _shu2
                safe_dir = f"downloads/{msg.id}"
                os.makedirs(safe_dir, exist_ok=True)
                df_name = f"{safe_dir}/{display_name}" if display_name else f"{safe_dir}/"
                async with _DOWNLOAD_SEM:
                    fp = await client.download_media(msg, file_name=df_name)
                if not fp: raise Exception("DownloadFailed")
                kw = {"chat_id": to_chat,
                      "caption": caption if caption is not None else (str(msg.caption) if msg.caption else "")}
                if to_topic: kw["message_thread_id"] = kw["reply_to_message_id"] = to_topic
                try:
                    if msg.photo:       await client.send_photo(photo=fp, **kw)
                    elif msg.video:     await client.send_video(video=fp, file_name=display_name, **kw)
                    elif msg.document:  await client.send_document(document=fp, file_name=display_name, **kw)
                    elif msg.audio:     await client.send_audio(audio=fp, file_name=display_name,
                                                               title=getattr(mo, 'title', None),
                                                               performer=getattr(mo, 'performer', None), **kw)
                    elif msg.voice:     await client.send_voice(voice=fp, **kw)
                    elif msg.animation: await client.send_animation(animation=fp, file_name=display_name, **kw)
                    elif msg.sticker:   await client.send_sticker(sticker=fp, **kw)
                finally:
                    _shu2.rmtree(safe_dir, ignore_errors=True)
                return True
            else:
                raw_t = caption if is_modified else (getattr(msg.text, 'html', str(msg.text)) if msg.text else "")
                kw_t = {"chat_id": to_chat, "text": raw_t}
                if to_topic: kw_t["message_thread_id"] = kw_t["reply_to_message_id"] = to_topic
                await client.send_message(**kw_t)
                return True
        except Exception as e2:
            logger.debug(f"[BatchJob] send fallback: {e2}")
            return False


# ══════════════════════════════════════════════════════════════════════════════
# Smart Schedule helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _schedule_next_for_channel(to_chat: int, _bot=None):
    """Called after a job finishes. Waits 60s then launches next queued job."""
    q = _ch_queue.get(to_chat, [])
    if not q:
        _ch_active.pop(to_chat, None)
        return

    # Take the next job
    next_jid, next_uid = q.pop(0)
    if not _ch_queue[to_chat]:
        del _ch_queue[to_chat]

    job = await _tj_get(next_jid)
    if not job:
        _ch_active.pop(to_chat, None)
        await _schedule_next_for_channel(to_chat, _bot)
        return

    # Check it wasn't cancelled/deleted while waiting
    if job.get("status") in ("stopped", "deleted"):
        _ch_active.pop(to_chat, None)
        await _schedule_next_for_channel(to_chat, _bot)
        return

    cname = job.get("custom_name") or next_jid[-6:]
    to_title = job.get("to_title", "?")

    # Notify user with 1-minute window
    if _bot:
        try:
            await _bot.send_message(next_uid,
                f"<b>╭──────❰ ✅ ʙᴀᴛᴄʜ ᴊᴏʙ ᴄᴏᴍᴘʟᴇᴛᴇᴅ — 1ᴍɪɴ ɢᴀᴘ ❱──────╮\n"
                f"┃\n"
                f"┣⊸ The previous batch job for <b>{to_title}</b> has finished.\n"
                f"┣⊸ 🕐 Next job starting in <b>1 minute</b>.\n"
                f"┣⊸ You can make updates to the channel now.\n"
                f"┣⊸ Next job: <b>{cname}</b>\n"
                f"┃\n"
                f"╰────────────────────────────────╯</b>"
            )
        except Exception:
            pass

    await asyncio.sleep(60)

    # Re-check — user might have cancelled while sleeping
    fresh = await _tj_get(next_jid)
    if not fresh or fresh.get("status") in ("stopped", "deleted"):
        _ch_active.pop(to_chat, None)
        await _schedule_next_for_channel(to_chat, _bot)
        return

    # Launch it
    _ch_active[to_chat] = next_jid
    await _tj_update(next_jid, status="running")
    _start_task(next_jid, next_uid, _bot=_bot)

    if _bot:
        try:
            await _bot.send_message(next_uid,
                f"<b>╭──────❰ ▶️ ʙᴀᴛᴄʜ ᴊᴏʙ sᴛᴀʀᴛᴇᴅ ❱──────╮\n"
                f"┃\n"
                f"┣⊸ <b>{cname}</b> is now running!\n"
                f"┣⊸ Target: <b>{to_title}</b>\n"
                f"┃\n"
                f"╰────────────────────────────────╯</b>"
            )
        except Exception:
            pass


def _queue_or_start(job_id: str, user_id: int, to_chat: int, _bot=None):
    """Smart schedule: start immediately or queue behind existing channel job."""
    if to_chat in _ch_active:
        # Queue it
        if to_chat not in _ch_queue:
            _ch_queue[to_chat] = []
        _ch_queue[to_chat].append((job_id, user_id))
        # Status will be set to "queued" by caller
        return False  # not started
    else:
        # No active job for this channel — start immediately
        _ch_active[to_chat] = job_id
        _start_task(job_id, user_id, _bot=_bot)
        return True  # started


# ══════════════════════════════════════════════════════════════════════════════
# Core runner
# ══════════════════════════════════════════════════════════════════════════════

BATCH_SIZE = 200

async def _run_task_job(job_id: str, user_id: int, _bot=None):
    job = await _tj_get(job_id)
    if not job: return

    if job_id not in _pause_events:
        ev = asyncio.Event(); ev.set()
        _pause_events[job_id] = ev
    pause_ev = _pause_events[job_id]
    last_notify = 0

    acc = client = None
    to_chat_ref = job.get("to_chat")
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _tj_update(job_id, status="error", error="Account not found"); return

        from plugins.jobs import _get_shared_client
        from config import Config
        _is_main = acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN
        if _is_main and getattr(_bot, "is_connected", False):
            client = _bot
        else:
            client = await _get_shared_client(acc)

        is_bot  = acc.get("is_bot", True)
        fc      = job["from_chat"]

        # Determine if source is channel
        fc_is_channel = False
        try:
            if str(fc).startswith("-100"):
                fc_is_channel = True
            else:
                try:
                    from pyrogram.raw.types import InputPeerChannel
                    peer = await client.resolve_peer(fc)
                    if isinstance(peer, InputPeerChannel):
                        fc_is_channel = True
                except Exception:
                    from pyrogram.enums import ChatType
                    c_obj = await client.get_chat(fc)
                    if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                        fc_is_channel = True
        except Exception:
            if getattr(client, 'me', None) and client.me.is_bot and isinstance(fc, str):
                fc_is_channel = True

        to_chat    = job["to_chat"]
        to_topic   = job.get("to_topic_id")
        from_topic = job.get("from_topic_id")
        end_id     = job.get("end_id", 0)
        current    = job.get("current_id", job.get("start_id", 1))

        await _tj_update(job_id, status="running", error="")

        while True:
            await pause_ev.wait()

            fresh = await _tj_get(job_id)
            if not fresh or fresh.get("status") in ("stopped", "error"): break

            if end_id > 0 and current > end_id:
                await _tj_update(job_id, status="done", current_id=current)
                break

            dis         = await db.get_filters(user_id)
            flgs        = await db.get_filter_flags(user_id)
            configs     = await db.get_configs(user_id)
            rm_cap      = flgs.get('rm_caption', False)
            block_links = flgs.get('block_links', False)
            cap_tpl     = configs.get('caption')
            forward_tag = configs.get('forward_tag', False)
            slp         = configs.get('duration', 0) or 0

            chunk_end = current + BATCH_SIZE - 1
            if end_id > 0: chunk_end = min(chunk_end, end_id)
            batch_ids = list(range(current, chunk_end + 1))

            try:
                msgs = []
                fetch_ok = False
                if fc_is_channel:
                    try:
                        msgs = await client.get_messages(fc, batch_ids)
                        if not isinstance(msgs, list): msgs = [msgs]
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as ge:
                        logger.warning(f"[BatchJob {job_id}] get_messages failed @ {current}: {ge}")

                if not fetch_ok:
                    try:
                        col = []
                        async for hmsg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_SIZE):
                            if hmsg.id < current: break
                            col.append(hmsg)
                        msgs = list(reversed(col))
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as he:
                        logger.warning(f"[BatchJob {job_id}] history fallback @ {current}: {he}")

                if not fetch_ok:
                    current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f"[BatchJob {job_id}] Outer fetch error {current}: {e}")
                current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue

            valid = sorted([m for m in msgs if m and not getattr(m, 'empty', False)
                            and not getattr(m, 'service', False)], key=lambda m: m.id)

            if not valid:
                consec = fresh.get("consecutive_empty", 0) + 1
                if consec >= 50:
                    logger.info(f"[BatchJob {job_id}] 50 empty chunks. Ending.")
                    await _tj_update(job_id, status="done", current_id=current)
                    break
                current += BATCH_SIZE
                await _tj_update(job_id, consecutive_empty=consec, current_id=current)
                await asyncio.sleep(1); continue

            await _tj_update(job_id, consecutive_empty=0)

            # Auto notify every 60s
            _now = int(time.time())
            if _bot and _now - last_notify >= 60:
                _fresh_j = await _tj_get(job_id)
                if _fresh_j:
                    await _tj_notify(_bot, _fresh_j, "ʀᴜɴɴɪɴɢ")
                last_notify = _now

            fwd = 0
            for msg in valid:
                if from_topic:
                    tid = getattr(msg, 'message_thread_id',
                          getattr(msg, 'reply_to_top_message_id',
                          getattr(msg, 'reply_to_message_id', None)))
                    if tid != from_topic and msg.id != from_topic:
                        continue

                await pause_ev.wait()
                f2 = await _tj_get(job_id)
                if not f2 or f2.get("status") in ("stopped",): return
                if not _passes_filters(msg, dis): continue
                ok = await _send_one(client, msg, to_chat, rm_cap, cap_tpl,
                                     forward_tag=forward_tag, from_chat=fc,
                                     block_links=block_links, to_topic=to_topic)
                if ok: fwd += 1; await _tj_inc(job_id)
                if slp: await asyncio.sleep(slp)
                else:   await asyncio.sleep(0)

            current = (valid[-1].id + 1) if valid else (current + BATCH_SIZE)
            await _tj_update(job_id, current_id=current)

    except asyncio.CancelledError:
        logger.info(f"[BatchJob {job_id}] Cancelled")
        await _tj_update(job_id, status="stopped")
    except Exception as e:
        logger.error(f"[BatchJob {job_id}] Fatal: {e}")
        await _tj_update(job_id, status="error", error=str(e))
    finally:
        _task_jobs.pop(job_id, None); _pause_events.pop(job_id, None)
        if acc:
            from plugins.jobs import _release_shared_client
            from config import Config as _Cfg2
            _im2 = acc.get("is_bot") and acc.get("token") == _Cfg2.BOT_TOKEN
            if not (_im2 and getattr(_bot, "is_connected", False)):
                await _release_shared_client(acc)

        # Smart Schedule: launch next job for same channel
        if to_chat_ref and _ch_active.get(to_chat_ref) == job_id:
            asyncio.create_task(_schedule_next_for_channel(to_chat_ref, _bot))


def _start_task(job_id: str, user_id: int, _bot=None):
    ev = asyncio.Event(); ev.set()
    _pause_events[job_id] = ev
    task = asyncio.create_task(_run_task_job(job_id, user_id, _bot=_bot))
    _task_jobs[job_id] = task
    return task


async def resume_batch_jobs(user_id: int = None, _bot=None):
    q = {"status": "running"}
    if user_id: q["user_id"] = user_id
    async for job in db.db[COLL].find(q):
        jid, uid, tc = job["job_id"], job["user_id"], job.get("to_chat")
        if jid not in _task_jobs:
            if tc and tc not in _ch_active:
                _ch_active[tc] = jid
            _start_task(jid, uid, _bot=_bot)


# ══════════════════════════════════════════════════════════════════════════════
# Job overview helper — Telegram collapsible blockquote
# ══════════════════════════════════════════════════════════════════════════════

def _job_overview_block(j: dict) -> str:
    """Returns a <blockquote expandable> block with full job details."""
    st     = _st(j.get("status", "stopped"))
    jid    = j["job_id"]
    cname  = j.get("custom_name") or ""
    fwd    = j.get("forwarded", 0)
    cur    = j.get("current_id", "?")
    end    = j.get("end_id", 0)
    start  = j.get("start_id", 1)
    rng    = f"{start} → {end}" if end else f"{start} → ∞"
    created_ts = j.get("created", 0)
    created_str = datetime.datetime.fromtimestamp(created_ts).strftime("%d %b %Y %H:%M") if created_ts else "?"
    err    = f"\n⚠️ Error: {j['error']}" if j.get("error") else ""
    queued_pos = ""
    # Check if queued
    status_str = j.get("status", "stopped")
    name_str = f" ({cname})" if cname else ""

    lines = (
        f"🆔 ID: {jid[-6:]}{name_str}\n"
        f"{st} Status: {status_str}\n"
        f"📤 Source: {j.get('from_title', '?')}\n"
        f"📥 Target: {j.get('to_title', '?')}\n"
        f"📊 Range: {rng}\n"
        f"📍 Current: {cur}\n"
        f"✅ Forwarded: {fwd}\n"
        f"🕐 Created: {created_str}"
        f"{err}"
    )
    return f"<blockquote expandable>{lines}</blockquote>"


# ══════════════════════════════════════════════════════════════════════════════
# UI — render list
# ══════════════════════════════════════════════════════════════════════════════

async def _render_batchjob_list(bot, user_id: int, mq):
    jobs  = await _tj_list(user_id)
    is_cb = hasattr(mq, "message")

    if not jobs:
        text = (
            "<b>Batch Jobs</b>\n\n  • No batch jobs yet.\n\nCopies all existing messages from a source to a target in the background."
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Create Batch Job", callback_data="bj#new")
        ]])
    else:
        # Build overview header with all jobs as collapsible blockquotes
        overview_parts = ["<b>📋 Batch Jobs Overview</b>\n"]
        for j in jobs:
            overview_parts.append(_job_overview_block(j))
        text = "\n".join(overview_parts)

        rows = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            s   = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏸ Pause [{s}]",  callback_data=f"bj#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Stop [{s}]",   callback_data=f"bj#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ Resume [{s}]", callback_data=f"bj#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Stop [{s}]",   callback_data=f"bj#stop#{jid}"))
            elif st == "queued":
                row.append(InlineKeyboardButton(f"▶️ Force Start [{s}]", callback_data=f"bj#forcestart#{jid}"))
                row.append(InlineKeyboardButton(f"❌ Remove Queue [{s}]", callback_data=f"bj#dequeue#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Start [{s}]",  callback_data=f"bj#start#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 [{s}]",  callback_data=f"bj#del#{jid}"))
            rows.append(row)

        rows.append([InlineKeyboardButton("➕ Create Batch Job", callback_data="bj#new")])
        rows.append([InlineKeyboardButton("🔄 ʀᴇғʀᴇsʜ",          callback_data="bj#list")])
        btns = InlineKeyboardMarkup(rows)

    try:
        if is_cb:
            await mq.message.edit_text(text, reply_markup=btns)
        else:
            await mq.reply_text(text, reply_markup=btns)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Commands
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command(["batchjobs", "batchjob"]))
async def batchjobs_cmd(bot, msg):
    await _render_batchjob_list(bot, msg.from_user.id, msg)


@Client.on_message(filters.private & filters.command("newbatchjob"))
async def newbatchjob_cmd(bot, msg):
    await _create_batchjob_flow(bot, msg.from_user.id)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

_CANCEL_BOX = (
    "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n"
    "┃\n╰────────────────────────────────╯</b>"
)


@Client.on_callback_query(filters.regex(r'^bj#list$'))
async def tj_list_cb(bot, q): await _render_batchjob_list(bot, q.from_user.id, q)


@Client.on_callback_query(filters.regex(r'^bj#new$'))
async def tj_new_cb(bot, q):
    await q.message.delete()
    await _create_batchjob_flow(bot, q.from_user.id)


@Client.on_callback_query(filters.regex(r'^bj#pause#'))
async def tj_pause_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    ev = _pause_events.get(job_id)
    if ev: ev.clear()
    await _tj_update(job_id, status="paused")
    await q.answer("⏸ Pauseᴅ.")
    await _render_batchjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^bj#resume#'))
async def tj_resume_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    ev = _pause_events.get(job_id)
    if ev and job_id in _task_jobs and not _task_jobs[job_id].done():
        ev.set()
        await _tj_update(job_id, status="running")
        await q.answer("▶️ Resumeᴅ!")
    else:
        to_chat = job.get("to_chat")
        if to_chat and to_chat in _ch_active and _ch_active[to_chat] != job_id:
            # Another job is active for this channel — re-queue
            if to_chat not in _ch_queue:
                _ch_queue[to_chat] = []
            _ch_queue[to_chat].insert(0, (job_id, uid))
            await _tj_update(job_id, status="queued")
            await q.answer("🕐 Re-queued behind active job!")
        else:
            if to_chat:
                _ch_active[to_chat] = job_id
            await _tj_update(job_id, status="running")
            _start_task(job_id, uid, _bot=bot)
            await q.answer("▶️ ʀᴇsᴛᴀʀᴛᴇᴅ ғʀᴏᴍ sᴀᴠᴇᴅ ᴘᴏsɪᴛɪᴏɴ!")
    await _render_batchjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^bj#stop#'))
async def tj_stop_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    task = _task_jobs.pop(job_id, None)
    if task and not task.done(): task.cancel()
    ev = _pause_events.pop(job_id, None)
    if ev: ev.set()
    # Remove from channel queue if queued
    to_chat = job.get("to_chat")
    if to_chat and to_chat in _ch_queue:
        _ch_queue[to_chat] = [(j, u) for j, u in _ch_queue[to_chat] if j != job_id]
    await _tj_update(job_id, status="stopped")
    await q.answer("⏹ Stopᴘᴇᴅ.")
    await _render_batchjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^bj#start#'))
async def tj_start_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    if job_id in _task_jobs and not _task_jobs[job_id].done():
        return await q.answer("ᴀʟʀᴇᴀᴅʏ ʀᴜɴɴɪɴɢ!", show_alert=True)
    to_chat = job.get("to_chat")
    started = _queue_or_start(job_id, uid, to_chat, _bot=bot)
    if started:
        await _tj_update(job_id, status="running")
        await q.answer("▶️ Startᴇᴅ!")
    else:
        await _tj_update(job_id, status="queued")
        await q.answer("🕐 Queued — will start after active job completes.", show_alert=True)
    await _render_batchjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^bj#forcestart#'))
async def tj_forcestart_cb(bot, q):
    """Force start a queued job immediately, bypassing the queue."""
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    # Remove from queue
    to_chat = job.get("to_chat")
    if to_chat and to_chat in _ch_queue:
        _ch_queue[to_chat] = [(j, u) for j, u in _ch_queue[to_chat] if j != job_id]
    await _tj_update(job_id, status="running")
    if to_chat:
        _ch_active[to_chat] = job_id
    _start_task(job_id, uid, _bot=bot)
    await q.answer("▶️ Force started!", show_alert=True)
    await _render_batchjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^bj#dequeue#'))
async def tj_dequeue_cb(bot, q):
    """Remove a job from the queue without deleting it."""
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    to_chat = job.get("to_chat")
    if to_chat and to_chat in _ch_queue:
        _ch_queue[to_chat] = [(j, u) for j, u in _ch_queue[to_chat] if j != job_id]
    await _tj_update(job_id, status="stopped")
    await q.answer("❌ Removed from queue.", show_alert=True)
    await _render_batchjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^bj#del#'))
async def tj_del_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    task = _task_jobs.pop(job_id, None)
    if task and not task.done(): task.cancel()
    ev = _pause_events.pop(job_id, None)
    if ev: ev.set()
    to_chat = job.get("to_chat")
    if to_chat and to_chat in _ch_queue:
        _ch_queue[to_chat] = [(j, u) for j, u in _ch_queue[to_chat] if j != job_id]
    if to_chat and _ch_active.get(to_chat) == job_id:
        _ch_active.pop(to_chat, None)
    await _tj_delete(job_id)
    await q.answer("🗑 ᴅᴇʟᴇᴛᴇᴅ.")
    await _render_batchjob_list(bot, uid, q)


# ══════════════════════════════════════════════════════════════════════════════
# Create Batch Job — Interactive flow
# ══════════════════════════════════════════════════════════════════════════════

async def _create_batchjob_flow(bot, user_id: int):
    # Step 1 — Account
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(user_id,
            "<b>╭──────❰ ❌ ɴᴏ ᴀᴄᴄᴏᴜɴᴛs ❱──────╮\n"
            "┃\n┣⊸ ᴀᴅᴅ ᴏɴᴇ ɪɴ /settings → ⚙️ Accounts\n"
            "┃\n╰────────────────────────────────╯</b>")

    acc_btns = [[KeyboardButton(
        f"{'🤖 ʙᴏᴛ' if a.get('is_bot', True) else '👤 ᴜsᴇʀʙᴏᴛ'}: "
        f"{a.get('username') or a.get('name', 'Unknown')} [{a['id']}]"
    )] for a in accounts]
    acc_btns.append([KeyboardButton("/cancel")])

    acc_r = await bot.ask(user_id,
        "<b>╭──────❰ 📦 ᴄʀᴇᴀᴛᴇ ʙᴀᴛᴄʜ ᴊᴏʙ — sᴛᴇᴘ 1/4 ❱──────╮\n"
        "┃\n┣⊸ ᴄʜᴏᴏsᴇ ᴡʜɪᴄʜ ᴀᴄᴄᴏᴜɴᴛ ᴛᴏ ᴜsᴇ\n"
        "┣⊸ ᴜsᴇʀʙᴏᴛ ʀᴇqᴜɪʀᴇᴅ ғᴏʀ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟs\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in acc_r.text:
        return await acc_r.reply(_CANCEL_BOX, reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in acc_r.text and "]" in acc_r.text:
        try: acc_id = int(acc_r.text.split('[')[-1].split(']')[0])
        except Exception: pass
    sel  = (await db.get_bot(user_id, acc_id)) if acc_id else accounts[0]
    ibot = sel.get("is_bot", True)

    # Step 2 — Source
    src_r = await bot.ask(user_id,
        "<b>╭──────❰ 📦 sᴛᴇᴘ 2/4 — sᴏᴜʀᴄᴇ ᴄʜᴀᴛ ❱──────╮\n"
        "┃\n"
        "┣⊸ @ᴜsᴇʀɴᴀᴍᴇ       — ᴘᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟ ᴏʀ ɢʀᴏᴜᴘ\n"
        "┣⊸ -1001234567890   — ɴᴜᴍᴇʀɪᴄ ᴄʜᴀɴɴᴇʟ ɪᴅ\n"
        "┣⊸ 123456789        — ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛ ɪᴅ (ᴅᴍ ᴡɪᴛʜ ʙᴏᴛ)\n"
        "┣⊸ me               — sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardRemove())

    if src_r.text.strip().startswith("/cancel"):
        return await src_r.reply(_CANCEL_BOX)

    raw = src_r.text.strip()
    lm  = re.search(r't\.me/c/(\d+)', raw)
    if lm:   fc = int(f"-100{lm.group(1)}")
    elif raw.lstrip('-').isdigit(): fc = int(raw)
    else: fc = raw

    try:
        co     = await bot.get_chat(fc)
        ftitle = getattr(co, "title", None) or str(fc)
    except Exception:
        ftitle = str(fc)

    # Step 3 — Range
    rng_r = await bot.ask(user_id,
        "<b>╭──────❰ 📦 sᴛᴇᴘ 3/4 — ᴍᴇssᴀɢᴇ ʀᴀɴɢᴇ ❱──────╮\n"
        "┃\n┣⊸ ALL      — ᴀʟʟ ᴍsɢs ғʀᴏᴍ ᴛʜᴇ ʙᴇɢɪɴɴɪɴɢ\n"
        "┣⊸ 500      — sᴛᴀʀᴛ ғʀᴏᴍ ɪᴅ 500\n"
        "┣⊸ 500:2000 — ᴏɴʟʏ ɪᴅs 500 ᴛʜʀᴏᴜɢʜ 2000\n"
        "┃\n╰────────────────────────────────╯</b>")

    if "/cancel" in rng_r.text:
        return await rng_r.reply(_CANCEL_BOX)

    start_id, end_id = 1, 0
    rt = rng_r.text.strip().lower()
    if rt != "all":
        if ":" in rt:
            p = rt.split(":", 1)
            try: start_id = int(p[0].strip())
            except Exception: pass
            try: end_id   = int(p[1].strip())
            except Exception: pass
        else:
            try: start_id = int(rt)
            except Exception: pass

    # Step 4 — Target
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(user_id,
            "<b>❌ ɴᴏ ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟs. ᴀᴅᴅ ᴠɪᴀ /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    ch_btns = [[KeyboardButton(ch['title'])] for ch in channels]
    ch_btns.append([KeyboardButton("/cancel")])

    ch_r = await bot.ask(user_id,
        "<b>╭──────❰ 📦 sᴛᴇᴘ 4/5 — ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟ ❱──────╮\n"
        "┃\n┣⊸ ᴄʜᴏᴏsᴇ ᴡʜᴇʀᴇ ᴛᴏ ᴄᴏᴘʏ ᴍᴇssᴀɢᴇs\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup(ch_btns, resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in ch_r.text:
        return await ch_r.reply(_CANCEL_BOX, reply_markup=ReplyKeyboardRemove())

    to_chat = to_title = None
    for ch in channels:
        if ch['title'] == ch_r.text.strip():
            to_chat  = ch['chat_id']
            to_title = ch['title']
            break

    if not to_chat:
        return await bot.send_message(user_id,
            "<b>❌ ɪɴᴠᴀʟɪᴅ sᴇʟᴇᴄᴛɪᴏɴ.</b>", reply_markup=ReplyKeyboardRemove())

    # Step 5 — Custom Name
    name_r = await bot.ask(user_id,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 5/5 — ᴊᴏʙ ɴᴀᴍᴇ (ᴏᴘᴛɪᴏɴᴀʟ) ❱──────╮\n"
        "┃\n┣⊸ sᴇɴᴅ ᴀ sʜᴏʀᴛ ɴᴀᴍᴇ ғᴏʀ ᴛʜɪs ᴊᴏʙ ᴛᴏ ɪᴅᴇɴᴛɪғʏ ɪᴛ ᴇᴀsɪʟʏ.\n"
        "┣⊸ ᴏʀ ᴄʟɪᴄᴋ sᴋɪᴘ ᴛᴏ ᴜsᴇ ᴅᴇғᴀᴜʟᴛ.\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup([
            [KeyboardButton("sᴋɪᴘ (ᴜsᴇ ᴅᴇғᴀᴜʟᴛ)")], [KeyboardButton("/cancel")]
        ], resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in name_r.text:
        return await name_r.reply(_CANCEL_BOX, reply_markup=ReplyKeyboardRemove())

    cname = None
    if "sᴋɪᴘ" not in name_r.text.lower() and "skip" not in name_r.text.lower():
        cname = name_r.text.strip()[:30]

    # Save & Smart-schedule
    job_id = f"bj-{user_id}-{int(time.time())}"
    job = {
        "job_id": job_id, "user_id": user_id, "account_id": sel["id"],
        "from_chat": fc, "from_title": ftitle,
        "to_chat": to_chat, "to_title": to_title,
        "start_id": start_id, "end_id": end_id, "current_id": start_id,
        "status": "running", "created": int(time.time()),
        "forwarded": 0, "consecutive_empty": 0, "error": "",
        "custom_name": cname,
    }

    started = _queue_or_start(job_id, user_id, to_chat, _bot=bot)
    if not started:
        job["status"] = "queued"
        queue_pos = len(_ch_queue.get(to_chat, []))

    await _tj_save(job)

    end_lbl = f"<code>{end_id}</code>" if end_id else "∞ (ᴀʟʟ ᴍsɢs)"
    if started:
        status_line = "▶️ ʀᴜɴɴɪɴɢ ɴᴏᴡ"
    else:
        status_line = f"🕐 Queued at position {queue_pos} (waiting for {to_title} to free up)"

    await bot.send_message(user_id,
        f"<b>╭──────❰ ✅ ʙᴀᴛᴄʜ ᴊᴏʙ ᴄʀᴇᴀᴛᴇᴅ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {ftitle}\n"
        f"┣⊸ ◈ 𝐓𝐚𝐫𝐠𝐞𝐭  : {to_title}\n"
        f"┣⊸ ◈ 𝐀𝐜𝐜𝐨𝐮𝐧𝐭 : {{'🤖 ʙᴏᴛ' if ibot else '👤 ᴜsᴇʀʙᴏᴛ'}} {sel.get('name','?')}\n"
        f"┣⊸ ◈ 𝐑𝐚𝐧𝐠𝐞   : <code>{start_id}</code> → {end_lbl}\n"
        f"┣⊸ ◈ 𝐉𝐨𝐛 𝐈𝐃  : <code>{job_id[-6:]}</code>" + (f" (<b>{cname}</b>)\n" if cname else "\n") +
        f"┣⊸ ◈ 𝐒𝐭𝐚𝐭𝐮𝐬  : {status_line}\n"
        f"┃\n"
        f"╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardRemove())
