"""
Task Jobs Plugin — Unicode-styled
===================================
Persistent background bulk-copy jobs with pause/resume.
Styled identically to the rest of Arya Bot (box borders, small-caps, 𝐛𝐨𝐥𝐝 𝐦𝐚𝐭𝐡 field names).
"""
import re
import os
import time
import asyncio
import logging
from database import db
from .test import CLIENT, start_clone_bot
from config import Config
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

COLL = "taskjobs"

# Global semaphore: limit concurrent heavy downloads to 2 so large files
# (500MB-1GB) don't starve other running task jobs. Copy/forward ops skip it.
_DOWNLOAD_SEM = asyncio.Semaphore(2)

# ── Unicode helpers ────────────────────────────────────────────────────────────
def _st(status: str) -> str:
    return {"running": "🟢", "paused": "⏸", "stopped": "🔴", "done": "✅", "error": "⚠️"}.get(status, "❓")


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
    """Send/edit a live task job status message to the user."""
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
    
    # Live data from progress tracking
    progress_p = ""
    if job.get("dl_size"):
        sz_mb = job['dl_size'] / (1024*1024)
        progress_p = f"\n  • <b>Current File:</b> <code>{sz_mb:.1f} MB</code>"
        if job.get("dl_progress"):
            progress_p += f"\n  • <b>Progress:</b> <code>{job['dl_progress']}%</code>"

    text = (
        f"<b>Task Job Progress</b>\n\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_p}\n"
        f"  • <b>Status:</b> {st} {job.get('status','running')}\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}\n\n"
        f"  • <b>Range:</b> {rng_p}\n"
        f"  • <b>Current:</b> <code>{cur}</code>\n"
        f"  • <b>Forwarded:</b> <code>{fwd}</code>"
        f"{phase_p}{progress_p}{err_p}"
    )
    key = (uid, job_id)
    try:
        existing_mid = _tj_status_msgs.get(key)
        if existing_mid:
            try:
                await bot.edit_message_text(uid, existing_mid, text)
                return
            except Exception as e:
                if "MESSAGE_NOT_MODIFIED" in str(e):
                    return
                pass
        sent = await bot.send_message(uid, text)
        _tj_status_msgs[key] = sent.id
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Filter helper
# ══════════════════════════════════════════════════════════════════════════════

def _passes_filters(msg, dis: list) -> bool:
    """Content-type check. If ALL filters are ON (none in dis), return True."""
    if msg.empty or msg.service: return False
    if not dis: return True # ALL filters are ON -> no content filtering
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

async def _send_one(client, msg, to_chat: int, remove_caption: bool, caption_tpl, forward_tag=False, from_chat=None, block_links=False, to_topic=None):
    caption = None
    is_modified = False

    # FIX: Preserve ORIGINAL formatting unless explicitly asked to modify
    # Filter behavior: if rm_caption is OFF and caption_tpl is NONE, keep raw.
    if caption_tpl and msg.media:
        caption = caption_tpl
        is_modified = True
    elif remove_caption and msg.media:
        caption = ""
        is_modified = True
    elif block_links and _has_links(msg):
        content = getattr(msg, 'caption' if msg.media else 'text', None)
        if content:
            # Use html to preserve bold/italic/etc
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
                await client.forward_messages(chat_id=to_chat, from_chat_id=from_id, message_ids=msg.id, **kw_fwd)
                return True
            except FloodWait as fw:
                raise fw
            except Exception:
                pass # fall through to copy

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

        # IF NOT MODIFIED: copy_message preserves formatting natively.
        # IF MODIFIED: we pass the modified text.
        if is_modified:
            await client.copy_message(chat_id=to_chat, from_chat_id=from_id,
                                      message_id=msg.id, caption=caption, **kw_msg)
        else:
            await client.copy_message(chat_id=to_chat, from_chat_id=from_id, message_id=msg.id, **kw_msg)
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
        return await _send_one(client, msg, to_chat, remove_caption, caption_tpl, forward_tag, from_chat, block_links, to_topic)
    except Exception as e:
        # Download fallback
        try:
            if msg.media:
                mo = getattr(msg, msg.media.value, None)
                # display_name = Telegram UI name (what user sees), may differ from disk name
                display_name = getattr(mo, 'file_name', None) if mo else None
                if display_name:
                    import re as _re4
                    display_name = _re4.sub(r'[\\/*?:"<>|]', '', display_name).strip() or None
                import shutil as _shu2
                import main
                safe_dir = f"downloads/{msg.id}"
                os.makedirs(safe_dir, exist_ok=True)
                df_name = f"{safe_dir}/{display_name}" if display_name else f"{safe_dir}/"
                
                f_size = getattr(mo, "file_size", 0)
                main.TOTAL_DOWNLOADS += 1
                main.TOTAL_BYTES_TRANSFERRED += f_size
                
                async def progress(current, total):
                    pc = int(current * 100 / total) if total > 0 else 0
                    # Note: updating job dict directly here might not be thread-safe for notification 
                    # but since only 1 worker handles 1 job, we just update local state if needed.
                    # This update is just for the local task runner to pass to _tj_notify
                    pass

                # Semaphore: only 2 heavy downloads can run simultaneously. Others wait, not blocked.
                # This prevents a 1GB file from delaying ALL other task jobs completely.
                async with _DOWNLOAD_SEM:
                    fp = await client.download_media(msg, file_name=df_name, progress=progress)
                if not fp: raise Exception("DownloadFailed")
                
                main.TOTAL_UPLOADS += 1
                kw = {"chat_id": to_chat, "caption": caption if caption is not None else (str(msg.caption) if msg.caption else "")}
                if to_topic: kw["message_thread_id"] = kw["reply_to_message_id"] = to_topic
                try:
                    if msg.photo:       await client.send_photo(photo=fp, **kw)
                    elif msg.video:     await client.send_video(video=fp, file_name=display_name, **kw)
                    elif msg.document:  await client.send_document(document=fp, file_name=display_name, **kw)
                    elif msg.audio:     await client.send_audio(audio=fp, file_name=display_name, title=getattr(mo, 'title', None), performer=getattr(mo, 'performer', None), **kw)
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
            logger.debug(f"[TaskJob] send fallback: {e2}")
            return False


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
    last_notify = 0  # for auto status notifications

    acc = client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _tj_update(job_id, status="error", error="Account not found"); return

        from config import Config
        is_main_bot = acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN
        if is_main_bot and getattr(_bot, 'is_connected', False):
            client = _bot
        else:
            from plugins.jobs import _get_shared_client
            client = await _get_shared_client(acc)
            if not client: raise Exception(f"Failed to start client for acc {acc.get('name')}")

        is_bot  = acc.get("is_bot", True)
        fc      = job["from_chat"]

        # CRITICAL BUG FIX: determine if source is channel safely
        fc_is_channel = False
        try:
            if str(fc).startswith("-100"):
                fc_is_channel = True
            else:
                try:
                    # Best: resolve_peer cleanly identifies users vs channels without joining
                    from pyrogram.raw.types import InputPeerChannel
                    peer = await client.resolve_peer(fc)
                    if isinstance(peer, InputPeerChannel):
                        fc_is_channel = True
                except Exception:
                    # Fallback to get_chat if resolve_peer fails (e.g. invite links)
                    from pyrogram.enums import ChatType
                    c_obj = await client.get_chat(fc)
                    if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                        fc_is_channel = True
        except Exception as e:
            # If all checks failed, and it's a string username, assume channel for bots
            # because bots can't use get_chat_history on public channels anyway.
            if getattr(client, 'me', None) and client.me.is_bot and isinstance(fc, str):
                fc_is_channel = True
        to_chat = job["to_chat"]
        to_topic = job.get("to_topic_id")
        from_topic = job.get("from_topic_id")
        end_id  = job.get("end_id", 0)
        current = job.get("current_id", job.get("start_id", 1))

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
                # Try get_messages first (works ONLY for channels/supergroups!)
                if fc_is_channel:
                    try:
                        msgs = await client.get_messages(fc, batch_ids)
                        if not isinstance(msgs, list): msgs = [msgs]
                        # Ordering fix: msgs from get_messages(IDs) might not be ordered.
                        msgs = [m for m in msgs if m and not m.empty]
                        msgs.sort(key=lambda x: x.id)
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as ge:
                        logger.warning(f"[TaskJob {job_id}] get_messages failed @ {current}: {ge}")
                else:
                    fetch_ok = False
                # Fallback: get_chat_history (for userbots and bot DMs)
                if not fetch_ok:
                    try:
                        col = []
                        async for hmsg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_SIZE):
                            if hmsg.id < current: break
                            col.append(hmsg)
                        msgs = list(reversed(col)) # History is newest-first, flip it
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as he:
                        logger.warning(f"[TaskJob {job_id}] history fallback failed @ {current}: {he}")
                
                if not fetch_ok or not msgs:
                    # FIX Auto-Stop: Instead of skipping large gaps, we probe history for the NEXT valid ID 
                    # before jumping ahead blindly.
                    try:
                        async for nm in client.get_chat_history(fc, offset_id=current, limit=1, reverse=True):
                            if nm.id > current:
                                current = nm.id
                                await _tj_update(job_id, current_id=current)
                                break
                        else:
                            # if no message after 'current', we are done
                            await _tj_update(job_id, status="done")
                            break
                    except:
                        current += BATCH_SIZE; await _tj_update(job_id, current_id=current)
                    continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f"[TaskJob {job_id}] Fetch outer exception {current}: {e}")
                current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue

            valid = sorted([m for m in msgs if m and not getattr(m, 'empty', False) and not getattr(m, 'service', False)], key=lambda m: m.id)

            if not valid:
                consec = fresh.get("consecutive_empty", 0) + 1
                if consec >= 50:  # Allow up to 10000 deleted/service messages in a row before giving up
                    logger.info(f"[TaskJob {job_id}] Hit {consec} empty chunks. Ending job.")
                    await _tj_update(job_id, status="done", current_id=current)
                    break
                logger.info(f"[TaskJob {job_id}] Empty chunk {current}->{current+BATCH_SIZE-1} (consec {consec}/50)")
                current += BATCH_SIZE
                await _tj_update(job_id, consecutive_empty=consec, current_id=current)
                await asyncio.sleep(1); continue

            await _tj_update(job_id, consecutive_empty=0)

            # Auto status notification every 60s
            _now = int(time.time())
            if _bot and _now - last_notify >= 60:
                _fresh_j = await _tj_get(job_id)
                if _fresh_j:
                    await _tj_notify(_bot, _fresh_j, "ʀᴜɴɴɪɴɢ")
                last_notify = _now

            # Parallel Batch Processing: launch concurrent forward tasks
            tasks = []
            for msg in valid:
                if from_topic:
                    if getattr(msg, 'message_thread_id', getattr(msg, 'reply_to_top_message_id', getattr(msg, 'reply_to_message_id', None))) != from_topic:
                        if msg.id != from_topic: # Allow the top message of topics through
                            continue
                            
                await pause_ev.wait()
                f2 = await _tj_get(job_id)
                if not f2 or f2.get("status") in ("stopped",): return
                if not _passes_filters(msg, dis): continue
                
                from plugins.jobs import _fwd_safe
                tasks.append(_fwd_safe(_send_one, client, msg, to_chat, rm_cap, cap_tpl, 
                                       forward_tag=forward_tag, from_chat=fc, 
                                       block_links=block_links, to_topic=to_topic))
            
            fwd = job.get("forwarded", 0)
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"[TaskJob {job_id}] Parallel error: {r}")
                        continue
                    if r is True:
                        import main
                        fwd += 1
                        main.TOTAL_FILES_FWD += 1
                        await _tj_inc(job_id)

            current = (msgs[-1].id + 1) if msgs else (current + BATCH_SIZE)
            await _tj_update(job_id, current_id=current)

        # ── End of Task Job logic ─────────────────────────────────────────────
        job_f = await _tj_get(job_id)
        if job_f and job_f.get("status") == "done":
            pass

    except asyncio.CancelledError:
        logger.info(f"[TaskJob {job_id}] Cancelled")
        await _tj_update(job_id, status="stopped")
    except Exception as e:
        logger.error(f"[TaskJob {job_id}] Fatal: {e}")
        await _tj_update(job_id, status="error", error=str(e))
    finally:
        _task_jobs.pop(job_id, None); _pause_events.pop(job_id, None)
        if acc:
            from config import Config
            is_main_bot = acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN
            if not (is_main_bot and getattr(_bot, 'is_connected', False)):
                from plugins.jobs import _release_shared_client
                await _release_shared_client(acc)


def _start_task(job_id: str, user_id: int, _bot=None):
    ev = asyncio.Event(); ev.set()
    _pause_events[job_id] = ev
    task = asyncio.create_task(_run_task_job(job_id, user_id, _bot=_bot))
    _task_jobs[job_id] = task
    return task


async def resume_task_jobs(user_id: int = None):
    q = {"status": "running"}
    if user_id: q["user_id"] = user_id
    async for job in db.db[COLL].find(q):
        jid, uid = job["job_id"], job["user_id"]
        if jid not in _task_jobs:
            _start_task(jid, uid)


# ══════════════════════════════════════════════════════════════════════════════
# Storage Cleanup Command
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("cleanup") & filters.user(Config.BOT_OWNER_ID))
async def cleanup_storage(bot, message):
    import shutil
    from config import Config
    
    msg = await message.reply_text("<b>🧹 ᴄʟᴇᴀɴɪɴɢ ᴜᴘ sᴛᴏʀᴀɢᴇ...</b>")
    
    freed = 0
    dirs = ["downloads", "tmp"]
    
    for d in dirs:
        if os.path.exists(d):
            # Calculate size before cleanup
            for root, _, files in os.walk(d):
                for f in files:
                    freed += os.path.getsize(os.path.join(root, f))
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d, exist_ok=True)

    freed_mb = freed / (1024*1024)
    
    await msg.edit(
        "<b>╭──────❰ 🧹 ᴄʟᴇᴀɴᴜᴘ ᴅᴏɴᴇ ❱──────╮\n"
        "┃\n"
        f"┣⊸ ᴅɪʀs ᴘᴜʀɢᴇᴅ: <code>downloads, tmp</code>\n"
        f"┣⊸ sᴘᴀᴄᴇ ғʀᴇᴇᴅ: <code>{freed_mb:.2f} MB</code>\n"
        "┣⊸ sʏsᴛᴇᴍ ɪs ɴᴏᴡ ᴏᴘᴛɪᴍɪᴢᴇᴅ ✅\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    )


# ══════════════════════════════════════════════════════════════════════════════
# UI — render list
# ══════════════════════════════════════════════════════════════════════════════

async def _render_taskjob_list(bot, user_id: int, mq):
    jobs  = await _tj_list(user_id)
    is_cb = hasattr(mq, "message")

    if not jobs:
        text = (
            "<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙs ❱──────╮\n"
            "┃\n"
            "┣⊸ ɴᴏ ᴛᴀsᴋ ᴊᴏʙs ʏᴇᴛ.\n"
            "┣⊸ ᴄᴏᴘɪᴇs ᴀʟʟ ᴇxɪsᴛɪɴɢ ᴍᴇssᴀɢᴇs.\n"
            "┃\n"
            "╰────────────────────────────────╯</b>"
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ ᴄʀᴇᴀᴛᴇ ᴛᴀsᴋ ᴊᴏʙ", callback_data="tj#new")
        ]])
    else:
        lines = ["<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙs ❱──────╮</b>\n┃"]
        for j in jobs:
            st  = _st(j.get("status", "stopped"))
            fwd = j.get("forwarded", 0)
            err = f"\n┃  ⚠️ <code>{j.get('error','')}</code>" if j.get("status") == "error" else ""
            c_name = j.get("custom_name")
            name_disp = f" <b>{c_name}</b>" if c_name else ""
            cur = j.get("current_id", "?")
            end = j.get("end_id", 0)
            rng_lbl = f"<code>{j.get('start_id',1)}</code> → <code>{end}</code>" if end else f"<code>{j.get('start_id',1)}</code> → ∞"
            
            lines.append(
                f"┣⊸ {st} <b>{j.get('from_title','?')} → {j.get('to_title','?')}</b>"
                f"  <code>[{j['job_id'][-6:]}]</code>{name_disp}"
                f"\n┃   ◈ 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐝: <code>{fwd}</code>  |  𝐂𝐮𝐫𝐫𝐞𝐧𝐭: <code>{cur}</code>"
                f"\n┃   ◈ 𝐑𝐚𝐧𝐠𝐞: {rng_lbl}{err}"
            )
        text = "\n".join(lines)

        rows = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            s   = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏸ Pause [{s}]",  callback_data=f"tj#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Stop [{s}]",   callback_data=f"tj#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ Resume [{s}]", callback_data=f"tj#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ Stop [{s}]",   callback_data=f"tj#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Start [{s}]",  callback_data=f"tj#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ [{s}]", callback_data=f"tj#info#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 [{s}]",  callback_data=f"tj#del#{jid}"))
            rows.append(row)

        rows.append([InlineKeyboardButton("➕ Create Task Job", callback_data="tj#new")])
        rows.append([InlineKeyboardButton("🔄 ʀᴇғʀᴇsʜ",          callback_data="tj#list")])
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

@Client.on_message(filters.private & filters.command(["taskjobs", "taskjob"]))
async def taskjobs_cmd(bot, msg):
    await _render_taskjob_list(bot, msg.from_user.id, msg)


@Client.on_message(filters.private & filters.command("newtaskjob"))
async def newtaskjob_cmd(bot, msg):
    await _create_taskjob_flow(bot, msg.from_user.id)


# ══════════════════════════════════════════════════════════════════════════════
# Callbacks
# ══════════════════════════════════════════════════════════════════════════════

_CANCEL_BOX = (
    "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n"
    "┃\n╰────────────────────────────────╯</b>"
)


@Client.on_callback_query(filters.regex(r'^tj#list$'))
async def tj_list_cb(bot, q): await _render_taskjob_list(bot, q.from_user.id, q)


@Client.on_callback_query(filters.regex(r'^tj#new$'))
async def tj_new_cb(bot, q):
    await q.message.delete()
    await _create_taskjob_flow(bot, q.from_user.id)


@Client.on_callback_query(filters.regex(r'^tj#info#'))
async def tj_info_cb(bot, query):
    job_id = query.data.split("#", 2)[2]
    job = await _tj_get(job_id)
    if not job: return await query.answer("ᴊᴏʙ ɴᴏᴛ ғᴏᴜɴᴅ!", show_alert=True)

    import datetime
    created = datetime.datetime.fromtimestamp(job.get("created", 0)).strftime("%d %b %Y · %H:%M")
    st = _st(job.get("status", "stopped"))
    cur = job.get("current_id", "?")
    end = job.get("end_id", 0)
    rng_lbl = f"<code>{job.get('start_id',1)}</code> → <code>{end}</code>" if end else f"<code>{job.get('start_id',1)}</code> → ∞"
    err_lbl = f"\n  • ⚠️ <b>Error:</b> <code>{job['error']}</code>" if job.get("error") else ""

    c_name   = job.get("custom_name")
    name_lbl = f" <b>({c_name})</b>" if c_name else ""

    text = (
        f"<b>📋 Task Job Information</b>\n\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_lbl}\n"
        f"  • <b>Status:</b> {st} {job.get('status','?')}\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}\n"
        f"  • <b>Range:</b> {rng_lbl}\n"
        f"  • <b>Current:</b> <code>{cur}</code>\n"
        f"  • <b>Forwarded:</b> <code>{job.get('forwarded', 0)}</code>\n"
        f"  • <b>Created:</b> {created}"
        f"{err_lbl}"
    )
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("↩ ʙᴀᴄᴋ", callback_data="tj#list")
    ]]))


@Client.on_callback_query(filters.regex(r'^tj#pause#'))
async def tj_pause_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    ev = _pause_events.get(job_id)
    if ev: ev.clear()
    await _tj_update(job_id, status="paused")
    await q.answer("⏸ Pauseᴅ.")
    await _render_taskjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^tj#resume#'))
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
        await _tj_update(job_id, status="running")
        _start_task(job_id, uid, _bot=bot)
        await q.answer("▶️ ʀᴇsᴛᴀʀᴛᴇᴅ ғʀᴏᴍ sᴀᴠᴇᴅ ᴘᴏsɪᴛɪᴏɴ!")
    await _render_taskjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^tj#stop#'))
async def tj_stop_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    task = _task_jobs.pop(job_id, None)
    if task and not task.done(): task.cancel()
    ev = _pause_events.pop(job_id, None)
    if ev: ev.set()
    await _tj_update(job_id, status="stopped")
    await q.answer("⏹ Stopᴘᴇᴅ.")
    await _render_taskjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^tj#start#'))
async def tj_start_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    if job_id in _task_jobs and not _task_jobs[job_id].done():
        return await q.answer("ᴀʟʀᴇᴀᴅʏ ʀᴜɴɴɪɴɢ!", show_alert=True)
    await _tj_update(job_id, status="running")
    _start_task(job_id, uid, _bot=bot)
    await q.answer("▶️ Startᴇᴅ!")
    await _render_taskjob_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^tj#del#'))
async def tj_del_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _tj_get(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    task = _task_jobs.pop(job_id, None)
    if task and not task.done(): task.cancel()
    ev = _pause_events.pop(job_id, None)
    if ev: ev.set()
    await _tj_delete(job_id)
    await q.answer("🗑 ᴅᴇʟᴇᴛᴇᴅ.")
    await _render_taskjob_list(bot, uid, q)


# ══════════════════════════════════════════════════════════════════════════════
# Create Task Job — Interactive flow
# ══════════════════════════════════════════════════════════════════════════════

def _clear_listeners(bot, user_id: int):
    try:
        import pyrogram.enums as _pe
        _lst = bot.listeners.get(_pe.ListenerTypes.MESSAGE, [])
        to_remove = [l for l in list(_lst) if (
            l.identifier.chat_id == user_id or
            l.identifier.from_user_id == user_id
        )]
        for l in to_remove:
            _lst.remove(l)
            if not l.future.done(): l.future.cancel()
    except Exception:
        pass

async def _create_taskjob_flow(bot, user_id: int):
    _clear_listeners(bot, user_id)
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
        "<b>╭──────❰ 📦 ᴄʀᴇᴀᴛᴇ ᴛᴀsᴋ ᴊᴏʙ — sᴛᴇᴘ 1/4 ❱──────╮\n"
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
        "┃\n"
        "┣⊸ <i>Pʀɪᴠᴀᴛᴇ ᴄʜᴀᴛ ɪᴅs ᴀʀᴇ ᴘᴏsɪᴛɪᴠᴇ ɴᴜᴍʙᴇʀs (ɴᴏ ᴍɪɴᴜs)</i>\n"
        "┣⊸ <i>ʙᴏᴛʜ ʙᴏᴛ ᴀɴᴅ ᴜsᴇʀʙᴏᴛ ᴄᴀɴ ᴍᴏɴɪᴛᴏʀ ᴅᴍs ᴠɪᴀ ᴍᴛᴘʀᴏᴛᴏ</i>\n"
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
        ftitle = getattr(co, "title", None) or getattr(co, "first_name", str(fc))
    except Exception:
        co = None
        ftitle = str(fc)

    if await db.is_protected(raw, co):
        return await bot.send_message(user_id,
            "<b>╭──────❰ ⚠️ Pʀᴏᴛᴇᴄᴛɪᴏɴ Eʀʀᴏʀ ❱──────╮\n"
            "┃\n┣⊸ Ohh no! ERROR — This source is protected by the owner.\n"
            "┣⊸ Please try another source.\n"
            "┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())

    from_topic_id = None
    _clear_listeners(bot, user_id)
    src_topic_r = await bot.ask(user_id,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 2b — sᴏᴜʀᴄᴇ ᴛᴏᴘɪᴄ ❱──────╮\n"
        "┃\n"
        "┣⊸ ɪғ sᴏᴜʀᴄᴇ ɪs ᴀ ɢʀᴏᴜᴘ ᴡɪᴛʜ ᴛᴏᴘɪᴄs, ᴇɴᴛᴇʀ ᴛʜᴇ ᴛᴏᴘɪᴄ ɪᴅ\n"
        "┣⊸ sᴇɴᴅ 0 ᴛᴏ ғᴏʀᴡᴀʀᴅ ᴀʟʟ ᴍᴇssᴀɢᴇs (ɴᴏ ᴛᴏᴘɪᴄ ғɪʟᴛᴇʀ)\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup([["0 (ɴᴏ ᴛᴏᴘɪᴄ ғɪʟᴛᴇʀ)"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True))
    if "/cancel" in src_topic_r.text:
        return await src_topic_r.reply(_CANCEL_BOX, reply_markup=ReplyKeyboardRemove())
    _st_raw = src_topic_r.text.strip()
    from_topic_id = int(_st_raw) if _st_raw.isdigit() and int(_st_raw) > 0 else None

    # Step 3 — Range
    _clear_listeners(bot, user_id)
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

    _clear_listeners(bot, user_id)
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

    to_topic_id = None
    _clear_listeners(bot, user_id)
    to_topic_r = await bot.ask(user_id,
        "<b>╭──────❰ 💬 ᴛᴏᴘɪᴄ ᴛʜʀᴇᴀᴅ — ᴅᴇsᴛɪɴᴀᴛɪᴏɴ ❱──────╮\n"
        "┃\n"
        "┣⊸ sᴇɴᴅ ᴛʜʀᴇᴀᴅ ɪᴅ ᴛᴏ ᴘᴏsᴛ ɪɴᴛᴏ ᴀ ᴛᴏᴘɪᴄ\n"
        "┣⊸ sᴇɴᴅ 0 ᴛᴏ ᴘᴏsᴛ ɪɴ ᴍᴀɪɴ ᴄʜᴀᴛ\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup([["0 (ɴᴏ ᴛᴏᴘɪᴄ)"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True))
    if "/cancel" in to_topic_r.text: return await to_topic_r.reply(_CANCEL_BOX, reply_markup=ReplyKeyboardRemove())
    _t = to_topic_r.text.strip()
    to_topic_id = int(_t) if _t.isdigit() and int(_t) > 0 else None

    # Step 5 — Custom Name
    _clear_listeners(bot, user_id)
    name_r = await bot.ask(user_id,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 5/6 — ᴊᴏʙ ɴᴀᴍᴇ (ᴏᴘᴛɪᴏɴᴀʟ) ❱──────╮\n"
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

    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # Auto-Scheduler Logic has been removed as per user request.
    # Task Jobs always start immediately.
    # ---------------------------------------------------------
    initial_status = "running"
    is_scheduled = False

    # Save & Start
    job_id = f"tj-{user_id}-{int(time.time())}"

    job = {
        "job_id": job_id, "user_id": user_id, "account_id": sel["id"],
        "from_chat": fc, "from_title": ftitle, "from_topic_id": from_topic_id,
        "to_chat": to_chat, "to_title": to_title, "to_topic_id": to_topic_id,
        "start_id": start_id, "end_id": end_id, "current_id": start_id,
        "status": initial_status, "created": int(time.time()),
        "forwarded": 0, "consecutive_empty": 0, "error": "",
        "custom_name": cname,
        "scheduled": False
    }
    await _tj_save(job)
    _start_task(job_id, user_id, _bot=bot)

    end_lbl = f"<code>{end_id}</code>" if end_id else "∞ (ᴀʟʟ ᴍsɢs)"
    
    import html as html_lib
    ftitle_safe = html_lib.escape(str(ftitle)) if ftitle else "?"
    to_title_safe = html_lib.escape(str(to_title)) if to_title else "?"
    cname_safe = html_lib.escape(str(cname)) if cname else None
    
    status_msg = (
        f"<b>╭──────❰ ✅ ᴛᴀsᴋ ᴊᴏʙ ᴄʀᴇᴀᴛᴇᴅ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {ftitle_safe}\n"
        f"┣⊸ ◈ 𝐓𝐚𝐫𝐠𝐞𝐭  : {to_title_safe}\n"
        f"┣⊸ ◈ 𝐀𝐜𝐜𝐨𝐮𝐧𝐭 : {'🤖 ʙᴏᴛ' if ibot else '👤 ᴜsᴇʀʙᴏᴛ'} {sel.get('name','?')}\n"
        f"┣⊸ ◈ 𝐑𝐚𝐧𝐠𝐞   : <code>{start_id}</code> → {end_lbl}\n"
        f"┣⊸ ◈ sᴛᴀᴛᴜs  : {initial_status.upper()}\n"
        f"┣⊸ ◈ 𝐉𝐨𝐛 𝐈𝐃  : <code>{job_id[-6:]}</code>" + (f" (<b>{cname_safe}</b>)\n" if cname_safe else "\n") +
        f"┃\n╰────────────────────────────────╯</b>"
    )

    try:
        await bot.send_message(user_id, status_msg, reply_markup=ReplyKeyboardRemove())
    except Exception as _e:
        await bot.send_message(user_id, f"✅ ᴛᴀsᴋ ᴊᴏʙ ᴄʀᴇᴀᴛᴇᴅ\nJob ID: <code>{job_id}</code>\n<i>(HTML syntax error saved)</i>", reply_markup=ReplyKeyboardRemove())

