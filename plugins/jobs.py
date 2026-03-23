"""
Live Jobs Plugin — v3
======================
Unicode-styled to match the rest of Arya Bot (small-caps, box borders, 𝐛𝐨𝐥𝐝 𝐦𝐚𝐭𝐡).
Features: batch-first mode, dual destinations, per-job size/duration limits, topic threads.
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

_job_tasks: dict[str, asyncio.Task] = {}

# ── Unicode helpers ────────────────────────────────────────────────────────────
def _box(title: str, lines: list[str]) -> str:
    body = "\n".join(f"  • {l}" for l in lines)
    return (f"✦ {title.upper()} ✦\n\n{body}")

def _st(status: str) -> str:
    """Status emoji."""
    return {"running": "🟢", "stopped": "🔴", "error": "⚠️", "done": "✅"}.get(status, "❓")

def _batch_tag(job: dict) -> str:
    if not job.get("batch_mode"):
        return ""
    if job.get("batch_done"):
        return "  📦 ✅"
    cur = job.get("batch_cursor") or job.get("batch_start_id") or "?"
    end = job.get("batch_end_id") or "…"
    return f"  📦{cur}/{end}"


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

async def _update_job(job_id: str, **kw):
    await db.db.jobs.update_one({"job_id": job_id}, {"$set": kw})

async def _inc_forwarded(job_id: str, n: int = 1):
    await db.db.jobs.update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})


# ── Auto-status notifier ────────────────────────────────────────────────────
# Holds (bot_instance, user_id) -> message_id of live status message
_status_msgs: dict = {}

async def _notify_status(bot, job: dict, phase: str = ""):
    """Send/edit a live status message to the user so they see real-time progress."""
    if not bot:
        return
    uid       = job["user_id"]
    job_id    = job["job_id"]
    st        = _st(job.get("status", "running"))
    fwd       = job.get("forwarded", 0)
    src       = job.get("from_title", "?")
    dst       = job.get("to_title", "?")
    cname     = job.get("custom_name", "")
    name_part = f" <b>{cname}</b>" if cname else ""
    batch_part = ""
    if job.get("batch_mode"):
        if job.get("batch_done"):
            batch_part = "\n  • <b>Batch:</b> ✅ Complete"
        else:
            cur = job.get("batch_cursor") or job.get("batch_start_id") or "?"
            end = job.get("batch_end_id") or "…"
            batch_part = f"\n  • <b>Batch:</b> 📦 <code>{cur}</code> / <code>{end}</code>"
    phase_part = f"\n  • <b>Phase:</b> <code>{phase}</code>" if phase else ""
    err_part   = f"\n  • ⚠️ <code>{job['error']}</code>" if job.get("error") else ""
    
    # Live data from progress tracking
    progress_part = ""
    if job.get("dl_size"):
        sz_mb = job['dl_size'] / (1024*1024)
        progress_part = f"\n  • <b>Current File:</b> <code>{sz_mb:.1f} MB</code>"
        if job.get("dl_progress"):
            progress_part += f"\n  • <b>Progress:</b> <code>{job['dl_progress']}%</code>"

    text = (
        f"<b>Live Job Progress</b>\n\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_part}\n"
        f"  • <b>Status:</b> {st} {job.get('status','running')}\n"
        f"  • <b>Source:</b> {src}\n"
        f"  • <b>Destination:</b> {dst}\n\n"
        f"  • <b>Forwarded:</b> <code>{fwd}</code>"
        f"{batch_part}{phase_part}{progress_part}{err_part}"
    )
    key = (uid, job_id)
    try:
        existing_msg_id = _status_msgs.get(key)
        if existing_msg_id:
            try:
                await bot.edit_message_text(uid, existing_msg_id, text)
                return
            except Exception as e:
                if "MESSAGE_NOT_MODIFIED" in str(e):
                    return
                pass  # message deleted or too old — send a new one
        sent = await bot.send_message(uid, text)
        _status_msgs[key] = sent.id
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Filter helpers
# ══════════════════════════════════════════════════════════════════════════════

import re as _re

# URL / link detection — catches http(s), t.me, @username, inline bot links,
# YouTube previews, channel/bot links, raw domains w/ known TLDs, etc.
_LINK_RE = _re.compile(
    r'(https?://\S+'
    r'|t\.me/\S+'
    r'|@[A-Za-z0-9_]{4,}'
    r'|\b(?:www\.|bit\.ly/|youtu\.be/)\S+'
    r'|\b[\w.-]+\.(?:com|net|org|io|co|me|tv|gg|app|xyz|info|news|link|site)(?:/\S*)?\b)',
    _re.IGNORECASE
)

def _has_links(msg) -> bool:
    """Return True if message text or caption contains any URL / link."""
    for field in ('text', 'caption'):
        content = getattr(msg, field, None)
        if content:
            raw = content.html if hasattr(content, 'html') else str(content)
            if _LINK_RE.search(raw):
                return True
    # Also check Pyrogram entities for URLs/text-mentions
    for field in ('entities', 'caption_entities'):
        ents = getattr(msg, field, None) or []
        for e in ents:
            if getattr(e, 'type', '') in ('url', 'text_link', 'mention', 'bot_command'):
                return True
    return False


def _passes_topic(msg, from_topic_id) -> bool:
    """Return True if message belongs to the configured source topic (or no topic is set)."""
    if not from_topic_id:
        return True
    if getattr(msg, 'message_thread_id', None) == from_topic_id:
        return True
    if getattr(msg, 'reply_to_top_message_id', None) == from_topic_id:
        return True
    rm = getattr(msg, 'reply_to_message', None)
    rm_id = getattr(rm, 'id', None) if rm else getattr(msg, 'reply_to_message_id', None)
    return rm_id == from_topic_id


def _passes_filters(msg, disabled: list) -> bool:
    """Check content-type filters. If ALL filters are ON (none in disabled), return True."""
    if not disabled: return True # ALL filters are ON -> no content filtering
    for typ, chk in [
        ('text',      lambda m: m.text and not m.media),
        ('audio',     lambda m: m.audio),
        ('voice',     lambda m: m.voice),
        ('video',     lambda m: m.video),
        ('photo',     lambda m: m.photo),
        ('document',  lambda m: m.document),
        ('animation', lambda m: m.animation),
        ('sticker',   lambda m: m.sticker),
        ('poll',      lambda m: m.poll),
    ]:
        if typ in disabled and chk(msg):
            return False
    return True


def _passes_size(msg, max_mb: int, max_secs: int) -> bool:
    if max_mb > 0:
        for attr in ('document', 'video', 'audio', 'voice', 'video_note', 'animation', 'photo'):
            obj = getattr(msg, attr, None)
            if obj:
                sz = getattr(obj, 'file_size', 0) or 0
                if sz > max_mb * 1024 * 1024:
                    return False
                break
    if max_secs > 0:
        for attr in ('video', 'audio', 'voice', 'video_note'):
            obj = getattr(msg, attr, None)
            if obj:
                dur = getattr(obj, 'duration', 0) or 0
                if dur > max_secs:
                    return False
                break
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Send helper — dual destinations + topic threads
# ══════════════════════════════════════════════════════════════════════════════

async def _fwd(client, msg, chat, thread, cap_empty: bool, forward_tag: bool, from_chat=None, block_links=False):
    """Forward one message to `chat`, optionally into a forum `thread`.
    Strategy (in order):
      1. forward_messages (when forward_tag=True)
      2. send_cached_media (fastest, avoids re-upload)
      3. copy_message (standard copy without tag)
      4. download + re-upload fallback (for restricted/private sources)
    All paths try message_thread_id for forum topics; fall back to reply_to_message_id.
    """
    from_id = from_chat or msg.chat.id
    
    modified_text = None
    is_modified = False

    if cap_empty and msg.media:
        modified_text = ""
        is_modified = True
    elif block_links and _has_links(msg):
        content = getattr(msg, 'caption' if msg.media else 'text', None)
        if content:
            raw = getattr(content, 'html', str(content))
            modified_text = _LINK_RE.sub('', raw).strip()
            is_modified = True

    if is_modified and forward_tag:
        forward_tag = False

    # Build thread kwargs — try message_thread_id first (Pyrogram >=2.0)
    def _thread_kw():
        return {"message_thread_id": thread} if thread else {}

    try:
        if forward_tag:
            try:
                try:
                    await client.forward_messages(
                        chat_id=chat, from_chat_id=from_id,
                        message_ids=msg.id, **_thread_kw())
                except TypeError:
                    await client.forward_messages(
                        chat_id=chat, from_chat_id=from_id, message_ids=msg.id)
                return True
            except FloodWait as fw:
                raise fw
            except Exception as e:
                pass # fallback to copy_message explicitly if forwarding fails (e.g., protected channel)

        # Try send_cached_media first (no re-upload, fastest)
        if msg.media:
            mo = getattr(msg, msg.media.value, None)
            if mo and hasattr(mo, "file_id"):
                ckw = _thread_kw()
                if is_modified:
                    ckw["caption"] = modified_text
                elif msg.caption:
                    ckw["caption"] = msg.caption
                try:
                    await client.send_cached_media(chat_id=chat, file_id=mo.file_id, **ckw)
                    return True
                except Exception:
                    pass  # fall through to copy_message

        if not msg.media and is_modified:
            mt_kw = _thread_kw()
            try:
                await client.send_message(chat_id=chat, text=modified_text or "", **mt_kw)
                return True
            except Exception as e:
                pass # fall through to copy_message
            return True

        # copy_message (works for public sources)
        copy_kw = _thread_kw()
        if msg.media and is_modified:
            copy_kw["caption"] = modified_text
        try:
            await client.copy_message(
                chat_id=chat, from_chat_id=from_id, message_id=msg.id, **copy_kw)
            return True
        except TypeError:
            # Pyrogram version doesn't support message_thread_id in copy_message
            # Fall back to reply_to_message_id
            alt_kw = {"reply_to_message_id": thread} if thread else {}
            if msg.media and is_modified:
                alt_kw["caption"] = modified_text
            await client.copy_message(
                chat_id=chat, from_chat_id=from_id, message_id=msg.id, **alt_kw)
            return True

    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
        return await _fwd(client, msg, chat, thread, cap_empty, forward_tag, from_chat, block_links)
    except Exception as e:
        # Download + re-upload fallback (restricted/private channels)
        try:
            import os, shutil
            # Get the Telegram display_name from the media object (this is what shows in Telegram UI)
            mo = getattr(msg, msg.media.value, None) if msg.media else None
            display_name = getattr(mo, 'file_name', None) if mo else None
            # Clean display_name for filesystem safety
            if display_name:
                import re as _re2
                display_name = _re2.sub(r'[\\/*?"<>|]', '', display_name).strip() or None
            safe_dir = f"downloads/{msg.id}"
            if msg.media:
                import main
                # Use the exact display name for downloading, else fallback to default name
                df_name = (f"{safe_dir}/{display_name}") if display_name else f"{safe_dir}/"
                
                mo = getattr(msg, msg.media.value, None)
                f_size = getattr(mo, "file_size", 0)
                main.TOTAL_DOWNLOADS += 1
                main.TOTAL_BYTES_TRANSFERRED += f_size
                
                async def progress(current, total):
                    pc = int(current * 100 / total) if total > 0 else 0
                    # Note: updating job dict directly here might not be thread-safe for notification 
                    # but since only 1 worker handles 1 job, we just update local state if needed.
                    pass

                fp = await client.download_media(msg, file_name=df_name, progress=progress)
                if not fp:
                    raise Exception("DownloadFailed")
                
                main.TOTAL_UPLOADS += 1
                cap = modified_text if is_modified else (str(msg.caption) if msg.caption else "")
                d_kw = {"chat_id": chat, **_thread_kw()}
                # display_name is passed as file_name= so Telegram shows the right name
                async def _send_with_fallback(kwargs):
                    try:
                        if msg.photo:       await client.send_photo(photo=fp, caption=cap, **kwargs)
                        elif msg.video:     await client.send_video(video=fp, caption=cap, file_name=display_name, **kwargs)
                        elif msg.document:  await client.send_document(document=fp, caption=cap, file_name=display_name, **kwargs)
                        elif msg.audio:     await client.send_audio(audio=fp, caption=cap, file_name=display_name, title=getattr(mo, 'title', None), performer=getattr(mo, 'performer', None), **kwargs)
                        elif msg.voice:     await client.send_voice(voice=fp, caption=cap, **kwargs)
                        elif msg.animation: await client.send_animation(animation=fp, caption=cap, file_name=display_name, **kwargs)
                        elif msg.sticker:   await client.send_sticker(sticker=fp, **kwargs)
                        else:               await client.send_document(document=fp, caption=cap, file_name=display_name, **kwargs)
                    except TypeError:
                        no_thread = {"chat_id": chat}
                        if msg.photo:       await client.send_photo(photo=fp, caption=cap, **no_thread)
                        elif msg.video:     await client.send_video(video=fp, caption=cap, file_name=display_name, **no_thread)
                        elif msg.document:  await client.send_document(document=fp, caption=cap, file_name=display_name, **no_thread)
                        elif msg.audio:     await client.send_audio(audio=fp, caption=cap, file_name=display_name, title=getattr(mo, 'title', None), performer=getattr(mo, 'performer', None), **no_thread)
                        elif msg.voice:     await client.send_voice(voice=fp, caption=cap, **no_thread)
                        elif msg.animation: await client.send_animation(animation=fp, caption=cap, file_name=display_name, **no_thread)
                        elif msg.sticker:   await client.send_sticker(sticker=fp, **no_thread)
                        else:               await client.send_document(document=fp, caption=cap, file_name=display_name, **no_thread)
                try:
                    await _send_with_fallback(d_kw)
                except FloodWait as fw:
                    logger.info(f"[Job fwd] Fallback FloodWait {fw.value}s for msg {msg.id}")
                    await asyncio.sleep(fw.value + 2)
                    await _send_with_fallback(d_kw)
                finally:
                    try:
                        shutil.rmtree(safe_dir, ignore_errors=True)
                    except Exception: pass
            else:
                raw_t = msg.text.html if (msg.text and hasattr(msg.text, 'html')) else (str(msg.text) if msg.text else "")
                send_kw = _thread_kw()
                try:
                    await client.send_message(chat_id=chat, text=raw_t, **send_kw)
                except TypeError:
                    await client.send_message(chat_id=chat, text=raw_t)
            return True
        except Exception as e2:
            logger.warning(f"[Job fwd] fallback failed for msg {msg.id} -> {chat}: {e2}")
            return False


# ── Parallelism & Rate Limiting ───────────────────────────────────────────────
_FWD_SEM = asyncio.Semaphore(25) # Up to 25 concurrent forwarding tasks (~2x speed)
_flood_state = {"count": 0}       # Use dict for mutability in async scope
_fwd_lock = asyncio.Lock()

async def _fwd_safe(f_func, *args, **kwargs):
    """Execution wrapper with parallelism and global rate limiting.
    Flood control: after 50 sends, sleep 30s to respect Telegram limits.
    """
    async with _FWD_SEM:
        res = await f_func(*args, **kwargs)
        async with _fwd_lock:
            _flood_state["count"] += 1
            if _flood_state["count"] >= 50:  # 50 msgs then 30s pause
                logger.info("[Flood Control] 50 msgs sent — cooling down 30s...")
                _flood_state["count"] = 0
                await asyncio.sleep(30)
        return res

async def _forward_message(client, msg, to1, th1, cap_empty, forward_tag, from_chat,
                           to2=None, th2=None, block_links=False):
    """Forward msg to primary (and optional secondary) destination.
    Uses _fwd_safe for speed + safety.
    """
    res = await _fwd_safe(_fwd, client, msg, to1, th1, cap_empty, forward_tag, from_chat, block_links)
    if to2:
        # Secondary send is usually faster (cached)
        await _fwd(client, msg, to2, th2, cap_empty, forward_tag, from_chat, block_links)
    return res


# ══════════════════════════════════════════════════════════════════════════════
# Latest-ID probe
# ══════════════════════════════════════════════════════════════════════════════

async def _get_latest_id(client, chat_id, is_bot: bool) -> int:
    """Get the latest message ID in a chat.
    Strategy: try get_chat_history first (works for all types including DMs/bots).
    Fallback to binary search via get_messages for numeric channel IDs.
    """
    # Try fastest method: get last message via history
    try:
        async for msg in client.get_chat_history(chat_id, limit=1):
            return msg.id
    except Exception:
        pass
    # Fallback: binary search (works ONLY for channels by numeric ID)
    # NEVER do this for private entities because get_messages queries the user's global inbox!
    is_ch = False
    try:
        if str(chat_id).startswith("-100"):
            is_ch = True
        else:
            try:
                from pyrogram.raw.types import InputPeerChannel
                peer = await client.resolve_peer(chat_id)
                if isinstance(peer, InputPeerChannel):
                    is_ch = True
            except Exception:
                from pyrogram.enums import ChatType
                c_obj = await client.get_chat(chat_id)
                if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                    is_ch = True
    except Exception:
        # If all checks fail and it's a bot reading a string username, assume channel
        if is_bot and isinstance(chat_id, str):
            is_ch = True
        
    if not is_ch:
        return 0

    try:
        lo, hi = 1, 9_999_999
        for _ in range(25):
            if hi - lo <= 50: break
            mid = (lo + hi) // 2
            try:
                p = await client.get_messages(chat_id, [mid])
                if not isinstance(p, list): p = [p]
                if any(m and not getattr(m, 'empty', True) for m in p):
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
# Core runner

async def resume_live_jobs(user_id: int = None):
    from database import db
    q = {"status": "running"}
    if user_id: q["user_id"] = user_id
    async for job in db.db["jobs"].find(q):
        jid, uid = job["job_id"], job["user_id"]
        if jid not in _job_tasks:
            _start_job_task(jid, uid)
# ══════════════════════════════════════════════════════════════════════════════

BATCH_CHUNK = 20

_active_clients = {}
_client_locks = {}

async def _get_shared_client(acc: dict):
    from plugins.test import start_clone_bot
    acc_id = str(acc.get("_id", acc.get("id")))
    
    if acc_id not in _client_locks:
        _client_locks[acc_id] = asyncio.Lock()
        
    async with _client_locks[acc_id]:
        if acc_id in _active_clients:
            c, refs = _active_clients[acc_id]
            # Verify client is somewhat alive (e.g., not fully disconnected)
            if not c.is_connected:
                try: await c.connect()
                except Exception: pass
            _active_clients[acc_id] = (c, refs + 1)
            return c
        c = await start_clone_bot(_CLIENT.client(acc))
        _active_clients[acc_id] = (c, 1)
        return c

async def _release_shared_client(acc: dict):
    if not acc: return
    acc_id = str(acc.get("_id", acc.get("id")))
    
    if acc_id not in _client_locks:
        return
        
    async with _client_locks[acc_id]:
        if acc_id in _active_clients:
            c, refs = _active_clients[acc_id]
            if refs <= 1:
                try: await c.stop()
                except Exception: pass
                _active_clients.pop(acc_id, None)
            else:
                _active_clients[acc_id] = (c, refs - 1)

async def _run_job(job_id: str, user_id: int, _bot=None):
    job = await _get_job(job_id)
    if not job: return

    acc = client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _update_job(job_id, status="error", error="Account not found"); return

        client        = await _get_shared_client(acc)
        is_bot        = acc.get("is_bot", True)
        fc            = job["from_chat"]

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
        to1           = job["to_chat"];     th1 = job.get("to_thread_id")
        to2           = job.get("to_chat_2"); th2 = job.get("to_thread_id_2")
        max_mb        = int(job.get("max_size_mb", 0) or 0)
        max_sec       = int(job.get("max_duration_secs", 0) or 0)
        seen          = job.get("last_seen_id", 0)
        from_topic_id = job.get("from_topic_id")
        is_private_src = not str(fc).startswith('-')

        if seen == 0:
            seen = await _get_latest_id(client, fc, is_bot)
            await _update_job(job_id, last_seen_id=seen)

        last_hb = 0  # last heartbeat timestamp
        last_notify = 0  # last status notification timestamp

        # ── Batch phase ─────────────────────────────────────────────────────
        if job.get("batch_mode") and not job.get("batch_done"):
            cur  = int(job.get("batch_cursor") or job.get("batch_start_id") or 1)
            bend = int(job.get("batch_end_id") or 0)
            if bend == 0:
                bend = seen
                await _update_job(job_id, batch_end_id=bend)

            while cur <= bend:
                fresh = await _get_job(job_id)
                if not fresh or fresh.get("status") != "running": return

                # Heartbeat every 30s
                ts = int(time.time())
                if ts - last_hb >= 30:
                    await _update_job(job_id, last_heartbeat=ts); last_hb = ts
                # Status notification every 60s
                if _bot and ts - last_notify >= 60:
                    fresh_for_notify = await _get_job(job_id)
                    if fresh_for_notify:
                        await _notify_status(_bot, fresh_for_notify, "ʙᴀᴛᴄʜ")
                    last_notify = ts

                dis         = await db.get_filters(user_id)
                flgs        = await db.get_filter_flags(user_id)
                cfg         = await db.get_configs(user_id)
                rm_cap      = flgs.get('rm_caption', False)
                block_links = flgs.get('block_links', False)
                forward_tag = cfg.get('forward_tag', False)
                slp         = max(0, int(cfg.get('duration', 0) or 0))

                chunk_end = min(cur + BATCH_CHUNK - 1, bend)
                try:
                    msgs = []
                    fetch_ok = False
                    # Primary: get_messages by ID list (ONLY works for channels/supergroups!)
                    if fc_is_channel:
                        try:
                            msgs = await client.get_messages(fc, list(range(cur, chunk_end + 1)))
                            if not isinstance(msgs, list): msgs = [msgs]
                            fetch_ok = True
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2); continue
                        except Exception as ge:
                            logger.warning(f"[Job {job_id}] Batch fetch @ {cur}: {ge}")
                    else:
                        fetch_ok = False  # DO NOT USE get_messages FOR DMs/BOTS! It fetches global inbox.
                    # Fallback: get_chat_history (works for all userbots and bot DMs)
                    if not fetch_ok:
                        try:
                            col: list = []
                            async for hmsg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_CHUNK):
                                if hmsg.id < cur: break
                                col.append(hmsg)
                            msgs = list(reversed(col))
                            fetch_ok = True
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2); continue
                        except Exception as he:
                            logger.warning(f"[Job {job_id}] history fallback also failed @ {cur}: {he}")
                        await _update_job(job_id, batch_cursor=cur)
                        continue
                except asyncio.CancelledError:
                    await _update_job(job_id, batch_cursor=cur)
                    raise
                except Exception as e:
                    logger.warning(f"[Job {job_id}] Batch fetch outer exception @ {cur}: {e}")
                    cur += BATCH_CHUNK; await _update_job(job_id, batch_cursor=cur); continue

                msgs.sort(key=lambda m: getattr(m, 'id', 0) if m else 0)
                
                # Parallel Batch Processing: launch concurrent forward tasks
                tasks = []
                for msg in msgs:
                    if not msg or getattr(msg, 'empty', False) or getattr(msg, 'service', False):
                        continue
                    if not _passes_topic(msg, from_topic_id): continue
                    if not _passes_filters(msg, dis):          continue
                    if not _passes_size(msg, max_mb, max_sec): continue
                    
                    tasks.append(_forward_message(
                        client, msg, to1, th1, rm_cap, forward_tag, fc,
                        to2, th2, block_links=block_links
                    ))
                
                if tasks:
                    try:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                    except asyncio.CancelledError:
                        await _update_job(job_id, batch_cursor=cur)
                        raise
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            logger.error(f"[Job {job_id}] Parallel error: {r}")
                            continue
                        if r is True:
                            import main
                            main.TOTAL_FILES_FWD += 1
                            fwd_n += 1
                            # granular step
                            if i < len(msgs):
                                cur = msgs[i].id
                
                # Advance seen after processing block
                if msgs: seen = max(seen, msgs[-1].id)

                cur = chunk_end + 1
                await _update_job(job_id, batch_cursor=cur)
                if fwd_n: await _inc_forwarded(job_id, fwd_n)

            await _update_job(job_id, batch_done=True, batch_cursor=bend,
                              last_seen_id=max(seen, bend))
            seen = max(seen, bend)
            logger.info(f"[Job {job_id}] Batch complete → live mode")

        # ── Live phase ───────────────────────────────────────────────────────
        while True:
            fresh = await _get_job(job_id)
            if not fresh or fresh.get("status") != "running": break

            # Heartbeat every 30s
            ts = int(time.time())
            if ts - last_hb >= 30:
                await _update_job(job_id, last_heartbeat=ts); last_hb = ts
            # Status notification every 60s
            if _bot and ts - last_notify >= 60:
                fresh_for_notify = await _get_job(job_id)
                if fresh_for_notify:
                    await _notify_status(_bot, fresh_for_notify, "ʟɪᴠᴇ")
                last_notify = ts

            dis         = await db.get_filters(user_id)
            flgs        = await db.get_filter_flags(user_id)
            cfg         = await db.get_configs(user_id)
            rm_cap      = flgs.get('rm_caption', False)
            block_links = flgs.get('block_links', False)
            forward_tag = cfg.get('forward_tag', False)
            poll_sleep  = max(3, int(cfg.get('duration', 3) or 3))
            new: list   = []

            try:
                chunk_msgs = []
                # ALWAYS use get_chat_history for Live polling so we don't query future empty IDs!
                try:
                    co = []
                    async for gmsg in client.get_chat_history(fc, limit=30):
                        if getattr(gmsg, 'id', 0) <= seen: 
                            break
                        co.append(gmsg)
                    chunk_msgs = list(reversed(co))  # Oldest to newest
                except Exception as e:
                    # If we can't use history (e.g. ChatAdminRequired), we must rely on get_messages
                    # But we only check max 10 future messages to prevent running away
                    if getattr(client, 'me', None) and client.me.is_bot and fc_is_channel:
                        try:
                            bids = list(range(seen + 1, seen + 11))
                            p = await client.get_messages(fc, bids)
                            if not isinstance(p, list): p = [p]
                            # Only include if actually existing
                            chunk_msgs = [m for m in p if m and not getattr(m, 'empty', True)]
                        except Exception:
                            pass
                
                if not chunk_msgs:
                    await asyncio.sleep(poll_sleep)
                    continue
                    
                chunk_msgs.sort(key=lambda m: getattr(m, 'id', 0) if m else 0)
                new.extend(m for m in chunk_msgs if m and getattr(m, 'id', 0) > seen)

            except FloodWait as fw: await asyncio.sleep(fw.value + 1); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f"[Job {job_id}] Live fetch: {e}")
                await asyncio.sleep(15); continue

            fwd_n = 0
            for msg in new:
                if not msg or getattr(msg, 'empty', False) or getattr(msg, 'service', False):
                    seen = max(seen, getattr(msg, 'id', 0) or seen)
                    continue
                    
                # Explicit skip → advance seen so we never reprocess
                if not _passes_topic(msg, from_topic_id) or not _passes_filters(msg, dis) or not _passes_size(msg, max_mb, max_sec):
                    seen = max(seen, msg.id)
                    continue

                # Advance seen ONLY after success — failed sends are retried next poll
                try:
                    ok = await _forward_message(
                        client, msg, to1, th1, rm_cap, forward_tag, fc,
                        to2, th2, block_links=block_links)
                    if ok:
                        import main
                        fwd_n += 1
                        main.TOTAL_FILES_FWD += 1
                        seen = max(seen, msg.id)
                        consec_fails = 0
                    else:
                        consec_fails = consec_fails + 1 if 'consec_fails' in locals() else 1
                        if consec_fails >= 3:
                            logger.warning(f"[Job {job_id}] Message {msg.id} failed 3 times, skipping.")
                            seen = max(seen, msg.id)
                            consec_fails = 0
                        else:
                            # Log failure and allow retry on next poll by BREAKING the chunk loop
                            logger.warning(f"[Job {job_id}] Message {msg.id} failed (attempt {consec_fails}/3)")
                            break
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    # If we hit floodwait, we just sleep and retry next cycle instead of marking it as done
                    break
                except asyncio.CancelledError: 
                    raise
                except Exception as e:
                    logger.debug(f"[Job {job_id}] Live fwd {msg.id}: {e}")
                    consec_fails = consec_fails + 1 if 'consec_fails' in locals() else 1
                    if consec_fails >= 3:
                        seen = max(seen, msg.id)
                        consec_fails = 0
                    else:
                        break
                        
                await asyncio.sleep(0)  # yield to event loop between messages

            if new:
                await _update_job(job_id, last_seen_id=seen)
            if fwd_n:
                await _inc_forwarded(job_id, fwd_n)
            await asyncio.sleep(poll_sleep)

    except asyncio.CancelledError:
        logger.info(f"[Job {job_id}] Cancelled")
    except Exception as e:
        logger.error(f"[Job {job_id}] Fatal: {e}")
        await _update_job(job_id, status="error", error=str(e))
    finally:
        _job_tasks.pop(job_id, None)
        if acc:
            await _release_shared_client(acc)



def _start_job_task(job_id: str, user_id: int, _bot=None) -> asyncio.Task:
    t = asyncio.create_task(_run_job(job_id, user_id, _bot=_bot))
    _job_tasks[job_id] = t
    return t


async def resume_live_jobs(user_id: int = None):
    q: dict = {"status": "running"}
    if user_id: q["user_id"] = user_id
    async for job in db.db.jobs.find(q):
        jid, uid = job["job_id"], job["user_id"]
        if jid not in _job_tasks:
            _start_job_task(jid, uid)  # no bot available during resume; user can press refresh
            logger.info(f"[Jobs] Resumed {jid} for {uid}")


# ══════════════════════════════════════════════════════════════════════════════
# UI — render list
# ══════════════════════════════════════════════════════════════════════════════

async def _render_jobs_list(bot, user_id: int, mq):
    jobs  = await _list_jobs(user_id)
    is_cb = hasattr(mq, "message")

    if not jobs:
        text = _box(
            "📋 ʟɪᴠᴇ ᴊᴏʙs",
            [
                "ɴᴏ ᴊᴏʙs ʏᴇᴛ.",
                "‣ ᴀᴜᴛᴏ-ғᴏʀᴡᴀʀᴅs ɴᴇᴡ ᴍsɢs ɪɴ ʙᴀᴄᴋɢʀᴏᴜɴᴅ",
                "‣ ʙᴀᴛᴄʜ ᴍᴏᴅᴇ: ᴄᴏᴘʏ ᴏʟᴅ ᴍsɢs ғɪʀsᴛ",
                "‣ ᴅᴜᴀʟ ᴅᴇsᴛɪɴᴀᴛɪᴏɴs sᴜᴘᴘᴏʀᴛᴇᴅ",
                "‣ ᴘᴇʀ-ᴊᴏʙ sɪᴢᴇ / ᴅᴜʀᴀᴛɪᴏɴ ʟɪᴍɪᴛ",
            ]
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ ᴄʀᴇᴀᴛᴇ ɴᴇᴡ ᴊᴏʙ", callback_data="job#new")
        ]])
    else:
        lines = ["<b>╭──────❰ 📋 ʟɪᴠᴇ ᴊᴏʙs ❱──────╮</b>\n┃"]
        for j in jobs:
            st  = _st(j.get("status", "stopped"))
            fwd = j.get("forwarded", 0)
            bp  = _batch_tag(j)
            d2  = f" ＋ {j.get('to_title_2','?')}" if j.get("to_chat_2") else ""
            err = f"\n┃  ⚠️ <code>{j.get('error','')}</code>" if j.get("status") == "error" else ""
            c_name = j.get("custom_name")
            name_disp = f" <b>{c_name}</b>" if c_name else ""
            lines.append(
                f"┣⊸ {st} <b>{j.get('from_title','?')} → {j.get('to_title','?')}{d2}</b>"
                f"  <code>[{j['job_id'][-6:]}]</code>{name_disp}"
                f"\n┃   ◈ 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐝: <code>{fwd}</code>{bp}{err}"
            )
        pass
        text = "\n".join(lines)

        rows = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            s   = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏹ Stop [{s}]",  callback_data=f"job#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ Start [{s}]", callback_data=f"job#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ [{s}]", callback_data=f"job#info#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 [{s}]",  callback_data=f"job#del#{jid}"))
            rows.append(row)
        rows.append([InlineKeyboardButton("➕ ᴄʀᴇᴀᴛᴇ ɴᴇᴡ ᴊᴏʙ", callback_data="job#new")])
        rows.append([InlineKeyboardButton("🔄 Refresh",         callback_data="job#list")])
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

@Client.on_message(filters.private & filters.command("jobs"))
async def jobs_cmd(bot, msg):
    await _render_jobs_list(bot, msg.from_user.id, msg)


@Client.on_callback_query(filters.regex(r'^job#list$'))
async def job_list_cb(bot, q):
    await q.answer()
    await _render_jobs_list(bot, q.from_user.id, q)


@Client.on_callback_query(filters.regex(r'^job#info#'))
async def job_info_cb(bot, query):
    await query.answer()
    job_id = query.data.split("#", 2)[2]
    job = await _get_job(job_id)
    if not job:
        return await query.answer("ᴊᴏʙ ɴᴏᴛ ғᴏᴜɴᴅ!", show_alert=True)

    import datetime
    created  = datetime.datetime.fromtimestamp(job.get("created", 0)).strftime("%d %b %Y · %H:%M")
    st       = _st(job.get("status", "stopped"))
    th1      = job.get("to_thread_id")
    t1_lbl   = f" [ᴛʜʀᴇᴀᴅ {th1}]" if th1 else ""
    d2_lbl   = ""
    if job.get("to_chat_2"):
        th2   = job.get("to_thread_id_2")
        d2_lbl = f"\n┣⊸ ◈ 𝐃𝐞𝐬𝐭 𝟐  : {job.get('to_title_2','?')}" + (f" [ᴛʜʀᴇᴀᴅ {th2}]" if th2 else "")

    batch_lbl = ""
    if job.get("batch_mode"):
        if job.get("batch_done"):
            batch_lbl = "\n  • <b>Batch:</b> ✅ Complete"
        else:
            cur = job.get("batch_cursor") or job.get("batch_start_id") or "?"
            end = job.get("batch_end_id") or "…"
            batch_lbl = f"\n  • <b>Batch:</b> 📦 <code>{cur}</code> / <code>{end}</code>"

    size_lbl = ""
    if job.get("max_size_mb"):
        size_lbl += f"\n  • <b>Max Size:</b> <code>{job['max_size_mb']} MB</code>"
    if job.get("max_duration_secs"):
        m, s = divmod(job['max_duration_secs'], 60)
        size_lbl += f"\n  • <b>Max Duration:</b> <code>{m}m {s}s</code>"

    err_lbl = f"\n  • ⚠️ <b>Error:</b> <code>{job['error']}</code>" if job.get("error") else ""

    c_name   = job.get("custom_name")
    name_lbl = f" <b>({c_name})</b>" if c_name else ""

    fst = job.get('from_topic_id')
    f_topic_lbl = f" [Topic {fst}]" if fst else ""

    text = (
        f"<b>📋 Live Job Information</b>\n\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_lbl}\n"
        f"  • <b>Status:</b> {st} {job.get('status','?')}\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}{f_topic_lbl}\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}{t1_lbl}{d2_lbl}{batch_lbl}{size_lbl}\n"
        f"  • <b>Forwarded:</b> <code>{job.get('forwarded', 0)}</code>\n"
        f"  • <b>Last ID:</b> <code>{job.get('last_seen_id', 0)}</code>\n"
        f"  • <b>Created:</b> {created}"
        f"{err_lbl}"
    )
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("↩ Back", callback_data="job#list")
    ]]))


@Client.on_callback_query(filters.regex(r'^job#stop#'))
async def job_stop_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    t = _job_tasks.pop(job_id, None)
    if t and not t.done(): t.cancel()
    await _update_job(job_id, status="stopped")
    await q.answer("⏹ ᴊᴏʙ sᴛᴏᴘᴘᴇᴅ.")
    await _render_jobs_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^job#start#'))
async def job_start_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    if job_id in _job_tasks and not _job_tasks[job_id].done():
        return await q.answer("ᴀʟʀᴇᴀᴅʏ ʀᴜɴɴɪɴɢ!", show_alert=True)
    await _update_job(job_id, status="running")
    _start_job_task(job_id, uid, _bot=bot)
    await q.answer("▶️ ᴊᴏʙ sᴛᴀʀᴛᴇᴅ.")
    await _render_jobs_list(bot, uid, q)


@Client.on_callback_query(filters.regex(r'^job#del#'))
async def job_del_cb(bot, q):
    job_id, uid = q.data.split("#", 2)[2], q.from_user.id
    job = await _get_job(job_id)
    if not job or job.get("user_id") != uid:
        return await q.answer("⛔ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ.", show_alert=True)
    t = _job_tasks.pop(job_id, None)
    if t and not t.done(): t.cancel()
    await _delete_job_db(job_id)
    await q.answer("🗑 ᴊᴏʙ ᴅᴇʟᴇᴛᴇᴅ.")
    await _render_jobs_list(bot, uid, q)


# ══════════════════════════════════════════════════════════════════════════════
# Create-job flow
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r'^job#new$'))
async def job_new_cb(bot, q):
    await q.answer()
    await q.message.delete()
    await _create_job_flow(bot, q.from_user.id)


@Client.on_message(filters.private & filters.command("newjob"))
async def newjob_cmd(bot, msg):
    await _create_job_flow(bot, msg.from_user.id)


async def _pick_channel(bot, uid: int, channels: list, prompt: str, optional=False):
    """Ask user to pick a target channel. Returns (chat_id, title, cancelled)."""
    btns = [[KeyboardButton(ch['title'])] for ch in channels]
    if optional:
        btns.append([KeyboardButton("⏭ sᴋɪᴘ (ɴᴏ sᴇᴄᴏɴᴅ ᴅᴇsᴛ)")])
    btns.append([KeyboardButton("/cancel")])
    r = await bot.ask(uid, prompt, reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True, one_time_keyboard=True))
    txt = r.text.strip()
    if "/cancel" in txt:
        await r.reply("<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
                      reply_markup=ReplyKeyboardRemove())
        return None, None, True
    if optional and "skip" in txt.lower():
        return None, None, False
    for ch in channels:
        if ch['title'] == txt:
            return ch['chat_id'], ch['title'], False
    return None, None, False


async def _pick_topic(bot, uid: int, label: str):
    """Ask for an optional topic thread ID."""
    r = await bot.ask(uid,
        f"<b>╭──────❰ 💬 ᴛᴏᴘɪᴄ ᴛʜʀᴇᴀᴅ — {label} ❱──────╮\n"
        f"┃\n"
        f"┣⊸ sᴇɴᴅ ᴛʜʀᴇᴀᴅ ɪᴅ ᴛᴏ ᴘᴏsᴛ ɪɴᴛᴏ ᴀ ᴛᴏᴘɪᴄ\n"
        f"┣⊸ sᴇɴᴅ 0 ᴛᴏ ᴘᴏsᴛ ɪɴ ᴍᴀɪɴ ᴄʜᴀᴛ\n"
        f"┃\n"
        f"╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("0 (ɴᴏ ᴛᴏᴘɪᴄ)")], [KeyboardButton("/cancel")]],
            resize_keyboard=True, one_time_keyboard=True
        ))
    t = r.text.strip()
    if "/cancel" in t: return None
    return int(t) if t.isdigit() and int(t) > 0 else None


async def _create_job_flow(bot, uid: int):
    # Clear any stale pyrofork listener from a previous unfinished flow.
    try:
        bot.stop_listening(uid)
    except Exception:
        pass

    # Step 1 — Account
    accounts = await db.get_bots(uid)
    if not accounts:
        return await bot.send_message(uid,
            "<b>╭──────❰ ❌ ɴᴏ ᴀᴄᴄᴏᴜɴᴛs ❱──────╮\n"
            "┃\n┣⊸ ᴀᴅᴅ ᴏɴᴇ ɪɴ /settings → ⚙️ Accounts\n"
            "┃\n╰────────────────────────────────╯</b>")

    acc_btns = [[KeyboardButton(
        f"{'🤖 ʙᴏᴛ' if a.get('is_bot', True) else '👤 ᴜsᴇʀʙᴏᴛ'}: "
        f"{a.get('username') or a.get('name', 'Unknown')} [{a['id']}]"
    )] for a in accounts]
    acc_btns.append([KeyboardButton("/cancel")])

    acc_r = await bot.ask(uid,
        "<b>╭──────❰ 📋 ᴄʀᴇᴀᴛᴇ ʟɪᴠᴇ ᴊᴏʙ — sᴛᴇᴘ 1/6 ❱──────╮\n"
        "┃\n┣⊸ ᴄʜᴏᴏsᴇ ᴡʜɪᴄʜ ᴀᴄᴄᴏᴜɴᴛ ᴛᴏ ᴜsᴇ\n"
        "┣⊸ ᴜsᴇʀʙᴏᴛ ʀᴇqᴜɪʀᴇᴅ ғᴏʀ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛs\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup(acc_btns, resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in acc_r.text:
        return await acc_r.reply(
            "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())

    acc_id = None
    if "[" in acc_r.text and "]" in acc_r.text:
        try: acc_id = int(acc_r.text.split('[')[-1].split(']')[0])
        except Exception: pass
    sel  = (await db.get_bot(uid, acc_id)) if acc_id else accounts[0]
    ibot = sel.get("is_bot", True)

    # Step 2 — Source
    src_r = await bot.ask(uid,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 2/6 — sᴏᴜʀᴄᴇ ᴄʜᴀᴛ ❱──────╮\n"
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
        return await src_r.reply(
            "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>")

    raw = src_r.text.strip()
    if raw.lower() in ("me", "saved"):
        if ibot:
            return await src_r.reply(
                "<b>❌ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs ʀᴇqᴜɪʀᴇs ᴀ ᴜsᴇʀʙᴏᴛ ᴀᴄᴄᴏᴜɴᴛ.</b>")
        fc, ftitle = "me", "sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs"
    else:
        fc = int(raw) if raw.lstrip('-').isdigit() else raw
        try:
            co = await bot.get_chat(fc)
            ftitle = getattr(co, "title", None) or getattr(co, "first_name", str(fc))
        except Exception:
            co = None
            ftitle = str(fc)

        if await db.is_protected(raw, co):
            return await bot.send_message(uid,
                "<b>╭──────❰ ⚠️ Pʀᴏᴛᴇᴄᴛɪᴏɴ Eʀʀᴏʀ ❱──────╮\n"
                "┃\n┣⊸ Ohh no! ERROR — This source is protected by the owner.\n"
                "┣⊸ Please try another source.\n"
                "┃\n╰────────────────────────────────╯</b>",
                reply_markup=ReplyKeyboardRemove())

    # Step 2b — Source Topic (optional, only for forum groups)
    from_topic_id = None
    if str(fc) != "me":
        src_topic_r = await bot.ask(uid,
            "<b>╭──────❰ 📋 sᴛᴇᴘ 2b — sᴏᴜʀᴄᴇ ᴛᴏᴘɪᴄ ❱──────╮\n"
            "┃\n"
            "┣⊸ ɪғ sᴏᴜʀᴄᴇ ɪs ᴀ ɢʀᴏᴜᴘ ᴡɪᴛʜ ᴛᴏᴘɪᴄs, ᴇɴᴛᴇʀ ᴛʜᴇ ᴛᴏᴘɪᴄ ɪᴅ\n"
            "┣⊸ sᴇɴᴅ 0 ᴛᴏ ғᴏʀᴡᴀʀᴅ ᴀʟʟ ᴍᴇssᴀɢᴇs (ɴᴏ ᴛᴏᴘɪᴄ ғɪʟᴛᴇʀ)\n"
            "┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("0 (ɴᴏ ᴛᴏᴘɪᴄ ғɪʟᴛᴇʀ)")], [KeyboardButton("/cancel")]],
                resize_keyboard=True, one_time_keyboard=True))
        if "/cancel" in src_topic_r.text:
            return await src_topic_r.reply(
                "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
                reply_markup=ReplyKeyboardRemove())
        _st_raw = src_topic_r.text.strip()
        from_topic_id = int(_st_raw) if _st_raw.isdigit() and int(_st_raw) > 0 else None

    # Step 3 — Dest 1
    channels = await db.get_user_channels(uid)
    if not channels:
        return await bot.send_message(uid,
            "<b>❌ ɴᴏ ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟs. ᴀᴅᴅ ᴏɴᴇ ᴠɪᴀ /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove())

    to1, ttl1, cancelled = await _pick_channel(bot, uid, channels,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 3/6 — ᴘʀɪᴍᴀʀʏ ᴅᴇsᴛɪɴᴀᴛɪᴏɴ ❱──────╮\n"
        "┃\n┣⊸ ᴡʜᴇʀᴇ sʜᴏᴜʟᴅ ɴᴇᴡ ᴍᴇssᴀɢᴇs ʙᴇ sᴇɴᴛ?\n"
        "┃\n╰────────────────────────────────╯</b>")
    if cancelled or not to1: return

    th1 = None
    if to1:
        th1 = await _pick_topic(bot, uid, "ᴅᴇsᴛ 1")

    # Step 4 — Dest 2
    to2, ttl2, cancelled2 = await _pick_channel(bot, uid, channels,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 4/6 — sᴇᴄᴏɴᴅ ᴅᴇsᴛɪɴᴀᴛɪᴏɴ (ᴏᴘᴛɪᴏɴᴀʟ) ❱──────╮\n"
        "┃\n┣⊸ ᴍᴇssᴀɢᴇs ᴡɪʟʟ ʙᴇ sᴇɴᴛ ᴛᴏ ʙᴏᴛʜ ᴅᴇsᴛɪɴᴀᴛɪᴏɴs\n"
        "┣⊸ ᴘʀᴇss sᴋɪᴘ ɪғ ᴏɴᴇ ᴅᴇsᴛɪɴᴀᴛɪᴏɴ ɪs ᴇɴᴏᴜɢʜ\n"
        "┃\n╰────────────────────────────────╯</b>",
        optional=True)
    if cancelled2: return

    th2 = None
    if to2:
        th2 = await _pick_topic(bot, uid, "ᴅᴇsᴛ 2")

    # Step 5 — Batch mode
    batch_r = await bot.ask(uid,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 5/6 — ʙᴀᴛᴄʜ ᴍᴏᴅᴇ ❱──────╮\n"
        "┃\n┣⊸ ✅ ᴏɴ  — ᴄᴏᴘʏ ᴏʟᴅ ᴍsɢs ғɪʀsᴛ, ᴛʜᴇɴ ɢᴏ ʟɪᴠᴇ\n"
        "┣⊸ ❌ ᴏFF — ᴏɴʟʏ ᴡᴀᴛᴄʜ ғᴏʀ ɴᴇᴡ ᴍsɢs (ᴅᴇғᴀᴜʟᴛ)\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("✅ ᴏɴ (ᴄᴏᴘʏ ᴏʟᴅ ᴍsɢs ғɪʀsᴛ)")],
             [KeyboardButton("❌ ᴏFF (ʟɪᴠᴇ ᴏɴʟʏ)")],
             [KeyboardButton("/cancel")]],
            resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in batch_r.text:
        return await batch_r.reply(
            "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())

    batch_mode  = "ᴏɴ" in batch_r.text.lower() or "on" in batch_r.text.lower()
    bstart, bend = 1, 0

    if batch_mode:
        rng_r = await bot.ask(uid,
            "<b>╭──────❰ 📋 ʙᴀᴛᴄʜ ʀᴀɴɢᴇ ❱──────╮\n"
            "┃\n┣⊸ ALL   — ᴀʟʟ ᴍsɢs ғʀᴏᴍ ᴛʜᴇ ʙᴇɢɪɴɴɪɴɢ\n"
            "┣⊸ 500   — sᴛᴀʀᴛ ғʀᴏᴍ ɪᴅ 500 ᴛᴏ ʟᴀᴛᴇsᴛ\n"
            "┣⊸ 500:2000 — ᴏɴʟʏ ɪᴅs 500 ᴛʜʀᴏᴜɢʜ 2000\n"
            "┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())
        if "/cancel" in rng_r.text:
            return await rng_r.reply(
                "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>")
        rt = rng_r.text.strip().lower()
        if rt != "all":
            if ":" in rt:
                p = rt.split(":", 1)
                try: bstart = int(p[0])
                except Exception: pass
                try: bend   = int(p[1])
                except Exception: pass
            else:
                try: bstart = int(rt)
                except Exception: pass

    # Step 6 — Size limit
    lim_r = await bot.ask(uid,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 6/7 — sɪᴢᴇ ʟɪᴍɪᴛ ❱──────╮\n"
        "┃\n┣⊸ 0         — ɴᴏ ʟɪᴍɪᴛ\n"
        "┣⊸ 50        — sᴋɪᴘ ғɪʟᴇs > 50 ᴍʙ\n"
        "┣⊸ 50:10     — sᴋɪᴘ > 50ᴍʙ ᴏʀ > 10 ᴍɪɴᴜᴛᴇs\n"
        "┣⊸ 0:5       — ɴᴏ sɪᴢᴇ ʟɪᴍɪᴛ, sᴋɪᴘ > 5 ᴍɪɴᴜᴛᴇs\n"
        "┃  ғᴏʀᴍᴀᴛ: max_mb:max_minutes\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("0 (ɴᴏ ʟɪᴍɪᴛ)")], [KeyboardButton("/cancel")]],
            resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in lim_r.text:
        return await lim_r.reply(
            "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())

    max_mb, max_sec = 0, 0
    lt = lim_r.text.strip()
    if lt != "0" and lt.lower() != "0 (ɴᴏ ʟɪᴍɪᴛ)":
        if ":" in lt:
            p = lt.split(":", 1)
            try: max_mb  = int(p[0].strip())
            except Exception: pass
            try: max_sec = int(p[1].strip()) * 60
            except Exception: pass
        else:
            try: max_mb = int(lt)
            except Exception: pass

    # Step 7 — Custom Name
    name_r = await bot.ask(uid,
        "<b>╭──────❰ 📋 sᴛᴇᴘ 7/7 — ᴊᴏʙ ɴᴀᴍᴇ (ᴏᴘᴛɪᴏɴᴀʟ) ❱──────╮\n"
        "┃\n┣⊸ sᴇɴᴅ ᴀ sʜᴏʀᴛ ɴᴀᴍᴇ ғᴏʀ ᴛʜɪs ᴊᴏʙ ᴛᴏ ɪᴅᴇɴᴛɪғʏ ɪᴛ ᴇᴀsɪʟʏ.\n"
        "┣⊸ ᴏʀ ᴄʟɪᴄᴋ sᴋɪᴘ ᴛᴏ ᴜsᴇ ᴅᴇғᴀᴜʟᴛ.\n"
        "┃\n╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardMarkup([
            [KeyboardButton("sᴋɪᴘ (ᴜsᴇ ᴅᴇғᴀᴜʟᴛ)")], [KeyboardButton("/cancel")]
        ], resize_keyboard=True, one_time_keyboard=True))

    if "/cancel" in name_r.text:
        return await name_r.reply(
            "<b>╭──────❰ ❌ Cancelʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())

    cname = None
    if "sᴋɪᴘ" not in name_r.text.lower() and "skip" not in name_r.text.lower():
        cname = name_r.text.strip()[:30]

    # Save & Start
    job_id = f"{uid}-{int(time.time())}"
    job = {
        "job_id": job_id, "user_id": uid, "account_id": sel["id"],
        "from_chat": fc, "from_title": ftitle, "from_topic_id": from_topic_id,
        "to_chat": to1, "to_title": ttl1, "to_thread_id": th1,
        "to_chat_2": to2, "to_title_2": ttl2, "to_thread_id_2": th2,
        "batch_mode": batch_mode, "batch_start_id": bstart, "batch_end_id": bend,
        "batch_cursor": bstart, "batch_done": False,
        "max_size_mb": max_mb, "max_duration_secs": max_sec,
        "status": "running", "created": int(time.time()), "forwarded": 0, "last_seen_id": 0,
        "custom_name": cname,
    }
    await _save_job(job)
    _start_job_task(job_id, uid, _bot=bot)

    # Safe escape for HTML parse mode
    def _esc(t): return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    th1_lbl  = f" [ᴛʜʀᴇᴀᴅ {th1}]" if th1 else ""
    d2_lbl   = f"\n┣⊸ ◈ 𝐃𝐞𝐬𝐭 𝟐  : {_esc(ttl2)}" + (f" [ᴛʜʀᴇᴀᴅ {th2}]" if th2 else "") if to2 else ""
    bt_lbl   = (f"\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ✅ ᴏɴ — ɪᴅ {bstart}" +
                (f" → {bend}" if bend else " → ʟᴀᴛᴇsᴛ")) if batch_mode else "\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ❌ ᴏFF"
    sz_lbl   = (f"\n┣⊸ ◈ 𝐌𝐚𝐱 𝐒𝐳   : {max_mb} ᴍʙ") if max_mb else ""
    dur_lbl  = (f"\n┣⊸ ◈ 𝐌𝐚𝐱 𝐃𝐮𝐫  : {max_sec // 60} ᴍɪɴ") if max_sec else ""

    await bot.send_message(uid,
        f"<b>╭──────❰ ✅ ʟɪᴠᴇ ᴊᴏʙ ᴄʀᴇᴀᴛᴇᴅ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {_esc(ftitle)}\n"
        f"┣⊸ ◈ 𝐃𝐞𝐬𝐭 𝟏  : {_esc(ttl1)}{th1_lbl}"
        f"{d2_lbl}{bt_lbl}{sz_lbl}{dur_lbl}\n"
        f"┣⊸ ◈ 𝐀𝐜𝐜𝐨𝐮𝐧𝐭 : {'🤖 ʙᴏᴛ' if ibot else '👤 ᴜsᴇʀʙᴏᴛ'} {_esc(sel.get('name','?'))}\n"
        f"┣⊸ ◈ 𝐉𝐨𝐛 𝐈𝐃  : <code>{job_id[-6:]}</code>" + (f" (<b>{_esc(cname)}</b>)\n" if cname else "\n") +
        f"┃\n"
        f"╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardRemove())
