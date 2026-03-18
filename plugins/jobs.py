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
    """Build a bordered box identical to the bot's existing style."""
    body = "\n".join(f"┣⊸ {l}" for l in lines)
    return (
        f"<b>╭──────❰ {title} ❱──────╮\n"
        f"┃\n"
        f"{body}\n"
        f"┃\n"
        f"╰────────────────────────────────╯</b>"
    )

def _st(status: str) -> str:
    """Status emoji."""
    return {"running": "🟢", "stopped": "🔴", "error": "⚠️", "done": "✅"}.get(status, "❓")

def _batch_tag(job: dict) -> str:
    if not job.get("batch_mode"):
        return ""
    if job.get("batch_done"):
        return "  📦✅"
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


# ══════════════════════════════════════════════════════════════════════════════
# Filter helpers
# ══════════════════════════════════════════════════════════════════════════════

def _passes_filters(msg, disabled: list) -> bool:
    if msg.empty or msg.service:
        return False
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

async def _fwd(client, msg, chat, thread, cap_empty: bool):
    kw = {"message_thread_id": thread} if thread else {}
    try:
        if cap_empty and msg.media:
            await client.copy_message(chat_id=chat, from_chat_id=msg.chat.id,
                                      message_id=msg.id, caption="", **kw)
        else:
            await client.copy_message(chat_id=chat, from_chat_id=msg.chat.id,
                                      message_id=msg.id, **kw)
    except Exception:
        try:
            await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id,
                                          message_ids=msg.id, **kw)
        except Exception as e:
            logger.debug(f"[Job fwd] {chat}: {e}")


async def _forward_message(client, msg, to1, th1, cap_empty, to2=None, th2=None):
    await _fwd(client, msg, to1, th1, cap_empty)
    if to2:
        await _fwd(client, msg, to2, th2, cap_empty)


# ══════════════════════════════════════════════════════════════════════════════
# Latest-ID probe
# ══════════════════════════════════════════════════════════════════════════════

async def _get_latest_id(client, chat_id, is_bot: bool) -> int:
    """Get the latest message ID in a chat.
    - For private chats (user DMs, saved messages): use get_chat_history (works for all account types via MTProto).
    - For channels/groups with a bot account: binary-search by get_messages.
    """
    is_private = (chat_id == "me") or (isinstance(chat_id, int) and chat_id > 0)
    try:
        if not is_bot or is_private:
            # get_chat_history works for userbots always, and for bots in private chats
            async for msg in client.get_chat_history(chat_id, limit=1):
                return msg.id
        else:
            # Binary search via get_messages — efficient for channels
            lo, hi = 1, 9_999_999
            for _ in range(25):
                if hi - lo <= 50: break
                mid = (lo + hi) // 2
                try:
                    p = await client.get_messages(chat_id, [mid])
                    if not isinstance(p, list): p = [p]
                    if any(m and not m.empty for m in p):
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
# ══════════════════════════════════════════════════════════════════════════════

BATCH_CHUNK = 200

async def _run_job(job_id: str, user_id: int):
    job = await _get_job(job_id)
    if not job: return

    acc = client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _update_job(job_id, status="error", error="Account not found"); return

        client  = await start_clone_bot(_CLIENT.client(acc))
        is_bot  = acc.get("is_bot", True)
        fc      = job["from_chat"]
        to1     = job["to_chat"];   th1 = job.get("to_thread_id")
        to2     = job.get("to_chat_2"); th2 = job.get("to_thread_id_2")
        max_mb  = int(job.get("max_size_mb", 0) or 0)
        max_sec = int(job.get("max_duration_secs", 0) or 0)
        seen    = job.get("last_seen_id", 0)

        # Private chat = user DM (positive int ID) or Saved Messages ("me")
        # Channels/supergroups have negative IDs (-100xxxxxxxxxx)
        # Private chats use get_chat_history (works for both bots & userbots via MTProto)
        # Channels use get_messages by ID probing (more reliable, no history limit)
        is_private_src = (fc == "me") or (isinstance(fc, int) and fc > 0)

        if seen == 0:
            seen = await _get_latest_id(client, fc, is_bot)
            await _update_job(job_id, last_seen_id=seen)

        # ── Batch phase ────────────────────────────────────────────────────
        if job.get("batch_mode") and not job.get("batch_done"):
            cur     = int(job.get("batch_cursor") or job.get("batch_start_id") or 1)
            bend    = int(job.get("batch_end_id") or 0)
            if bend == 0:
                bend = seen
                await _update_job(job_id, batch_end_id=bend)

            while cur <= bend:
                fresh = await _get_job(job_id)
                if not fresh or fresh.get("status") != "running": return

                dis    = await db.get_filters(user_id)
                cfg    = await db.get_configs(user_id)
                rm_cap = 'rm_caption' in dis
                slp    = max(1, int(cfg.get('duration', 1) or 1))

                chunk_end = min(cur + BATCH_CHUNK - 1, bend)
                # Use get_chat_history for private sources because get_messages by ID fails there
                try:
                    if not is_bot or is_private_src:
                        col = []
                        async for msg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_CHUNK):
                            if msg.id < cur: break
                            col.append(msg)
                        msgs = list(reversed(col))
                    else:
                        msgs = await client.get_messages(fc, list(range(cur, chunk_end + 1)))
                        if not isinstance(msgs, list): msgs = [msgs]
                except FloodWait as fw: await asyncio.sleep(fw.value + 2); continue
                except asyncio.CancelledError: raise
                except Exception as e:
                    logger.warning(f"[Job {job_id}] Batch fetch: {e}")
                    cur += BATCH_CHUNK; await _update_job(job_id, batch_cursor=cur); continue

                valid = sorted([m for m in msgs if m and not m.empty and not m.service], key=lambda m: m.id)
                fwd_n = 0
                for msg in valid:
                    f2 = await _get_job(job_id)
                    if not f2 or f2.get("status") != "running": return
                    if not _passes_filters(msg, dis): continue
                    if not _passes_size(msg, max_mb, max_sec): continue
                    try:
                        await _forward_message(client, msg, to1, th1, rm_cap, to2, th2)
                        fwd_n += 1
                    except FloodWait as fw: await asyncio.sleep(fw.value + 1)
                    except asyncio.CancelledError: raise
                    except Exception as e: logger.debug(f"[Job {job_id}] Batch fwd {msg.id}: {e}")
                    await asyncio.sleep(slp)

                cur = chunk_end + 1
                await _update_job(job_id, batch_cursor=cur)
                if fwd_n: await _inc_forwarded(job_id, fwd_n)

            await _update_job(job_id, batch_done=True, batch_cursor=bend,
                              last_seen_id=max(seen, bend))
            seen = max(seen, bend)
            logger.info(f"[Job {job_id}] Batch complete → live mode")

        # ── Live phase ─────────────────────────────────────────────────────
        while True:
            fresh = await _get_job(job_id)
            if not fresh or fresh.get("status") != "running": break

            dis    = await db.get_filters(user_id)
            cfg    = await db.get_configs(user_id)
            rm_cap = 'rm_caption' in dis
            new: list = []

            try:
                if not is_bot or is_private_src:
                    # ── get_chat_history path ──────────────────────────────
                    # Used for: all userbots, AND bots with private chats.
                    # Pyrogram exposes this via MTProto even for bot accounts.
                    col = []
                    async for msg in client.get_chat_history(fc, limit=50):
                        if msg.id <= seen: break
                        col.append(msg)
                    new = list(reversed(col))
                else:
                    # ── ID-probing path (bots + channels only) ─────────────
                    # Works because channel message IDs are globally sequential.
                    # Does NOT work for private user chats.
                    probe = seen + 1
                    while True:
                        bids = list(range(probe, probe + 50))
                        try: msgs = await client.get_messages(fc, bids)
                        except FloodWait as fw: await asyncio.sleep(fw.value + 1); continue
                        except Exception: break
                        if not isinstance(msgs, list): msgs = [msgs]
                        v = [m for m in msgs if m and not m.empty and not m.service]
                        if not v: break
                        v.sort(key=lambda m: m.id)
                        new.extend(v)
                        probe = v[-1].id + 1
                        if len(v) < 49: break
            except FloodWait as fw: await asyncio.sleep(fw.value + 1); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f"[Job {job_id}] Fetch: {e}"); await asyncio.sleep(15); continue

            fwd_n = 0
            for msg in new:
                if not _passes_filters(msg, dis):
                    seen = max(seen, msg.id); continue
                if not _passes_size(msg, max_mb, max_sec):
                    seen = max(seen, msg.id); continue
                try:
                    await _forward_message(client, msg, to1, th1, rm_cap, to2, th2)
                    fwd_n += 1
                except FloodWait as fw: await asyncio.sleep(fw.value + 1)
                except asyncio.CancelledError: raise
                except Exception as e: logger.debug(f"[Job {job_id}] Live fwd: {e}")
                seen = max(seen, msg.id)
                await asyncio.sleep(1)

            if new: await _update_job(job_id, last_seen_id=seen)
            if fwd_n: await _inc_forwarded(job_id, fwd_n)
            await asyncio.sleep(max(5, cfg.get("duration", 5) or 5))

    except asyncio.CancelledError: logger.info(f"[Job {job_id}] Cancelled")
    except Exception as e:
        logger.error(f"[Job {job_id}] Fatal: {e}")
        await _update_job(job_id, status="error", error=str(e))
    finally:
        _job_tasks.pop(job_id, None)
        if client:
            try: await client.stop()
            except Exception: pass


def _start_job_task(job_id: str, user_id: int) -> asyncio.Task:
    t = asyncio.create_task(_run_job(job_id, user_id))
    _job_tasks[job_id] = t
    return t


async def resume_live_jobs(user_id: int = None):
    q: dict = {"status": "running"}
    if user_id: q["user_id"] = user_id
    async for job in db.db.jobs.find(q):
        jid, uid = job["job_id"], job["user_id"]
        if jid not in _job_tasks:
            _start_job_task(jid, uid)
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
        lines.append("┃\n<b>╰────────────────────────────────╯</b>")
        text = "\n".join(lines)

        rows = []
        for j in jobs:
            st  = j.get("status", "stopped")
            jid = j["job_id"]
            s   = jid[-6:]
            row = []
            if st == "running":
                row.append(InlineKeyboardButton(f"⏹ sᴛᴏᴘ [{s}]",  callback_data=f"job#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ sᴛᴀʀᴛ [{s}]", callback_data=f"job#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ [{s}]", callback_data=f"job#info#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 [{s}]",  callback_data=f"job#del#{jid}"))
            rows.append(row)
        rows.append([InlineKeyboardButton("➕ ᴄʀᴇᴀᴛᴇ ɴᴇᴡ ᴊᴏʙ", callback_data="job#new")])
        rows.append([InlineKeyboardButton("🔄 ʀᴇғʀᴇsʜ",         callback_data="job#list")])
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
    await _render_jobs_list(bot, q.from_user.id, q)


@Client.on_callback_query(filters.regex(r'^job#info#'))
async def job_info_cb(bot, query):
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
            batch_lbl = "\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ✅ ᴄᴏᴍᴘʟᴇᴛᴇ"
        else:
            cur = job.get("batch_cursor") or job.get("batch_start_id") or "?"
            end = job.get("batch_end_id") or "…"
            batch_lbl = f"\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : 📦 <code>{cur}</code> / <code>{end}</code>"

    size_lbl = ""
    if job.get("max_size_mb"):
        size_lbl += f"\n┣⊸ ◈ 𝐌𝐚𝐱 𝐒𝐳   : <code>{job['max_size_mb']} ᴍʙ</code>"
    if job.get("max_duration_secs"):
        m, s = divmod(job['max_duration_secs'], 60)
        size_lbl += f"\n┣⊸ ◈ 𝐌𝐚𝐱 𝐃𝐮𝐫  : <code>{m}ᴍ {s}s</code>"

    err_lbl = f"\n┣⊸ ⚠️ ᴇʀʀᴏʀ: <code>{job['error']}</code>" if job.get("error") else ""

    c_name   = job.get("custom_name")
    name_lbl = f"\n┣⊸ ◈ 𝐍𝐚𝐦𝐞    : <b>{c_name}</b>" if c_name else ""

    text = (
        f"<b>╭──────❰ 📋 ʟɪᴠᴇ ᴊᴏʙ ɪɴғᴏ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐈𝐃      : <code>{job_id[-6:]}</code>{name_lbl}\n"
        f"┣⊸ ◈ 𝐒𝐭𝐚𝐭𝐮𝐬  : {st} {job.get('status','?')}\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {job.get('from_title','?')}\n"
        f"┣⊸ ◈ 𝐃𝐞𝐬𝐭 𝟏  : {job.get('to_title','?')}{t1_lbl}"
        f"{d2_lbl}{batch_lbl}{size_lbl}\n"
        f"┣⊸ ◈ 𝐅𝐰𝐝     : <code>{job.get('forwarded', 0)}</code>\n"
        f"┣⊸ ◈ 𝐋𝐚𝐬𝐭 𝐈𝐃 : <code>{job.get('last_seen_id', 0)}</code>\n"
        f"┣⊸ ◈ 𝐂𝐫𝐞𝐚𝐭𝐞𝐝 : {created}"
        f"{err_lbl}\n"
        f"┃\n"
        f"╰────────────────────────────────╯</b>"
    )
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("↩ ʙᴀᴄᴋ", callback_data="job#list")
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
    _start_job_task(job_id, uid)
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
        await r.reply("<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
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
            "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
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
            "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>")

    raw = src_r.text.strip()
    if raw.lower() in ("me", "saved"):
        if ibot:
            return await src_r.reply(
                "<b>❌ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs ʀᴇqᴜɪʀᴇs ᴀ ᴜsᴇʀʙᴏᴛ ᴀᴄᴄᴏᴜɴᴛ.</b>")
        fc, ftitle = "me", "sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs"
    else:
        fc = int(raw) if raw.lstrip('-').isdigit() else raw
        try:
            co     = await bot.get_chat(fc)
            ftitle = getattr(co, "title", None) or getattr(co, "first_name", str(fc))
        except Exception:
            ftitle = str(fc)

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
            "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
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
                "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>")
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
            "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
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
            "<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>",
            reply_markup=ReplyKeyboardRemove())

    cname = None
    if "sᴋɪᴘ" not in name_r.text.lower() and "skip" not in name_r.text.lower():
        cname = name_r.text.strip()[:30]

    # Save & Start
    job_id = f"{uid}-{int(time.time())}"
    job = {
        "job_id": job_id, "user_id": uid, "account_id": sel["id"],
        "from_chat": fc, "from_title": ftitle,
        "to_chat": to1, "to_title": ttl1, "to_thread_id": th1,
        "to_chat_2": to2, "to_title_2": ttl2, "to_thread_id_2": th2,
        "batch_mode": batch_mode, "batch_start_id": bstart, "batch_end_id": bend,
        "batch_cursor": bstart, "batch_done": False,
        "max_size_mb": max_mb, "max_duration_secs": max_sec,
        "status": "running", "created": int(time.time()), "forwarded": 0, "last_seen_id": 0,
        "custom_name": cname,
    }
    await _save_job(job)
    _start_job_task(job_id, uid)

    th1_lbl  = f" [ᴛʜʀᴇᴀᴅ {th1}]" if th1 else ""
    d2_lbl   = f"\n┣⊸ ◈ 𝐃𝐞𝐬𝐭 𝟐  : {ttl2}" + (f" [ᴛʜʀᴇᴀᴅ {th2}]" if th2 else "") if to2 else ""
    bt_lbl   = (f"\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ✅ ᴏɴ — ɪᴅ {bstart}" +
                (f" → {bend}" if bend else " → ʟᴀᴛᴇsᴛ")) if batch_mode else "\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ❌ ᴏFF"
    sz_lbl   = (f"\n┣⊸ ◈ 𝐌𝐚𝐱 𝐒𝐳   : {max_mb} ᴍʙ") if max_mb else ""
    dur_lbl  = (f"\n┣⊸ ◈ 𝐌𝐚𝐱 𝐃𝐮𝐫  : {max_sec // 60} ᴍɪɴ") if max_sec else ""

    await bot.send_message(uid,
        f"<b>╭──────❰ ✅ ʟɪᴠᴇ ᴊᴏʙ ᴄʀᴇᴀᴛᴇᴅ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {ftitle}\n"
        f"┣⊸ ◈ 𝐃𝐞𝐬𝐭 𝟏  : {ttl1}{th1_lbl}"
        f"{d2_lbl}{bt_lbl}{sz_lbl}{dur_lbl}\n"
        f"┣⊸ ◈ 𝐀𝐜𝐜𝐨𝐮𝐧𝐭 : {'🤖 ʙᴏᴛ' if ibot else '👤 ᴜsᴇʀʙᴏᴛ'} {sel.get('name','?')}\n"
        f"┣⊸ ◈ 𝐉𝐨𝐛 𝐈𝐃  : <code>{job_id[-6:]}</code>" + (f" (<b>{cname}</b>)\n" if cname else "\n") +
        f"┃\n"
        f"╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardRemove())
