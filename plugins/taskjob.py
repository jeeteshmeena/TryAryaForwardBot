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

async def _send_one(client, msg, to_chat: int, remove_caption: bool, caption_tpl, forward_tag=False, from_chat=None):
    caption = None
    if caption_tpl and msg.media: caption = caption_tpl
    elif remove_caption and msg.media: caption = ""
    
    from_id = from_chat or msg.chat.id
    
    try:
        if forward_tag:
            await client.forward_messages(chat_id=to_chat, from_chat_id=from_id, message_ids=msg.id)
            return True

        try:
            if msg.media:
                mo = getattr(msg, msg.media.value, None)
                if mo and hasattr(mo, "file_id"):
                    kw = {}
                    if caption is not None: kw["caption"] = caption
                    elif msg.caption: kw["caption"] = msg.caption
                    await client.send_cached_media(chat_id=to_chat, file_id=mo.file_id, **kw)
                    return True
        except Exception:
            pass

        try:
            if msg.media:
                mo = getattr(msg, msg.media.value, None)
                if mo and hasattr(mo, "file_id"):
                    kw = {}
                    if caption is not None: kw["caption"] = caption
                    elif msg.caption: kw["caption"] = msg.caption
                    await client.send_cached_media(chat_id=to_chat, file_id=mo.file_id, **kw)
                    return True
        except Exception:
            pass

        if caption is not None:
            await client.copy_message(chat_id=to_chat, from_chat_id=from_id,
                                      message_id=msg.id, caption=caption)
        else:
            await client.copy_message(chat_id=to_chat, from_chat_id=from_id, message_id=msg.id)
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
        return await _send_one(client, msg, to_chat, remove_caption, caption_tpl, forward_tag, from_chat)
    except Exception as e:
        if forward_tag:
            return False

        # Download fallback
        try:
            if msg.media:
                mo = getattr(msg, msg.media.value, None)
                orig = getattr(mo, 'file_name', None) if mo else None
                if orig:
                    import re
                    orig = re.sub(r'[\\/*?:"<>|]', "", orig)
                safe = f"downloads/{msg.id}_{orig}" if orig else f"downloads/{msg.id}"
                fp = await client.download_media(msg, file_name=safe)
                if not fp: raise Exception("DownloadFailed")
                kw = {"chat_id": to_chat, "caption": caption if caption is not None else (msg.caption or "")}
                if msg.photo:       await client.send_photo(photo=fp, **kw)
                elif msg.video:     await client.send_video(video=fp, file_name=orig, **kw)
                elif msg.document:  await client.send_document(document=fp, file_name=orig, **kw)
                elif msg.audio:     await client.send_audio(audio=fp, file_name=orig, **kw)
                elif msg.voice:     await client.send_voice(voice=fp, **kw)
                elif msg.animation: await client.send_animation(animation=fp, **kw)
                elif msg.sticker:   await client.send_sticker(sticker=fp, **kw)
                if os.path.exists(fp): os.remove(fp)
                return True
            else:
                await client.send_message(chat_id=to_chat, text=msg.text or "")
                return True
        except Exception as e2:
            logger.debug(f"[TaskJob] send fallback: {e2}")
            return False


# ══════════════════════════════════════════════════════════════════════════════
# Core runner
# ══════════════════════════════════════════════════════════════════════════════

BATCH_SIZE = 200

async def _run_task_job(job_id: str, user_id: int):
    job = await _tj_get(job_id)
    if not job: return

    if job_id not in _pause_events:
        ev = asyncio.Event(); ev.set()
        _pause_events[job_id] = ev
    pause_ev = _pause_events[job_id]

    acc = client = None
    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _tj_update(job_id, status="error", error="Account not found"); return

        client  = await start_clone_bot(_CLIENT.client(acc))
        is_bot  = acc.get("is_bot", True)
        fc      = job["from_chat"]
        to_chat = job["to_chat"]
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

            dis      = await db.get_filters(user_id)
            configs  = await db.get_configs(user_id)
            rm_cap   = 'rm_caption' in dis
            cap_tpl  = configs.get('caption')
            forward_tag = configs.get('forward_tag', False)
            slp      = max(1, configs.get('duration', 1) or 1)

            chunk_end = current + BATCH_SIZE - 1
            if end_id > 0: chunk_end = min(chunk_end, end_id)
            batch_ids = list(range(current, chunk_end + 1))

            is_private_src = (fc == "me") or (isinstance(fc, int) and fc > 0)
            try:
                if not is_bot or is_private_src:
                    col = []
                    async for msg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_SIZE):
                        if msg.id < current: break
                        col.append(msg)
                    msgs = list(reversed(col))
                else:
                    msgs = await client.get_messages(fc, batch_ids)
                    if not isinstance(msgs, list): msgs = [msgs]
            except FloodWait as fw: await asyncio.sleep(fw.value + 2); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f"[TaskJob {job_id}] Fetch {current}: {e}")
                current += BATCH_SIZE; await _tj_update(job_id, current_id=current); continue

            valid = sorted([m for m in msgs if m and not m.empty], key=lambda m: m.id)

            if not valid:
                consec = fresh.get("consecutive_empty", 0) + 1
                if consec >= 3:
                    await _tj_update(job_id, status="done", current_id=current)
                    break
                current += BATCH_SIZE
                await _tj_update(job_id, consecutive_empty=consec, current_id=current)
                await asyncio.sleep(2); continue

            await _tj_update(job_id, consecutive_empty=0)

            fwd = 0
            for msg in valid:
                await pause_ev.wait()
                f2 = await _tj_get(job_id)
                if not f2 or f2.get("status") in ("stopped",): return
                if not _passes_filters(msg, dis): continue
                ok = await _send_one(client, msg, to_chat, rm_cap, cap_tpl, forward_tag=forward_tag, from_chat=fc)
                if ok: fwd += 1; await _tj_inc(job_id)
                await asyncio.sleep(slp)

            current = (valid[-1].id + 1) if valid else (current + BATCH_SIZE)
            await _tj_update(job_id, current_id=current)

    except asyncio.CancelledError:
        logger.info(f"[TaskJob {job_id}] Cancelled")
        await _tj_update(job_id, status="stopped")
    except Exception as e:
        logger.error(f"[TaskJob {job_id}] Fatal: {e}")
        await _tj_update(job_id, status="error", error=str(e))
    finally:
        _task_jobs.pop(job_id, None); _pause_events.pop(job_id, None)
        if client:
            try: await client.stop()
            except Exception: pass


def _start_task(job_id: str, user_id: int):
    ev = asyncio.Event(); ev.set()
    _pause_events[job_id] = ev
    task = asyncio.create_task(_run_task_job(job_id, user_id))
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
            "┣⊸ ‣ ᴄᴏᴘɪᴇs ᴀʟʟ ᴇxɪsᴛɪɴɢ ᴍsɢs ɪɴ ᴛʜᴇ ʙᴀᴄᴋɢʀᴏᴜɴᴅ\n"
            "┣⊸ ‣ ᴘᴀᴜsᴇ / ʀᴇsᴜᴍᴇ sᴜᴘᴘᴏʀᴛ\n"
            "┣⊸ ‣ ᴍᴜʟᴛɪᴘʟᴇ ᴊᴏʙs sɪᴍᴜʟᴛᴀɴᴇᴏᴜsʟʏ\n"
            "┣⊸ ‣ sᴜʀᴠɪᴠᴇs ʙᴏᴛ ʀᴇsᴛᴀʀᴛs\n"
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
            cur = j.get("current_id", "?")
            end = j.get("end_id", 0)
            rng = f"<code>{cur}</code>/{end if end else '∞'}"
            err = f"\n┃   ⚠️ <code>{j.get('error','')}</code>" if j.get("status") == "error" else ""
            c_name = j.get("custom_name")
            name_disp = f" <b>{c_name}</b>" if c_name else ""
            lines.append(
                f"┣⊸ {st} <b>{j.get('from_title','?')} → {j.get('to_title','?')}</b>"
                f"  <code>[{j['job_id'][-6:]}]</code>{name_disp}"
                f"\n┃   ◈ 𝐅𝐰𝐝: <code>{fwd}</code>  ◈ 𝐏𝐨𝐬: {rng}{err}"
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
                row.append(InlineKeyboardButton(f"⏸ ᴘᴀᴜsᴇ [{s}]",  callback_data=f"tj#pause#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ sᴛᴏᴘ [{s}]",   callback_data=f"tj#stop#{jid}"))
            elif st == "paused":
                row.append(InlineKeyboardButton(f"▶️ ʀᴇsᴜᴍᴇ [{s}]", callback_data=f"tj#resume#{jid}"))
                row.append(InlineKeyboardButton(f"⏹ sᴛᴏᴘ [{s}]",   callback_data=f"tj#stop#{jid}"))
            else:
                row.append(InlineKeyboardButton(f"▶️ sᴛᴀʀᴛ [{s}]",  callback_data=f"tj#start#{jid}"))
            row.append(InlineKeyboardButton(f"ℹ️ [{s}]", callback_data=f"tj#info#{jid}"))
            row.append(InlineKeyboardButton(f"🗑 [{s}]",  callback_data=f"tj#del#{jid}"))
            rows.append(row)

        rows.append([InlineKeyboardButton("➕ ᴄʀᴇᴀᴛᴇ ᴛᴀsᴋ ᴊᴏʙ", callback_data="tj#new")])
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
    err_lbl = f"\n┣⊸ ⚠️ ᴇʀʀᴏʀ : <code>{job['error']}</code>" if job.get("error") else ""

    c_name   = job.get("custom_name")
    name_lbl = f"\n┣⊸ ◈ 𝐍𝐚𝐦𝐞    : <b>{c_name}</b>" if c_name else ""

    text = (
        f"<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙ ɪɴғᴏ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐈𝐃      : <code>{job_id[-6:]}</code>{name_lbl}\n"
        f"┣⊸ ◈ 𝐒𝐭𝐚𝐭𝐮𝐬  : {st} {job.get('status','?')}\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {job.get('from_title','?')}\n"
        f"┣⊸ ◈ 𝐓𝐚𝐫𝐠𝐞𝐭  : {job.get('to_title','?')}\n"
        f"┣⊸ ◈ 𝐑𝐚𝐧𝐠𝐞   : {rng_lbl}\n"
        f"┣⊸ ◈ 𝐂𝐮𝐫𝐫𝐞𝐧𝐭 : <code>{cur}</code>\n"
        f"┣⊸ ◈ 𝐅𝐰𝐝     : <code>{job.get('forwarded', 0)}</code>\n"
        f"┣⊸ ◈ 𝐂𝐫𝐞𝐚𝐭𝐞𝐝 : {created}"
        f"{err_lbl}\n"
        f"┃\n"
        f"╰────────────────────────────────╯</b>"
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
    await q.answer("⏸ ᴘᴀᴜsᴇᴅ.")
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
        await q.answer("▶️ ʀᴇsᴜᴍᴇᴅ!")
    else:
        await _tj_update(job_id, status="running")
        _start_task(job_id, uid)
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
    await q.answer("⏹ sᴛᴏᴘᴘᴇᴅ.")
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
    _start_task(job_id, uid)
    await q.answer("▶️ sᴛᴀʀᴛᴇᴅ!")
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

async def _create_taskjob_flow(bot, user_id: int):
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

    # Save & Start
    job_id = f"tj-{user_id}-{int(time.time())}"
    job = {
        "job_id": job_id, "user_id": user_id, "account_id": sel["id"],
        "from_chat": fc, "from_title": ftitle,
        "to_chat": to_chat, "to_title": to_title,
        "start_id": start_id, "end_id": end_id, "current_id": start_id,
        "status": "running", "created": int(time.time()),
        "forwarded": 0, "consecutive_empty": 0, "error": "",
        "custom_name": cname,
    }
    await _tj_save(job)
    _start_task(job_id, user_id)

    end_lbl = f"<code>{end_id}</code>" if end_id else "∞ (ᴀʟʟ ᴍsɢs)"
    await bot.send_message(user_id,
        f"<b>╭──────❰ ✅ ᴛᴀsᴋ ᴊᴏʙ ᴄʀᴇᴀᴛᴇᴅ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {ftitle}\n"
        f"┣⊸ ◈ 𝐓𝐚𝐫𝐠𝐞𝐭  : {to_title}\n"
        f"┣⊸ ◈ 𝐀𝐜𝐜𝐨𝐮𝐧𝐭 : {'🤖 ʙᴏᴛ' if ibot else '👤 ᴜsᴇʀʙᴏᴛ'} {sel.get('name','?')}\n"
        f"┣⊸ ◈ 𝐑𝐚𝐧𝐠𝐞   : <code>{start_id}</code> → {end_lbl}\n"
        f"┣⊸ ◈ 𝐉𝐨𝐛 𝐈𝐃  : <code>{job_id[-6:]}</code>" + (f" (<b>{cname}</b>)\n" if cname else "\n") +
        f"┃\n"
        f"╰────────────────────────────────╯</b>",
        reply_markup=ReplyKeyboardRemove())
