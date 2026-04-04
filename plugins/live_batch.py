"""
Live Batch System — Arya Bot
============================
Automatically monitors a source database, buffers incoming media,
and automatically builds Batch-Link delivery messages with inline
buttons when the defined threshold is hit.
"""
import asyncio
import logging
import time
import uuid
import re
import os
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, 
    ReplyKeyboardRemove, Message, CallbackQuery
)
from pyrogram.errors import FloodWait
from database import db
from plugins.test import CLIENT
from plugins.share_jobs import _deep_extract_ep, _sc

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()
COLL = "live_batch_jobs"

_lb_tasks: dict[str, asyncio.Task] = {}
_lb_paused: dict[str, asyncio.Event] = {}
_lb_waiter: dict[int, asyncio.Future] = {}

# ─────────────────────────────────────────────────────────────────────────────
# DB & Router Helpers
# ─────────────────────────────────────────────────────────────────────────────
async def _lb_save_job(job: dict):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)

async def _lb_get_job(jid: str):
    return await db.db[COLL].find_one({"job_id": jid})

async def _lb_get_all_jobs(uid: int):
    return [j async for j in db.db[COLL].find({"user_id": uid})]

async def _lb_delete_job(jid: str):
    await db.db[COLL].delete_one({"job_id": jid})

async def _lb_update_job(jid: str, kw: dict):
    await db.db[COLL].update_one({"job_id": jid}, {"$set": kw})

@Client.on_message(filters.private, group=-17)
async def _lb_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _lb_waiter:
        fut = _lb_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation

async def _lb_ask(bot, user_id, text, reply_markup=None, timeout=300):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    old = _lb_waiter.pop(user_id, None)
    if old and not old.done(): old.cancel()
    _lb_waiter[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _lb_waiter.pop(user_id, None)
        raise

# ─────────────────────────────────────────────────────────────────────────────
# Core Engine
# ─────────────────────────────────────────────────────────────────────────────
def _bold_sans(s):
    res = ''
    for c in str(s):
        if 'A' <= c <= 'Z': res += chr(0x1D5D4 + ord(c) - ord('A'))
        elif 'a' <= c <= 'z': res += chr(0x1D5D4 + ord(c) - ord('a'))
        else: res += c
    return res

async def _post_live_batch(bot, job: dict, chunk_msgs: list):
    """Generates the aesthetic button block and securely stores appUrls."""
    uid = job["user_id"]
    share_bot_id = job["share_bot_id"]
    target_ch = job["target"]
    protect = job.get("protect", True)
    
    sb = await db.db.bots.find_one({"id": share_bot_id, "user_id": uid})
    if not sb: return False
    bot_usr = sb.get("username")
    
    # Process episodes and metadata
    raw_buttons = []
    
    for m in chunk_msgs:
        # Standard extraction matching deepscan rules perfectly
        fname = getattr(m.document or m.audio or m.video or m.voice, "file_name", None) or m.caption or ""
        extracted = _deep_extract_ep(fname)
        ep_val = extracted[0] if extracted else "?"
        
        # Save securely mapped to Delivery Bot schema
        uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
        await db.save_share_link(uuid_str, [m.id], job["source"], protect=protect, access_hash=None)
        url = f"https://t.me/{bot_usr}?start={uuid_str}"
        
        raw_buttons.append({"btn": InlineKeyboardButton(_sc(f"{ep_val}"), url=url), "ep": ep_val})
        
    first_ep = raw_buttons[0]["ep"]
    last_ep  = raw_buttons[-1]["ep"]
    
    # Ensure numerical formatting cleanly falls back
    if str(first_ep).isdigit() and str(last_ep).isdigit():
        if int(first_ep) > int(last_ep): first_ep, last_ep = last_ep, first_ep
        
    txt = f"{_bold_sans(job['story'])} 𝗘𝗣𝗦 {first_ep} - {last_ep}"
    
    keyboard = []
    for j in range(0, len(raw_buttons), 2):
        row = [c["btn"] for c in raw_buttons[j:j + 2]]
        keyboard.append(row)
        
    # Standard Footer parity exactly matching batch output
    keyboard.append([
        InlineKeyboardButton(_sc("tutorial"), url="https://t.me/StoriesLinkopningguide"),
        InlineKeyboardButton(_sc("support"), url="https://t.me/AryaHelpTG")
    ])
    
    # Post it
    for attempt in range(5):
        try:
            await _CLIENT.send_message(
                chat_id=target_ch, text=txt,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            logger.warning(f"Live Batch Post Error: {e}")
            await asyncio.sleep(5)
    return False

async def _lb_run_job(job_id: str):
    logger.info(f"Starting Live Batch job {job_id}")
    while True:
        ev = _lb_paused.get(job_id)
        if ev and not ev.is_set():
            await ev.wait()
            
        job = await _lb_get_job(job_id)
        if not job or job.get("status") in ("stopped", "failed"):
            break
            
        try:
            source = job["source"]
            target = job["target"]
            thresh = job["threshold"]
            last_seen = job.get("last_seen_id", 0)
            buffer_mids = job.get("buffer_mids", [])
            fwd_count = job.get("forwarded", 0)
            
            # Setup Progress Bar
            prog_id = job.get("prog_id")
            if not prog_id:
                try:
                    p = await _CLIENT.send_message(target, 
                        f"📡 <b>Live Job Active — monitoring for new messages…</b>\n\n"
                        f"✅ Forwarded so far: <code>{fwd_count}</code>\n"
                        f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                        f"<i>This message updates every 60s. Powered by Arya Forward Bot</i>"
                    )
                    prog_id = p.id
                    await _lb_update_job(job_id, {"prog_id": prog_id})
                    try: await _CLIENT.pin_chat_message(target, prog_id, disable_notification=True)
                    except: pass
                except: pass

            fetched = []
            
            # Very carefully extract new messages using batch methodology
            batch_req = list(range(last_seen + 1, last_seen + 201))
            try:
                msgs = await _CLIENT.get_messages(source, batch_req)
                if not isinstance(msgs, list): msgs = [msgs]
            except Exception as e:
                msgs = []
                
            valid = [m for m in msgs if m and not m.empty and not m.service and (m.audio or m.voice or m.document or m.video)]
            valid.sort(key=lambda m: m.id)
            
            for m in valid:
                buffer_mids.append(m.id)
                
            if valid:
                last_seen = valid[-1].id
                
            # If nothing valid, but there might be a gap, we must still advance `last_seen` if
            # an empty ID was fetched so we don't get permanently stuck polling deleted messages.
            # (To be extremely safe, we only advance if the messages request succeeded)
            if not valid and msgs and any(x is not None for x in msgs):
                # Jump over gaps
                valid_probe = [m for m in msgs if m and not m.empty]
                if valid_probe:
                    last_seen = max(last_seen, valid_probe[-1].id)

            await _lb_update_job(job_id, {"last_seen_id": last_seen, "buffer_mids": buffer_mids})

            # Check threshold trigger
            if len(buffer_mids) >= thresh:
                # We reached threshold!
                chunk_ids = buffer_mids[:thresh]
                rem_mids  = buffer_mids[thresh:]
                
                actual_msgs = await _CLIENT.get_messages(source, chunk_ids)
                if not isinstance(actual_msgs, list): actual_msgs = [actual_msgs]
                actual_msgs = [m for m in actual_msgs if m and not m.empty]
                
                # Execute payload
                success = await _post_live_batch(_CLIENT, job, actual_msgs)
                if success:
                    fwd_count += len(chunk_ids)
                    await _lb_update_job(job_id, {"buffer_mids": rem_mids, "forwarded": fwd_count})
            
            # Progress Updates
            now_t = time.time()
            up_time = job.get("last_prog_update", 0)
            if prog_id and (now_t - up_time) > 60:
                try:
                    await _CLIENT.edit_message_text(target, prog_id,
                        f"📡 <b>Live Job Active — monitoring for new messages…</b>\n\n"
                        f"✅ Forwarded so far: <code>{fwd_count}</code>\n"
                        f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                        f"<i>This message updates every 60s. Powered by Arya Forward Bot</i>"
                    )
                    await _lb_update_job(job_id, {"last_prog_update": now_t})
                except: pass

            await asyncio.sleep(20)

        except Exception as e:
            logger.error(f"Live Batch generic loop error: {e}")
            await asyncio.sleep(20)

# ─────────────────────────────────────────────────────────────────────────────
# Setups and Ask flow
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^lb#(main|setup|view|pause|resume|stop|del)"))
async def _lb_callbacks(bot, update: CallbackQuery):
    uid = update.from_user.id
    data = update.data.split("#")
    action = data[1]
    
    if action == "setup":
        await update.message.edit_reply_markup(None)
        
        # Share Bot
        share_bots = await db.get_share_bots()
        if not share_bots:
            return await bot.send_message(uid, "<b>❌ No Share Bots available. Add in /settings.</b>")
            
        sb_kb = [[f"🤖 @{b['username']}"] for b in share_bots]
        sb_kb.append(["⛔ Cᴀɴᴄᴇʟ"])
        
        msg1 = await _lb_ask(bot, uid, "<b>📡 Live Auto-Batch Link Setup</b>\n\n<b>Phase 1:</b> Select your Share Bot:", reply_markup=ReplyKeyboardMarkup(sb_kb, resize_keyboard=True, one_time_keyboard=True))
        if not msg1.text or "⛔" in msg1.text: return await bot.send_message(uid, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        s_usr = msg1.text.replace("🤖 @", "").strip()
        share_bot = next((b for b in share_bots if b['username'] == s_usr), None)
        if not share_bot: return await bot.send_message(uid, "Invalid bot.", reply_markup=ReplyKeyboardRemove())
        
        # Databases (Source and Target)
        chans = await db.get_user_channels(uid)
        ch_kb = [[ch['title']] for ch in chans]
        ch_kb.append(["⛔ Cᴀɴᴄᴇʟ"])
        
        msg2 = await _lb_ask(bot, uid, "<b>Phase 2:</b> Select SOURCE Database Channel:", reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
        if not msg2.text or "⛔" in msg2.text: return await bot.send_message(uid, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        src_ch = next((c for c in chans if c["title"] == msg2.text.strip()), None)
        if not src_ch: return await bot.send_message(uid, "Invalid source.", reply_markup=ReplyKeyboardRemove())
        
        msg3 = await _lb_ask(bot, uid, "<b>Phase 3:</b> Select TARGET Public Channel:", reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True))
        if not msg3.text or "⛔" in msg3.text: return await bot.send_message(uid, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        tgt_ch = next((c for c in chans if c["title"] == msg3.text.strip()), None)
        if not tgt_ch: return await bot.send_message(uid, "Invalid target.", reply_markup=ReplyKeyboardRemove())
        
        # Story Name
        msg4 = await _lb_ask(bot, uid, "<b>Phase 4:</b> Name of the Story/Series? (e.g. `TDMB`)", reply_markup=ReplyKeyboardRemove())
        if not msg4.text or "⛔" in msg4.text: return await bot.send_message(uid, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        story = msg4.text.strip()
        
        # Threshold
        msg5 = await _lb_ask(bot, uid, "<b>Phase 5:</b> Threshold limit?\n<i>When exactly how many new files arrive should it combine them and post? (Default: 10)</i>")
        if not msg5.text or "⛔" in msg5.text: return await bot.send_message(uid, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        thresh = int(msg5.text.strip()) if msg5.text.strip().isdigit() else 10
        
        # Security Protect
        protect_kb = ReplyKeyboardMarkup([["🔐 Protect (No Forwards)", "🔓 Open (Allow Forwards)"]], resize_keyboard=True, one_time_keyboard=True)
        msg6 = await _lb_ask(bot, uid, "<b>Phase 6:</b> Should link files be protected?", reply_markup=protect_kb)
        protect = "Protect" in (msg6.text or "")
        await bot.send_message(uid, "<b>Scanning database for starting point...</b>", reply_markup=ReplyKeyboardRemove())
        
        # Get offset ID globally so it starts tracking accurately NOW
        last_seen = 0
        try:
            async for m in _CLIENT.get_chat_history(int(src_ch['chat_id']), limit=1):
                last_seen = m.id
        except: pass

        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id, "user_id": uid, "status": "running",
            "share_bot_id": share_bot["id"],
            "source": int(src_ch['chat_id']),
            "target": int(tgt_ch['chat_id']),
            "story": story,
            "threshold": thresh,
            "protect": protect,
            "last_seen_id": last_seen,
            "buffer_mids": [],
            "forwarded": 0
        }
        await _lb_save_job(job)
        
        _lb_paused[job_id] = asyncio.Event()
        _lb_paused[job_id].set()
        _lb_tasks[job_id] = asyncio.create_task(_lb_run_job(job_id))
        
        await bot.send_message(uid, f"<b>✅ Live Batch system activated for {story}!</b>\n\nIt is now continuously monitoring the source channel.")
        update.data = f"lb#main"
        return await _lb_callbacks(bot, update)

    elif action == "main":
        jobs = await _lb_get_all_jobs(uid)
        active = [j for j in jobs if j.get("status") not in ("failed", "stopped")]
        kb = [[InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ ʟɪᴠᴇ ʙᴀᴛᴄʜ", callback_data="lb#setup")]]
        
        row = []
        for i, j in enumerate(active):
            name = str(j.get('story', 'Batch'))[:12]
            row.append(InlineKeyboardButton(f"📡 {name}", callback_data=f"lb#view#{j['job_id']}"))
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ ᴛᴏ Mᴀɪɴ", callback_data="sl#start")])
        
        txt = (
            "<b><u>📡 Oɴɢᴏɪɴɢ Lɪᴠᴇ Bᴀᴛᴄʜ Sʏsᴛᴇᴍ</u></b>\n\n"
            "This daemon seamlessly monitors your Database channel. Once the threshold count is hit, "
            "it effortlessly aggregates the tracked media into structured interactive Batch Buttons and ships them out dynamically."
        )
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))
        
    elif action == "view":
        jid = data[2]
        job = await _lb_get_job(jid)
        if not job: return await update.answer("Job not found.", show_alert=True)
        
        st = job.get("status")
        kb = []
        if st in ("running", "queued"):
            kb.append([
                InlineKeyboardButton("⏸ Pᴀᴜsᴇ", callback_data=f"lb#pause#{jid}"),
                InlineKeyboardButton("⏹ Sᴛᴏᴘ", callback_data=f"lb#stop#{jid}")
            ])
        elif st == "paused":
            kb.append([
                InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ", callback_data=f"lb#resume#{jid}"),
                InlineKeyboardButton("⏹ Sᴛᴏᴘ", callback_data=f"lb#stop#{jid}")
            ])
        
        kb.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ", callback_data=f"lb#view#{jid}")])
        if st in ("completed", "stopped", "failed"):
            kb.append([InlineKeyboardButton("🗑 Dᴇʟᴇᴛᴇ Rᴇᴄᴏʀᴅ", callback_data=f"lb#del#{jid}")])
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="lb#main")])
        
        buf = len(job.get("buffer_mids", []))
        trgt = job.get("threshold", 10)
        
        txt = (
            f"<b>📡 Lɪᴠᴇ Bᴀᴛᴄʜ Sᴛᴀᴛᴜs</b>\n\n"
            f"<b>📖 Sᴛᴏʀʏ:</b> <code>{job.get('story')}</code>\n"
            f"<b>ℹ️ Sᴛᴀᴛᴜs:</b> <code>{st.upper()}</code>\n"
            f"<b>🎯 Tʜʀᴇsʜᴏʟᴅ:</b> Wait for {trgt} files\n"
            f"<b>📦 Cᴜʀʀᴇɴᴛ Bᴜғғᴇʀ:</b> <code>{buf} / {trgt}</code>\n"
            f"<b>✅ Tᴏᴛᴀʟ Pᴏsᴛᴇᴅ Bᴀᴛᴄʜᴇs:</b> {int(job.get('forwarded', 0) / max(1, trgt))}\n\n"
            f"<i>Auto-checks source database continuously.</i>"
        )
        try: await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))
        except: pass

    elif action == "pause":
        jid = data[2]
        if jid in _lb_paused: _lb_paused[jid].clear()
        await _lb_update_job(jid, {"status": "paused"})
        update.data = f"lb#view#{jid}"
        return await _lb_callbacks(bot, update)

    elif action == "resume":
        jid = data[2]
        await _lb_update_job(jid, {"status": "running"})
        if jid not in _lb_paused: _lb_paused[jid] = asyncio.Event()
        _lb_paused[jid].set()
        if jid not in _lb_tasks or _lb_tasks[jid].done():
            _lb_tasks[jid] = asyncio.create_task(_lb_run_job(jid))
        update.data = f"lb#view#{jid}"
        return await _lb_callbacks(bot, update)

    elif action == "stop":
        jid = data[2]
        await _lb_update_job(jid, {"status": "stopped"})
        if jid in _lb_paused: _lb_paused[jid].set()
        if jid in _lb_tasks and not _lb_tasks[jid].done():
            _lb_tasks[jid].cancel()
        update.data = f"lb#view#{jid}"
        return await _lb_callbacks(bot, update)

    elif action == "del":
        jid = data[2]
        await _lb_delete_job(jid)
        update.data = "lb#main"
        return await _lb_callbacks(bot, update)

async def resume_live_batches():
    jobs = []
    async for j in db.db[COLL].find({"status": "running"}):
        jobs.append(j)
    for j in jobs:
        jid = j["job_id"]
        _lb_paused[jid] = asyncio.Event()
        _lb_paused[jid].set()
        _lb_tasks[jid] = asyncio.create_task(_lb_run_job(jid))
        logger.info(f"[LiveBatch] Resumed job {jid}")
