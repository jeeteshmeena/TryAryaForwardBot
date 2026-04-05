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
from bot import BOT_INSTANCE

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

async def _post_live_batch(sb_client, job: dict, chunk_msgs: list):
    """Generates the aesthetic button block and securely stores appUrls inside the Target Channel."""
    try:
        uid = job["user_id"]
        share_bot_id = job.get("share_bot_id")
        target_ch = int(job["target"])
        protect = job.get("protect", True)
        
        if not chunk_msgs:
            logger.warning("Live Batch Post: chunk_msgs is completely empty!")
            return False
            
        bot_usr = ""
        if share_bot_id == "bot":
            from bot import BOT_INSTANCE
            if not BOT_INSTANCE or not getattr(BOT_INSTANCE, "me", None):
                from plugins.test import Config
                from pyrogram.types import User
                # Simple fallback if me is not loaded
                bot_usr = Config.BOT_USERNAME.replace("@", "") if hasattr(Config, "BOT_USERNAME") else "arya_bot"
            else:
                bot_usr = BOT_INSTANCE.me.username
        else:
            share_bot_id = str(share_bot_id)
            share_bots = await db.get_share_bots()
            sb = next((b for b in share_bots if str(b['id']) == share_bot_id), None)
            if not sb: 
                logger.warning(f"Live Batch Post Error: Share bot missing from DB (ID: {share_bot_id})")
                return False
            bot_usr = sb.get("username", "")
        
        raw_buttons = []
        
        for m in chunk_msgs:
            fname = getattr(m.document or m.audio or m.video or m.voice, "file_name", None) or m.caption or ""
            extracted = _deep_extract_ep(fname)
            ep_val = extracted[0] if extracted else "?"
            
            uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
            await db.save_share_link(uuid_str, [m.id], job["source"], protect=protect, access_hash=None)
            url = f"https://t.me/{bot_usr}?start={uuid_str}"
            
            raw_buttons.append({"btn": InlineKeyboardButton(_sc(f"{ep_val}"), url=url), "ep": ep_val})
            
        first_ep = raw_buttons[0]["ep"]
        last_ep  = raw_buttons[-1]["ep"]
        
        if str(first_ep).isdigit() and str(last_ep).isdigit():
            if int(first_ep) > int(last_ep): first_ep, last_ep = last_ep, first_ep
            
        txt = f"{_bold_sans(job['story'])} 𝗘𝗣𝗦 {first_ep} - {last_ep}"
        
        keyboard = []
        for j in range(0, len(raw_buttons), 2):
            row = [c["btn"] for c in raw_buttons[j:j + 2]]
            keyboard.append(row)
            
        keyboard.append([
            InlineKeyboardButton(_sc("tutorial"), url="https://t.me/StoriesLinkopningguide"),
            InlineKeyboardButton(_sc("support"), url="https://t.me/AryaHelpTG")
        ])
        
        for attempt in range(5):
            try:
                await sb_client.send_message(
                    chat_id=target_ch, text=txt,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return True
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
            except Exception as tg_err:
                logger.warning(f"Live Batch Post TG Send Error: {tg_err}")
                await asyncio.sleep(5)
                
        return False
    except Exception as grand_err:
        import traceback
        logger.error(f"FATAL Exception in _post_live_batch: {traceback.format_exc()}")
        return False

async def _lb_run_job(job_id: str):
    logger.info(f"Starting Live Batch job {job_id}")
    from plugins.share_bot import share_clients
    from plugins.test import CLIENT as _FACTORY
    
    src_client = None
    ub_sess = None
    
    while True:
        ev = _lb_paused.get(job_id)
        if ev and not ev.is_set():
            await ev.wait()
            
        job = await _lb_get_job(job_id)
        if not job or job.get("status") in ("stopped", "failed"):
            if src_client:
                try: await src_client.disconnect()
                except: pass
            break
            
        try:
            source = job["source"]
            target = job["target"]
            thresh = job["threshold"]
            last_seen = job.get("last_seen_id", 0)
            buffer_mids = job.get("buffer_mids", [])
            fwd_count = job.get("forwarded", 0)
            sb_client = share_clients.get(str(job["share_bot_id"]))
            
            # Reconnect Source Client 
            if not src_client or not getattr(src_client, "is_connected", False):
                acc_id = job.get("account_id", "bot")
                if acc_id == "bot":
                    src_client = BOT_INSTANCE
                else:
                    bots = await db.get_bots(job["user_id"])
                    acc_bot = next((b for b in bots if str(b.get("id")) == str(acc_id)), None)
                    if acc_bot and not acc_bot.get("is_bot", False):
                        ub_sess = acc_bot["session"]
                        src_client = _FACTORY().client({"session": ub_sess}, False)
                        try:
                            await src_client.connect()
                        except Exception as e:
                            logger.error(f"Live Batch: Failed to connect user account: {e}")
                            await asyncio.sleep(60)
                            continue
                    else:
                        # Fallback to main bot if specified account disappears
                        src_client = BOT_INSTANCE
            
            if not sb_client:
                logger.error("Live Batch: Share Bot is entirely offline.")
                await asyncio.sleep(60)
                continue
            
            # Setup Progress Bar
            prog_id = job.get("prog_id")
            if not prog_id:
                try:
                    p = await sb_client.send_message(target, 
                        f"📡 <b>Bᴀᴛᴄʜ Lɪɴᴋs Lɪᴠᴇ Aᴜᴛᴏ-Gᴇɴᴇʀᴀᴛᴏʀ</b>\n\n"
                        f"✅ Auto-Generated Blocks: <code>{fwd_count}</code>\n"
                        f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                        f"<i>This message updates every 60s. Powered by Arya Forward Bot</i>"
                    )
                    prog_id = p.id
                    await _lb_update_job(job_id, {"prog_id": prog_id})
                    try: await sb_client.pin_chat_message(target, prog_id, disable_notification=True)
                    except: pass
                except: pass

            fetched = []
            batch_req = []
            
            try:
                # Pyrogram's get_chat_history is blocked for Bot accounts!
                # We systematically just chunk forward using get_messages.
                batch_req = []
                for mid in range(last_seen + 1, last_seen + 201):
                    batch_req.append(mid)
                    
                msgs = await src_client.get_messages(source, batch_req)
                if not isinstance(msgs, list): msgs = [msgs]
                
            except Exception as e:
                logger.error(f"Live Batch get_messages error: {e}")
                msgs = []
                
            valid = [m for m in msgs if m and not m.empty and getattr(m, 'media', None) and not m.service]
            valid.sort(key=lambda m: m.id)
            
            raw_exists = [m for m in msgs if m and not getattr(m, 'empty', True)]
            
            if not raw_exists:
                # The entire 200-ID chunk is completely empty.
                # Could be the physical end of the channel, OR a deleted gap larger than 200.
                try:
                    probe = await src_client.get_messages(source, [last_seen + 250, last_seen + 500, last_seen + 1000])
                    if isinstance(probe, list) and any(p for p in probe if p and not getattr(p, 'empty', True)):
                        # Massive gap confirmed! Jump forward.
                        last_seen += 200
                        await _lb_update_job(job_id, {"last_seen_id": last_seen})
                        continue
                except: pass
                # No messages ahead. We are at the bleeding edge.
                await asyncio.sleep(20)
                continue
                
            # Valid media found inside chunk! Process it.
            for m in valid:
                buffer_mids.append(m.id)
                
            # Safely advance last_seen to the highest physically detected message in this chunk!
            last_seen = max(m.id for m in raw_exists)

            await _lb_update_job(job_id, {"last_seen_id": last_seen, "buffer_mids": buffer_mids})

            force = job.get("force_flush")
            if len(buffer_mids) >= thresh or (force and buffer_mids):
                if force: await _lb_update_job(job_id, {"force_flush": False})
                
                post_limit = len(buffer_mids) if force else thresh
                if post_limit > 100: post_limit = 100 # Safety for telegram API
                
                chunk_ids = buffer_mids[:post_limit]
                rem_mids  = buffer_mids[post_limit:]
                
                actual_msgs = await src_client.get_messages(source, chunk_ids)
                if not isinstance(actual_msgs, list): actual_msgs = [actual_msgs]
                actual_msgs = [m for m in actual_msgs if m and not m.empty]
                
                # Execute payload
                success = await _post_live_batch(sb_client, job, actual_msgs)
                if success:
                    fwd_count += len(chunk_ids)
                    await _lb_update_job(job_id, {"buffer_mids": rem_mids, "forwarded": fwd_count})
            
            now_t = time.time()
            up_time = job.get("last_prog_update", 0)
            if prog_id and (now_t - up_time) > 60:
                try:
                    await sb_client.edit_message_text(target, prog_id,
                        f"📡 <b>Bᴀᴛᴄʜ Lɪɴᴋs Lɪᴠᴇ Aᴜᴛᴏ-Gᴇɴᴇʀᴀᴛᴏʀ</b>\n\n"
                        f"✅ Auto-Generated Blocks: <code>{fwd_count}</code>\n"
                        f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                        f"<i>This message updates every 60s. Powered by Arya Forward Bot</i>"
                    )
                    await _lb_update_job(job_id, {"last_prog_update": now_t})
                except: pass

            await asyncio.sleep(20)

        except Exception as e:
            logger.error(f"Live Batch generic loop error: {e}")
            await asyncio.sleep(20)

@Client.on_callback_query(filters.regex(r"^lb#(main|setup|view|pause|resume|stop|del)"))
async def _lb_callbacks(bot, update: CallbackQuery):
    uid = update.from_user.id
    data = update.data.split("#")
    action = data[1]
    if action == "setup":
        from plugins.share_jobs import _create_share_flow
        try:
            await update.message.delete()
        except:
            pass
        asyncio.create_task(_create_share_flow(bot, uid, force_live=True))
        return True

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
        
        buf = len(job.get("buffer_mids", []))
        trgt = job.get("threshold", 10)
        
        if buf > 0 and st in ("running", "queued", "paused"):
            kb.append([InlineKeyboardButton(f"🚀 Fᴏʀᴄᴇ Pᴏsᴛ Nᴏᴡ ({buf} Fɪʟᴇs)", callback_data=f"lb#force#{jid}")])
            
        kb.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ", callback_data=f"lb#view#{jid}")])
        if st in ("completed", "stopped", "failed"):
            kb.append([InlineKeyboardButton("🗑 Dᴇʟᴇᴛᴇ Rᴇᴄᴏʀᴅ", callback_data=f"lb#del#{jid}")])
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="lb#main")])
        
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
        
    elif action == "force":
        jid = data[2]
        await _lb_update_job(jid, {"force_flush": True})
        update.data = f"lb#view#{jid}"
        await update.answer("🚀 Triggered forced buffer flush!", show_alert=False)
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
