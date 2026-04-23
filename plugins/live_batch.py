"""
Live Batch System — Arya Bot
============================
Automatically monitors a source database, buffers incoming media,
and automatically builds Batch-Link delivery messages with inline
buttons when the defined threshold is hit.
"""
import asyncio
import logging
logger = logging.getLogger(__name__)
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
from bot import BOT_INSTANCE
from plugins.test import CLIENT
from plugins.utils import extract_ep_label_robust, format_tg_error
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
def _sc(text: str) -> str:
    """Convert ASCII letters to Unicode Small-Caps."""
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
    ))

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
        
        batch_size = int(job.get("batch_size", 10))
        
        # --- BUCKET GROUPING ---
        buckets = []
        for i in range(0, len(chunk_msgs), batch_size):
            buckets.append(chunk_msgs[i : i + batch_size])
            
        raw_buttons = []
        for bucket in buckets:
            mids = [m.id for m in bucket]
            
            # Extract numbers logically for the label
            eps = []
            for m in bucket:
                media_obj = getattr(m, 'document', None) or getattr(m, 'audio', None) or getattr(m, 'video', None) or getattr(m, 'voice', None)
                fname = getattr(media_obj, "file_name", "") or ""
                t = getattr(media_obj, "title", "") or ""
                cap = m.caption or ""
                combo_name = f"{t} - {fname}" if t else fname
                if not combo_name.strip(): combo_name = cap
                
                res = extract_ep_label_robust(combo_name)
                extracted = (res["numbers"][0], res["numbers"][-1]) if res.get("numbers") else None
                if extracted:
                    eps.append(int(extracted[0]))
            
            if eps:
                b_s = min(eps)
                b_e = max(eps)
                btn_text = str(b_s) if b_s == b_e else f"{b_s}–{b_e}"
            else:
                # Absolute fallback if no numeric episodes are detected
                b_s, b_e = "?", "?"
                btn_text = "Fɪʟᴇs"
            
            uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
            await db.save_share_link(uuid_str, mids, job["source"], protect=protect, access_hash=None)
            url = f"https://t.me/{bot_usr}?start={uuid_str}"
            
            raw_buttons.append({
                "text": _sc(btn_text),
                "url": url,
                "ep_start": b_s,
                "ep_end": b_e
            })
            
        buttons_per_post = int(job.get("buttons_per_post", 10))
        all_buttons = job.get("all_buttons", [])
        
        prev_total = len(all_buttons)
        all_buttons.extend(raw_buttons)
        
        old_mids = job.get("posted_mids", [])
        new_mids = list(old_mids)
        blocks = []
        for i in range(0, len(all_buttons), buttons_per_post):
            blocks.append(all_buttons[i : i + buttons_per_post])
            
        # Determine from which block index we need to start updating.
        # If the previous last block was perfectly full, start from the new block.
        # Otherwise, start from the previously incomplete block.
        changed_idx = prev_total // buttons_per_post
        if prev_total > 0 and prev_total % buttons_per_post == 0:
            changed_idx = prev_total // buttons_per_post
            
        for idx in range(changed_idx, len(blocks)):
            block = blocks[idx]
            v_starts = [b["ep_start"] for b in block if str(b["ep_start"]).isdigit()]
            v_ends   = [b["ep_end"] for b in block if str(b["ep_end"]).isdigit()]
            first_ep = min(v_starts) if v_starts else "?"
            last_ep  = max(v_ends) if v_ends else "?"
            txt = f"{_bold_sans(job['story'])} 𝗘𝗣𝗦 {first_ep} - {last_ep}"
            
            keyboard = []
            for j in range(0, len(block), 2):
                row = [InlineKeyboardButton(c["text"], url=c["url"]) for c in block[j:j+2]]
                keyboard.append(row)
                
            keyboard.append([
                InlineKeyboardButton(_sc("tutorial"), url="https://t.me/StoriesLinkopningguide"),
                InlineKeyboardButton(_sc("support"), url="https://t.me/AryaHelpTG")
            ])
            
            # User requirement: DELETE the last incomplete post, and CREATE a NEW post.
            # If idx is within old_mids, it means we are replacing a previously sent incomplete block.
            if idx < len(old_mids):
                for d_attempt in range(5):
                    try:
                        await sb_client.delete_messages(target_ch, old_mids[idx])
                        break
                    except FloodWait as dfw:
                        logger.info(f"[LiveBatch] Flood {dfw.value}s on delete")
                        await asyncio.sleep(dfw.value + 2)
                    except Exception:
                        break
            
            for attempt in range(5):
                try:
                    m = await sb_client.send_message(
                        chat_id=target_ch, text=txt,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        reply_to_message_id=job.get('target_topic_id')
                    )
                    
                    if idx < len(new_mids):
                        new_mids[idx] = m.id
                    else:
                        new_mids.append(m.id)
                    break
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2)
                except Exception as tg_err:
                    logger.warning(f"Live Batch Post TG Send Error: {tg_err}")
                    await asyncio.sleep(5)
            
        # ── Record successful post for Duplicate Handling ──
        try:
            # Extract all numbers from all files in this entire batch call
            all_posted_nums = []
            for bucket_msg in chunk_msgs:
                _media = getattr(bucket_msg, 'document', None) or getattr(bucket_msg, 'audio', None) or getattr(bucket_msg, 'video', None) or getattr(bucket_msg, 'voice', None)
                _fn = getattr(_media, "file_name", "") or ""
                _t = getattr(_media, "title", "") or ""
                fn = f"{_t} - {_fn}" if _t else _fn
                if not fn.strip(): fn = bucket_msg.caption or ""
                
                r = extract_ep_label_robust(fn)
                if r["numbers"]:
                    all_posted_nums.extend(r["numbers"])
            
            if all_posted_nums:
                await db.db["live_batch_posted_eps"].update_one(
                    {"target": target_ch, "story": job['story']},
                    {
                        "$addToSet": {"nums": {"$each": all_posted_nums}},
                        "$set": {"at": time.time()}
                    },
                    upsert=True
                )
        except Exception as e:
            logger.error(f"[LiveBatch] Error recording posted nums: {e}")

        return True, new_mids, all_buttons
    except Exception as grand_err:
        import traceback
        logger.error(f"FATAL Exception in _post_live_batch: {traceback.format_exc()}")
        return False

async def _lb_run_job(job_id: str):
    logger.info(f"Starting Live Batch job {job_id}")
    src_client = None
    ub_sess = None

    try:
        from plugins.share_bot import share_clients
        from plugins.test import CLIENT as _FACTORY
    except Exception as import_err:
        logger.error(f"[LiveBatch {job_id}] Import error on startup: {import_err}")
        return

    try:
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
                
                # ── Protected Chat Guard ───────────────────────────────────────────────
                from plugins.utils import check_chat_protection
                prot_err = await check_chat_protection(job["user_id"], source)
                if prot_err:
                    await _lb_update_job(job_id, {"status": "error", "error": prot_err})
                    try:
                        await BOT_INSTANCE.send_message(job["user_id"], prot_err)
                    except Exception:
                        pass
                    return
                # ──────────────────────────────────────────────────────────────────────
                
                thresh = job["threshold"]
                last_seen = job.get("last_seen_id", 0)
                buffer_mids = job.get("buffer_mids", [])
                buffer_mids = list(dict.fromkeys(buffer_mids))
                fwd_count = job.get("forwarded", 0)

                raw_sb_id = job["share_bot_id"]
                sb_client = share_clients.get(str(raw_sb_id)) or share_clients.get(int(raw_sb_id) if str(raw_sb_id).isdigit() else raw_sb_id)

                if not sb_client:
                    logger.error(f"[LiveBatch {job_id}] Share bot client not found for ID={raw_sb_id}. Available: {list(share_clients.keys())}")
                    await asyncio.sleep(30)
                    continue
                
                if not src_client or not getattr(src_client, "is_connected", False):
                    acc_id = job.get("account_id", "bot")
                    if not acc_id or acc_id == "bot":
                        src_client = BOT_INSTANCE
                    else:
                        bots = await db.get_bots(job["user_id"])
                        acc_bot = next((b for b in bots if str(b.get("id")) == str(acc_id)), None)
                        if acc_bot and not acc_bot.get("is_bot", True):
                            ub_sess = acc_bot["session"]
                            src_client = _FACTORY().client({"session": ub_sess}, False)
                            try:
                                await src_client.connect()
                            except Exception as e:
                                logger.error(f"Live Batch: Failed to connect user account: {e}")
                                src_client = None
                                await asyncio.sleep(60)
                                continue
                        else:
                            src_client = BOT_INSTANCE

                for c in (src_client, sb_client):
                    if c:
                        try: await c.get_chat(source)
                        except: pass
                        try: await c.get_chat(target)
                        except: pass

                prog_id = job.get("prog_id")
                if not prog_id:
                    try:
                        p = await sb_client.send_message(target, 
                            f"📡 <b>Bᴀᴛᴄʜ Lɪɴᴋs Lɪᴠᴇ Aᴜᴛᴏ-Gᴇɴᴇʀᴀᴛᴏʀ</b>\n\n"
                            f"✅ Auto-Generated Blocks: <code>{fwd_count}</code>\n"
                            f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                            f"<i>This message updates every 60s. Arya Bot</i>",
                            reply_to_message_id=job.get('target_topic_id')
                        )
                        prog_id = p.id
                        await _lb_update_job(job_id, {"prog_id": prog_id})
                        try: await sb_client.pin_chat_message(target, prog_id, disable_notification=True)
                        except: pass
                    except Exception as pe:
                        logger.error(f"Live Batch progress msg error: {pe}")

                msgs = []
                is_topic = job.get("is_topic")
                topic_id = job.get("topic_id")

                try:
                    if is_topic and topic_id:
                        try:
                            all_replies = []
                            async for m in src_client.get_discussion_replies(source, topic_id):
                                if m.id <= last_seen: 
                                    break
                                all_replies.append(m)
                                if len(all_replies) >= 50: break
                            
                            msgs = sorted(all_replies, key=lambda x: x.id)
                        except Exception as te:
                            logger.error(f"Live Batch Topic Scan Error: {te}")
                            msgs = []
                    else:
                        batch_req = list(range(last_seen + 1, last_seen + 101))
                        msgs = await src_client.get_messages(source, batch_req)
                        if not isinstance(msgs, list): msgs = [msgs]

                except Exception as e:
                    logger.error(f"Live Batch Scan Error: {e}")
                    await asyncio.sleep(20)
                    continue

                valid = []
                for m in msgs:
                    if m and not getattr(m, 'empty', True) and not getattr(m, 'service', False):
                        has_media = bool(getattr(m, 'audio', None) or getattr(m, 'document', None) or getattr(m, 'video', None) or getattr(m, 'voice', None) or getattr(m, 'photo', None))
                        if has_media:
                            valid.append(m)
                valid.sort(key=lambda m: m.id)
                
                raw_exists = [m for m in msgs if m and not getattr(m, 'empty', True)]
                
                if not raw_exists:
                    try:
                        probe = await src_client.get_messages(source, [last_seen + 250, last_seen + 500, last_seen + 1000])
                        if isinstance(probe, list) and any(p for p in probe if p and not getattr(p, 'empty', True)):
                            last_seen += 200
                            await _lb_update_job(job_id, {"last_seen_id": last_seen})
                    except: pass
                else:
                    existing_buf = set(buffer_mids)
                    new_added = 0
                    
                    use_dup_check = job.get("duplicate_handling") == "yes"
                    target_ch_int = int(job["target"])
                    story_name = job["story"]

                    for m in valid:
                        if m.id in existing_buf:
                            continue

                        if use_dup_check:
                            media_obj = getattr(m, 'document', None) or getattr(m, 'audio', None) or getattr(m, 'video', None) or getattr(m, 'voice', None) or getattr(m, 'photo', None)
                            f_uid = getattr(media_obj, "file_unique_id", None)
                            _fn = getattr(media_obj, "file_name", "") or ""
                            _t = getattr(media_obj, "title", "") or ""
                            cap = m.caption or ""
                            fname = f"{_t} - {_fn}" if _t else _fn
                            if not fname.strip(): fname = cap

                            if f_uid:
                                uid_dup = await db.db["live_batch_seen"].find_one({"job_id": job_id, "file_uid": f_uid})
                                if uid_dup:
                                    logger.info(f"[LiveBatch {job_id}] Skipping same-file re-upload (file_unique_id={f_uid})")
                                    last_seen = max(last_seen, m.id)
                                    continue
                                await db.db["live_batch_seen"].update_one(
                                    {"job_id": job_id, "file_uid": f_uid},
                                    {"$set": {"file_uid": f_uid, "msg_id": m.id, "fname": fname, "at": time.time()}},
                                    upsert=True
                                )

                            ep_res = extract_ep_label_robust(fname)
                            incoming_nums = ep_res.get("numbers", [])
                            if incoming_nums:
                                already_posted = await db.db["live_batch_posted_eps"].find_one({
                                    "target": target_ch_int,
                                    "story": story_name,
                                    "nums": {"$in": incoming_nums}
                                })
                                if already_posted:
                                    logger.info(f"[LiveBatch {job_id}] Skipping episodes {incoming_nums} — already posted to destination")
                                    last_seen = max(last_seen, m.id)
                                    continue

                        buffer_mids.append(m.id)
                        existing_buf.add(m.id)
                        new_added += 1

                    if new_added:
                        logger.info(f"[LiveBatch] Added {new_added} new IDs to buffer. Total: {len(buffer_mids)}")

                    last_seen = max(m.id for m in raw_exists)
                    await _lb_update_job(job_id, {"last_seen_id": last_seen, "buffer_mids": buffer_mids})

                fresh_job = await _lb_get_job(job_id)
                buffer_mids = fresh_job.get("buffer_mids", buffer_mids)
                force = fresh_job.get("force_flush", False)

                if force:
                    await _lb_update_job(job_id, {"force_flush": False})

                if buffer_mids and (len(buffer_mids) >= thresh or force):
                    to_post = buffer_mids if force else buffer_mids[:thresh]
                    while to_post:
                        chunk_ids = to_post[:100]
                        remaining_post = to_post[100:]
                        
                        actual_msgs = await src_client.get_messages(source, chunk_ids)
                        if not isinstance(actual_msgs, list): actual_msgs = [actual_msgs]
                        actual_msgs = [m for m in actual_msgs if m and not m.empty]
                        
                        if not actual_msgs:
                            logger.info(f"[LiveBatch] All {len(chunk_ids)} messages in chunk were deleted or invalid. Removing from buffer.")
                            buffer_mids = [mid for mid in buffer_mids if mid not in chunk_ids]
                            await _lb_update_job(job_id, {"buffer_mids": buffer_mids})
                            to_post = remaining_post if force else (buffer_mids[:thresh] if len(buffer_mids) >= thresh else [])
                            continue
                        
                        res = await _post_live_batch(sb_client, job, actual_msgs)
                        success = res[0] if isinstance(res, tuple) else res
                        
                        if success:
                            new_mids = res[1] if isinstance(res, tuple) else []
                            upd_btns = res[2] if isinstance(res, tuple) else []
                            
                            fwd_count += len(chunk_ids)
                            buffer_mids = [mid for mid in buffer_mids if mid not in chunk_ids]
                            
                            update_dict = {"buffer_mids": buffer_mids, "forwarded": fwd_count}
                            if new_mids: update_dict["posted_mids"] = new_mids
                            if upd_btns: update_dict["all_buttons"] = upd_btns
                            
                            await _lb_update_job(job_id, update_dict)
                            job = await _lb_get_job(job_id)
                            logger.info(f"[LiveBatch] Posted batch of {len(chunk_ids)} files. Buffer remaining: {len(buffer_mids)}")
                        else:
                            logger.warning(f"[LiveBatch] Post failed, will retry next cycle.")
                            break
                        
                        to_post = remaining_post if force else (buffer_mids[:thresh] if len(buffer_mids) >= thresh else [])

                now_t = time.time()
                up_time = job.get("last_prog_update", 0)
                if prog_id and (now_t - up_time) > 60:
                    try:
                        await sb_client.edit_message_text(target, prog_id,
                            f"📡 <b>Bᴀᴛᴄʜ Lɪɴᴋs Lɪᴠᴇ Aᴜᴛᴏ-Gᴇɴᴇʀᴀᴛᴏʀ</b>\n\n"
                            f"✅ Auto-Generated Blocks: <code>{fwd_count}</code>\n"
                            f"»  Last updated: <code>{time.strftime('%H:%M:%S')}</code>\n\n"
                            f"<i>This message updates every 60s. Arya Bot</i>"
                        )
                        await _lb_update_job(job_id, {"last_prog_update": now_t})
                    except: pass

                await asyncio.sleep(20)

            except asyncio.CancelledError:
                logger.info(f"Live Batch job {job_id} was cancelled.")
                raise
            except Exception as e:
                logger.error(f"Live Batch generic loop error: {e}")
                await asyncio.sleep(20)

    finally:
        logger.info(f"Stopping Live Batch job {job_id}")
        _lb_tasks.pop(job_id, None)
        if src_client and src_client is not BOT_INSTANCE:
            try: await src_client.disconnect()
            except: pass

# ─────────────────────────────────────────────────────────────────────────────
# Change-Source flow (runs as a background task so the callback returns fast)
# ─────────────────────────────────────────────────────────────────────────────
async def _lb_do_change_source(bot, uid: int, jid: str):
    """
    Interactive flow to change the source chat of a Live Batch job without
    recreating it.  Pauses the job during selection, then resumes it.
    """
    from pyrogram.types import ReplyKeyboardRemove
    from plugins.utils import ask_channel_picker, check_chat_protection

    job = await _lb_get_job(jid)
    if not job:
        await bot.send_message(uid, "<b>❌ Job not found.</b>")
        return

    # ── Pause the running task while we change things ──────────────────────
    was_running = job.get("status") == "running"
    if was_running:
        if jid in _lb_paused:
            _lb_paused[jid].clear()
        await _lb_update_job(jid, {"status": "paused"})

    await bot.send_message(
        uid,
        "<b>✏️ Change Live Job Source</b>\n\n"
        "Select a new source from your saved channels, or tap "
        "<b>✍️ Manual Input</b> to paste a chat ID / topic link directly.\n\n"
        "<i>The job will pause during selection and auto-resume once updated.</i>",
        reply_markup=__import__('pyrogram.types', fromlist=['ReplyKeyboardMarkup'])
            .__class__  # dummy — we call ask_channel_picker below which sends its own KB
    )

    # Use the shared channel picker first
    picked = await ask_channel_picker(
        bot, uid,
        prompt="Select the new source channel / group:",
        extra_options=["✍️ Manual Input"],
        timeout=300
    )

    new_source = None
    new_source_title = None

    if picked is None:
        # User cancelled
        pass
    elif picked == "✍️ Manual Input":
        # User wants to type a raw chat ID, @username, or topic URL
        try:
            from pyrogram.types import ReplyKeyboardRemove
            ask_msg = await bot.ask(
                uid,
                "✍️ <b>Enter the source:</b>\n\n"
                "Accepted formats:\n"
                "• Numeric chat ID: <code>-1001234567890</code>\n"
                "• @username: <code>@mychannel</code>\n"
                "• Topic URL: <code>https://t.me/c/1234567890/5</code> or "
                "<code>https://t.me/mychannel/5</code>\n"
                "• Group invite link: <code>https://t.me/+XXXXXX</code>\n\n"
                "<i>Send ⛔ to cancel.</i>",
                timeout=300,
                reply_markup=ReplyKeyboardRemove()
            )
            text = (ask_msg.text or "").strip()
            if not text or "⛔" in text or text.lower() == "cancel":
                await bot.send_message(uid, "<i>Cancelled.</i>")
            else:
                # Parse topic URL like https://t.me/c/1234567/5
                import re as _re
                m = _re.match(r'https?://t\.me/c/(\d+)/(\d+)', text)
                if m:
                    new_source = f"-100{m.group(1)}"
                    new_source_title = f"Topic /c/{m.group(1)}/{m.group(2)}"
                elif _re.match(r'https?://t\.me/([^/]+)/(\d+)', text):
                    mm = _re.match(r'https?://t\.me/([^/]+)/(\d+)', text)
                    new_source = f"@{mm.group(1)}"
                    new_source_title = f"@{mm.group(1)}"
                elif text.lstrip('-').isdigit():
                    new_source = text
                    new_source_title = text
                elif text.startswith('@'):
                    new_source = text
                    new_source_title = text
                else:
                    await bot.send_message(uid, "<b>❌ Unrecognised format. Source not changed.</b>")
        except asyncio.TimeoutError:
            await bot.send_message(uid, "<i>⏱ Timed out. Source not changed.</i>")
    elif isinstance(picked, dict):
        # Came from the channel picker
        new_source = str(picked.get("chat_id", ""))
        new_source_title = picked.get("title", new_source)

    if new_source:
        # ── Protection check ──
        prot = await check_chat_protection(uid, new_source)
        if prot:
            await bot.send_message(uid, prot)
            # Re-resume if it was running before
            if was_running:
                await _lb_update_job(jid, {"status": "running"})
                if jid not in _lb_paused:
                    _lb_paused[jid] = asyncio.Event()
                _lb_paused[jid].set()
                if jid not in _lb_tasks or _lb_tasks[jid].done():
                    _lb_tasks[jid] = asyncio.create_task(_lb_run_job(jid))
            return

        # ── Apply the new source & reset scan position ──
        await _lb_update_job(jid, {
            "source": new_source,
            "last_seen_id": 0,    # restart from the beginning of the new source
            "buffer_mids": [],    # clear stale buffer
        })
        await bot.send_message(
            uid,
            f"<b>✅ Source updated!</b>\n\n"
            f"<b>New Source:</b> <code>{new_source_title}</code>\n"
            f"<b>Scan Position:</b> Reset to 0\n"
            f"<b>Buffer:</b> Cleared\n\n"
            "<i>The job will continue monitoring the new source from the start.</i>"
        )
    else:
        if picked is not None:  # not a clean cancel
            await bot.send_message(uid, "<i>Source unchanged.</i>")

    # ── Resume if job was running before ──────────────────────────────────────
    if was_running:
        await _lb_update_job(jid, {"status": "running"})
        if jid not in _lb_paused:
            _lb_paused[jid] = asyncio.Event()
        _lb_paused[jid].set()
        if jid not in _lb_tasks or _lb_tasks[jid].done():
            _lb_tasks[jid] = asyncio.create_task(_lb_run_job(jid))
        await bot.send_message(uid, "▶️ <b>Job resumed and now monitoring the new source.</b>")


@Client.on_callback_query(filters.regex(r"^lb#(main|setup|view|pause|resume|stop|del|change_src)"))
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

        # ── Change Source button — available when job is running or paused ──
        if st in ("running", "queued", "paused"):
            kb.append([
                InlineKeyboardButton("✏️ Cʜᴀɴɢᴇ Sᴏᴜʀᴄᴇ", callback_data=f"lb#change_src#{jid}")
            ])
        
        buf = len(job.get("buffer_mids", []))
        trgt = job.get("threshold", 10)
        
        if buf > 0 and st in ("running", "queued", "paused"):
            kb.append([InlineKeyboardButton(f"🚀 Fᴏʀᴄᴇ Pᴏsᴛ Nᴏᴡ ({buf} Fɪʟᴇs)", callback_data=f"lb#force_ask#{jid}")])
            
        kb.append([InlineKeyboardButton("🔄 Rᴇғʀᴇsʜ", callback_data=f"lb#view#{jid}")])
        if st in ("completed", "stopped", "failed"):
            kb.append([InlineKeyboardButton("🗑 Dᴇʟᴇᴛᴇ Rᴇᴄᴏʀᴅ", callback_data=f"lb#del#{jid}")])
        kb.append([InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="lb#main")])
        
        src_display = str(job.get("source", "?"))
        dup_st = "✅ Enabled" if job.get("duplicate_handling") == "yes" else "❌ Disabled"
        txt = (
            f"<b>📡 Lɪᴠᴇ Bᴀᴛᴄʜ Sᴛᴀᴛᴜs</b>\n\n"
            f"<b>📖 Sᴛᴏʀʏ:</b> <code>{job.get('story')}</code>\n"
            f"<b>📥 Sᴏᴜʀᴄᴇ:</b> <code>{src_display}</code>\n"
            f"<b>ℹ️ Sᴛᴀᴛᴜs:</b> <code>{st.upper()}</code>\n"
            f"<b>🔄 Dᴜᴘʟɪᴄᴀᴛᴇ Hᴀɴᴅʟɪɴɢ:</b> <code>{dup_st}</code>\n"
            f"<b>🎯 Tʜʀᴇsʜᴏʟᴅ:</b> Wait for {trgt} files\n"
            f"<b>📦 Cᴜʀʀᴇɴᴛ Bᴜғғᴇʀ:</b> <code>{buf} / {trgt}</code>\n"
            f"<b>✅ Tᴏᴛᴀʟ Fᴏʀᴡᴀʀᴅᴇᴅ:</b> <code>{job.get('forwarded', 0)}</code>\n\n"
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
        
    elif action == "force_ask":
        jid = data[2]
        txt = (
            "⚠️ <b>WARNING: FORCE BATCH POST</b>\n\n"
            "You are about to force this batch post before the normal buffer threshold is met.\n\n"
            "<b>Potential Issues:</b>\n"
            "• <b>Spam Rules:</b> Posting smaller batches too rapidly can annoy subscribers and trigger Telegram floodwaits.\n"
            "• <b>Incomplete Batches:</b> Generating a post with fewer episodes than normally expected.\n\n"
            "Are you sure you want to force this post immediately?"
        )
        kb = [
            [InlineKeyboardButton("✅ Yes, Force Post Now", callback_data=f"lb#force#{jid}")],
            [InlineKeyboardButton("⛔ Cancel (Keep Buffer)", callback_data=f"lb#view#{jid}")]
        ]
        return await update.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

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
        # 1. Mark stopped in DB FIRST so the loop exits on its next status check
        await _lb_update_job(jid, {"status": "stopped"})
        # 2. Unblock the pause-event so a sleeping loop wakes up immediately
        if jid in _lb_paused:
            _lb_paused[jid].set()
        # 3. Cancel the asyncio task and wait for it
        task = _lb_tasks.pop(jid, None)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        _lb_paused.pop(jid, None)
        update.data = f"lb#view#{jid}"
        return await _lb_callbacks(bot, update)

    elif action == "del":
        jid = data[2]
        # 1. Mark stopped to make the loop exit cleanly on next iteration
        await _lb_update_job(jid, {"status": "stopped"})
        # 2. Unblock any paused wait
        if jid in _lb_paused:
            _lb_paused[jid].set()
        # 3. Cancel + wait with a hard timeout so UI never hangs
        task = _lb_tasks.pop(jid, None)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        _lb_paused.pop(jid, None)
        # 4. Delete from DB
        await _lb_delete_job(jid)
        update.data = "lb#main"
        return await _lb_callbacks(bot, update)

    elif action == "change_src":
        jid = data[2]
        await update.answer("Opening source change wizard…")
        asyncio.create_task(_lb_do_change_source(bot, uid, jid))

async def resume_live_batches():
    jobs = []
    async for j in db.db[COLL].find({"status": "running"}):
        jobs.append(j)
    for j in jobs:
        jid = j["job_id"]
        if jid in _lb_tasks and not _lb_tasks[jid].done():
            continue  # Already running
            
        _lb_paused[jid] = asyncio.Event()
        _lb_paused[jid].set()
        _lb_tasks[jid] = asyncio.create_task(_lb_run_job(jid))
        logger.info(f"[LiveBatch] Resumed job {jid}")
