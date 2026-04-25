"""
Share Batch Links Automator
===========================
Generates File-Sharing deep links from a hidden database channel
and automatically posts the grouped batch buttons into a Public Channel.
"""
import uuid
import math
import asyncio
import logging
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
)
from database import db
from plugins.test import CLIENT

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

# ── Self-contained Future-based ask() — avoids cross-module routing conflicts ──
_sj_waiting: dict[int, asyncio.Future] = {}

@Client.on_message(filters.private, group=-14)
async def _sj_input_router(bot, message):
    """Route private messages to share_jobs _ask() futures."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _sj_waiting:
        fut = _sj_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation

async def _ask(bot, user_id: int, text: str, reply_markup=None, timeout: int = 300):
    """Send text and wait for the next private message from user_id."""
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    old = _sj_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _sj_waiting[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _sj_waiting.pop(user_id, None)
        raise

def _sc(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
    ))

new_share_job = {}

async def _create_share_flow(bot, user_id, force_live=False):
    try:
        new_share_job[user_id] = {}
        share_bots = await db.get_share_bots()
        
        if not share_bots:
            return await bot.send_message(user_id, "<b>‣  No Share Bots available. Please add a Bot Token in /settings -> Share Bots.</b>")
            
        kb = []
        for b in share_bots:
            kb.append([f"{b['name']} (@{b['username']})"])
            
        kb.append(["⛔ Cᴀɴᴄᴇʟ"])
        kb.append(["Scan Database Channel"])
        
        msg = await _ask(bot, user_id, 
            "<b>❪ SHARE LINKS: SELECT ACCOUNT ❫</b>\n\nChoose the Share Bot you want to use for link generation and delivery:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or (getattr(msg, 'text', None) and any(x in msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔'])) or "⛔" in msg.text or "Cᴀɴᴄᴇʟ" in msg.text:
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

        #  Scan option 
        if "Scan Database" in msg.text:
            await bot.send_message(user_id, "<b>»  Opening Database Scanner...</b>", reply_markup=ReplyKeyboardRemove())
            from plugins.db_scanner import _scan_flow
            return await _scan_flow(bot, user_id)

        # Match bot selection
        import re
        sel = msg.text
        match = re.search(r"@([a-zA-Z0-9_]+)", sel)
        if not match:
            return await bot.send_message(user_id, "<b>‣  Invalid selection.</b>", reply_markup=ReplyKeyboardRemove())

            
        username = match.group(1)
        selected_bot = next((b for b in share_bots if b['username'] == username), None)
        if not selected_bot:
            return await bot.send_message(user_id, "<b>‣  Account not found.</b>", reply_markup=ReplyKeyboardRemove())
            
        new_share_job[user_id]['bot_id'] = selected_bot['id']

        from plugins.utils import ask_channel_picker
        
        picked = await ask_channel_picker(bot, user_id, "<b>❪ STEP 2: SOURCE DATABASE ❫</b>\n\nWhere are the files stored securely?")
        if not picked: return
        new_share_job[user_id]['source'] = int(picked['chat_id'])
        
        picked = await ask_channel_picker(bot, user_id, "<b>❪ STEP 3: TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?", extra_options=["↩️ Uɴᴅᴏ"])
        if not picked: return
        if picked == "↩️ Uɴᴅᴏ":
            picked = await ask_channel_picker(bot, user_id, "<b>❪ STEP 2 (REDO): SOURCE DATABASE ❫</b>\n\nWhere are the files stored?")
            if not picked: return
            new_share_job[user_id]['source'] = int(picked['chat_id'])
            
            picked = await ask_channel_picker(bot, user_id, "<b>❪ STEP 3: TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?")
            if not picked: return
            
        new_share_job[user_id]['target'] = int(picked['chat_id'])

        # STEP 3.5: Target Group Topic
        markup_tt = ReplyKeyboardMarkup([["Skip"], ["↩️ Uɴᴅᴏ", "⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True)
        msg_tt = await _ask(bot, user_id,
            "<b>❪ STEP 3.5: TARGET GROUP TOPIC ❫</b>\n\nIf the destination is a Group with Topics enabled, please send the <b>Topic ID</b> (a number). Otherwise, just press <b>Skip</b>.\n\n<i>(To find it, copy a message link from the topic. The middle number is the Topic ID. e.g. /c/1234/<b>56</b>/78)</i>",
            reply_markup=markup_tt
        )
        if getattr(msg_tt, 'text', None) and any(x in msg_tt.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_tt, "text", None) and any(x in msg_tt.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
            # Go back to Step 3
            msg3 = await _ask(bot, user_id, 
                "<b>❪ STEP 3 (REDO): TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?", 
                reply_markup=ReplyKeyboardMarkup(ch_kb + [["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if not msg3.text or (getattr(msg3, "text", None) and any(x in msg3.text.lower() for x in ["cancel", "cᴀɴᴄᴇʟ", "⛔", "/cancel"])): 
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            title3 = msg3.text.replace("»  ", "").strip()
            ch3 = next((c for c in chans if c["title"] == title3), None)
            if ch3:
                new_share_job[user_id]['target'] = int(ch3['chat_id'])
            # re-ask topic
            msg_tt = await _ask(bot, user_id,
                "<b>❪ STEP 3.5: TARGET GROUP TOPIC ❫</b>\n\nIf the destination is a Group with Topics enabled, please send the <b>Topic ID</b>. Otherwise, press <b>Skip</b>.",
                reply_markup=markup_tt
            )
            if getattr(msg_tt, 'text', None) and any(x in msg_tt.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

        tt_text = (msg_tt.text or msg_tt.caption or "").strip()
        if tt_text.lower() == "skip" or not tt_text.isdigit():
            new_share_job[user_id]['target_topic_id'] = None
        else:
            new_share_job[user_id]['target_topic_id'] = int(tt_text)

        markup = ReplyKeyboardMarkup([[KeyboardButton("↩️ Uɴᴅᴏ"), KeyboardButton("⛔ Cᴀɴᴄᴇʟ")]], resize_keyboard=True, one_time_keyboard=True)
            
        def parse_id(msg) -> int:
            if getattr(msg, 'forward_from_message_id', None):
                return msg.forward_from_message_id
                
            text = (msg.text or msg.caption or "").strip().rstrip('/')
            if text.isdigit(): return int(text)
            if "t.me/" in text:
                parts = text.split('/')
                if parts[-1].isdigit(): return int(parts[-1])
            raise ValueError("Invalid Message ID or Link (must be forwarded or contain ID)")
            
        markup_status = ReplyKeyboardMarkup([["»  Completed", "»  Ongoing"], ["↩️ Uɴᴅᴏ", "⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True)
        msg_status = await _ask(bot, user_id, 
            "<b>❪ STEP 4: STORY STATUS ❫</b>\n\nIs this story Completed or Ongoing?", 
            reply_markup=markup_status
        )
        if getattr(msg_status, 'text', None) and any(x in msg_status.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_status, "text", None) and any(x in msg_status.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
            # Go back to Step 3
            msg3 = await _ask(bot, user_id,
                "<b>❪ STEP 3 (REDO): TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?",
                reply_markup=ReplyKeyboardMarkup(ch_kb + [["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if not msg3.text or (getattr(msg3, "text", None) and any(x in msg3.text.lower() for x in ["cancel", "cᴀɴᴄᴇʟ", "⛔", "/cancel"])): 
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            title3 = msg3.text.replace("»  ", "").strip()
            ch3 = next((c for c in chans if c["title"] == title3), None)
            if ch3:
                new_share_job[user_id]['target'] = int(ch3['chat_id'])
            msg_status = await _ask(bot, user_id,
                "<b>❪ STEP 4: STORY STATUS ❫</b>\n\nIs this story Completed or Ongoing?",
                reply_markup=markup_status
            )
            if getattr(msg_status, 'text', None) and any(x in msg_status.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        is_completed = "completed" in (msg_status.text or "").lower()
        new_share_job[user_id]['is_completed'] = is_completed

        msg_story = await _ask(bot, user_id, 
            "<b>❪ STEP 5: STORY NAME ❫</b>\n\nEnter the clean name of the Series/Story (e.g. <code>TDMB</code>):", 
            reply_markup=markup
        )
        if getattr(msg_story, 'text', None) and any(x in msg_story.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_story, "text", None) and any(x in msg_story.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
            # Re-ask Step 4
            msg_status2 = await _ask(bot, user_id,
                "<b>❪ STEP 4 (REDO): STORY STATUS ❫</b>\n\nIs this story Completed or Ongoing?",
                reply_markup=markup_status
            )
            if getattr(msg_status2, 'text', None) and any(x in msg_status2.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            new_share_job[user_id]['is_completed'] = "completed" in (msg_status2.text or "").lower()
            msg_story = await _ask(bot, user_id,
                "<b>❪ STEP 5: STORY NAME ❫</b>\n\nEnter the clean name of the Series/Story (e.g. <code>TDMB</code>):",
                reply_markup=markup
            )
            if getattr(msg_story, 'text', None) and any(x in msg_story.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        new_share_job[user_id]['story'] = (msg_story.text or msg_story.caption or "").strip()
        
        markup_source = ReplyKeyboardMarkup([["»  Regular Channel", "»  Group Topic"], ["↩️ Uɴᴅᴏ", "⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True)
        msg_stype = await _ask(bot, user_id, 
            "<b>❪ STEP 6: SOURCE STRUCTURE ❫</b>\n\nAre the files in a normal Channel (requires start/end IDs)\nor inside a specific Group Topic (auto-scans entire topic)?", 
            reply_markup=markup_source
        )
        if getattr(msg_stype, 'text', None) and any(x in msg_stype.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_stype, "text", None) and any(x in msg_stype.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
            # Re-ask story name
            msg_story2 = await _ask(bot, user_id,
                "<b>❪ STEP 5 (REDO): STORY NAME ❫</b>\n\nEnter story name:",
                reply_markup=markup
            )
            if getattr(msg_story2, 'text', None) and any(x in msg_story2.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            new_share_job[user_id]['story'] = (msg_story2.text or "").strip()
            msg_stype = await _ask(bot, user_id,
                "<b>❪ STEP 6: SOURCE STRUCTURE ❫</b>\n\nChannel or Group Topic?",
                reply_markup=markup_source
            )
            if getattr(msg_stype, 'text', None) and any(x in msg_stype.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        is_topic = "topic" in (msg_stype.text or "").lower()
        new_share_job[user_id]['is_topic'] = is_topic

        #  STEP 6.5: SELECT ACCOUNT 
        if is_topic:
            accounts = await db.get_bots(user_id)
            if not accounts:
                return await bot.send_message(user_id, "<b>❌ No accounts found. Add one in /settings → Accounts first.</b>")
                
            userbots = [a for a in accounts if not a.get("is_bot", True)]
            if not userbots:
                return await bot.send_message(user_id, "<b>❌ You selected 'Group Topic', but you have no Userbot added!</b>\nBots cannot scan Group Topics. Please go to /settings → Accounts and add a Userbot first.")
            valid_accounts = userbots
                
            acc_kb = [[KeyboardButton(f"»  Userbot: {a.get('name', '?')}")] for a in valid_accounts]
            acc_kb.append([KeyboardButton("↩️ Uɴᴅᴏ"), KeyboardButton("⛔ Cᴀɴᴄᴇʟ")])
            
            msg_acc = await _ask(bot, user_id,
                "<b>❪ STEP 6.5: SCANNING ACCOUNT ❫</b>\n\nChoose the Userbot to use for reading files from the Group Topic:\n"
                "<i>(⚠️ NOTE: Group Topics MUST be scanned by a Userbot.)</i>",
                reply_markup=ReplyKeyboardMarkup(acc_kb, resize_keyboard=True, one_time_keyboard=True)
            )
            if not msg_acc.text or (getattr(msg_acc, "text", None) and any(x in msg_acc.text.lower() for x in ["cancel", "cᴀɴᴄᴇʟ", "⛔", "/cancel"])): 
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if getattr(msg_acc, "text", None) and any(x in msg_acc.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
                # Re-ask source structure
                msg_stype2 = await _ask(bot, user_id,
                    "<b>❪ STEP 6 (REDO): SOURCE STRUCTURE ❫</b>\n\nChannel or Group Topic?",
                    reply_markup=markup_source
                )
                if getattr(msg_stype2, 'text', None) and any(x in msg_stype2.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                is_topic = "topic" in (msg_stype2.text or "").lower()
                new_share_job[user_id]['is_topic'] = is_topic
                if not is_topic:
                    new_share_job[user_id]['account_id'] = None
                else:
                    msg_acc = await _ask(bot, user_id,
                        "<b>❪ STEP 6.5: SCANNING ACCOUNT ❫</b>\n\nChoose Userbot:",
                        reply_markup=ReplyKeyboardMarkup(acc_kb, resize_keyboard=True, one_time_keyboard=True)
                    )
                    if not msg_acc.text or (getattr(msg_acc, "text", None) and any(x in msg_acc.text.lower() for x in ["cancel", "cᴀɴᴄᴇʟ", "⛔", "/cancel"])): 
                        return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

            if is_topic:
                acc_name = msg_acc.text.split(": ", 1)[-1].strip()
                sel_acc = next((a for a in valid_accounts if a.get("name") == acc_name), None)
                if not sel_acc:
                    return await bot.send_message(user_id, "<b>‣ Account not found.</b>", reply_markup=ReplyKeyboardRemove())
                new_share_job[user_id]['account_id'] = sel_acc['id']
        else:
            new_share_job[user_id]['account_id'] = None  # Default to Main Bot for normal channels.

        if is_topic:
            msg_topic = await _ask(bot, user_id, 
                "<b>❪ STEP 7: GROUP TOPIC LINK ❫</b>\n\nPaste the link to the Topic (e.g. <code>https://t.me/c/123/45</code>):", 
                reply_markup=markup
            )
            if getattr(msg_topic, 'text', None) and any(x in msg_topic.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if getattr(msg_topic, "text", None) and any(x in msg_topic.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
                return await bot.send_message(user_id, "<b>‣ Undo: Please restart the Batch Links flow from the menu.</b>", reply_markup=ReplyKeyboardRemove())
            topic_id = parse_id(msg_topic)
            new_share_job[user_id]['topic_id'] = topic_id
            new_share_job[user_id]['start_id'] = topic_id
            new_share_job[user_id]['end_id'] = topic_id
        else:
            msg_start = await _ask(bot, user_id, 
                "<b>❪ STEP 7: START MESSAGE ❫</b>\n\nForward the first message, send its Message ID, or paste its Link (e.g. <code>https://t.me/c/123/456</code>):", 
                reply_markup=markup
            )
            if getattr(msg_start, 'text', None) and any(x in msg_start.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if getattr(msg_start, "text", None) and any(x in msg_start.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
                # Re-ask Step 6
                msg_stype3 = await _ask(bot, user_id,
                    "<b>❪ STEP 6 (REDO): SOURCE STRUCTURE ❫</b>\n\nChannel or Group Topic?",
                    reply_markup=markup_source
                )
                if getattr(msg_stype3, 'text', None) and any(x in msg_stype3.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                new_share_job[user_id]['is_topic'] = "topic" in (msg_stype3.text or "").lower()
                msg_start = await _ask(bot, user_id,
                    "<b>❪ STEP 7: START MESSAGE ❫</b>\n\nForward or paste the first message:",
                    reply_markup=markup
                )
                if getattr(msg_start, 'text', None) and any(x in msg_start.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            start_id = parse_id(msg_start)
            new_share_job[user_id]['start_id'] = start_id
            
            msg_end = await _ask(bot, user_id, 
                "<b>❪ STEP 8: LAST MESSAGE ❫</b>\n\nForward the last message, send its Msg ID, or paste its Link:", 
                reply_markup=markup
            )
            if getattr(msg_end, 'text', None) and any(x in msg_end.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if getattr(msg_end, "text", None) and any(x in msg_end.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
                # Re-ask start_id
                msg_start2 = await _ask(bot, user_id,
                    "<b>❪ STEP 7 (REDO): START MESSAGE ❫</b>\n\nForward or paste the first message:",
                    reply_markup=markup
                )
                if getattr(msg_start2, 'text', None) and any(x in msg_start2.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                start_id = parse_id(msg_start2)
                new_share_job[user_id]['start_id'] = start_id
                msg_end = await _ask(bot, user_id,
                    "<b>❪ STEP 8: LAST MESSAGE ❫</b>\n\nForward or paste the last message:",
                    reply_markup=markup
                )
                if getattr(msg_end, 'text', None) and any(x in msg_end.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            end_id = parse_id(msg_end)
            new_share_job[user_id]['end_id'] = end_id
            
            if start_id > end_id:
                start_id, end_id = end_id, start_id
                new_share_job[user_id]['start_id'] = start_id
                new_share_job[user_id]['end_id'] = end_id
            
        # ── Steps 9, 10, 11 are now asked AFTER the pre-scan inside _build_share_links ──
        # This ensures users see the diagnosis BEFORE configuring batch sizes.
        # Set placeholder defaults for now; _build_share_links will override them.
        new_share_job[user_id].setdefault('batch_size', 20)
        new_share_job[user_id].setdefault('buttons_per_post', 10)
        new_share_job[user_id].setdefault('live_threshold', 0)

        sj = new_share_job[user_id]
        # Send a quick summary before starting the scan
        is_tp = sj.get('is_topic')
        sub_str = f"Source: {sj.get('topic_id', 'N/A')} (Topic)" if is_tp else f"Range: {sj.get('start_id')} → {sj.get('end_id')}"
        notify_msg = await bot.send_message(
            user_id,
            f"<b>»  Starting Channel Scan…</b>\n\n"
            f"<b>Story:</b> {sj['story']}\n"
            f"<b>{sub_str}</b>\n\n"
            f"<i>The Pre-Scan Diagnosis will appear next so you can review missing episodes before configuring button sizes.</i>",
            reply_markup=ReplyKeyboardRemove()
        )
        await _build_share_links(bot, user_id, sj, notify_msg)
            
    except Exception as e:
        await bot.send_message(user_id, f"<b>Error during link setup:</b> {e}", reply_markup=ReplyKeyboardRemove())
    
@Client.on_callback_query(filters.regex(r'^sl#'))
async def sl_callback(bot, query):
    from plugins.owner_utils import is_feature_enabled, is_any_owner, FEATURE_LABELS
    user_id = query.from_user.id
    if not await is_any_owner(user_id) and not await is_feature_enabled("batch_links"):
        return await query.answer(f"🔒 {FEATURE_LABELS['batch_links']} is temporarily disabled by admin.", show_alert=True)
    data = query.data.split('#')
    cmd = data[1]

    if cmd == "start":
        kb = [
            [InlineKeyboardButton("📦 Cᴏᴍᴘʟᴇᴛᴇ Mᴏᴅᴇ (Oɴᴇ-Tɪᴍᴇ)", callback_data="sl#complete")],
            [InlineKeyboardButton("📡 Lɪᴠᴇ Aᴜᴛᴏ-Bᴀᴛᴄʜ (Oɴɢᴏɪɴɢ)", callback_data="lb#main")],
            [InlineKeyboardButton("✖️ Dɪsᴍɪss", callback_data="close_btn")]
        ]
        await query.message.edit_text(
            "<b><u>Bᴀᴛᴄʜ Lɪɴᴋs Sʏsᴛᴇᴍ</u></b>\n\nChoose your link generation mode:\n\n"
            "• <b>Cᴏᴍᴘʟᴇᴛᴇ Mᴏᴅᴇ:</b> Manually select a range to immediately generate Batch Buttons for existing files.\n"
            "• <b>Oɴɢᴏɪɴɢ Lɪᴠᴇ Bᴀᴛᴄʜ:</b> Runs infinitely in the background, bundling and posting new messages as they stream in.",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif cmd == "complete":
        await query.message.delete()
        asyncio.create_task(_create_share_flow(bot, user_id))

    elif cmd == "scan":
        await query.answer()
        try:
            await query.message.delete()
        except Exception:
            pass
        from plugins.db_scanner import _scan_flow
        asyncio.create_task(_scan_flow(bot, user_id))

async def _build_share_links(bot, user_id, sj, info_msg):
    sts = await info_msg.reply_text("<i>»  Initializing share worker...</i>", reply_markup=ReplyKeyboardRemove())

    async def safe_edit(text):
        try:
            await sts.edit_text(text)
        except Exception:
            try:
                await bot.send_message(user_id, text)
            except Exception:
                pass

    try:
        import plugins.share_bot as share_mod
        
        selected_bot_id = sj['bot_id']
        poster = share_mod.share_clients.get(selected_bot_id)
        
        if not poster or not getattr(poster, 'is_initialized', None):
            try:
                await share_mod.start_share_bot()  # reload bots if missing
                poster = share_mod.share_clients.get(selected_bot_id)
            except Exception:
                pass

        if not poster or not getattr(poster, 'is_initialized', None):
            return await safe_edit("‣  Share Bot failed to start or connect. Check settings.")

        bot_usr = poster.me.username

        if sj.get("account_id"):
            await safe_edit("<i>»  Starting scanning client...</i>")
            try:
                from plugins.test import CLIENT, start_clone_bot
                acc = await db.get_bot(user_id, sj.get("account_id"))
                if not acc:
                    return await safe_edit("‣  Scanning Account not found.")
                scanner_client = await start_clone_bot(CLIENT().client(acc))
                # Pre-fetch cache dialogs
                try:
                    await scanner_client.get_chat(sj['source'])
                except:
                    pass
            except Exception as e:
                return await safe_edit(f"‣  Failed to start scanning account: {e}")
        else:
            scanner_client = bot

        await safe_edit("<i>»  Scanning database channel and generating links...</i>")

        # ===== DEFINITIVE CHANNEL_INVALID FIX =====
        # The Share Bot uses in_memory=True; it has ZERO peer cache after every restart.
        # SOLUTION: Use the MAIN BOT (which has a persistent SQLite session + is admin)
        # to resolve the InputPeerChannel, then invoke channels.GetMessages on the raw layer
        # of the SCANNING CLIENT directly — we never ask the Share Bot (worker) to touch the DB channel.
        # The Share Bot is only used for POSTING to the public target channel and for
        # DELIVERING files to users (it IS admin there by the user's configuration).
        from pyrogram.raw.functions.channels import GetMessages as ChannelGetMessages
        from pyrogram.raw.types import InputMessageID, InputPeerChannel

        source_chat_id = sj['source']

        # Step 1: Resolve the database channel peer using the SCANNING CLIENT (always works)
        try:
            db_peer = await scanner_client.resolve_peer(source_chat_id)
        except Exception as e:
            return await safe_edit(
                f"<b>‣  Cannot Access Database Channel</b>\n\n"
                f"<code>{e}</code>\n\n"
                f"The scanning account must be a member or admin in the hidden database channel."
            )

        # Inject TARGET CHANNEL peer into poster so userbots don't get CHANNEL_INVALID
        target_chat_id = sj['target']
        try:
            from pyrogram.raw.types import InputPeerChannel as _IPC
            _tpeer = await bot.resolve_peer(target_chat_id)
            if isinstance(_tpeer, _IPC):
                await poster.storage.update_peers([(_tpeer.channel_id, _tpeer.access_hash, 'channel', None, None)])
        except Exception:
            pass  # non-fatal

        # Save db channel access_hash for delivery-time peer injection in the Share Bot
        db_access_hash   = db_peer.access_hash if hasattr(db_peer, 'access_hash') else 0
        protect          = await db.get_share_protect_global()
        buttons_per_post = sj.get('buttons_per_post', 10)


        source_chat_id = sj['source']
        current_id     = sj['start_id']
        end_ep         = sj['end_id']
        batch_size     = sj['batch_size']
        story          = sj['story']
        SCAN_CHUNK     = 100  # Telegram allows up to 100 IDs per GetMessages call

        import re as _re
        all_valid_msgs = []
        total_scanned = 0

        from plugins.utils import format_tg_error

        if sj.get('is_topic'):
            await safe_edit(f"<i>»  Scanning entire Group Topic {sj['topic_id']}...</i>")
            while True:
                try:
                    # Iterate all messages inside the topic
                    async for m in scanner_client.get_discussion_replies(sj['source'], sj['topic_id']):
                        if m and not m.empty:
                            all_valid_msgs.append(m)
                        total_scanned += 1
                        if total_scanned % 100 == 0:
                            try: await safe_edit(f"<i>»  Scanned {total_scanned} files from topic...</i>")
                            except: pass
                    # get_discussion_replies yields newest to oldest by default, so reverse it
                    all_valid_msgs.reverse()
                    break
                except Exception as e:
                    err_msg = format_tg_error(e, "Topic Scan Error")
                    await safe_edit(f"{err_msg}\n\n<i>Waiting for your response...</i>")
                    try:
                        ask_res = await _ask(bot, user_id,
                            f"{err_msg}\n\n<i>Fix the issue (e.g. ensure bot is Admin), then send 🔄 Retry:</i>",
                            reply_markup=ReplyKeyboardMarkup([["🔄 Retry Scan"], ["⛔ Cancel Process"]], resize_keyboard=True),
                            timeout=600)
                        if not ask_res or not ask_res.text or any(x in ask_res.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
                            await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                            return
                        try: await ask_res.delete()
                        except: pass
                    except asyncio.TimeoutError:
                        return await safe_edit("<b>‣ Scan Error:</b> Timed out waiting for retry.")

        else:
            await safe_edit(f"<i>»  Scanning and analyzing files {current_id}–{end_ep}...</i>")
            while current_id <= end_ep:
                chunk_end = min(current_id + SCAN_CHUNK - 1, end_ep)
                msg_ids   = list(range(current_id, chunk_end + 1))

                while True:
                    try:
                        msgs = await scanner_client.get_messages(sj['source'], msg_ids)
                        if not isinstance(msgs, list): msgs = [msgs]
                        
                        for m in msgs:
                            if m and not m.empty:
                                all_valid_msgs.append(m)
                        break
                    except Exception as e:
                        err_str = str(e)
                        if "FLOOD_WAIT" in err_str or "420" in err_str:
                            mw = _re.search(r'wait of (\d+)', err_str)
                            wait_secs = (int(mw.group(1)) + 2) if mw else 15
                            await safe_edit(f"<i>»  Flood Wait {wait_secs}s... (scanned {total_scanned})</i>")
                            await asyncio.sleep(wait_secs)
                            continue
                        
                        err_msg = format_tg_error(e, "Scan Error")
                        await safe_edit(f"{err_msg}\n\n<i>Waiting for your response...</i>")
                        try:
                            ask_res = await _ask(bot, user_id,
                                f"{err_msg}\n\n<i>Fix the issue (e.g. ensure bot/clone is Admin), then send 🔄 Retry:</i>",
                                reply_markup=ReplyKeyboardMarkup([["🔄 Retry Scan"], ["⛔ Cancel Process"]], resize_keyboard=True),
                                timeout=600)
                            if not ask_res or not ask_res.text or any(x in ask_res.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
                                await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                                return
                            try: await ask_res.delete()
                            except: pass
                            continue  # retry the scan!
                        except asyncio.TimeoutError:
                            return await safe_edit("<b>‣ Scan Error:</b> Timed out waiting for retry.")
                
                total_scanned += len(msg_ids)
                current_id = chunk_end + 1
                await asyncio.sleep(0.3)

        if not all_valid_msgs:
            return await safe_edit("‣  No files found in that range.")

        all_valid_msgs.sort(key=lambda x: x.id)  # chronological

        #  Episode extraction helpers (Priority: file_name > caption > audio_title) 

        _NOISE_RE = [
            # Resolutions MUST have p/i suffix
            _re.compile(r'(?i)\b(?:360|480|720|1080|2160|4k)[pi]\b'),
            # Codec/format labels
            _re.compile(r'(?i)\b(?:x264|x265|h\.?264|h\.?265|hevc|avc|aac|mp[34]|m4a|m4v|m4b|mkv|avi|mov|wmv|flv|flac|opus|ogg|wav|webm|3gp|mts|m2ts)\b'),
            # Filename Date/Time Encampments (Blocks auto-generated device timestamps from being seen as Ep 2025)
            _re.compile(r'(?i)(?:record|screenrecorder|vid|aud|voice|audio|img|pic|screenshot)[-_.0-9a-zA-Z]*\d{4}[-_.0-9]*'),
            _re.compile(r'(?i)\b20\d{2}[-_. ]?\d{2}[-_. ]?\d{2}[-_.0-9]*'),
            # File sizes
            _re.compile(r'(?i)\b\d+(?:\.\d+)?\s*(?:mb|gb|kb)\b'),
            # Track/season-episode labels like S01E05
            _re.compile(r'(?i)\b(?:s[0-9]{1,2}e[0-9]{1,2})(?=\s|$)'),
            # Common text noise
            _re.compile(r'(?i)\b(?:copy|final|v\d+|new|latest|audio|track)\b'),
        ]

        def _clean(text: str) -> str:
            for rx in _NOISE_RE:
                text = rx.sub(' ', text)
            # Normalize common delimiters to spaces to break words apart
            text = _re.sub(r'[_#\.]', ' ', text)
            return text
            
        def _extract_range_from_text(text: str):
            """
            Unified robust episode extraction.
            Returns (min, max, is_range) or None.
            """
            from plugins.utils import extract_ep_label_robust
            res = extract_ep_label_robust(text)
            nums = res.get("numbers", [])
            if not nums: return None
            return (min(nums), max(nums), res.get("is_range", False))

        def _get_file_names(msg):
            """Collect file_name strings (without extension) from all media attributes."""
            import os as _os
            names = []
            for attr in ("audio", "voice", "document", "video"):
                media = getattr(msg, attr, None)
                if media:
                    fname = getattr(media, "file_name", None)
                    if fname:
                        # Strip extension to prevent '4' in '.m4a' / '3' in '.mp3' etc.
                        # from contaminating the number pool
                        base, _ = _os.path.splitext(str(fname))
                        names.append(base)
            return names

        def _get_audio_title(msg):
            """Get audio title tag (can contain misleading track numbers)."""
            for attr in ("audio", "voice"):
                media = getattr(msg, attr, None)
                if media:
                    t = getattr(media, "title", None)
                    if t: return str(t)
            return ""

        def extract_ep_final(msg):
            """
            Unified robust episode extraction.
            Checks all sources (filename, title, caption) and picks the most descriptive result:
            - Prefers ranges (e.g., 15-20) over single numbers (15).
            - Prefers explicit labels over noisy ones.
            """
            sources = []
            
            # Source A: Combined Audio Title + Filename
            names_str = " ".join(_get_file_names(msg))
            title_str = _get_audio_title(msg)
            combo = f"{title_str} @@@ {names_str}" if title_str else names_str
            if combo.strip():
                sources.append(combo)
            
            # Source B: Caption
            cap = msg.caption or msg.text or ""
            if cap.strip():
                sources.append(cap)

            best_res = (-1, -1, False)
            
            for src in sources:
                res = _extract_range_from_text(src)
                if not res: continue
                
                # Update best logic:
                # 1. If we have nothing yet, take this.
                # 2. If this is a range and previous wasn't, take this.
                # 3. If both are ranges, take the wider one or just stick.
                if best_res[0] == -1:
                    best_res = res
                elif res[2] and not best_res[2]:
                    best_res = res
                elif res[2] and best_res[2]:
                    # Wider range check (optional, but robust)
                    if (res[1] - res[0]) > (best_res[1] - best_res[0]):
                        best_res = res
            
            return best_res

        extract_ep_individual = extract_ep_final
        extract_ep_grouped    = extract_ep_final

        # ══ TWO-PASS PARSING ══════════════════════════════════════════════════
        # PASS 1: Parse all msgs as individual to detect MODE
        pass1 = []
        unparseable_count = 0
        for msg in all_valid_msgs:
            ep_s, ep_e, is_r = extract_ep_individual(msg)
            if ep_s < 1:
                unparseable_count += 1
                continue
            pass1.append((msg, ep_s, ep_e, is_r))

        if not pass1:
            return await safe_edit("‣  Could not extract any episode numbers from the scanned messages.")

        #  DETECT MODE 
        # Check if a significant fraction of files look like ranges ("57-79")
        # Now checks both Captions and Filenames to be robust.
        range_hint_count = 0
        range_rx = r'(?<!\d)\d{1,4}\s*[\-\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE63\uFF0D~～]\s*\d{1,4}(?!\d)'
        for msg in all_valid_msgs:
            found = False
            cap = msg.caption or msg.text or ""
            if _re.search(range_rx, cap):
                found = True
            if not found:
                for fname in _get_file_names(msg):
                    if _re.search(range_rx, fname):
                        found = True; break
            if found: range_hint_count += 1
            
        GROUPED_MODE = range_hint_count > (len(all_valid_msgs) * 0.40) # Lower threshold to 40% to be safer

        # PASS 2: Re-parse with the correct extractor based on mode
        parsed_msgs = []
        unparseable_count = 0
        extractor = extract_ep_grouped if GROUPED_MODE else extract_ep_individual
        for msg in all_valid_msgs:
            ep_s, ep_e, is_r = extractor(msg)
            if ep_s < 1:
                unparseable_count += 1
                continue
            parsed_msgs.append((msg, ep_s, ep_e, is_r))

        if not parsed_msgs:
            return await safe_edit("‣  Could not extract any episode numbers from the scanned messages.")

        # ══ PASS 3: Album-aware gap-fill ═══════════════════════════════════════
        # In a Telegram media group (album), only the FIRST message carries the
        # caption. Subsequent files in the same album often have no file_name /
        # caption and therefore parse as (-1, -1, False). When we can see SOME
        # parsed episodes in an album, we can infer the rest sequentially.
        #
        # Strategy:
        #   • Group every scanned message by media_group_id.
        #   • For each album group that has ≥1 parsed episode AND ≥1 unparseable:
        #       – Collect all parsed (ep_s, msg) pairs, sort by msg.id.
        #       – For each unparseable message (sorted by id), find its position
        #         relative to the parsed ones and assign the next sequential ep.
        #   • Add the inferred entries to parsed_msgs.
        # ── PASS 3: Album-aware gap-fill ──────────────────────────────────────
        # Applied to ALL modes to ensure albums (media groups) are correctly indexed.
        _all_msgs_by_id = {m.id: m for m in all_valid_msgs}
        _album_groups: dict = {}
        for msg in all_valid_msgs:
            gid = getattr(msg, 'media_group_id', None) or getattr(msg, 'group_id', None)
            if gid:
                _album_groups.setdefault(gid, []).append(msg)

        if _album_groups:
            parsed_ids = {m.id for m, _, _, _ in parsed_msgs}
            parsed_ep_map = {m.id: (ep_s, ep_e, is_r) for m, ep_s, ep_e, is_r in parsed_msgs}
            extra_parsed = []

            for gid, grp_msgs in _album_groups.items():
                grp_sorted = sorted(grp_msgs, key=lambda x: x.id)
                grp_parsed  = [(m, *parsed_ep_map[m.id]) for m in grp_sorted if m.id in parsed_ids]
                grp_unparsed = [m for m in grp_sorted if m.id not in parsed_ids]

                if not grp_parsed or not grp_unparsed:
                    continue

                all_sorted_ids = [m.id for m in grp_sorted]
                parsed_ep_by_pos = {m.id: parsed_ep_map[m.id][0] for m, *_ in grp_parsed}

                last_anchor_ep  = None
                last_anchor_idx = None
                for idx, mid in enumerate(all_sorted_ids):
                    if mid in parsed_ep_by_pos:
                        last_anchor_ep  = parsed_ep_by_pos[mid]
                        last_anchor_idx = idx
                    elif mid not in parsed_ids:
                        if last_anchor_ep is not None:
                            offset = idx - last_anchor_idx
                            inferred_ep = last_anchor_ep + offset
                        else:
                            future = [(i, parsed_ep_by_pos[x]) for i, x in enumerate(all_sorted_ids)
                                      if x in parsed_ep_by_pos and i > idx]
                            if future:
                                first_future_idx, first_future_ep = future[0]
                                inferred_ep = first_future_ep - (first_future_idx - idx)
                            else:
                                continue

                        if inferred_ep < 1:
                            continue
                        msg_obj = _all_msgs_by_id[mid]
                        extra_parsed.append((msg_obj, inferred_ep, inferred_ep, False))

            parsed_msgs = parsed_msgs + extra_parsed
            parsed_msgs.sort(key=lambda x: x[0].id)
            unparseable_count = len([m for m in all_valid_msgs if m.id not in {p[0].id for p in parsed_msgs}])

        total_count = len(all_valid_msgs)


        #  Build ep_to_msgs dict and track duplicates 
        ep_to_msgs: dict = {}      # ep_start → [msg_ids]
        duplicate_eps:  list = []  # list of ep numbers with >1 file
        grouped_files:  list = []  # list of "(name, start-end)" for grouped files

        range_msg_ids = set()
        for msg, ep_s, ep_e, is_r in parsed_msgs:
            if is_r:
                range_msg_ids.add(msg.id)
                # Range file — track for report
                range_label = f"{ep_s}\u2013{ep_e}"
                grouped_files.append(range_label)
                if GROUPED_MODE:
                    # Grouped mode: 1 button per file, keyed by ep_s
                    if ep_s not in ep_to_msgs:
                        ep_to_msgs[ep_s] = []
                    if msg.id not in ep_to_msgs[ep_s]:
                        ep_to_msgs[ep_s].append(msg.id)
                else:
                    # Individual mode: EXPAND so every ep in range gets this msg_id
                    for expanded_ep in range(ep_s, min(ep_e + 1, ep_s + 500)):
                        if expanded_ep not in ep_to_msgs:
                            ep_to_msgs[expanded_ep] = []
                        if msg.id not in ep_to_msgs[expanded_ep]:
                            ep_to_msgs[expanded_ep].append(msg.id)
            else:
                if ep_s not in ep_to_msgs:
                    ep_to_msgs[ep_s] = []
                if msg.id not in ep_to_msgs[ep_s]:
                    ep_to_msgs[ep_s].append(msg.id)

        # Identify true duplicates (same ep_num, multiple messages)
        # Exclude generated range overlaps to prevent grouped files from appearing as duplicates
        duplicate_eps = []
        for ep, ids in ep_to_msgs.items():
            non_range_ids = [m for m in ids if m not in range_msg_ids]
            if len(non_range_ids) > 1:
                duplicate_eps.append(ep)
        duplicate_eps = sorted(set(duplicate_eps))


        all_ep_nums    = sorted(ep_to_msgs.keys())
        chrono_eps     = [ep for _, ep, _, _ in parsed_msgs if ep > 0]
        first_ep_num   = chrono_eps[0] if chrono_eps else (all_ep_nums[0] if all_ep_nums else 0)
        last_ep_num    = all_ep_nums[-1] if all_ep_nums else 0
        valid_ep_nums  = [e for e in all_ep_nums if e >= first_ep_num]

        # ── Missing episode detection (ACCURATE 3-tier method) ─────────────────
        missing_eps: list = []
        truly_missing_count: int = 0
        unassigned_count: int = 0

        # Always build present_set by expanding ALL identified ranges/numbers
        # This prevents grouped files from being marked as "missing" in the report
        present_eps_expanded = set()
        for _, ep_s, ep_e, _ in parsed_msgs:
            if ep_s > 0:
                # Expand range min-max into the present set
                for e in range(ep_s, min(ep_e + 1, ep_s + 500)):
                    present_eps_expanded.add(e)

        if present_eps_expanded:
            expected_range = set(range(first_ep_num, last_ep_num + 1))
            raw_missing    = sorted(expected_range - present_eps_expanded)

            # Count files that physically exist but couldn't get an episode label
            added_msg_ids_pre = {m.id for m, _, _, _ in parsed_msgs}
            unassigned_count = len([m for m in all_valid_msgs if m.id not in added_msg_ids_pre])

            # True gaps = raw gaps not covered by unassigned files
            truly_missing_count = max(0, len(raw_missing) - unassigned_count)
            # Only list episode numbers as missing if they exceed our unassigned buffer
            if truly_missing_count > 0:
                missing_eps = raw_missing[unassigned_count:]

        #  BUILD BUCKETS 
        # GROUPED_MODE: each file = 1 button using its own range label
        # INDIVIDUAL_MODE: bucket by batch_size
        buckets = []  # list of (label_start, label_end, [msg_ids])

        if GROUPED_MODE:
            parsed_ids = {m.id for m, _, _, _ in parsed_msgs}
            current_bucket_mids = None
            
            for m in sorted(all_valid_msgs, key=lambda x: x.id):
                if m.id in parsed_ids:
                    p_tuple = next(pt for pt in parsed_msgs if pt[0].id == m.id)
                    ep_s, ep_e = p_tuple[1], p_tuple[2]
                    
                    mids = ep_to_msgs.get(ep_s, [])
                    if mids and mids[0] == m.id:
                        current_bucket_mids = [m.id]
                        buckets.append([ep_s, ep_e, current_bucket_mids])
                    elif current_bucket_mids is not None:
                        current_bucket_mids.append(m.id)
                else:
                    if current_bucket_mids is None:
                        current_bucket_mids = [m.id]
                        buckets.append(["Extra", "Files", current_bucket_mids])
                    else:
                        current_bucket_mids.append(m.id)
        else:
            # Individual mode: dynamic-size buckets using chronological traversal
            msg_to_ep = {m.id: ep for m, ep, _, _ in parsed_msgs}
            msg_to_end = {m.id: ep_e for m, _, ep_e, _ in parsed_msgs}
            
            b_s = None
            b_e = None
            b_mids = []
            pending_unparsed = []

            all_msgs_sorted = sorted(all_valid_msgs, key=lambda x: x.id)

            for m in all_msgs_sorted:
                mid = m.id
                if mid in msg_to_ep:
                    ep = msg_to_ep[mid]
                    math_start = ((ep - 1) // batch_size) * batch_size + 1
                    math_end   = math_start + batch_size - 1
                    
                    if b_s is None:
                        b_s = math_start
                        b_e = math_end
                    elif ep > b_e:
                        if b_mids:
                            buckets.append([b_s, b_e, b_mids])
                        b_s = math_start
                        b_e = math_end
                        b_mids = []

                    # Flush any unparsed messages before this bucket started into this bucket
                    if pending_unparsed:
                        for umid in pending_unparsed:
                            if umid not in b_mids: b_mids.append(umid)
                        pending_unparsed = []

                    if mid not in b_mids:
                        b_mids.append(mid)
                        
                    span_e = msg_to_end.get(mid, ep)
                    if span_e > b_e:
                        b_e = span_e
                else:
                    # Unparseable message -> Embed it natively!
                    if b_s is None:
                        pending_unparsed.append(mid)
                    else:
                        if len(b_mids) >= batch_size:
                            buckets.append([b_s, b_e, b_mids])
                            b_s = b_e + 1
                            b_e = b_s + batch_size - 1
                            b_mids = []
                        if mid not in b_mids:
                            b_mids.append(mid)

            if b_s is not None and b_mids:
                buckets.append([b_s, b_e, b_mids])
            elif pending_unparsed:
                buckets.append(["Extra", "Files", pending_unparsed])
                pending_unparsed = []

            # Cap ONLY last bucket label at actual last ep (cosmetic only, if it has numeric ends)
            if buckets and buckets[-1][0] != "Extra" and last_ep_num:
                last_b = buckets[-1]
                buckets[-1] = (last_b[0], min(last_b[1], last_ep_num), last_b[2])

        #  PRE-SCAN INTERVENTION 
        prescan_report_lines = [
            f"<b>🔍 PRE-SCAN DIAGNOSIS</b>",
            f"\n<blockquote expandable>",
            f"»  <b>Files located:</b> {total_count}",
            f"🎯 <b>Detected Bounds:</b> {first_ep_num}–{last_ep_num}",
        ]
        
        if not GROUPED_MODE:
            if unassigned_count > 0:
                prescan_report_lines.append(f"📎 <b>{unassigned_count} files lack episode labels</b> (Will embed silently inside buttons)")
            if truly_missing_count > 0:
                miss_preview = ", ".join(str(e) for e in missing_eps[:15])
                if len(missing_eps) > 15: miss_preview += f" (+{len(missing_eps)-15} more)"
                prescan_report_lines.append(f"❌ <b>{truly_missing_count} Truly Missing Episodes:</b> {miss_preview}")
            elif unassigned_count == 0:
                prescan_report_lines.append(f"✅ <b>Zero Missing Episodes!</b> All slots correctly found.")
        elif duplicate_eps:
             prescan_report_lines.append(f"‣  <b>Duplicates Detected:</b> {len(duplicate_eps)}")
             
        prescan_report_lines.append(f"</blockquote>")
        prescan_report_lines.append(f"\n<i>Do you want to proceed and generate links for these files?</i>")
        
        try:
            prescan_msg = await _ask(bot, user_id, "\n".join(prescan_report_lines), reply_markup=ReplyKeyboardMarkup([
                ["✅ Proceed & Generate"],
                ["⛔ Cancel Job"]
            ], resize_keyboard=True, one_time_keyboard=True), timeout=1800)
            
            if getattr(prescan_msg, 'text', None) and "Cancel" in prescan_msg.text:
                await bot.send_message(user_id, "<b>❌ Process Cancelled during Pre-Scan.</b>", reply_markup=ReplyKeyboardRemove())
                return await safe_edit("<b>❌ Process Cancelled during Pre-Scan.</b>")
                
            await bot.send_message(user_id, "<i>»  Pre-Scan Accepted!</i>", reply_markup=ReplyKeyboardRemove())
            await safe_edit("<i>»  Pre-Scan Accepted. Now collecting your batch settings...</i>")

            # ── Ask Steps 9, 10, 11 NOW (after scan so user has all context) ──
            from pyrogram.types import ReplyKeyboardMarkup as _RKM, ReplyKeyboardRemove as _RKR

            def _is_cancel(m): return getattr(m, 'text', None) and any(x in (m.text or '').lower() for x in ['cancel', '⛔', '/cancel'])

            # Step 9: Episodes per button
            _m9 = await _ask(bot, user_id,
                "<b>❪ STEP 9: EPISODES PER BUTTON ❫</b>\n\nHow many episodes per link button?\n"
                f"<i>You have {total_count} files detected across {first_ep_num}–{last_ep_num}.</i>\n\nExample: <code>20</code>",
                reply_markup=_RKM([["5", "10", "20"], ["25", "50", "⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if _is_cancel(_m9):
                await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                return await safe_edit("<i>Process Cancelled.</i>")
            _raw9 = (_m9.text or "20").strip()
            sj['batch_size'] = int(_raw9) if _raw9.isdigit() and int(_raw9) > 0 else 20
            batch_size = sj['batch_size']

            # Step 10: Buttons per post
            _m10 = await _ask(bot, user_id,
                "<b>❪ STEP 10: BUTTONS PER POST ❫</b>\n\nHow many buttons per channel post?\nExample: <code>10</code>",
                reply_markup=_RKM([["5", "10", "15"], ["20", "25", "⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if _is_cancel(_m10):
                await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                return await safe_edit("<i>Process Cancelled.</i>")
            _raw10 = (_m10.text or "10").strip()
            sj['buttons_per_post'] = int(_raw10) if _raw10.isdigit() and int(_raw10) > 0 else 10
            buttons_per_post = sj['buttons_per_post']

            # Step 11: Live monitoring (only for non-force-live)
            if not sj.get('live_threshold'):
                _m11 = await _ask(bot, user_id,
                    "<b>❪ STEP 11: LIVE MONITORING ❫</b>\n\nHow many new episodes should trigger auto-posting?\n"
                    "Send <code>0</code> or <code>Skip</code> to disable.\nExample: <code>10</code>",
                    reply_markup=_RKM([["0", "5", "10"], ["15", "25", "⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
                )
                if _is_cancel(_m11):
                    await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                    return await safe_edit("<i>Process Cancelled.</i>")
                _raw11 = (_m11.text or "0").strip()
                sj['live_threshold'] = int(_raw11) if _raw11.isdigit() else 0

            # Step 12: Duplicate Handling (New)
            _m12 = await _ask(bot, user_id,
                "<b>❪ STEP 12: DUPLICATE HANDLING ❫</b>\n\n"
                "Should the bot skip files that already exist in the destination or are repeated in the source?\n\n"
                "• <b>Yes, Skip Duplicates:</b> Identifies and skips repeated episode numbers.\n"
                "• <b>No, Post Everything:</b> Forwards every file regardless of repetitions.",
                reply_markup=_RKM([["✅ Yes, Skip Duplicates"], ["❌ No, Post Everything"], ["⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if _is_cancel(_m12):
                await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                return await safe_edit("<i>Process Cancelled.</i>")
            sj['duplicate_handling'] = "yes" if "yes" in (_m12.text or "").lower() else "no"

            # Step 13: Post Format
            _m13 = await _ask(bot, user_id,
                "<b>❪ STEP 13: POST FORMAT ❫</b>\n\n"
                "What layout should be used when sending the post to the channel?\n\n"
                "• <b>Normal Batch:</b> (Story EPS 1-100)\n"
                "• <b>Missing Episodes:</b> (Special missing episodes template with list)",
                reply_markup=_RKM([["Normal Batch"], ["Missing Episodes Format"], ["⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if _is_cancel(_m13):
                await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                return await safe_edit("<i>Process Cancelled.</i>")
            sj['post_format'] = "missing" if "missing" in (_m13.text or "").lower() else "normal"

            # ── NOW rebuild buckets with the real batch_size from Step 9 ──
            # The initial bucket building used placeholder batch_size=20.
            # We have the real answer now, so rebuild with the correct value.
            batch_size     = sj['batch_size']
            buttons_per_post = sj['buttons_per_post']

            import uuid as _uuid_mod
            # Rebuild buckets
            if GROUPED_MODE:
                buckets_final = buckets  # grouped mode doesn't depend on batch_size
            else:
                msg_to_ep  = {m.id: ep for m, ep, _, _ in parsed_msgs}
                msg_to_end = {m.id: ep_e for m, _, ep_e, _ in parsed_msgs}
                b_s2 = None; b_e2 = None; b_mids2 = []; pending2 = []
                buckets_final = []
                for m in sorted(all_valid_msgs, key=lambda x: x.id):
                    mid = m.id
                    if mid in msg_to_ep:
                        ep = msg_to_ep[mid]
                        math_start2 = ((ep - 1) // batch_size) * batch_size + 1
                        math_end2   = math_start2 + batch_size - 1
                        if b_s2 is None:
                            b_s2 = math_start2; b_e2 = math_end2
                        elif ep > b_e2:
                            if b_mids2: buckets_final.append([b_s2, b_e2, b_mids2])
                            b_s2 = math_start2; b_e2 = math_end2; b_mids2 = []
                        if pending2:
                            for umid2 in pending2:
                                if umid2 not in b_mids2: b_mids2.append(umid2)
                            pending2 = []
                        if mid not in b_mids2: b_mids2.append(mid)
                        span_e2 = msg_to_end.get(mid, ep)
                        if span_e2 > b_e2: b_e2 = span_e2
                    else:
                        if b_s2 is None:
                            pending2.append(mid)
                        else:
                            if len(b_mids2) >= batch_size:
                                buckets_final.append([b_s2, b_e2, b_mids2])
                                b_s2 = b_e2 + 1; b_e2 = b_s2 + batch_size - 1; b_mids2 = []
                            if mid not in b_mids2: b_mids2.append(mid)
                if b_s2 is not None and b_mids2:
                    buckets_final.append([b_s2, b_e2, b_mids2])
                elif pending2:
                    buckets_final.append(["Extra", "Files", pending2])
                if buckets_final and buckets_final[-1][0] != "Extra" and last_ep_num:
                    lb2 = buckets_final[-1]
                    buckets_final[-1] = (lb2[0], min(lb2[1], last_ep_num), lb2[2])

            await bot.send_message(user_id, "<i>»  Generating unique secure links...</i>", reply_markup=_RKR())
            await safe_edit("<i>»  Generating unique secure links...</i>")
        except Exception:
            await bot.send_message(user_id, "<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            return await safe_edit("<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>")

        raw_buttons = []
        # Use the rebuilt buckets (with real batch_size) if available, else fall back
        _buckets_to_use = buckets_final if 'buckets_final' in locals() else buckets
        for b_s, b_e, mids in _buckets_to_use:
            if not mids:
                continue
            uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
            await db.save_share_link(
                uuid_str, mids, source_chat_id,
                protect=protect, access_hash=db_access_hash
            )
            url = f"https://t.me/{bot_usr}?start={uuid_str}"
            btn_text = str(b_s) if (b_s == b_e or batch_size == 1) else f"{b_s}–{b_e}"
            raw_buttons.append({
                "btn":      InlineKeyboardButton(_sc(btn_text), url=url),
                "ep_start": b_s,
                "ep_end":   b_e,
            })

        # Calculate unparseable count for the display report (removed per user request)
        added_msg_ids = set()
        for mids in ep_to_msgs.values():
            added_msg_ids.update(mids)
        unparseable_msgs_list = [m.id for m in all_valid_msgs if m.id not in added_msg_ids]


        #  PHASE 3: Post to target channel 
        post_count = 0
        for i in range(0, len(raw_buttons), buttons_per_post):
            chunk = raw_buttons[i : i + buttons_per_post]
            first_ep = chunk[0]["ep_start"]
            last_ep  = chunk[-1]["ep_end"]
            def _bold_sans(s):
                res = ''
                for c in str(s):
                    if 'A' <= c <= 'Z':
                        res += chr(0x1D5D4 + ord(c) - ord('A'))
                    elif 'a' <= c <= 'z':
                        res += chr(0x1D5D4 + ord(c) - ord('a'))
                    else:
                        res += c
                return res
            
            if sj.get('post_format') == "missing":
                ep_strs = []
                for b_s, b_e, _ in _buckets_to_use:
                    if b_s == "Extra": continue
                    ep_strs.append(f"• {b_s}" if b_s == b_e else f"• {b_s}-{b_e}")
                
                # Split roughly to line wrap nicely
                formatted_eps = ""
                for idx, ev in enumerate(ep_strs):
                    formatted_eps += ev + "  "
                    if (idx + 1) % 4 == 0: formatted_eps += "\n"
                formatted_eps = formatted_eps.strip()

                txt = (
                    f"👉🏻 {_bold_sans(story)} (English) •\n"
                    f"<blockquote expandable>{_bold_sans(story)} Missing Episode\n"
                    f"{formatted_eps}</blockquote>\n"
                    f"<blockquote expandable>Note :\n"
                    f"Comment Below 👇 I'll Add Missing Episodes As Soon As Possible</blockquote>"
                )
            else:
                txt = f"{_bold_sans(story)} 𝗘𝗣𝗦 {first_ep} - {last_ep}"

            keyboard = []
            for j in range(0, len(chunk), 2):
                row = [c["btn"] for c in chunk[j:j + 2]]
                keyboard.append(row)
            keyboard.append([
                InlineKeyboardButton(_sc("tutorial"), url="https://t.me/StoriesLinkopningguide"),
                InlineKeyboardButton(_sc("support"), url="https://t.me/AryaHelpTG")
            ])
            for attempt in range(6):
                try:
                    await poster.send_message(
                        chat_id=sj['target'], text=txt,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        reply_to_message_id=sj.get('target_topic_id')
                    )
                    break
                except Exception as e:
                    err_str = str(e)
                    import re as _re2
                    if "FLOOD_WAIT" in err_str or "420" in err_str:
                        mw = _re2.search(r'wait of (\d+)', err_str)
                        wait_secs = (int(mw.group(1)) + 2) if mw else 35
                        await safe_edit(f"<i>»  Rate limit... waiting {wait_secs}s</i>")
                        await asyncio.sleep(wait_secs)
                        continue
                    else:
                        return await safe_edit(
                            f"<b>‣  Failed to post to target channel:</b> <code>{e}</code>\n\n"
                            f"<i>Make sure the selected account is an admin in the target channel.</i>"
                        )
            else:
                return await safe_edit("‣  Posting aborted after 6 retries due to FloodWait.")
            post_count += 1
            await asyncio.sleep(1)

        #  FINAL REPORT 
        mode_str = "🗂 Grouped files (1 button/file)" if GROUPED_MODE else f"📑 Individual (batch size: {batch_size})"

        report_lines = [
            f"<b>»  Share Links Generated!</b>",
            f"\n<blockquote expandable>",
            f"»  <b>Files processed:</b> {total_count}",
            f"🎯 <b>Episode range:</b> {first_ep_num}–{last_ep_num}",
            f"»  <b>Link buttons created:</b> {len(raw_buttons)}",
            f"»  <b>Posts sent to channel:</b> {post_count}",
            f"»  <b>Mode:</b> {mode_str}",
        ]

        if grouped_files:
            gf_preview = ", ".join(grouped_files[:8])
            if len(grouped_files) > 8:
                gf_preview += f" (+{len(grouped_files)-8} more)"
            report_lines.append(f"🗂 <b>Grouped files ({len(grouped_files)}):</b> {gf_preview}")

        if duplicate_eps:
            dup_preview = ", ".join(str(e) for e in duplicate_eps[:10])
            if len(duplicate_eps) > 10:
                dup_preview += f" (+{len(duplicate_eps)-10} more)"
            report_lines.append(f"‣  <b>Duplicates detected ({len(duplicate_eps)}) — all files kept:</b> {dup_preview}")

        if unassigned_count > 0:
            report_lines.append(
                f"📎 <b>Files with no episode label ({unassigned_count}):</b> "
                f"<i>exist in DB but filename had no episode number — embedded chronologically (NOT missing)</i>"
            )
        if truly_missing_count > 0:
            miss_preview = ", ".join(str(e) for e in missing_eps[:15])
            if len(missing_eps) > 15:
                miss_preview += f" (+{len(missing_eps)-15} more)"
            report_lines.append(
                f"❌ <b>Truly missing episodes ({truly_missing_count}) — not found in DB:</b> {miss_preview}"
            )
        elif unassigned_count == 0:
            report_lines.append(f"✅ <b>No missing episodes</b> — all {last_ep_num - first_ep_num + 1} slots accounted for!")

        report_lines.append("</blockquote>")
        report_lines.append(f"")
        report_lines.append(f"<i>Users click any button to receive their episodes from @{bot_usr}.</i>")

        await safe_edit("\n".join(report_lines))

        #  SEND DOWNLOADABLE REPORT FILE 
        import io, datetime
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
        plain_report = []
        if unparseable_msgs_list:
            plain_report.append("-" * 60)
            plain_report.append(f"FILES WITH NO EPISODE LABEL: {len(unparseable_msgs_list)}")
            plain_report.append(f"  These files exist in the database but their filename had no")
            plain_report.append(f"  recognizable episode number. They are NOT missing — they were")
            plain_report.append(f"  automatically embedded into buttons at their chronological position.")
            plain_report.append("  IDs: " + ", ".join(str(m) for m in unparseable_msgs_list))
            
        plain_report += [
            "=" * 50,
            "  ARYA BOT  —  Share Links Generation Report",
            "=" * 50,
            f"Story    : {story.upper()}",
            f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S IST')}",
            f"Bot      : @{bot_usr}",
            "-" * 50,
            f"Files processed      : {total_count}",
            f"Episode range        : {first_ep_num} – {last_ep_num}",
            f"Link buttons created : {len(raw_buttons)}",
            f"Posts sent           : {post_count}",
            f"Mode                 : {'Grouped (1 button/file)' if GROUPED_MODE else f'Individual (batch={batch_size})'}",
        ]
        if grouped_files:
            plain_report.append("-" * 50)
            plain_report.append(f"GROUPED FILES ({len(grouped_files)}):")
            for gf in grouped_files:
                plain_report.append(f"  • {gf}")
        if duplicate_eps:
            plain_report.append("-" * 60)
            plain_report.append(f"DUPLICATES DETECTED — all files kept ({len(duplicate_eps)}):")
            plain_report.append("  " + ", ".join(str(e) for e in duplicate_eps))
        if unassigned_count > 0:
            plain_report.append("-" * 60)
            plain_report.append(f"FILES EMBEDDED WITHOUT EPISODE LABEL: {unassigned_count}")
            plain_report.append(f"  These files EXIST in the database but their filenames had")
            plain_report.append(f"  no readable episode number (e.g. auto-generated names).")
            plain_report.append(f"  They are NOT missing — they are already delivered inside buttons.")
        if truly_missing_count > 0:
            plain_report.append("-" * 60)
            plain_report.append(f"TRULY MISSING EPISODES (not in DB): {truly_missing_count}")
            plain_report.append("  " + ", ".join(str(e) for e in missing_eps))
        elif unassigned_count == 0:
            plain_report.append("-" * 60)
            plain_report.append("NO MISSING EPISODES — all slots accounted for!")

        plain_report += [
            "=" * 60,
            "ACCURACY NOTE:",
            "  'Files embedded without label' = PRESENT in DB, delivered to users.",
            "  'Truly missing' = NOT in DB at all, cannot be delivered.",
            "  Duplicates = multiple files for same ep — all included.",
            "-" * 60,
            "Arya Bot",
            "=" * 60,
        ]
        report_text = "\n".join(plain_report)
        report_bytes = io.BytesIO(report_text.encode('utf-8'))
        report_bytes.name = f"arya_report_{story.replace(' ','_')}.txt"

        import html
        try:
            usr_obj = await bot.get_users(user_id)
            u_name = html.escape(usr_obj.first_name) if usr_obj and usr_obj.first_name else "User"
            poster_me = await poster.get_me()
            p_name = html.escape(poster_me.first_name) if poster_me and poster_me.first_name else "Bot"
            bot_link = f"<a href='https://t.me/{bot_usr}'>{p_name}</a>"
            story_sz = _sc(story)

            if sj.get('is_completed'):
                dm_header  = f"›› {_sc('Hey')} <a href='tg://user?id={user_id}'>{u_name}</a>\n\n"
                ch_header  = f"›› {_sc('Hey Strangers')}\n\n"

                en_body = (
                    _sc("This ") + story_sz + _sc(" is completed by ") + bot_link +
                    _sc(". I've tried to ensure accuracy and provided a final report with details. "
                        "Missing episodes can occur naturally—nothing can be done. "
                        "If 10+ are missing, contact support. Unparsed files are safely mapped "
                        "inside buttons. Duplicates may appear if the source had identically "
                        "named files. I am not responsible for the content as these files are "
                        "purely forwarded via Arya bot, strictly not scraped.")
                )

                hi_body = (
                    f"यह {story_sz} {bot_link} द्वारा पूरी की गई है। मैंने सटीकता सुनिश्चित करने का "
                    "प्रयास किया है और अंतिम रिपोर्ट संलग्न है। गायब एपिसोड स्वाभाविक हैं। "
                    "अगर 10+ गायब हैं, तो सपोर्ट से संपर्क करें। अनपार्स फ़ाइलें सुरक्षित रूप से "
                    "बटनों में मैप की गई हैं। डुप्लिकेट फ़ाइलें स्रोत की वजह से हो सकती हैं। मैं "
                    "सामग्री के लिए जिम्मेदार नहीं हूँ क्योंकि ये फ़ाइलें आर्या बॉट के माध्यम से "
                    "अग्रेषित हैं, बिल्कुल स्क्रैप नहीं की गई हैं।"
                )

                dm_cap = (
                    f"<blockquote expandable>{dm_header}{en_body}</blockquote>\n\n<blockquote expandable>{hi_body}\n\n"
                    "<i>Note: If some existing files were wrongly marked as missing, you can use /deepscanbatch with this report to auto-correct them!</i></blockquote>"
                )
                ch_cap = (
                    f"<blockquote expandable>{ch_header}{en_body}</blockquote>\n\n<blockquote expandable>{hi_body}</blockquote>"
                )

            else:
                dm_header  = f"›› {_sc('Hey')} <a href='tg://user?id={user_id}'>{u_name}</a>\n\n"
                ch_header  = f"›› {_sc('Hey Strangers')}\n\n"
                
                en_body = _sc("All currently available files have been posted here. "
                           "New episodes will be added as they arrive. Enjoy and stay tuned!")
                           
                hi_body = ("वर्तमान में उपलब्ध सभी फ़ाइलें यहाँ पोस्ट कर दी गई हैं। "
                           "जैसे ही नए एपिसोड आएंगे, उन्हें जोड़ दिया जाएगा। आनंद लें और जुड़े रहें!")

                dm_cap = f"<blockquote expandable>{dm_header}{en_body}</blockquote>\n\n<blockquote expandable>{hi_body}\n\n<i>Note: If some existing files were wrongly marked as missing, you can use /deepscanbatch with this report to auto-correct them!</i></blockquote>"
                ch_cap = f"<blockquote expandable>{ch_header}{en_body}</blockquote>\n\n<blockquote expandable>{hi_body}</blockquote>"

            # Send to admin DM — independent of channel
            try:
                await bot.send_document(
                    user_id, report_bytes,
                    caption=dm_cap, parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML,
                    file_name=report_bytes.name
                )
            except Exception as dm_err:
                logger.error(f"[Report] DM send failed: {dm_err}", exc_info=True)

            # Send to target channel — always attempted independently
            try:
                # Peer injection for Share Bot
                target_chat_id = sj['target']
                from pyrogram.raw.types import InputPeerChannel as _IPC
                _tpeer = await bot.resolve_peer(target_chat_id)
                if isinstance(_tpeer, _IPC):
                    await poster.storage.update_peers([(_tpeer.channel_id, _tpeer.access_hash, 'channel', None, None)])
            except: pass

            try:
                report_bytes.seek(0)
                await poster.send_document(
                    sj['target'], report_bytes,
                    caption=ch_cap, parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML,
                    file_name=report_bytes.name,
                    reply_to_message_id=sj.get('target_topic_id')
                )
            except Exception as ch_err:
                logger.error(f"[Report] Channel send failed: {ch_err}", exc_info=True)

        except Exception as rep_err:
            logger.error(f"[Report] Could not prepare report: {rep_err}", exc_info=True)

        if sj.get('live_threshold', 0) > 0:
            try:
                from plugins.live_batch import _lb_save_job, _lb_paused, _lb_tasks, _lb_run_job
                job_id = str(uuid.uuid4())
                ljob = {
                    "job_id": job_id, "user_id": user_id, "status": "running",
                    "share_bot_id": selected_bot_id,
                    "account_id": sj.get('account_id', 'bot'),
                    "source": sj['source'],
                    "is_topic": sj.get('is_topic', False),
                    "topic_id": sj.get('topic_id'),
                    "target": sj['target'],
                    "target_topic_id": sj.get('target_topic_id'),
                    "story": sj['story'],
                    "duplicate_handling": sj.get('duplicate_handling', 'no'),
                    "threshold": sj['live_threshold'],
                    "batch_size": sj.get('batch_size', 10),
                    "buttons_per_post": sj.get('buttons_per_post', 10),
                    "protect": True,
                    "last_seen_id": int(sj.get('end_id') or 0),
                    "buffer_mids": [],
                    "forwarded": 0
                }
                await _lb_save_job(ljob)
                _lb_paused[job_id] = asyncio.Event()
                _lb_paused[job_id].set()
                _lb_tasks[job_id] = asyncio.create_task(_lb_run_job(job_id))

                await bot.send_message(user_id, f"<b>✅ Live Batch Monitoring automatically activated for {sj['story']}!</b>\nMonitoring for new files arriving after Msg ID <code>{ljob['last_seen_id']}</code>.")

            except Exception as lb_err:
                logger.error(f"Live Batch Kickoff error: {lb_err}", exc_info=True)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error(f"Share link generation error:\n{tb}")
        retry_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔁 Rᴇᴛʀʏ", callback_data="sl#complete"),
            InlineKeyboardButton("✖️ Dɪsᴍɪss", callback_data="close_btn")
        ]])
        err_txt = (
            f"<b>❌ Error during link generation:</b>\n<code>{e}</code>\n\n"
            f"<i>Click Retry to start a new job, or Dismiss to cancel.</i>"
        )
        try:
            await sts.edit_text(err_txt, reply_markup=retry_kb)
        except Exception:
            await bot.send_message(user_id, err_txt, reply_markup=retry_kb)
    finally:
        new_share_job.pop(user_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# DEEP SCAN SELF-CORRECTION SYSTEM
# /deepscanbatch — Upload a scan report to diagnose and self-correct missing ep detection
# ══════════════════════════════════════════════════════════════════════════════

import re as _deepre

def _deep_extract_ep(filename: str) -> tuple[int, int] | None:
    """
    Multi-strategy episode extractor. Unified with robust engine.
    Returns (ep_start, ep_end) or None.
    """
    from plugins.utils import extract_ep_label_robust
    res = extract_ep_label_robust(filename)
    nums = res.get("numbers", [])
    if not nums: return None
    return (min(nums), max(nums))


def _analyze_scan_report(report_text: str) -> dict:
    """
    Parse a plain-text scan report file (as generated by share_jobs or db_scanner)
    and build a structured diagnosis.
    Returns: {
        story, total_files, ep_range, missing, unassigned,
        file_entries: [{msg_id, filename, ep, parsed_ok, suggested_ep}]
    }
    """
    lines = report_text.splitlines()
    result = {
        "story": "", "total_files": 0, "ep_range": (0, 0),
        "missing": [], "unassigned": [],
        "file_entries": [], "raw_lines": len(lines)
    }

    # Parse header info
    for line in lines[:30]:
        m = _deepre.search(r'Story\s*:\s*(.+)|Channel\s*:\s*(.+)', line)
        if m: result["story"] = m.group(1) or m.group(2) or ""
        m = _deepre.search(r'Files processed\s*:\s*(\d+)|Files\s*:\s*(\d+)', line)
        if m: result["total_files"] = int(m.group(1) or m.group(2) or 0)
        m = _deepre.search(r'Episode range\s*:\s*(\d+)\s*[–\-]\s*(\d+)', line)
        if m: result["ep_range"] = (int(m.group(1)), int(m.group(2)))
        m = _deepre.search(r"Truly missing.*?:\s*(.+)", line)
        if m:
            nums = [int(x) for x in _deepre.findall(r'\d+', m.group(1))]
            result["missing"].extend(nums)

    # Parse file entries
    for line in lines:
        line = line.strip()
        msg_id = None
        fname = None
        parsed_ep_str = ""
        
        # Format 1: db_scanner output "    1     15148  document  000047_Filename.mp3  [2.1MB]"
        m_db = _deepre.search(r'^\d+\s+(\d+)\s+(audio|voice|document|video|\?)\s+(.+?)(?:\s+\[\d.*?\]|\s+↳.*)?$', line)
        if m_db:
            msg_id = int(m_db.group(1))
            fname = m_db.group(3).strip()
        else:
            # Format 2: Old share_jobs format "  123456  |  000047_Filename.mp3  |  Ep 47"
            m_old = _deepre.search(r'(\d{5,})\s*[|\-:]\s*([^\|]+?)(?:\s*[|\-:]\s*(.+))?$', line)
            if m_old:
                msg_id = int(m_old.group(1))
                fname = m_old.group(2).strip()
                parsed_ep_str = (m_old.group(3) or "").strip()

        if msg_id and fname and fname != '(no name)' and fname != 'FileName / Title':
            suggested = _deep_extract_ep(fname)
            entry = {
                "msg_id": msg_id,
                "filename": fname,
                "reported_ep": parsed_ep_str,
                "suggested_ep": suggested,
                "parsed_ok": suggested is not None,
            }
            result["file_entries"].append(entry)

    return result


@Client.on_message(filters.private & filters.command(["deepscanbatch", "batchdiag"]))
async def cmd_deep_scan_batch(bot, message):
    """
    /deepscanbatch — Upload a txt scan report, get a deep diagnosis of why files
    were wrongly marked as missing, and receive a corrected summary.
    """
    from config import Config
    uid = message.from_user.id


    help_txt = (
        "<b>»  Deep Scan Self-Correction</b>\n\n"
        "Upload the <b>.txt report file</b> from a previous Batch Links run "
        "(the one with episode entries and filenames).\n\n"
        "The bot will:\n"
        "• Re-parse all filenames with 5 fallback strategies\n"
        "• Identify which 'missing' episodes are actually present with bad filename\n"
        "• Generate a corrected diagnosis report\n"
        "• Show you exactly which files failed to parse and why\n\n"
        "<i>Send the .txt file now, or /cancel to abort.</i>"
    )
    await message.reply_text(help_txt)

    try:
        resp = await _ask(bot, uid, "📎 <i>Waiting for your scan report file...</i>", timeout=300)
    except asyncio.TimeoutError:
        return await bot.send_message(uid, "<i>Timed out. Use /deepscanbatch again.</i>")

    if resp.text and any(x in resp.text.lower() for x in ['/cancel', 'cancel', '⛔']):
        return await bot.send_message(uid, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())

    doc = resp.document
    if not doc:
        return await bot.send_message(uid, "⚠️ Please send a <b>.txt file</b> (not text message).")
    if doc.file_size > 5 * 1024 * 1024:
        return await bot.send_message(uid, "⚠️ File too large (max 5MB).")

    sts = await bot.send_message(uid, "<i>Downloading and analyzing report...</i>")

    try:
        buf = await bot.download_media(resp, in_memory=True)
        buf.seek(0)
        report_text = buf.read().decode('utf-8', errors='replace')
    except Exception as e:
        return await sts.edit_text(f"<b>❌ Download failed:</b> <code>{e}</code>")

    await sts.edit_text("<i>Running deep analysis...</i>")

    diagnosis = _analyze_scan_report(report_text)

    # Re-analyze all filenames in the report
    file_entries = diagnosis["file_entries"]
    total_entries = len(file_entries)
    parsed_ok   = [e for e in file_entries if e["parsed_ok"]]
    failed_parse = [e for e in file_entries if not e["parsed_ok"]]

    # Find entries reported as missing but our deep extractor can parse
    corrected = []
    for e in failed_parse:
        sug = _deep_extract_ep(e["filename"])
        if sug:
            corrected.append(e)

    # Build diagnosis lines
    lines_out = [
        f"<b>»  Deep Scan Diagnosis</b>",
        f"<b>Story:</b> {diagnosis['story'] or 'Unknown'}",
        f"<b>Report lines:</b> {diagnosis['raw_lines']}",
        f"<b>File entries found:</b> {total_entries}",
        f"",
        f"<b>✅ Correctly parsed by original system:</b> {len(parsed_ok)}",
        f"<b>⚠️ Failed original parse:</b> {len(failed_parse)}",
        f"<b>🔧 Deep extractor can fix:</b> {len(corrected)}",
        f"",
    ]

    if diagnosis["missing"]:
        lines_out.append(f"<b>❌ Episodes reported as missing by original system:</b> {len(diagnosis['missing'])}")
        miss_str = ", ".join(str(e) for e in sorted(diagnosis["missing"])[:20])
        if len(diagnosis["missing"]) > 20:
            miss_str += f" (+{len(diagnosis['missing'])-20} more)"
        lines_out.append(f"  <code>{miss_str}</code>")
        lines_out.append("")

    if corrected:
        lines_out.append(f"<b>🔧 Files the deep extractor could parse (were NOT missing):</b>")
        for e in corrected[:15]:
            sug = e["suggested_ep"]
            ep_label = f"Ep {sug[0]}–{sug[1]}" if sug[0] != sug[1] else f"Ep {sug[0]}"
            lines_out.append(f"  • <code>{e['filename'][:50]}</code> → <b>{ep_label}</b>")
        if len(corrected) > 15:
            lines_out.append(f"  ... and {len(corrected)-15} more files")
        lines_out.append("")

    if failed_parse:
        still_unknown = [e for e in failed_parse if not e.get("suggested_ep")]
        if still_unknown:
            lines_out.append(f"<b>❓ Truly unparseable files (even with deep scan):</b> {len(still_unknown)}")
            lines_out.append("<i>These files genuinely have no episode number in their name.</i>")
            lines_out.append("<i>→ They will be embedded chronologically in buttons (NOT missing).</i>")
            lines_out.append("")

    # Corrected missing count
    actually_missing = max(0, len(diagnosis["missing"]) - len(corrected))
    lines_out += [
        f"<b>─────────────────────</b>",
        f"<b>🎯 CORRECTED VERDICT:</b>",
        f"  • Files originally flagged missing: <code>{len(diagnosis['missing'])}</code>",
        f"  • Files deep-scan can recover: <code>{len(corrected)}</code>",
        f"  • <b>Truly missing (not in DB at all): <code>{actually_missing}</code></b>",
        f"",
        f"<i>💡 To fix: Run Batch Links again — the re-runs benefit from the improved parser.</i>",
        f"<i>If filenames are genuinely missing episode numbers, rename them in the DB channel and re-run.</i>",
    ]

    # Save full corrected report as file
    import datetime
    import io
    now = datetime.datetime.now()
    report_bytes_out = io.BytesIO()
    full_report_lines = [
        "=" * 60,
        "  ARYA BOT — Deep Scan Correction Report",
        "=" * 60,
        f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Story    : {diagnosis['story'] or 'Unknown'}",
        f"Total entries in report: {total_entries}",
        "-" * 60,
        f"Correctly parsed by original: {len(parsed_ok)}",
        f"Failed original parse:        {len(failed_parse)}",
        f"Deep extractor can fix:       {len(corrected)}",
        f"Actually missing (confirmed):  {actually_missing}",
        "=" * 60,
        "FILES THAT DEEP SCAN RECOVERED (were NOT missing):",
        "-" * 60,
    ]
    for e in corrected:
        sug = e["suggested_ep"]
        full_report_lines.append(f"  MsgID {e['msg_id']:>10}  |  {e['filename'][:60]}  |  Ep {sug[0]}–{sug[1]}")

    full_report_lines += ["", "=" * 60, "TRULY UNPARSEABLE FILES (no ep number at all):", "-" * 60]
    for e in failed_parse:
        if not e.get("suggested_ep"):
            full_report_lines.append(f"  MsgID {e['msg_id']:>10}  |  {e['filename'][:60]}")

    report_bytes_out.write("\n".join(full_report_lines).encode('utf-8'))
    report_bytes_out.seek(0)
    report_bytes_out.name = f"deep_scan_{diagnosis['story'] or 'report'}_{now.strftime('%Y%m%d_%H%M')}.txt"

    final_txt = "\n".join(lines_out)
    try:
        await sts.edit_text(final_txt)
    except Exception:
        await bot.send_message(uid, final_txt)

    await bot.send_document(uid, report_bytes_out, caption="📎 Full deep scan correction report", file_name=report_bytes_out.name)
