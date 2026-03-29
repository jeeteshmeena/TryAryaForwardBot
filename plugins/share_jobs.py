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
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from database import db
from plugins.test import CLIENT
from plugins.jobs import _ask

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()
import math
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from database import db
from plugins.test import CLIENT
from plugins.jobs import _ask

_CLIENT = CLIENT()

new_share_job = {}

async def _create_share_flow(bot, user_id):
    try:
        new_share_job[user_id] = {}
        share_bots = await db.get_share_bots()
        
        if not share_bots:
            return await bot.send_message(user_id, "<b>❌ No Share Bots available. Please add a Bot Token in /settings -> Share Bots.</b>")
            
        kb = []
        for b in share_bots:
            kb.append([f"🤖 {b['name']} (@{b['username']})"])
            
        kb.append(["❌ Cancel"])
        
        msg = await _ask(bot, user_id, 
            "<b>❪ SHARE LINKS: SELECT ACCOUNT ❫</b>\n\nChoose the Share Bot you want to use for link generation and delivery:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        # Match selection
        import re
        sel = msg.text
        match = re.search(r"@([a-zA-Z0-9_]+)", sel)
        if not match:
            return await bot.send_message(user_id, "<b>❌ Invalid selection.</b>", reply_markup=ReplyKeyboardRemove())
            
        username = match.group(1)
        selected_bot = next((b for b in share_bots if b['username'] == username), None)
        if not selected_bot:
            return await bot.send_message(user_id, "<b>❌ Account not found.</b>", reply_markup=ReplyKeyboardRemove())
            
        new_share_job[user_id]['bot_id'] = selected_bot['id']

        chans = await db.get_user_channels(user_id)
        if not chans:
            return await bot.send_message(user_id, "<b>❌ No channels added in /settings.</b>", reply_markup=ReplyKeyboardRemove())
            
        ch_kb = [[f"📢 {ch['title']}"] for ch in chans]
        ch_kb.append(["❌ Cancel"])
        msg = await _ask(bot, user_id, 
            "<b>❪ STEP 2: SOURCE DATABASE ❫</b>\n\nWhere are the files stored securely?", 
            reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        title = msg.text.replace("📢 ", "").strip()
        ch = next((c for c in chans if c["title"] == title), None)
        if not ch:
            return await bot.send_message(user_id, "<b>❌ Source Channel not found.</b>", reply_markup=ReplyKeyboardRemove())
        new_share_job[user_id]['source'] = int(ch['chat_id'])
        
        msg = await _ask(bot, user_id, 
            "<b>❪ STEP 3: TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?", 
            reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        title = msg.text.replace("📢 ", "").strip()
        ch = next((c for c in chans if c["title"] == title), None)
        if not ch:
            return await bot.send_message(user_id, "<b>❌ Target Channel not found.</b>", reply_markup=ReplyKeyboardRemove())
        new_share_job[user_id]['target'] = int(ch['chat_id'])

        markup = ReplyKeyboardMarkup([[KeyboardButton("/cancel")]], resize_keyboard=True, one_time_keyboard=True)
            
        def parse_id(msg) -> int:
            if getattr(msg, 'forward_from_message_id', None):
                return msg.forward_from_message_id
                
            text = (msg.text or msg.caption or "").strip().rstrip('/')
            if text.isdigit(): return int(text)
            if "t.me/" in text:
                parts = text.split('/')
                if parts[-1].isdigit(): return int(parts[-1])
            raise ValueError("Invalid Message ID or Link (must be forwarded or contain ID)")
            
        msg_story = await _ask(bot, user_id, 
            "<b>❪ STEP 4: STORY NAME ❫</b>\n\nEnter the clean name of the Series/Story (e.g. <code>TDMB</code>):", 
            reply_markup=markup
        )
        if (msg_story.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        new_share_job[user_id]['story'] = (msg_story.text or msg_story.caption or "").strip()
        
        msg_start = await _ask(bot, user_id, 
            "<b>❪ STEP 5: START MESSAGE ❫</b>\n\nForward the first message, send its Message ID, or paste its Link (e.g. <code>https://t.me/c/123/456</code>):", 
            reply_markup=markup
        )
        if (msg_start.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        start_id = parse_id(msg_start)
        new_share_job[user_id]['start_id'] = start_id
        
        msg_end = await _ask(bot, user_id, 
            "<b>❪ STEP 6: LAST MESSAGE ❫</b>\n\nForward the last message, send its Msg ID, or paste its Link:", 
            reply_markup=markup
        )
        if (msg_end.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        end_id = parse_id(msg_end)
        new_share_job[user_id]['end_id'] = end_id
        
        if start_id > end_id:
            start_id, end_id = end_id, start_id
            new_share_job[user_id]['start_id'] = start_id
            new_share_job[user_id]['end_id'] = end_id
            
        msg_batch = await _ask(bot, user_id, 
            "<b>❪ STEP 7: EPISODES PER BUTTON ❫</b>\n\nHow many episodes per link button?\nExample: <code>20</code>", 
            reply_markup=markup
        )
        if (msg_batch.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        
        raw_b = (msg_batch.text or msg_batch.caption or "20").strip()
        batch_size = int(raw_b) if raw_b.isdigit() else 20
        if batch_size < 1: batch_size = 20
        new_share_job[user_id]['batch_size'] = batch_size

        msg_bpp = await _ask(bot, user_id, 
            "<b>❪ STEP 8: BUTTONS PER POST ❫</b>\n\nHow many buttons should appear in one post in the channel?\nExample: <code>10</code>", 
            reply_markup=markup
        )
        if (msg_bpp.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        
        raw_bpp = (msg_bpp.text or msg_bpp.caption or "10").strip()
        bpp = int(raw_bpp) if raw_bpp.isdigit() else 10
        if bpp < 1: bpp = 10
        new_share_job[user_id]['buttons_per_post'] = bpp

        sj = new_share_job[user_id]
        total_msgs  = (sj['end_id'] - sj['start_id']) + 1
        
        markup_conf = ReplyKeyboardMarkup([["🚀 Generate & Group Links"], ["❌ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
        conf_msg = await _ask(bot, user_id,
            f"<b>📋 CONFIRM SHARE BATCH</b>\n\n"
            f"<b>Story Name:</b> {sj['story']}\n"
            f"<b>Source ID:</b> <code>{sj['source']}</code>\n"
            f"<b>Target ID:</b> <code>{sj['target']}</code>\n"
            f"<b>Msg ID Range:</b> {sj['start_id']} → {sj['end_id']} ({total_msgs} slots)\n"
            f"<b>Episodes/Button:</b> {sj['batch_size']}\n"
            f"<b>Buttons/Post:</b> {sj['buttons_per_post']}\n"
            f"\n<i>🤖 Smart Parse active: I will read filenames & captions to correctly group duplicates and missing episodes.</i>",
            reply_markup=markup_conf
        )
        
        if not conf_msg.text or conf_msg.text == "/cancel" or "Cancel" in conf_msg.text:
            new_share_job.pop(user_id, None)
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        if "Generate" in conf_msg.text:
            await _build_share_links(bot, user_id, sj, conf_msg)
            
    except Exception as e:
        await bot.send_message(user_id, f"<b>Error during link setup:</b> {e}", reply_markup=ReplyKeyboardRemove())
    
@Client.on_callback_query(filters.regex(r'^sl#'))
async def sl_callback(bot, query):
    user_id = query.from_user.id
    data = query.data.split('#')
    cmd = data[1]

    if cmd == "start":
        await query.message.delete()
        asyncio.create_task(_create_share_flow(bot, user_id))

async def _build_share_links(bot, user_id, sj, info_msg):
    sts = await info_msg.reply_text("<i>⏳ Initializing share worker...</i>", reply_markup=ReplyKeyboardRemove())

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
        
        if not poster or not getattr(poster, 'is_connected', False):
            try:
                await share_mod.start_share_bot()  # reload bots if missing
                poster = share_mod.share_clients.get(selected_bot_id)
            except Exception:
                pass

        if not poster or not getattr(poster, 'is_connected', False):
            return await safe_edit("❌ Share Bot failed to start or connect. Check settings.")

        bot_usr = poster.me.username

        await safe_edit("<i>⏳ Scanning database channel and generating links...</i>")

        # ===== DEFINITIVE CHANNEL_INVALID FIX =====
        # The Share Bot uses in_memory=True; it has ZERO peer cache after every restart.
        # SOLUTION: Use the MAIN BOT (which has a persistent SQLite session + is admin)
        # to resolve the InputPeerChannel, then invoke channels.GetMessages on the raw layer
        # of the MAIN BOT directly — we never ask the Share Bot (worker) to touch the DB channel.
        # The Share Bot is only used for POSTING to the public target channel and for
        # DELIVERING files to users (it IS admin there by the user's configuration).
        from pyrogram.raw.functions.channels import GetMessages as ChannelGetMessages
        from pyrogram.raw.types import InputMessageID, InputPeerChannel

        source_chat_id = sj['source']

        # Step 1: Resolve the database channel peer using the MAIN BOT (always works)
        try:
            db_peer = await bot.resolve_peer(source_chat_id)
        except Exception as e:
            return await safe_edit(
                f"<b>❌ Cannot Access Database Channel</b>\n\n"
                f"<code>{e}</code>\n\n"
                f"The Main Bot (@{(await bot.get_me()).username}) must be an admin in the hidden database channel."
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

        # ── PHASE 1: Scan entire range, reading raw objects ──────────
        import re as _re
        all_valid_msgs = []
        total_scanned = 0

        await safe_edit(f"<i>⏳ Scanning and analyzing files {current_id}–{end_ep}...</i>")

        while current_id <= end_ep:
            chunk_end = min(current_id + SCAN_CHUNK - 1, end_ep)
            msg_ids   = list(range(current_id, chunk_end + 1))

            for attempt in range(6):
                try:
                    msgs = await bot.get_messages(sj['source'], msg_ids)
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
                        await safe_edit(f"<i>⏳ Flood Wait {wait_secs}s... (scanned {total_scanned})</i>")
                        await asyncio.sleep(wait_secs)
                        continue
                    return await safe_edit(f"<b>❌ Scan Error:</b> <code>{e}</code>")
            else:
                return await safe_edit("❌ Scan aborted after 6 retries due to FloodWait.")

            total_scanned += len(msg_ids)
            current_id = chunk_end + 1
            await asyncio.sleep(0.3)

        if not all_valid_msgs:
            return await safe_edit("❌ No files found in that range.")

        # ── PARSE EPISODE NUMBERS NATIVELY ──
        all_valid_msgs.sort(key=lambda x: x.id) # Sort chronologically

        def extract_eps(msg):
            text = (msg.caption or "") + " " + (msg.text or "")
            if msg.document and getattr(msg.document, "file_name", None):
                text += " " + str(msg.document.file_name)
            
            # Match "31-40", "31 to 40", "31_40"
            m_range = _re.search(r'\b(?:ep|episode|ch|chapter|part|audio)?\s*(\d+)\s*[-_to]+\s*(\d+)\b', text, _re.IGNORECASE)
            if m_range:
                s, e = int(m_range.group(1)), int(m_range.group(2))
                if s < e and (e - s) < 500: return s, e
            
            m_single = _re.search(r'\b(?:ep|episode|ch|chapter|part|audio)\s*[-_.:]?\s*(\d+)\b', text, _re.IGNORECASE)
            if m_single: return int(m_single.group(1)), int(m_single.group(1))
            
            numbers = _re.findall(r'\b\d{1,4}\b', text)
            if numbers: return int(numbers[-1]), int(numbers[-1])
            return None, None

        parsed_data = [] # List of tuples: (msg_id, assigned_start, assigned_end)
        
        s, e = extract_eps(all_valid_msgs[0])
        current_ep = s if s is not None else 1
        
        for idx, m in enumerate(all_valid_msgs):
            if idx == 0:
                parsed_data.append((m.id, current_ep, e if e is not None else current_ep))
                current_ep = (e if e is not None else current_ep)
                continue
                
            s, e = extract_eps(m)
            if s is None:
                current_ep += 1
                parsed_data.append((m.id, current_ep, current_ep))
            else:
                if s <= current_ep:
                    if s != e:
                        val_s = current_ep + 1
                        val_e = current_ep + 1 + (e - s)
                        parsed_data.append((m.id, val_s, val_e))
                        current_ep = val_e
                    else:
                        current_ep += 1
                        parsed_data.append((m.id, current_ep, current_ep))
                else:
                    parsed_data.append((m.id, s, e))
                    current_ep = e

        first_ep_num = parsed_data[0][1]
        last_ep_num  = parsed_data[-1][2]

        buckets = {} # (w_start, w_end): [msg_ids...]
        
        for m_id, a_s, a_e in parsed_data:
            for ep in range(a_s, a_e + 1):
                if ep < 1: continue
                # Calculate strictly aligned mathematical bucket interval (e.g. 1-10, 11-20)
                b_s = ((ep - 1) // batch_size) * batch_size + 1
                b_e = b_s + batch_size - 1
                b_key = (b_s, b_e)
                
                if b_key not in buckets:
                    buckets[b_key] = []
                if m_id not in buckets[b_key]:
                    buckets[b_key].append(m_id)

        raw_buttons = []
        for (w_start, w_end), batch in sorted(buckets.items()):
            if not batch: continue
            
            uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
            await db.save_share_link(
                uuid_str, batch, source_chat_id,
                protect=protect, access_hash=db_access_hash
            )
            url = f"https://t.me/{bot_usr}?start={uuid_str}"

            # Strictly display numbers only per user request
            btn_text = str(w_start) if batch_size == 1 else f"{w_start}\u2013{w_end}"
            raw_buttons.append({
                "btn":      InlineKeyboardButton(btn_text, url=url),
                "ep_start": w_start,
                "ep_end":   w_end,
            })


        # ── PHASE 3: Group buttons into posts and send to target channel ──────
        post_count = 0
        for i in range(0, len(raw_buttons), buttons_per_post):
            chunk = raw_buttons[i : i + buttons_per_post]

            first_ep = chunk[0]["ep_start"]
            last_ep  = chunk[-1]["ep_end"]

            txt = f"<b>📂 {story.upper()} | Episodes {first_ep}–{last_ep}</b>"

            keyboard = []
            for j in range(0, len(chunk), 2):
                row = [c["btn"] for c in chunk[j:j + 2]]
                keyboard.append(row)

            keyboard.append([
                InlineKeyboardButton("Tutorial 🎥", url="https://t.me/StoriesLinkopningguide"),
                InlineKeyboardButton("Support ❓", url="https://t.me/+EAc-6v1bmZ1iMDBl")
            ])

            for attempt in range(6):
                try:
                    await poster.send_message(
                        chat_id=sj['target'],
                        text=txt,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    break
                except Exception as e:
                    err_str = str(e)
                    import re as _re
                    if "FLOOD_WAIT" in err_str or "420" in err_str:
                        mw = _re.search(r'wait of (\d+)', err_str)
                        wait_secs = (int(mw.group(1)) + 2) if mw else 35
                        await safe_edit(f"<i>⏳ Rate limit while posting... waiting {wait_secs}s</i>")
                        await asyncio.sleep(wait_secs)
                        continue
                    else:
                        return await safe_edit(
                            f"<b>❌ Failed to post to target channel:</b> <code>{e}</code>\n\n"
                            f"<i>Make sure the selected account is an admin in the target/public channel.</i>"
                        )
            else:
                return await safe_edit("❌ Posting aborted after 6 retries due to FloodWait.")

            post_count += 1
            await asyncio.sleep(1)

        await safe_edit(
            f"<b>✅ Share Links Generated!</b>\n\n"
            f"📊 <b>Files processed:</b> {len(parsed_data)}\n"
            f"🎯 <b>Episode range:</b> {first_ep_num}–{last_ep_num}\n"
            f"🔗 <b>Link buttons created:</b> {len(raw_buttons)}\n"
            f"📝 <b>Posts sent to channel:</b> {post_count}\n\n"
            f"<i>Users click any button to receive their episodes from @{bot_usr}.</i>"
        )

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        try:
            await sts.edit_text(f"<b>Error during link generation:</b>\n<code>{e}</code>")
        except Exception:
            await bot.send_message(user_id, f"<b>Error during link generation:</b>\n<code>{e}</code>")
        logger.error(f"Share link generation error:\n{tb}")
    finally:
        new_share_job.pop(user_id, None)
