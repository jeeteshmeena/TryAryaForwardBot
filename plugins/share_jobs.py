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

def _sc(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭"
    ))

new_share_job = {}

async def _create_share_flow(bot, user_id):
    try:
        new_share_job[user_id] = {}
        share_bots = await db.get_share_bots()
        
        if not share_bots:
            return await bot.send_message(user_id, "<b>‣  No Share Bots available. Please add a Bot Token in /settings -> Share Bots.</b>")
            
        kb = []
        for b in share_bots:
            kb.append([f"»  {b['name']} (@{b['username']})"])
            
        kb.append(["‣  Cancel"])
        kb.append(["»  Scan Database Channel"])
        
        msg = await _ask(bot, user_id, 
            "<b>❪ SHARE LINKS: SELECT ACCOUNT ❫</b>\n\nChoose the Share Bot you want to use for link generation and delivery:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

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

        chans = await db.get_user_channels(user_id)
        if not chans:
            return await bot.send_message(user_id, "<b>‣  No channels added in /settings.</b>", reply_markup=ReplyKeyboardRemove())
            
        ch_kb = [[f"»  {ch['title']}"] for ch in chans]
        ch_kb.append(["‣  Cancel"])
        msg = await _ask(bot, user_id, 
            "<b>❪ STEP 2: SOURCE DATABASE ❫</b>\n\nWhere are the files stored securely?", 
            reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        title = msg.text.replace("»  ", "").strip()
        ch = next((c for c in chans if c["title"] == title), None)
        if not ch:
            return await bot.send_message(user_id, "<b>‣  Source Channel not found.</b>", reply_markup=ReplyKeyboardRemove())
        new_share_job[user_id]['source'] = int(ch['chat_id'])
        
        msg = await _ask(bot, user_id, 
            "<b>❪ STEP 3: TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?", 
            reply_markup=ReplyKeyboardMarkup(ch_kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        title = msg.text.replace("»  ", "").strip()
        ch = next((c for c in chans if c["title"] == title), None)
        if not ch:
            return await bot.send_message(user_id, "<b>‣  Target Channel not found.</b>", reply_markup=ReplyKeyboardRemove())
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
            
        markup_status = ReplyKeyboardMarkup([["»  Completed", "»  Ongoing"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True)
        msg_status = await _ask(bot, user_id, 
            "<b>❪ STEP 4: STORY STATUS ❫</b>\n\nIs this story Completed or Ongoing?", 
            reply_markup=markup_status
        )
        if (msg_status.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        is_completed = "completed" in (msg_status.text or "").lower()
        new_share_job[user_id]['is_completed'] = is_completed

        msg_story = await _ask(bot, user_id, 
            "<b>❪ STEP 5: STORY NAME ❫</b>\n\nEnter the clean name of the Series/Story (e.g. <code>TDMB</code>):", 
            reply_markup=markup
        )
        if (msg_story.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        new_share_job[user_id]['story'] = (msg_story.text or msg_story.caption or "").strip()
        
        markup_source = ReplyKeyboardMarkup([["»  Regular Channel", "»  Group Topic"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True)
        msg_stype = await _ask(bot, user_id, 
            "<b>❪ STEP 6: SOURCE STRUCTURE ❫</b>\n\nAre the files in a normal Channel (requires start/end IDs)\nor inside a specific Group Topic (auto-scans entire topic)?", 
            reply_markup=markup_source
        )
        if (msg_stype.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
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
            acc_kb.append([KeyboardButton("/cancel")])
            
            msg_acc = await _ask(bot, user_id,
                "<b>❪ STEP 6.5: SCANNING ACCOUNT ❫</b>\n\nChoose the Userbot to use for reading files from the Group Topic:\n"
                "<i>(⚠️ NOTE: Group Topics MUST be scanned by a Userbot.)</i>",
                reply_markup=ReplyKeyboardMarkup(acc_kb, resize_keyboard=True, one_time_keyboard=True)
            )
            if not msg_acc.text or "/cancel" in msg_acc.text:
                return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
                
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
            if (msg_topic.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
            topic_id = parse_id(msg_topic)
            new_share_job[user_id]['topic_id'] = topic_id
            new_share_job[user_id]['start_id'] = topic_id
            new_share_job[user_id]['end_id'] = topic_id
        else:
            msg_start = await _ask(bot, user_id, 
                "<b>❪ STEP 7: START MESSAGE ❫</b>\n\nForward the first message, send its Message ID, or paste its Link (e.g. <code>https://t.me/c/123/456</code>):", 
                reply_markup=markup
            )
            if (msg_start.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
            start_id = parse_id(msg_start)
            new_share_job[user_id]['start_id'] = start_id
            
            msg_end = await _ask(bot, user_id, 
                "<b>❪ STEP 8: LAST MESSAGE ❫</b>\n\nForward the last message, send its Msg ID, or paste its Link:", 
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
            "<b>❪ STEP 9: EPISODES PER BUTTON ❫</b>\n\nHow many episodes per link button?\nExample: <code>20</code>", 
            reply_markup=markup
        )
        if (msg_batch.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        
        raw_b = (msg_batch.text or msg_batch.caption or "20").strip()
        batch_size = int(raw_b) if raw_b.isdigit() else 20
        if batch_size < 1: batch_size = 20
        new_share_job[user_id]['batch_size'] = batch_size

        msg_bpp = await _ask(bot, user_id, 
            "<b>❪ STEP 10: BUTTONS PER POST ❫</b>\n\nHow many buttons should appear in one post in the channel?\nExample: <code>10</code>", 
            reply_markup=markup
        )
        if (msg_bpp.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        
        raw_bpp = (msg_bpp.text or msg_bpp.caption or "10").strip()
        bpp = int(raw_bpp) if raw_bpp.isdigit() else 10
        if bpp < 1: bpp = 10
        new_share_job[user_id]['buttons_per_post'] = bpp

        sj = new_share_job[user_id]
        
        is_tp = sj.get('is_topic')
        sub_str = f"<b>Topic ID:</b> {sj.get('topic_id', 'N/A')}\n" if is_tp else f"<b>Msg ID Range:</b> {sj['start_id']} → {sj['end_id']}\n"

        markup_conf = ReplyKeyboardMarkup([["Gᴇɴᴇʀᴀᴛᴇ & Pᴏsᴛ Lɪɴᴋs"], ["‣  Cancel"]], resize_keyboard=True, one_time_keyboard=True)
        conf_msg = await _ask(bot, user_id,
            f"<b>»  CONFIRM SHARE BATCH</b>\n\n"
            f"<b>Story Name:</b> {sj['story']}\n"
            f"<b>Status:</b> {'Completed' if sj.get('is_completed') else 'Ongoing'}\n"
            f"<b>Source:</b> <code>{sj['source']}</code> ({'Topic' if is_tp else 'Channel'})\n"
            f"<b>Target ID:</b> <code>{sj['target']}</code>\n"
            f"{sub_str}"
            f"<b>Episodes/Button:</b> {sj['batch_size']}\n"
            f"<b>Buttons/Post:</b> {sj['buttons_per_post']}\n"
            f"\n<i>»  Smart Parse active: Auto-groups duplicate eps smoothly.</i>",
            reply_markup=markup_conf
        )
        
        if not conf_msg.text or conf_msg.text == "/cancel" or "Cancel" in conf_msg.text:
            new_share_job.pop(user_id, None)
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        if "Generate" in conf_msg.text or "Gᴇɴᴇʀᴀᴛᴇ" in conf_msg.text:
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

        if sj.get('is_topic'):
            await safe_edit(f"<i>»  Scanning entire Group Topic {sj['topic_id']}...</i>")
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
            except Exception as e:
                return await safe_edit(f"<b>‣  Topic Scan Error:</b> <code>{e}</code>")
        else:
            await safe_edit(f"<i>»  Scanning and analyzing files {current_id}–{end_ep}...</i>")
            while current_id <= end_ep:
                chunk_end = min(current_id + SCAN_CHUNK - 1, end_ep)
                msg_ids   = list(range(current_id, chunk_end + 1))

                for attempt in range(6):
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
                        return await safe_edit(f"<b>‣  Scan Error:</b> <code>{e}</code>")
                else:
                    return await safe_edit("‣  Scan aborted after 6 retries due to FloodWait.")
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
            # Calendar years
            _re.compile(r'(?<!\d)(?:19[0-9]{2}|20[0-9]{2})(?!\d)'),
            # File sizes
            _re.compile(r'(?i)\b\d+(?:\.\d+)?\s*(?:mb|gb|kb)\b'),
            # Track/season-episode labels like S01E05
            _re.compile(r'(?i)\b(?:s[0-9]{1,2}e[0-9]{1,2})(?=\s|$)'),
            # Trailing duplicate markers ONLY when preceded by a word character:
            # " (1)" " (2)" at END of string means duplicate copy — safe to strip.
            # (12), (80), etc. should NOT be stripped — they are often episode numbers!
            # Only strip SINGLE digits (1-9) as trailing copy indicators.
            _re.compile(r'(?<=\w)\s*\(\s*[1-9]\s*\)\s*$'),
            _re.compile(r'(?<=\w)\s*\[\s*[1-9]\s*\]\s*$'),
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
            Ultra-robust episode extraction combining both range detection
            and smart fallback logic for single episodes.
            """
            c = _clean(text)
            
            # 1. Comma / Space sequence of numbers (e.g. 1 2 3 4 5, or 10,11,12)
            # Must strictly be multiple isolated numbers
            s = _re.search(r'(?<!\d)(\d{1,4}(?:(?:,\s*|\s+)\d{1,4}){2,})(?!\d)', c)
            if s:
                nums = [int(x) for x in _re.findall(r'\d+', s.group(1))]
                if max(nums) < 5000: return (min(nums), max(nums), True)

            # 2. explicit range with '-', 'to' etc
            r = _re.search(r'(?<!\d)(\d{1,4}(?:(?:\s*[-\u2013\u2014]|(?i:\s+to\s+))\s*\d{1,4})+)(?!\d)', c)
            if r:
                nums = [int(x) for x in _re.findall(r'\d+', r.group(1))]
                if max(nums) < 5000: return (min(nums), max(nums), True)
                
            # 3. Explicit keywords: "Ep 23", "Episode 23", "Part 23", "Ch 2", hindi
            kw = _re.search(r'(?i)\b(?:ep|episode|e|ch|chapter|part|एपिसोड|भाग)\s*(\d{1,4})(?!\d)', c)
            if kw:
                n = int(kw.group(1))
                if 0 < n < 5000: return (n, n, False)
                
            # 4. Fallback: take the largest standalone number in the text
            # Since words can have numbers embedded like 'Veera66angadh', separate words from numbers first
            c2 = _re.sub(r'([a-zA-Z])(\d)', r'\1 \2', c)
            c2 = _re.sub(r'(\d)([a-zA-Z])', r'\1 \2', c2)
            
            nums = [int(x) for x in _re.findall(r'(?<!\d)(\d{1,4})(?!\d)', c2) if 0 < int(x) < 5000]
            if nums:
                # take the LARGEST number, not the rightmost, since trailing numbers are often '2' for duplicates
                return (max(nums), max(nums), False)
                
            return None

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

        def extract_ep_individual(msg):
            """
            Deep episode extraction — individual mode.
            Now correctly uses range parsing so range files span multiple episodes.
            """
            # Priority 1: file_name only (most reliable)
            for fname in _get_file_names(msg):
                r = _extract_range_from_text(fname)
                if r: return r

            # Priority 2: caption text
            cap = msg.caption or msg.text or ""
            if cap.strip():
                r = _extract_range_from_text(cap)
                if r: return r

            # Priority 3: audio title (last resort — often has track numbers)
            t = _get_audio_title(msg)
            if t:
                r = _extract_range_from_text(t)
                if r: return r

            return (-1, -1, False)

        def extract_ep_grouped(msg):
            """
            Deep episode extraction — grouped mode.
            Tries to find a range (start–end). Falls back to individual.
            """
            # Priority 1: file_name (range aware)
            for fname in _get_file_names(msg):
                r = _extract_range_from_text(fname)
                if r: return r

            # Priority 2: caption
            cap = msg.caption or msg.text or ""
            if cap.strip():
                r = _extract_range_from_text(cap)
                if r: return r

            # Priority 3: audio title
            t = _get_audio_title(msg)
            if t:
                r = _extract_range_from_text(t)
                if r: return r

            return (-1, -1, False)

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
        range_hint_count = 0
        for msg in all_valid_msgs:
            cap = msg.caption or msg.text or ""
            if _re.search(r'(?<!\d)\d{1,4}\s*[-–—]\s*\d{1,4}(?!\d)', cap):
                range_hint_count += 1
        GROUPED_MODE = range_hint_count > (len(all_valid_msgs) * 0.50)

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

        total_count = len(parsed_msgs)  # used in final report


        #  Build ep_to_msgs dict and track duplicates 
        ep_to_msgs: dict = {}      # ep_start → [msg_ids]
        duplicate_eps:  list = []  # list of ep numbers with >1 file
        grouped_files:  list = []  # list of "(name, start-end)" for grouped files

        for msg, ep_s, ep_e, is_r in parsed_msgs:
            if is_r:
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
        duplicate_eps = sorted(set(ep for ep, ids in ep_to_msgs.items() if len(ids) > 1))


        all_ep_nums    = sorted(ep_to_msgs.keys())
        first_ep_num   = all_ep_nums[0] if all_ep_nums else 0
        last_ep_num    = all_ep_nums[-1] if all_ep_nums else 0

        # Missing episode detection (only meaningful in individual mode)
        missing_eps: list = []
        if not GROUPED_MODE and all_ep_nums:
            expected_range = set(range(first_ep_num, last_ep_num + 1))
            present_set    = set(all_ep_nums)
            missing_eps    = sorted(expected_range - present_set)

        #  BUILD BUCKETS 
        # GROUPED_MODE: each file = 1 button using its own range label
        # INDIVIDUAL_MODE: bucket by batch_size
        buckets = []  # list of (label_start, label_end, [msg_ids])

        if GROUPED_MODE:
            # Each grouped file becomes exactly one button
            # For mixed (some individual, some grouped): still one button per entry
            for msg, ep_s, ep_e, is_r in parsed_msgs:
                mids = ep_to_msgs.get(ep_s, [])
                # Deduplicate: only take the first msg for each ep_s
                if mids and mids[0] == msg.id:
                    buckets.append((ep_s, ep_e, [msg.id]))
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

        raw_buttons = []
        for b_s, b_e, mids in buckets:
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

        # Calculate unparseable count for the display report
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
            def _bold(t):
                r = ""
                for c in str(t):
                    if 'A' <= c <= 'Z': r += chr(0x1D5D4 + ord(c) - ord('A'))
                    elif 'a' <= c <= 'z': r += chr(0x1D5EE + ord(c) - ord('a'))
                    else: r += c
                return r
            txt = f"{_bold(story.upper())} {_bold('EPS')} {first_ep}–{last_ep}"
            keyboard = []
            for j in range(0, len(chunk), 2):
                row = [c["btn"] for c in chunk[j:j + 2]]
                keyboard.append(row)
            keyboard.append([
                InlineKeyboardButton(_sc("Tutorial") + " » ", url="https://t.me/StoriesLinkopningguide"),
                InlineKeyboardButton(_sc("Support") + " » ", url="https://t.me/+EAc-6v1bmZ1iMDBl")
            ])
            for attempt in range(6):
                try:
                    await poster.send_message(
                        chat_id=sj['target'], text=txt,
                        reply_markup=InlineKeyboardMarkup(keyboard)
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
            f"",
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

        if missing_eps and not GROUPED_MODE:
            miss_preview = ", ".join(str(e) for e in missing_eps[:15])
            if len(missing_eps) > 15:
                miss_preview += f" (+{len(missing_eps)-15} more)"
            report_lines.append(f"»  <b>Missing episodes ({len(missing_eps)}):</b> {miss_preview}")

        if unparseable_msgs_list:
            report_lines.append(f"🚫  <b>Unparseable messages ({len(unparseable_msgs_list)}):</b> Kept at original positions inside buttons.")


        report_lines.append(f"")
        report_lines.append(f"<i>Users click any button to receive their episodes from @{bot_usr}.</i>")

        await safe_edit("\n".join(report_lines))

        #  SEND DOWNLOADABLE REPORT FILE 
        import io, datetime
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
        plain_report = [
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
            plain_report.append("-" * 50)
            plain_report.append(f"DUPLICATES DETECTED — all files kept ({len(duplicate_eps)}):")
            plain_report.append("  " + ", ".join(str(e) for e in duplicate_eps))
        if missing_eps and not GROUPED_MODE:
            plain_report.append("-" * 50)
            plain_report.append(f"MISSING EPISODES ({len(missing_eps)}):")
            plain_report.append("  " + ", ".join(str(e) for e in missing_eps))
        if unparseable_msgs_list:
            plain_report.append("-" * 50)
            plain_report.append(f"UNPARSEABLE FILES: {len(unparseable_msgs_list)}")
            plain_report.append(f"  These files had no readable episode number in their name.")
            plain_report.append(f"  They were naturally embedded into the buttons at their original chronological positions.")
            plain_report.append("  IDs: " + ", ".join(str(m) for m in unparseable_msgs_list))
        plain_report += [
            "=" * 50,
            "Note: Duplicates = multiple files had the same episode number.",
            "ALL were included — nothing was skipped.",
            "-" * 50,
            "This report is auto-generated. | Powered by Arya Bot",
            "=" * 50,
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
                    _sc(". I have tried to make everything correct and have also provided"
                        " you with a final report containing all details. Some episodes may"
                        " naturally be missing — nothing can be done about that. But if 10+"
                        " episodes are missing, you can complain in support. Non-logical or"
                        " unparsed files will be available in '»  Extra/Skipped files'."
                        " Some duplicates are shown — either real duplicates, or the uploader"
                        " uploaded multiple files with the same name. I am not responsible"
                        " as these files were forwarded via Arya bot and not scraped.")
                )

                hi_body = (
                    f"यह {story_sz} {bot_link} द्वारा पूरी कर दी गई है। मैंने सब कुछ सही करने की"
                    " कोशिश की है और आपको सभी विवरणों के साथ एक अंतिम रिपोर्ट भी प्रदान की है।"
                    " कुछ एपिसोड स्वाभाविक रूप से गायब हो सकते हैं — कृपया घबराएं नहीं,"
                    " उसका कुछ नहीं किया जा सकता। लेकिन अगर 10+ एपिसोड गायब हैं, तो आप"
                    " सपोर्ट में शिकायत कर सकते हैं। गैर-तार्किक या अनपार्स की गई फ़ाइलें"
                    " '»  Extra/Skipped files' में उपलब्ध होंगी। कुछ डुप्लिकेट दिखाए गए हैं —"
                    " या तो वे वास्तविक डुप्लिकेट हैं, या अपलोडर ने एक ही नाम से कई फ़ाइलें"
                    " अपलोड की हैं। मैं जिम्मेदार नहीं हूँ क्योंकि ये फ़ाइलें आर्या बॉट के"
                    " माध्यम से अग्रेषित की गई थीं, स्क्रैप नहीं की गई थीं।"
                )

                dm_cap = (
                    f"<b>Report File</b>\n\n<blockquote>{dm_header}{en_body}</blockquote>"
                    f"\n\n<blockquote>{hi_body}</blockquote>"
                )
                ch_cap = (
                    f"<b>Report File</b>\n\n<blockquote>{ch_header}{en_body}</blockquote>"
                    f"\n\n<blockquote>{hi_body}</blockquote>"
                )

            else:
                dm_ongoing = (
                    f"›› {_sc('Hey')} <a href='tg://user?id={user_id}'>{u_name}</a>\n\n"
                    + _sc("I have posted all the files currently available. "
                           "As new episodes arrive, I will post them. Enjoy!")
                )
                ch_ongoing = (
                    f"›› {_sc('Hey Strangers')}\n\n"
                    + _sc("All currently available files have been posted here. "
                           "New episodes will be added as they arrive. Enjoy and stay tuned!")
                )
                dm_cap = f"<b>Status</b>\n\n<blockquote>{dm_ongoing}</blockquote>"
                ch_cap = f"<b>Status</b>\n\n<blockquote>{ch_ongoing}</blockquote>"

            # Send to admin DM — independent of channel
            try:
                await bot.send_document(
                    user_id, report_bytes,
                    caption=dm_cap, parse_mode="html",
                    file_name=report_bytes.name
                )
            except Exception as dm_err:
                logger.error(f"[Report] DM send failed: {dm_err}", exc_info=True)

            # Send to target channel — always attempted independently
            try:
                report_bytes.seek(0)
                await poster.send_document(
                    sj['target'], report_bytes,
                    caption=ch_cap, parse_mode="html",
                    file_name=report_bytes.name
                )
            except Exception as ch_err:
                logger.error(f"[Report] Channel send failed: {ch_err}", exc_info=True)

        except Exception as rep_err:
            logger.error(f"[Report] Could not prepare report: {rep_err}", exc_info=True)

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
