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
        bots = await db.get_bots(user_id)
        if not bots:
            return await bot.send_message(user_id, "<b>❌ No accounts. Add one in /settings → Accounts first.</b>")
            
        kb = []
        share_token = await db.get_share_bot_token()
        if share_token:
            kb.append(["🤖 (Dedicated) Share Bot"])
            
        for b in bots:
            typ = "🤖" if b.get('is_bot', True) else "👤"
            kb.append([f"{typ} {b['name']}"])
            
        kb.append(["❌ Cancel"])
        
        msg = await _ask(bot, user_id, 
            "<b>❪ SHARE LINKS: SELECT ACCOUNT ❫</b>\n\nChoose the account that has Admin access to both the Source Database Channel and Target Channel:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
        )
        if not msg.text or msg.text == "/cancel" or "Cancel" in msg.text:
            return await bot.send_message(user_id, "<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            
        if "Share Bot" in msg.text:
            new_share_job[user_id]['bot_id'] = "SHAREBOT"
        else:
            sel_name = msg.text.split(" ", 1)[1] if " " in msg.text else msg.text
            acc = next((a for a in bots if a["name"] == sel_name), None)
            if not acc:
                return await bot.send_message(user_id, "<b>❌ Account not found.</b>", reply_markup=ReplyKeyboardRemove())
            new_share_job[user_id]['bot_id'] = acc['id']

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
            "<b>❪ STEP 7: EPISODES PER LINK ❫</b>\n\nHow many files should be grouped in one link button?\nExample: <code>20</code>", 
            reply_markup=markup
        )
        if (msg_batch.text or "") == "/cancel": return await bot.send_message(user_id, "Cancelled.", reply_markup=ReplyKeyboardRemove())
        
        raw_b = (msg_batch.text or msg_batch.caption or "20").strip()
        batch_size = int(raw_b) if raw_b.isdigit() else 20
        if batch_size < 1: batch_size = 20
        new_share_job[user_id]['batch_size'] = batch_size
        
        sj = new_share_job[user_id]
        total_msgs = (sj['end_id'] - sj['start_id']) + 1
        total_links = math.ceil(total_msgs / sj['batch_size'])
        total_posts = math.ceil(total_links / 10)
        
        markup_conf = ReplyKeyboardMarkup([["🚀 Generate & Group Links"], ["❌ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
        conf_msg = await _ask(bot, user_id,
            f"<b>📋 CONFIRM SHARE BATCH</b>\n\n"
            f"<b>Story Name:</b> {sj['story']}\n"
            f"<b>Source ID:</b> <code>{sj['source']}</code>\n"
            f"<b>Target ID:</b> <code>{sj['target']}</code>\n"
            f"<b>Range:</b> {sj['start_id']} to {sj['end_id']} ({total_msgs} files)\n"
            f"<b>Batch Size:</b> {sj['batch_size']} files per link\n"
            f"<b>Total Buttons to create:</b> {total_links}\n"
            f"<b>Total Grouped Posts (10 btns each):</b> {total_posts}\n",
            reply_markup=markup_conf
        )
        
        if not conf_msg.text or conf_msg.text == "/cancel" or "Cancel" in conf_msg.text:
            if user_id in new_share_job: del new_share_job[user_id]
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
        token = await db.get_share_bot_token()
        if not token:
            return await safe_edit("❌ You must set the Share Bot Token in /settings first!")

        import plugins.share_bot as share_mod
        if not share_mod.share_client or not getattr(share_mod.share_client, 'is_connected', False):
            try:
                await share_mod.start_share_bot(token)
            except Exception:
                pass

        if not share_mod.share_client or not getattr(share_mod.share_client, 'is_connected', False):
            return await safe_edit("❌ Share Bot failed to start. Check terminal logs.")

        bot_usr = share_mod.share_client.me.username

        # If SHAREBOT selected → poster is the Share Bot (it must be admin in target channel)
        # The MAIN bot is used for scanning ONLY (it has the SQLite peer cache)
        if sj['bot_id'] == "SHAREBOT":
            poster = share_mod.share_client  # posts the link messages to target channel
        else:
            from plugins.test import start_clone_bot
            bot_info = await db.get_bot(sj['bot_id'])
            if not bot_info:
                return await safe_edit("❌ Worker account not found in DB.")
            poster = await start_clone_bot(_CLIENT.client(bot_info))

        if not poster:
            return await safe_edit("❌ Failed to start worker account.")

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

        protect  = await db.get_share_protect(user_id)
        auto_del = await db.get_share_autodelete(user_id)

        current_id = sj['start_id']
        end_ep     = sj['end_id']
        chunk_size = sj['batch_size']
        story      = sj['story']

        raw_buttons = []
        ep_counter  = 1  # Episode number (1-based, tracks across all batches)

        while current_id <= end_ep:
            chunk_end = min(current_id + chunk_size - 1, end_ep)
            msg_ids   = list(range(current_id, chunk_end + 1))

            # Step 2: Fetch messages via raw API on the MAIN BOT with the resolved peer
            # This completely bypasses Pyrogram's peer resolution on the worker side
            try:
                raw_result = await bot.invoke(
                    ChannelGetMessages(
                        channel=db_peer,
                        id=[InputMessageID(id=mid) for mid in msg_ids]
                    )
                )
                messages = raw_result.messages
            except Exception as e:
                return await safe_edit(
                    f"<b>❌ Scan Error:</b> <code>{e}</code>\n\n"
                    f"<i>Make sure the Main Bot is an admin in the database channel.</i>"
                )

            # Filter out empty/service messages
            valid_ids = [m.id for m in messages if hasattr(m, 'id') and not getattr(m, 'deleted', False) and m.id > 0 and m.QUALNAME if hasattr(m, 'QUALNAME') else m]

            # More robust filter: only keep actual media/document/text messages
            valid_ids = []
            for m in messages:
                # Raw Pyrogram message types: Message (has content), MessageEmpty, MessageService
                cls = type(m).__name__
                if cls in ('MessageEmpty', 'MessageService'):
                    continue
                valid_ids.append(m.id)

            if valid_ids:
                ep_start = ep_counter
                ep_end   = ep_counter + len(valid_ids) - 1

                uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
                await db.save_share_link(uuid_str, valid_ids, source_chat_id, protect, auto_del)

                url = f"https://t.me/{bot_usr}?start={uuid_str}"

                # Episode-named button label
                if len(valid_ids) == 1:
                    btn_text = f"{story} Ep. {ep_start}"
                else:
                    btn_text = f"{story} Ep. {ep_start}–{ep_end}"

                raw_buttons.append({
                    "btn": InlineKeyboardButton(btn_text, url=url),
                    "ep_start": ep_start,
                    "ep_end":   ep_end,
                })

                ep_counter = ep_end + 1

            current_id = chunk_end + 1
            await asyncio.sleep(0.5)  # Flood-wait safety

        if not raw_buttons:
            return await safe_edit("❌ No valid messages found in the given range. Check Start/End IDs.")

        # Phase 2: Group into posts of 10 buttons (2 per row) and send to target channel
        post_count = 0
        for i in range(0, len(raw_buttons), 10):
            chunk = raw_buttons[i:i + 10]

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

            try:
                await poster.send_message(
                    chat_id=sj['target'],
                    text=txt,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e:
                return await safe_edit(
                    f"<b>❌ Failed to post to target channel:</b> <code>{e}</code>\n\n"
                    f"<i>Make sure the selected account is an admin in the target/public channel.</i>"
                )

            post_count += 1
            await asyncio.sleep(1)

        total_ep = ep_counter - 1
        await safe_edit(
            f"<b>✅ Share Links Generated!</b>\n\n"
            f"📊 <b>Episodes covered:</b> {total_ep}\n"
            f"🔗 <b>Links created:</b> {len(raw_buttons)}\n"
            f"📝 <b>Posts sent to channel:</b> {post_count}\n\n"
            f"<i>Users can now click any button to receive their episodes from @{bot_usr}.</i>"
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
        if user_id in new_share_job:
            del new_share_job[user_id]


