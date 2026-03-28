"""
Share Batch Links Automator
===========================
Generates File-Sharing deep links from a hidden database channel
and automatically posts the grouped batch buttons into a Public Channel.
"""
import uuid
import math
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from database import db
from plugins.test import CLIENT
from plugins.jobs import _ask

_CLIENT = CLIENT()

new_share_job = {}

@Client.on_callback_query(filters.regex(r'^sl#'))
async def sl_callback(bot, query):
    user_id = query.from_user.id
    data = query.data.split('#')
    cmd = data[1]

    if cmd == "start":
        await query.message.delete()
        # 1. Accounts
        bots = await db.get_bots(user_id)
        if not bots:
            return await bot.send_message(user_id, "<b>❌ No accounts. Add one in /settings → Accounts first.</b>")
            
        new_share_job[user_id] = {}
        
        btns = []
        
        # Explicitly add the Share Bot Token if it's set
        share_token = await db.get_share_bot_token()
        if share_token:
            btns.append([InlineKeyboardButton("🤖 (Dedicated) Share Bot", callback_data="sl#acc_SHAREBOT")])
        
        for b in bots:
            typ = "🤖" if b.get('is_bot', True) else "👤"
            btns.append([InlineKeyboardButton(f"{typ} {b['name']}", callback_data=f"sl#acc_{b['id']}")])
        btns.append([InlineKeyboardButton("❌ Cancel", callback_data="back")])
        
        await bot.send_message(
            user_id,
            "<b>❪ SHARE LINKS: SELECT ACCOUNT ❫</b>\n\nChoose the account that has Admin access to both the Source Database Channel and Target Channel:",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif cmd.startswith("acc_"):
        bot_id = cmd.split('_')[1]
        new_share_job[user_id]['bot_id'] = bot_id
        
        await query.message.edit_text("<i>Loading channels...</i>")
        chans = await db.get_user_channels(user_id)
        if not chans:
            return await query.message.edit_text("<b>❌ No channels added in /settings.</b>")
            
        btns = []
        for c in chans:
            btns.append([InlineKeyboardButton(c['title'], callback_data=f"sl#src_{c['chat_id']}")])
        btns.append([InlineKeyboardButton("❌ Cancel", callback_data="back")])
        
        await query.message.edit_text(
            "<b>❪ STEP 2: SOURCE DATABASE ❫</b>\n\nWhere are the files stored securely?",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif cmd.startswith("src_"):
        src_id = int(cmd.split('_')[1])
        new_share_job[user_id]['source'] = src_id
        
        chans = await db.get_user_channels(user_id)
        btns = []
        for c in chans:
            btns.append([InlineKeyboardButton(c['title'], callback_data=f"sl#tgt_{c['chat_id']}")])
        btns.append([InlineKeyboardButton("❌ Cancel", callback_data="back")])
        
        await query.message.edit_text(
            "<b>❪ STEP 3: TARGET PUBLIC CHANNEL ❫</b>\n\nWhere should I post the Share Links?",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif cmd.startswith("tgt_"):
        tgt_id = int(cmd.split('_')[1])
        new_share_job[user_id]['target'] = tgt_id
        
        await query.message.delete()
        
        # Ranges
        try:
            msg = await _ask(bot, user_id, "<b>❪ STEP 4: MESSAGE RANGE ❫</b>\n\nFormat: <code>START_ID-END_ID</code>\nExample: <code>1-558</code>\n\nType /cancel to abort.")
            if msg.text == "/cancel": return await msg.reply_text("Cancelled.")
            
            parts = msg.text.strip().split('-')
            start_id, end_id = int(parts[0]), int(parts[1])
            new_share_job[user_id]['start_id'] = start_id
            new_share_job[user_id]['end_id'] = end_id
            
            # Batch Size
            msg2 = await _ask(bot, user_id, "<b>❪ STEP 5: EPISODES PER LINK ❫</b>\n\nHow many files should be grouped in one link?\nExample: <code>50</code>")
            batch_size = int(msg2.text.strip())
            new_share_job[user_id]['batch_size'] = batch_size
            
            sj = new_share_job[user_id]
            total_msgs = (sj['end_id'] - sj['start_id']) + 1
            total_links = math.ceil(total_msgs / sj['batch_size'])
            
            btn = [[InlineKeyboardButton("🚀 Generate & Post Links", callback_data="sl#build")]]
            await bot.send_message(
                user_id,
                f"<b>📋 CONFIRM SHARE BATCH</b>\n\n"
                f"<b>Source ID:</b> <code>{sj['source']}</code>\n"
                f"<b>Target ID:</b> <code>{sj['target']}</code>\n"
                f"<b>Range:</b> {sj['start_id']} to {sj['end_id']} ({total_msgs} files)\n"
                f"<b>Batch Size:</b> {sj['batch_size']} files per link\n"
                f"<b>Total Posts to create:</b> {total_links}\n",
                reply_markup=InlineKeyboardMarkup(btn)
            )
        except Exception as e:
            await bot.send_message(user_id, f"<b>Error parsing input:</b> {e}")


    elif cmd == "build":
        sj = new_share_job.get(user_id)
        if not sj: return await query.answer("Expired session.", show_alert=True)
        
        # Check token
        token = await db.get_share_bot_token()
        if not token:
            return await query.answer("❌ You must set the Share Bot Token in /settings first!", show_alert=True)
        
        from plugins.share_bot import share_client
        if not share_client:
            return await query.answer("❌ Share Bot is not running. Check token in settings.", show_alert=True)
            
        bot_usr = share_client.me.username if share_client.me else "ShareBot"
        
        if sj['bot_id'] == "SHAREBOT":
            # Direct use of the Share Bot client
            worker = share_client
        else:
            from plugins.test import start_clone_bot
            worker = await start_clone_bot(_CLIENT, sj['bot_id'])
            
        if not worker:
            return await query.message.edit_text("❌ Failed to start worker account.")

        await query.message.edit_text("<i>⏳ Generating links and posting... Please wait.</i>")
        
        try:
            current_id = sj['start_id']
            end_ep = sj['end_id']
            chunk_size = sj['batch_size']
            
            post_count = 0
            
            while current_id <= end_ep:
                chunk_end = min(current_id + chunk_size - 1, end_ep)
                
                # We need exact message IDs
                msg_ids = list(range(current_id, chunk_end + 1))
                
                # Check valid messages in source
                valid_ids = []
                messages = await worker.get_messages(sj['source'], msg_ids)
                for m in messages:
                    if m.empty or m.service: continue
                    valid_ids.append(m.id)
                
                if valid_ids:
                    # Save mapping in DB
                    uuid_str = str(uuid.uuid4()).replace('-', '')[:16]
                    await db.save_share_link(uuid_str, valid_ids, sj['source'])
                    
                    # Target post UI
                    txt = f"<b>Episodes {valid_ids[0]} to {valid_ids[-1]}</b>"
                    url = f"https://t.me/{bot_usr}?start={uuid_str}"
                    btn = [[InlineKeyboardButton(f"▶️ Ep {valid_ids[0]} - {valid_ids[-1]}", url=url)]]
                    
                    # Post to target
                    await worker.send_message(
                        chat_id=sj['target'],
                        text=txt,
                        reply_markup=InlineKeyboardMarkup(btn)
                    )
                    post_count += 1
                    
                current_id = chunk_end + 1
                await asyncio.sleep(2) # Floodwaits
                
            await query.message.edit_text(f"<b>✅ Completed!</b>\n\nGenerated ({post_count}) batch posts mapped to {bot_usr}.")
            
        except Exception as e:
            await query.message.reply_text(f"<b>Error during linking:</b>\n<code>{e}</code>")
        finally:
            del new_share_job[user_id]
