"""
Management UI for Arya Premium
==============================
Provides the exact Batch-Links style Inline Keyboard Menu 
for configuring the Marketplace instead of manual commands!
"""
import asyncio
import logging
import json
import tempfile
import time
from datetime import datetime
from pymongo.errors import PyMongoError
from pyrogram import Client, filters, enums
from pyrogram.errors import MessageNotModified
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove,
    CallbackQuery
)
from database import db
from config import Config
import utils
from utils import native_ask
def _is_cancel(msg):
    if hasattr(msg, "data") and msg.data == "ask_cancel":
        return True
    if hasattr(msg, "text") and msg.text and ("c\u1d00\u0274\u1d04\u1d07\u029f" in msg.text.lower() or "/cancel" in msg.text.lower()):
        return True
    return False

import time

logger = logging.getLogger(__name__)


def _is_owner(user_id: int) -> bool:
    return int(user_id) in set(Config.OWNER_IDS or [])


async def _deny_if_not_owner(client, user_id: int):
    if _is_owner(user_id):
        return False
    await client.send_message(user_id, "❌ Access denied. This panel is for owners only.")
    return True


async def _safe_answer(query, *args, **kwargs):
    try:
        return await query.answer(*args, **kwargs)
    except Exception:
        return None


async def _render_home(client, chat_id: int, *, edit_message=None):
    bots = await db.db.premium_bots.count_documents({})
    stories = await db.db.premium_stories.count_documents({})
    pendings = await db.db.premium_checkout.count_documents({"status": "pending_admin_approval"})
    buyers = await db.db.users.count_documents({"purchases.0": {"$exists": True}})
    db_ch = await db.db.premium_channels.count_documents({"type": "db"})
    dl_ch = await db.db.premium_channels.count_documents({"type": "delivery"})

    txt = (
        f"<b>⟦ 𝗠𝗔𝗥𝗞𝗘𝗧𝗣𝗟𝗔𝗖𝗘 𝗗𝗔𝗦𝗛𝗕𝗢𝗔𝗥𝗗 ⟧</b>\n\n"
        f'<blockquote expandable="true">'
        f"<b>⧉ ʙᴏᴛꜱ        ⟶</b> <code>{bots}</code>\n"
        f"<b>⧉ ꜱᴛᴏʀɪᴇꜱ     ⟶</b> <code>{stories}</code>\n"
        f"<b>⧉ ᴘᴇɴᴅɪɴɢ     ⟶</b> <code>{pendings}</code>\n"
        f"<b>⧉ ʙᴜʏᴇʀꜱ      ⟶</b> <code>{buyers}</code>\n"
        f"<b>⧉ ᴅʙ ᴄʜᴀɴɴᴇʟꜱ ⟶</b> <code>{db_ch}</code>\n"
        f"<b>⧉ ᴅᴇʟɪᴠᴇʀʏ    ⟶</b> <code>{dl_ch}</code>"
        f'</blockquote>\n'
        f'<blockquote expandable="true"><i>💡 <b>𝗧𝗶𝗽:</b> Use "Channels → Bulk Add" for large delivery pools.</i></blockquote>'
    )



    kb = [
        [InlineKeyboardButton("🛒 " + utils.to_smallcap("Add Story"), callback_data="mk#add_story"),
         InlineKeyboardButton("💸 " + utils.to_smallcap("Pending"), callback_data="mk#pending")],
        [InlineKeyboardButton("📝 " + utils.to_smallcap("Story Requests"), callback_data="mk#reqs_0")],
        [InlineKeyboardButton("📦 " + utils.to_smallcap("Manage Stories"), callback_data="mk#manage_stories"),
         InlineKeyboardButton("📡 " + utils.to_smallcap("Channels"), callback_data="mk#channels")],
        [InlineKeyboardButton("🤖 " + utils.to_smallcap("Accounts"), callback_data="mk#accounts"),
         InlineKeyboardButton("👥 " + utils.to_smallcap("Users"), callback_data="mk#users")],
        [InlineKeyboardButton("⚙️ " + utils.to_smallcap("Settings"), callback_data="mk#settings"),
         InlineKeyboardButton("🔄 " + utils.to_smallcap("Refresh"), callback_data="mk#refresh")],
        [InlineKeyboardButton("✖️ " + utils.to_smallcap("Close"), callback_data="mk#close")]
    ]
    markup = InlineKeyboardMarkup(kb)

    if edit_message:
        return await edit_message.edit_text(txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
    return await client.send_message(chat_id, txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML)




def _cfg_list(cfg: dict, key: str):
    v = cfg.get(key)
    return v if isinstance(v, list) else []


@Client.on_message(filters.command("start") & filters.private)
async def mgmt_start(client, message):
    user_id = message.from_user.id
    if await _deny_if_not_owner(client, user_id):
        return
    return await _render_home(client, user_id)

# Parse ID helper
def parse_id(msg) -> int:
    if getattr(msg, 'forward_from_message_id', None):
        return msg.forward_from_message_id
    text = (getattr(msg, 'text', None) or getattr(msg, 'caption', None) or "").strip().rstrip('/')
    if text.isdigit(): return int(text)
    if "t.me/" in text:
        parts = text.split('/')
        if parts[-1].isdigit(): return int(parts[-1])
    raise ValueError("Invalid Message ID or Link")


def parse_chat_from_link(text: str):
    t = (text or "").strip()
    if "t.me/c/" in t:
        import re
        m = re.search(r"t\.me/c/(\d+)/(\d+)", t)
        if m:
            return int("-100" + m.group(1)), int(m.group(2))
    if "t.me/" in t:
        import re
        m = re.search(r"t\.me/([^/\s]+)/(\d+)", t.replace("https://", "").replace("http://", ""))
        if m:
            return m.group(1), int(m.group(2))
    return None, None


@Client.on_callback_query(filters.regex(r'^mk#'))
async def market_callback(client, query):
    try:
        user_id = query.from_user.id
        if not _is_owner(user_id):
            return await _safe_answer(query, "Access denied.", show_alert=True)
        data = query.data.split('#')
        cmd = data[1]

        if cmd == "close":
            return await query.message.delete()
        
        elif cmd == "refresh":
            if "query" in locals() and query:
                await query.answer()
            return await _render_home(client, user_id, edit_message=query.message)
        
        elif cmd == "settings":
            if "query" in locals() and query:
                await query.answer()
            kb = [
                [InlineKeyboardButton("💳 Set UPI ID", callback_data="mk#set_upi")],
                [InlineKeyboardButton("« Back", callback_data="mk#back")]
            ]
            await query.message.edit_text("<b>⚙️ Ecosystem Settings</b>\n\nConfigure your global payment settings here. Channel management is available in Channels.", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("reqs_"):
            page = int(cmd.replace("reqs_", ""))
            reqs = await db.db.premium_requests.find({}).sort("created_at", -1).to_list(length=None)
            
            if not reqs:
                if "query" in locals() and query:
                    return await query.answer("No story requests found.", show_alert=True)
                return
                
            items_per_page = 10
            total_pages = max(1, (len(reqs) + items_per_page - 1) // items_per_page)
            if page < 0: page = 0
            if page >= total_pages: page = total_pages - 1
            
            subset = reqs[page*items_per_page : (page+1)*items_per_page]
            
            txt_req = f"<b>📝 User Story Requests (Page {page+1}/{total_pages})</b>\n\nClick on any request to manage it:"
            kb = []
            for r in subset:
                sname = r.get('story_name', 'Unknown')
                if len(sname) > 25: sname = sname[:22] + "..."
                status_emoji = {
                    "Sent": "📮", "Pending": "⏳", "Searching": "🔍", "Posting": "📤", "Posted": "✅", "Completed": "🎉"
                }.get(r.get('status', 'Sent'), "📌")
                kb.append([InlineKeyboardButton(f"{status_emoji} {sname} ({r.get('user_id')})", callback_data=f"mk#req_{str(r['_id'])}")])
            
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("❬ Prev", callback_data=f"mk#reqs_{page-1}"))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton("Next ❭", callback_data=f"mk#reqs_{page+1}"))
            if nav: kb.append(nav)
            
            kb.append([InlineKeyboardButton("« " + utils.to_smallcap("Home"), callback_data="mk#back")])
            
            await query.message.edit_text(txt_req, reply_markup=InlineKeyboardMarkup(kb))
            return

        elif cmd.startswith("req_"):
            req_id = cmd.replace("req_", "")
            try:
                from bson import ObjectId
                r = await db.db.premium_requests.find_one({"_id": ObjectId(req_id)})
            except: r = None
            
            if not r:
                if "query" in locals() and query:
                    return await query.answer("Request not found.", show_alert=True)
                return
                
            t_str = r.get("created_at").strftime('%d %b %Y, %H:%M') if r.get("created_at") else "Unknown"
            status = r.get('status', 'Sent')
            txt_d = (
                f"<b>📝 MANAGE STORY REQUEST</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>📖 Name:</b> {r.get('story_name')}\n"
                f"<b>🎧 Platform:</b> {r.get('platform')}\n"
                f"<b>📑 Type:</b> {r.get('completion_type', 'N/A')}\n"
                f"<b>👤 User ID:</b> <code>{r.get('user_id')}</code>\n"
                f"<b>🤖 Bot ID:</b> <code>{r.get('bot_id')}</code>\n"
                f"<b>📅 Date:</b> {t_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>📌 Status:</b> <code>{status}</code>\n\n"
                f"<i>Select a new status below to notify the user instantly:</i>"
            )
            
            kb = [
                [InlineKeyboardButton("Pending ⏳", callback_data=f"mk#rstat#{req_id}#Pending"),
                 InlineKeyboardButton("Searching 🔍", callback_data=f"mk#rstat#{req_id}#Searching")],
                [InlineKeyboardButton("Posting 📤", callback_data=f"mk#rstat#{req_id}#Posting"),
                 InlineKeyboardButton("Posted ✅", callback_data=f"mk#rstat#{req_id}#Posted")],
                [InlineKeyboardButton("Completed 🎉", callback_data=f"mk#rstat#{req_id}#Completed")],
                [InlineKeyboardButton("❌ Reject with Reason", callback_data=f"mk#req_rej#{req_id}")],
                [InlineKeyboardButton("« " + utils.to_smallcap("Back to List"), callback_data="mk#reqs_0")]
            ]
            await query.message.edit_text(txt_d, reply_markup=InlineKeyboardMarkup(kb))
            return

        elif cmd == "rstat":
            req_id = data[2]
            new_status = data[3]
            try:
                from bson import ObjectId
                r = await db.db.premium_requests.find_one({"_id": ObjectId(req_id)})
            except: r = None
            if not r: 
                if "query" in locals() and query:
                    return await query.answer("Not found.", show_alert=True)
                return
            
            await db.db.premium_requests.update_one({"_id": r['_id']}, {"$set": {"status": new_status, "updated_at": datetime.now()}})
            
            # Log to Arya Core Log
            try:
                from utils import log_arya_event
                user_info = await db.get_user(r.get('user_id'))
                await log_arya_event(
                    "STORY REQUEST UPDATED", r.get('user_id'), user_info or {}, 

                    f"<b>Story:</b> {r.get('story_name')}\n<b>Old Status:</b> {r.get('status')}\n<b>New Status:</b> {new_status}"
                )
            except Exception as e:
                logger.error(f"Failed to log request update: {e}")
                
            # Alert User via Store Bot
            try:
                bot_id_str = str(r.get('bot_id'))
                from plugins.userbot.market_seller import market_clients
                seller_cli = market_clients.get(bot_id_str)
                if seller_cli:
                    u_id = r.get('user_id')
                    alert_txt = (
                        f"🛎️ <b>Update on your Story Request!</b>\n\n"
                        f"<b>Story:</b> {r.get('story_name')}\n"
                        f"<b>New Status:</b> <code>{new_status}</code>\n\n"
                        f"<i>Check 'My Requests' in your Profile for more info!</i>"
                    )
                    await seller_cli.send_message(u_id, alert_txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Open Profile", callback_data="mb#main_profile")]]))
            except Exception as e:
                logger.error(f"Failed to DM user about request: {e}")
                
            if "query" in locals() and query:
                await query.answer(f"Status updated to {new_status} and user notified!", show_alert=True)
            
            # Reload manage screen directly
            query.data = f"mk#req_{req_id}"
            return await market_callback(client, query)

        elif cmd == "req_rej":
            req_id = data[2]
            await query.message.delete()
            import asyncio
            asyncio.create_task(_reject_request_flow(client, user_id, req_id))

        elif cmd == "back":
            if "query" in locals() and query:
                await query.answer()
            return await _render_home(client, user_id, edit_message=query.message)

        elif cmd in ["set_upi"]:
            await query.message.delete()
            asyncio.create_task(_settings_flow(client, user_id, cmd))

        # ── Channels System ──
        elif cmd == "channels":
            await _safe_answer(query)
            db_channels = await db.db.premium_channels.find({"type": "db"}).to_list(length=None)
            delivery_channels = await db.db.premium_channels.find({"type": "delivery"}).to_list(length=None)
            kb = [
                [InlineKeyboardButton(f"🗄 DB Channels ({len(db_channels)})", callback_data="mk#ch_list_db"),
                 InlineKeyboardButton(f"📢 Delivery Channels ({len(delivery_channels)})", callback_data="mk#ch_list_delivery")],
                [InlineKeyboardButton("➕ Add DB Channel", callback_data="mk#ch_add_db"),
                 InlineKeyboardButton("➕ Add Delivery Channel", callback_data="mk#ch_add_delivery")],
                [InlineKeyboardButton("📥 Bulk Add Delivery", callback_data="mk#ch_bulk_delivery")],
                [InlineKeyboardButton("🔄 Sync Names", callback_data="mk#ch_sync_db"),
                 InlineKeyboardButton("🔄 Sync Delivery", callback_data="mk#ch_sync_delivery")],
                [InlineKeyboardButton("« Back", callback_data="mk#back")]
            ]
            await query.message.edit_text(
                "<b>📡 Channels Manager</b>\n\n"
                "<b>DB Channels:</b> Source channels containing story files.\n"
                "<b>Delivery Channels:</b> Channels used to generate one-time invite links for buyers.\n\n"
                "<i>Like main Arya Bot, manage channels from one panel and reuse them in story setup.</i>",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("ch_list_"):
            await _safe_answer(query)
            parts = cmd.split("_")
            ch_type = parts[2]
            page = int(parts[3]) if len(parts) > 3 else 0
            
            channels = await db.db.premium_channels.find({"type": ch_type}).to_list(length=None)
            label = "🗄 DB" if ch_type == "db" else "📢 Delivery"
            
            items_per_page = 10
            total_pages = max(1, (len(channels) + items_per_page - 1) // items_per_page)
            if page >= total_pages: page = total_pages - 1
            if page < 0: page = 0
            
            start_idx = page * items_per_page
            end_idx = start_idx + items_per_page
            pg_chans = channels[start_idx:end_idx]
            
            kb = []
            for ch in pg_chans:
                cid = ch.get('channel_id')
                name = ch.get('name', str(cid))
                kb.append([InlineKeyboardButton(f"{name} ({cid})", callback_data=f"mk#ch_view_{ch_type}_{cid}")])
            
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"mk#ch_list_{ch_type}_{page-1}"))
            if total_pages > 1:
                nav_row.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="mk#ignore"))
            if page < total_pages - 1:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"mk#ch_list_{ch_type}_{page+1}"))
            if nav_row:
                kb.append(nav_row)

            kb.append([InlineKeyboardButton(f"➕ Add {label}", callback_data=f"mk#ch_add_{ch_type}")])
            if len(channels) > 1:
                kb.append([InlineKeyboardButton("🗑 Delete All", callback_data=f"mk#ch_delall_{ch_type}")])
            kb.append([InlineKeyboardButton("🔄 Sync Names", callback_data=f"mk#ch_sync_{ch_type}")])
            kb.append([InlineKeyboardButton("« Back", callback_data="mk#channels")])
            
            ch_lines = "\n".join(f"• <code>{c['channel_id']}</code> — {c.get('name', '?')}" for c in pg_chans) or "<i>None added yet.</i>"
            await query.message.edit_text(
                f"<b>{label} Channels {f'(Page {page+1}/{total_pages})' if total_pages > 1 else ''}</b>\n\n{ch_lines}",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("ch_view_"):
            parts = cmd.split("_")
            ch_type = parts[2]
            ch_id = int(parts[3])
            await _safe_answer(query)
            kb = [
                [InlineKeyboardButton("🗑 Remove", callback_data=f"mk#ch_rm_{ch_type}_{ch_id}")],
                [InlineKeyboardButton("« Back", callback_data=f"mk#ch_list_{ch_type}")]
            ]
            await query.message.edit_text(
                f"<b>Channel Info</b>\n\n<b>ID:</b> <code>{ch_id}</code>\n<b>Type:</b> {ch_type}",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("ch_rm_"):
            parts = cmd.split("_")
            ch_type = parts[2]
            ch_id = int(parts[3])
            await db.db.premium_channels.delete_one({"channel_id": ch_id, "type": ch_type})
            await _safe_answer(query, "Channel Removed!", show_alert=True)
            query.data = f"mk#ch_list_{ch_type}"
            return await market_callback(client, query)

        elif cmd.startswith("ch_delall_"):
            ch_type = cmd.replace("ch_delall_", "")
            await db.db.premium_channels.delete_many({"type": ch_type})
            await _safe_answer(query, "All channels deleted!", show_alert=True)
            query.data = "mk#channels"
            return await market_callback(client, query)

        elif cmd.startswith("ch_sync_"):
            ch_type = cmd.replace("ch_sync_", "")
            channels = await db.db.premium_channels.find({"type": ch_type}).to_list(length=None)
            updated = failed = 0
            for ch in channels:
                try:
                    info = await client.get_chat(ch["channel_id"])
                    title = getattr(info, "title", None) or ch.get("name") or str(ch["channel_id"])
                    await db.db.premium_channels.update_one(
                        {"_id": ch["_id"]},
                        {"$set": {"name": title}}
                    )
                    updated += 1
                except Exception:
                    failed += 1
            await _safe_answer(query, f"Sync done: {updated} updated, {failed} failed", show_alert=True)
            query.data = f"mk#ch_list_{ch_type}"
            return await market_callback(client, query)

        elif cmd.startswith("ch_add_"):
            ch_type = cmd.replace("ch_add_", "")
            await query.message.delete()
            asyncio.create_task(_add_channel_flow(client, user_id, ch_type))

        elif cmd == "ch_bulk_delivery":
            await query.message.delete()
            asyncio.create_task(_bulk_add_delivery_channels(client, user_id))

        elif cmd == "accounts":
            await _safe_answer(query)
            bots = await db.db.premium_bots.find().to_list(length=10)
            kb = []
            for b in bots:
                kb.append([InlineKeyboardButton(f"{b.get('name', b.get('username', 'Bot'))}", callback_data=f"mk#bot_view_{b['id']}")])
            kb.append([InlineKeyboardButton('➕ Aᴅᴅ Bᴏᴛ', callback_data="mk#add_bot")])
            kb.append([InlineKeyboardButton("« Back", callback_data="mk#back")])
            await query.message.edit_text("<b>🤖 <u>Premium Accounts</u></b>\n\nSelect a bot to configure, or add a new one:", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd == "users":
            await _safe_answer(query)
            users = await db.db.users.find({"purchases.0": {"$exists": True}}).sort("id", -1).to_list(length=100)
            kb = []
            for u in users:
                uid = u.get("id")
                kb.append([InlineKeyboardButton(f"👤 {uid} • stories: {len(u.get('purchases', []))}", callback_data=f"mk#usr_view_{uid}")])
            if users:
                kb.append([InlineKeyboardButton("📤 Export All Users Snapshot", callback_data="mk#usr_export_all")])
            kb.append([InlineKeyboardButton("« Back", callback_data="mk#back")])
            await query.message.edit_text(
                f"<b>👥 Buyers ({len(users)})</b>\n\nTap a user to view profile and payment history.",
                reply_markup=InlineKeyboardMarkup(kb),
            )

        elif cmd.startswith("usr_view_"):
            uid = int(cmd.split("_")[2])
            user_doc = await db.db.users.find_one({"id": uid}) or {"id": uid}
            tg_user = None
            try:
                tg_user = await client.get_users(uid)
            except Exception:
                pass
            purchases = user_doc.get("purchases", [])
            used_channels = user_doc.get("used_channels", [])
            joined = user_doc.get("joined_date", "N/A")
            lang = user_doc.get("lang", "en")

            checkouts = await db.db.premium_checkout.find({"user_id": uid}).sort("_id", -1).to_list(length=20)
            
            
            if hasattr(joined, "strftime"):
                joined = joined.strftime('%d %b %Y')

            name = f"{getattr(tg_user, 'first_name', '') or ''} {getattr(tg_user, 'last_name', '') or ''}".strip() or "Unknown"
            uname = f"@{tg_user.username}" if tg_user and tg_user.username else "N/A"
            lang_label = "English" if lang == 'en' else "हिंदी"
            
            # Payment history — clean, no emojis
            lines = []
            for c in checkouts[:6]:
                st = await db.db.premium_stories.find_one({"_id": c.get("story_id")})
                sn = st.get("story_name_en", "Unknown") if st else "Deleted Story"
                stt = c.get('status', 'unknown')
                status_label = {
                    "approved": "PAID",
                    "waiting_screenshot": "PENDING",
                    "rejected": "REJECTED",
                    "pending_gateway": "PROCESSING",
                }.get(stt, stt.upper())
                mthd = c.get('method', 'unknown').upper()
                lines.append(f"<b>»</b> {sn}\n  <code>{mthd}</code>  ·  {utils.to_smallcap(status_label)}")
            history = "\n".join(lines) if lines else "  ɴᴏ ᴘᴀʏᴍᴇɴᴛ ʜɪꜱᴛᴏʀʏ ꜰᴏᴜɴᴅ"
            
            txt = (
                "<b>╔═⟦ 𝗣𝗥𝗢𝗙𝗜𝗟𝗘 ⟧═╗</b>\n\n"
                f"<b>⧉ ɴᴀᴍᴇ        ⟶</b> {name}\n"
                f"<b>⧉ ᴜꜱᴇʀɴᴀᴍᴇ    ⟶</b> {uname}\n"
                f"<b>⧉ ᴛɢ ɪᴅ       ⟶</b> <code>{uid}</code>\n\n"
                "<b>╠══════════════════╣</b>\n\n"
                f"<b>⧉ ᴘᴜʀᴄʜᴀꜱᴇꜱ   ⟶</b> {len(purchases)}\n"
                f"<b>⧉ ʟᴀɴɢᴜᴀɢᴇ    ⟶</b> {lang_label}\n"
                f"<b>⧉ ᴊᴏɪɴᴇᴅ      ⟶</b> {joined}\n\n"
                "<b>╠══════════════════╣</b>\n\n"
                f"<b>⧉ ᴘᴀʏᴍᴇɴᴛ ʜɪꜱᴛᴏʀʏ</b>\n"
                f"{history}\n\n"
                "<b>╚══════════════════╝</b>"
            )
            kb = [
                [InlineKeyboardButton("📄 Export User Data", callback_data=f"mk#usr_export_{uid}")],
                [InlineKeyboardButton("🧾 View Payment Entries", callback_data=f"mk#usr_pay_{uid}")],
                [InlineKeyboardButton("« Back", callback_data="mk#users")],
            ]
            await query.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("usr_pay_"):
            uid = int(cmd.split("_")[2])
            checkouts = await db.db.premium_checkout.find({"user_id": uid}).sort("_id", -1).to_list(length=30)
            kb = []
            for c in checkouts:
                sid = c.get("story_id")
                st = await db.db.premium_stories.find_one({"_id": sid})
                sn = st.get("story_name_en", "Unknown") if st else "Deleted"
                kb.append([InlineKeyboardButton(f"{sn} | {c.get('method','?')} | {c.get('status','?')}", callback_data=f"mk#usr_payv_{str(c['_id'])}")])
            kb.append([InlineKeyboardButton("« Back", callback_data=f"mk#usr_view_{uid}")])
            await query.message.edit_text(f"<b>🧾 Payment History for {uid}</b>", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("usr_payv_"):
            from bson.objectid import ObjectId
            pid = cmd.split("_")[2]
            c = await db.db.premium_checkout.find_one({"_id": ObjectId(pid)})
            if not c:
                return await _safe_answer(query, "Entry not found.", show_alert=True)
            st = await db.db.premium_stories.find_one({"_id": c.get("story_id")})
            sn = st.get("story_name_en", "Unknown") if st else "Deleted"
            txt = (
                f"<b>Payment Entry</b>\n\n"
                f"<b>User:</b> <code>{c.get('user_id')}</code>\n"
                f"<b>Story:</b> {sn}\n"
                f"<b>Method:</b> {c.get('method', 'unknown')}\n"
                f"<b>Status:</b> {c.get('status', 'unknown')}\n"
                f"<b>Created:</b> {c.get('created_at', 'N/A')}\n"
                f"<b>Paid:</b> {c.get('paid_at', 'N/A')}\n"
                f"<b>Approved:</b> {c.get('approved_at', 'N/A')}\n"
                f"<b>Reject Reason:</b> {c.get('reject_reason', 'N/A')}\n"
            )
            kb = [[InlineKeyboardButton("« Back", callback_data=f"mk#usr_pay_{c.get('user_id')}")]]
            await query.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))
            if c.get("proof_file_id"):
                try:
                    await client.send_photo(user_id, c["proof_file_id"], caption=f"Manual proof • user {c.get('user_id')} • {sn}")
                except Exception:
                    pass

        elif cmd.startswith("usr_export_") or cmd == "usr_export_all":
            uid = None if cmd == "usr_export_all" else int(cmd.split("_")[2])
            payload = {}
            if uid is None:
                payload["users"] = await db.db.users.find({"purchases.0": {"$exists": True}}).to_list(length=500)
                payload["payments"] = await db.db.premium_checkout.find().sort("_id", -1).to_list(length=1000)
                back_cb = "mk#users"
                title = "premium_users_export_all.json"
            else:
                payload["user"] = await db.db.users.find_one({"id": uid})
                payload["payments"] = await db.db.premium_checkout.find({"user_id": uid}).sort("_id", -1).to_list(length=200)
                back_cb = f"mk#usr_view_{uid}"
                title = f"premium_user_{uid}_export.json"
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json", encoding="utf-8") as fp:
                json.dump(payload, fp, default=str, indent=2, ensure_ascii=False)
                tmp = fp.name
            try:
                await client.send_document(user_id, tmp, file_name=title, caption="Export completed.")
            finally:
                try:
                    import os
                    os.remove(tmp)
                except Exception:
                    pass
            await query.message.edit_text("✅ Export sent.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data=back_cb)]]))

        elif cmd == "add_bot":
            await query.message.delete()
            asyncio.create_task(_add_store_bot_flow(client, user_id))

        elif cmd.startswith("bot_view_"):
            b_id = data[2] if len(data) > 2 else cmd.split("_")[2]
            bt = await db.db.premium_bots.find_one({"id": int(b_id)})
            if not bt:
                return await _safe_answer(query, "Bot not found!")

            cfg = bt.get("config", {}) or {}
            ad_val = cfg.get("autodel", 0)
            if ad_val == 0:
                ad_state = "OFF"
            elif ad_val < 3600:
                ad_state = f"{ad_val // 60}M"
            elif ad_val == 86400:
                ad_state = "1D"
            else:
                ad_state = f"{ad_val // 3600}H"

            prot_state = "ON" if cfg.get("protect", False) else "OFF"
            upi_state = "ON" if cfg.get("upi_enabled", True) else "OFF"

            kb = [
                [InlineKeyboardButton("📢 " + utils.to_smallcap('Broadcast Message'), callback_data=f"mk#bot_broadcast_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap('Welcome & About'), callback_data=f"mk#p_wa_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap('Delivery Report Msg'), callback_data=f"mk#pset_{b_id}_delivery_report")],
                [InlineKeyboardButton(utils.to_smallcap('Custom Caption'), callback_data=f"mk#pset_{b_id}_caption")],
                [InlineKeyboardButton(utils.to_smallcap('Fetching Media (GIF/Img)'), callback_data=f"mk#pset_{b_id}_fetching_media")],
                [InlineKeyboardButton(f"Auto-Delete: {ad_state}", callback_data=f"mk#p_autodel_{b_id}"),
                 InlineKeyboardButton(f"Protection: {prot_state}", callback_data=f"mk#p_protect_{b_id}")],
                [InlineKeyboardButton(f"UPI: {upi_state}", callback_data=f"mk#p_upi_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap('Remove Bot'), callback_data=f"mk#bot_confirm_rm_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap("Back"), callback_data="mk#accounts")],
            ]
            await query.message.edit_text(
                f"<b>❪ PREMIUM BOT PROFILE ❫</b>\n\n"
                f"<b>» Name:</b> {bt.get('name')}\n"
                f"<b>» Username:</b> @{bt.get('username')}\n"
                f"<b>ID:</b> <code>{bt.get('id')}</code>\n\n"
                "<i>All settings below are identical to Delivery bot options.</i>",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("bot_broadcast_"):
            b_id = cmd.split("_", 2)[2]
            asyncio.create_task(_bot_broadcast_flow(client, user_id, b_id))

        elif cmd.startswith("bot_confirm_rm_"):
            b_id = data[2] if len(data) > 2 else cmd.split("_")[3]
            kb = [
                [InlineKeyboardButton("🚫 " + utils.to_smallcap("Yes, Remove It"), callback_data=f"mk#bot_rm_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap("Cancel"), callback_data=f"mk#bot_view_{b_id}")]
            ]
            await query.message.edit_text(
                "<b>⚠️ CRITICAL WARNING</b>\n\n"
                "Are you sure you want to completely remove this bot from your Marketplace?\n"
                "<i>This action will disconnect the bot and stop all incoming buyer processing.</i>",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("p_autodel_"):
            parts = cmd.split("_")
            b_id = parts[2]
            
            if len(parts) == 3:
                bt = await db.db.premium_bots.find_one({"id": int(b_id)})
                if not bt: return await _safe_answer(query, "Not found!")
                cfg = bt.get("config", {}) or {}
                ad_val = cfg.get("autodel", 0)
                
                curr_str = "OFF"
                if ad_val > 0:
                    if ad_val < 3600: curr_str = f"{ad_val // 60}M"
                    elif ad_val == 86400: curr_str = "1D"
                    else: curr_str = f"{ad_val // 3600}H"

                kb = [
                   [InlineKeyboardButton("OFF", callback_data=f"mk#p_autodel_{b_id}_0"),
                    InlineKeyboardButton("5M", callback_data=f"mk#p_autodel_{b_id}_300"),
                    InlineKeyboardButton("15M", callback_data=f"mk#p_autodel_{b_id}_900")],
                   [InlineKeyboardButton("30M", callback_data=f"mk#p_autodel_{b_id}_1800"),
                    InlineKeyboardButton("1H", callback_data=f"mk#p_autodel_{b_id}_3600"),
                    InlineKeyboardButton("3H", callback_data=f"mk#p_autodel_{b_id}_10800")],
                   [InlineKeyboardButton("6H", callback_data=f"mk#p_autodel_{b_id}_21600"),
                    InlineKeyboardButton("9H", callback_data=f"mk#p_autodel_{b_id}_32400"),
                    InlineKeyboardButton("12H", callback_data=f"mk#p_autodel_{b_id}_43200")],
                   [InlineKeyboardButton("15H", callback_data=f"mk#p_autodel_{b_id}_54000"),
                    InlineKeyboardButton("18H", callback_data=f"mk#p_autodel_{b_id}_64800"),
                    InlineKeyboardButton("21H", callback_data=f"mk#p_autodel_{b_id}_75600")],
                   [InlineKeyboardButton("24H", callback_data=f"mk#p_autodel_{b_id}_86400"),
                    InlineKeyboardButton("1D", callback_data=f"mk#p_autodel_{b_id}_86400")],
                   [InlineKeyboardButton("« " + utils.to_smallcap("Back"), callback_data=f"mk#bot_view_{b_id}")]
                ]
                await query.message.edit_text(
                    f"<b>⏳ Auto-Delete Configuration</b>\n\n"
                    f"Current setting: <b>{curr_str}</b>\n\n"
                    f"Select the time after which delivered files should be automatically deleted from the user's DM:",
                    reply_markup=InlineKeyboardMarkup(kb)
                )
                return
            else:
                new_val = int(parts[3])
                bt = await db.db.premium_bots.find_one({"id": int(b_id)})
                if not bt: return await _safe_answer(query, "Not found!")
                cfg = bt.get("config", {}) or {}
                cfg["autodel"] = new_val
                await db.db.premium_bots.update_one({"id": int(b_id)}, {"$set": {"config": cfg}})
                await _safe_answer(query, f"Auto-Delete updated!", show_alert=True)
                query.data = f"mk#bot_view_{b_id}"
                return await market_callback(client, query)

        elif cmd.startswith("p_protect_"):
            b_id = cmd.split("_")[2]
            bt = await db.db.premium_bots.find_one({"id": int(b_id)})
            if not bt: return await _safe_answer(query, "Not found!")
            cfg = bt.get("config", {}) or {}
            curr = cfg.get("protect", False)
            cfg["protect"] = not curr
            await db.db.premium_bots.update_one({"id": int(b_id)}, {"$set": {"config": cfg}})
            await _safe_answer(query, f"Content Protection set to {'ON' if cfg['protect'] else 'OFF'}", show_alert=True)
            query.data = f"mk#bot_view_{b_id}"
            return await market_callback(client, query)

        elif cmd.startswith("p_upi_"):
            b_id = cmd.split("_")[2]
            bt = await db.db.premium_bots.find_one({"id": int(b_id)})
            if not bt: return await _safe_answer(query, "Not found!")
            cfg = bt.get("config", {}) or {}
            curr = cfg.get("upi_enabled", True)
            cfg["upi_enabled"] = not curr
            await db.db.premium_bots.update_one({"id": int(b_id)}, {"$set": {"config": cfg}})
            await _safe_answer(query, f"UPI Payments set to {'ON' if cfg['upi_enabled'] else 'OFF'}", show_alert=True)
            query.data = f"mk#bot_view_{b_id}"
            return await market_callback(client, query)

        elif cmd.startswith("bot_rm_"):
            b_id = int(cmd.split("_")[2])
            await db.db.premium_bots.delete_one({"id": b_id})
            await _safe_answer(query, "Bot Removed!", show_alert=True)
            from plugins.userbot.market_seller import market_clients
            if str(b_id) in market_clients:
                try:
                    await market_clients[str(b_id)].stop()
                    del market_clients[str(b_id)]
                except Exception:
                    pass
            query.data = "mk#accounts"
            return await market_callback(client, query)

        elif cmd == "manage_stories":
            await _safe_answer(query)
            stories = await db.db.premium_stories.find().to_list(length=30)
            kb = []
            for s in stories:
                kb.append([InlineKeyboardButton(f"📖 {s.get('story_name_en', 'Unknown')} - ₹{s.get('price')}", callback_data=f"mk#st_view_{str(s['_id'])}")])
            kb.append([InlineKeyboardButton("« Back", callback_data="mk#back")])
            await query.message.edit_text("<b>📦 Manage Stories</b>\n\nTap a story below to manage or delete it:", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("st_view_"):
            s_id = cmd.split("_")[2]
            from bson.objectid import ObjectId
            story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
            if not story:
                return await _safe_answer(query, "Not found!")
        
            kb = [
                [InlineKeyboardButton(utils.to_smallcap("Edit Name"), callback_data=f"mk#st_edit_{s_id}_name"),
                 InlineKeyboardButton(utils.to_smallcap("Edit Price"), callback_data=f"mk#st_edit_{s_id}_price")],
                [InlineKeyboardButton(utils.to_smallcap("Edit Image"), callback_data=f"mk#st_edit_{s_id}_image"),
                 InlineKeyboardButton(utils.to_smallcap("Edit Desc"), callback_data=f"mk#st_edit_{s_id}_desc")],
                [InlineKeyboardButton(utils.to_smallcap("Edit Status"), callback_data=f"mk#st_edit_{s_id}_status"),
                 InlineKeyboardButton(utils.to_smallcap("Edit Genre"), callback_data=f"mk#st_edit_{s_id}_genre")],
                [InlineKeyboardButton(utils.to_smallcap("Edit Episodes"), callback_data=f"mk#st_edit_{s_id}_episodes")],
                [InlineKeyboardButton(utils.to_smallcap("Edit DB Range"), callback_data=f"mk#st_edit_{s_id}_eps")],
                [InlineKeyboardButton(utils.to_smallcap("Remove Story"), callback_data=f"mk#st_confirm_rm_{s_id}")],
                [InlineKeyboardButton(utils.to_smallcap("Back"), callback_data="mk#manage_stories")],
            ]
            await query.message.edit_text(f"<b>📖 {story.get('story_name_en')}\n» Platform: {story.get('platform', 'N/A')}\n» Price: ₹{story.get('price', 0)}\n» DB ID: <code>{s_id}</code></b>\n\nWhat would you like to update?", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("st_edit_"):
            parts = cmd.split("_")
            s_id = parts[2]
            action = parts[3]
            await query.message.delete()
            asyncio.create_task(_edit_story_flow(client, user_id, s_id, action))

        elif cmd.startswith("st_confirm_rm_"):
            s_id = cmd.split("_")[3]
            kb = [
                [InlineKeyboardButton("🚫 " + utils.to_smallcap("Yes, Remove It"), callback_data=f"mk#st_rm_{s_id}")],
                [InlineKeyboardButton(utils.to_smallcap("Cancel"), callback_data=f"mk#st_view_{s_id}")]
            ]
            await query.message.edit_text(
                "<b>⚠️ CRITICAL WARNING</b>\n\n"
                "Are you sure you want to completely remove this story from the Marketplace?\n"
                "<i>This will permanently delete it and users will no longer see it.</i>",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("st_rm_"):
            s_id = cmd.split("_")[2]
            from bson.objectid import ObjectId
            await db.db.premium_stories.delete_one({"_id": ObjectId(s_id)})
            await _safe_answer(query, "Story completely removed!", show_alert=True)
            query.data = "mk#manage_stories"
            return await market_callback(client, query)

        elif cmd == "pending":
            await _safe_answer(query)
            pendings = await db.db.premium_checkout.find({"status": "pending_admin_approval"}).to_list(length=30)
            kb = []
            for p in pendings:
                st = await db.db.premium_stories.find_one({"_id": p.get('story_id')})
                s_name = st.get('story_name_en', 'Unknown') if st else 'Deleted Story'
                kb.append([InlineKeyboardButton(f"⏳ {s_name} (User: {p.get('user_id')})", callback_data=f"mk#pnd_view_{str(p['_id'])}")])
            kb.append([InlineKeyboardButton("« Back", callback_data="mk#back")])
            await query.message.edit_text("<b>💸 Pending Payments Queue</b>\n\nHere are all the users waiting for payment verification. Select one to view their screenshot:", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("pnd_view_"):
            p_id = cmd.split("_")[2]
            from bson.objectid import ObjectId
            checkout = await db.db.premium_checkout.find_one({"_id": ObjectId(p_id)})
            if not checkout:
                return await _safe_answer(query, "Ticket not found!", show_alert=True)
            st = await db.db.premium_stories.find_one({"_id": checkout.get('story_id')})
            s_name = st.get('story_name_en', 'Unknown') if st else 'Deleted Story'
            txt = f"<b>🧾 Pending Approval</b>\n\n<b>User:</b> <code>{checkout.get('user_id')}</code>\n<b>Story:</b> {s_name}\n<b>Bot Context:</b> @{checkout.get('bot_username')}"
            kb = [
                [InlineKeyboardButton("✅ Approve", callback_data=f"mk#pnd_app_{p_id}"),
                 InlineKeyboardButton("❌ Reject", callback_data=f"mk#pnd_rej_{p_id}")],
                [InlineKeyboardButton("« Back", callback_data="mk#pending")]
            ]
            await query.message.delete()
            if checkout.get("proof_path"):
                await client.send_photo(user_id, photo=checkout.get("proof_path"), caption=txt, reply_markup=InlineKeyboardMarkup(kb))
            else:
                await client.send_message(user_id, txt + "\n\n<i>No local screenshot found.</i>", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd.startswith("pnd_app_"):
            p_id = cmd.split("_")[2]
            from bson.objectid import ObjectId
            checkout = await db.db.premium_checkout.find_one({"_id": ObjectId(p_id)})
            if not checkout:
                return await _safe_answer(query, "Ticket not found!", show_alert=True)
            await db.db.premium_checkout.update_one(
                {"_id": ObjectId(p_id)},
                {"$set": {"status": "approved", "approved_at": datetime.utcnow(), "approved_by": user_id, "updated_at": datetime.utcnow()}}
            )
            await db.add_purchase(checkout['user_id'], str(checkout['story_id']))
            await query.message.delete()
            await client.send_message(user_id, f"✅ Payment Approved for user `{checkout['user_id']}`!")
            
            st = await db.db.premium_stories.find_one({"_id": checkout['story_id']})
            if st:
                # Log success for Manual UPI with Screenshot
                from utils import log_payment
                user_info = await db.get_user(checkout['user_id'])
                s_name = st.get("story_name_en", "Unknown")
                asyncio.create_task(log_payment(
                    user_id=checkout['user_id'],
                    user_first_name=user_info.get("first_name", "User"),
                    s_name=s_name,
                    amount=st.get("price", "0"),
                    method="manual_upi",
                    receipt_id=str(checkout.get('_id', '')),
                    photo_path=checkout.get("proof_path")
                ))

                from plugins.userbot.market_seller import market_clients, dispatch_delivery_choice
                u_cli = market_clients.get(str(checkout['bot_id']))
                if u_cli:
                    try:
                        await u_cli.delete_messages(checkout['user_id'], checkout.get('status_msg_id', 0))
                    except Exception:
                        pass
                    asyncio.create_task(dispatch_delivery_choice(u_cli, checkout['user_id'], st))

        elif cmd.startswith("pnd_rej_"):
            p_id = cmd.split("_")[2]
            await query.message.delete()
            asyncio.create_task(_reject_payment_flow(client, user_id, p_id))

        elif cmd.startswith("p_wa_"):
            b_id = cmd.split("_")[2]
            kb = [
                [InlineKeyboardButton(utils.to_smallcap('Welcome Msg'), callback_data=f"mk#welcome_cfg_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap('Menu Media (Photos/GIF/Video)'), callback_data=f"mk#menu_media_{b_id}")],
                [InlineKeyboardButton(utils.to_smallcap('UPI Open-App Link'), callback_data=f"mk#pset_{b_id}_upi_redirect"),
                 InlineKeyboardButton(utils.to_smallcap('UPI Payee Name'), callback_data=f"mk#pset_{b_id}_upi_name")],
                [InlineKeyboardButton(utils.to_smallcap('Bot Logo (UPI QR)'), callback_data=f"mk#pset_{b_id}_logo")],
                [InlineKeyboardButton(utils.to_smallcap("Back"), callback_data=f"mk#bot_view_{b_id}")],
            ]
            await query.message.edit_text(
                "<b>❪ WELCOME & MENU ❫</b>\n\n"
                "Configure only what is used in delivery bot menu:\n"
                "• Welcome Message\n"
                "• Menu Media (up to 10, random)\n\n"
                "<i>UPI options below are for payment button behavior.</i>",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("welcome_cfg_"):
            b_id = cmd.split("_")[2]
            kb = [
                [InlineKeyboardButton(utils.to_smallcap('Welcome Msg'), callback_data=f"mk#pset_{b_id}_welcome")],
                [InlineKeyboardButton(utils.to_smallcap('About'), callback_data=f"mk#pset_{b_id}_about")],
                [InlineKeyboardButton(utils.to_smallcap('Quote'), callback_data=f"mk#pset_{b_id}_quote"),
                 InlineKeyboardButton(utils.to_smallcap('Quote Author'), callback_data=f"mk#pset_{b_id}_quote_author")],
                [InlineKeyboardButton(utils.to_smallcap('Back'), callback_data=f"mk#p_wa_{b_id}")],
            ]
            await query.message.edit_text(
                "<b>❪ WELCOME MESSAGE SETTINGS ❫</b>\n\n"
                "Set each block shown in delivery main menu card.",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        elif cmd.startswith("menu_media_add_"):
            b_id = cmd.split("_")[3]
            await query.message.delete()
            asyncio.create_task(_menu_media_add_flow(client, user_id, b_id))

        elif cmd.startswith("menu_media_bulk_"):
            b_id = cmd.split("_")[3]
            await query.message.delete()
            asyncio.create_task(_menu_media_bulk_add_flow(client, user_id, b_id))

        elif cmd.startswith("menu_media_prev_"):
            parts = cmd.split("_")
            b_id = parts[3]
            idx = int(parts[4])
            await query.message.delete()
            asyncio.create_task(_menu_media_preview_flow(client, user_id, b_id, idx))

        elif cmd.startswith("menu_media_del_"):
            # mk#menu_media_del_<bId>_<idx>
            parts = cmd.split("_")
            b_id = parts[3]
            idx = int(parts[4])
            bot = await db.db.premium_bots.find_one({"id": int(b_id)})
            if not bot:
                return await _safe_answer(query, "Bot not found!", show_alert=True)
            cfg = bot.get("config", {}) or {}
            items = _cfg_list(cfg, "menu_media")
            real_items = [x for x in items if isinstance(x, dict)]
            if idx < 1 or idx > len(real_items):
                return await _safe_answer(query, "Invalid item index", show_alert=True)
            real_items.pop(idx - 1)
            await db.db.premium_bots.update_one({"id": int(b_id)}, {"$set": {"config.menu_media": real_items}})
            query.data = f"mk#menu_media_{b_id}"
            return await market_callback(client, query)

        elif cmd.startswith("menu_media_"):
            b_id = cmd.split("_")[2]
            bot = await db.db.premium_bots.find_one({"id": int(b_id)})
            if not bot:
                return await _safe_answer(query, "Bot not found!", show_alert=True)

            cfg = bot.get("config", {}) or {}
            items = _cfg_list(cfg, "menu_media")
            if not items and cfg.get("menuimg"):
                # Backward compatible: show legacy single image as item 1 (read-only hint)
                items = [{"type": "photo", "file_id": cfg.get("menuimg"), "legacy": True}]

            lines = [f"<b>🖼️ Menu Media</b>\n\n<b>Bot:</b> @{bot.get('username', '')}\n"]
            lines.append("<i>Shown randomly to users on /start. Supports Photo, GIF, Video. Max 30 items.</i>\n")
            if items:
                for i, it in enumerate(items, start=1):
                    t = (it or {}).get("type", "media")
                    legacy = " (legacy)" if (it or {}).get("legacy") else ""
                    lines.append(f"<b>{i}.</b> <code>{t}</code>{legacy}")
            else:
                lines.append("<blockquote>No media added yet.</blockquote>")

            kb = []
            if len([x for x in items if not (x or {}).get("legacy")]) < 30:
                kb.append([InlineKeyboardButton("➕ Add Media", callback_data=f"mk#menu_media_add_{b_id}")])
                kb.append([InlineKeyboardButton("📥 Bulk Add Media", callback_data=f"mk#menu_media_bulk_{b_id}")])
            if items:
                kb.append([InlineKeyboardButton("👁 Preview", callback_data=f"mk#menu_media_prev_{b_id}_1")])
            kb.append([InlineKeyboardButton("« Back", callback_data=f"mk#p_wa_{b_id}")])
            try:
                await query.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb))
            except MessageNotModified:
                pass


        elif cmd.startswith("pset_"):
            parts = cmd.split("_")
            b_id = parts[1]
            key = "_".join(parts[2:])
            await query.message.delete()
            asyncio.create_task(_premium_bot_set(client, user_id, b_id, key, key.capitalize()))

        elif cmd == "add_story":
            await query.message.delete()
            asyncio.create_task(_add_story_flow(client, user_id))

        elif cmd == "approve":
            p_id = data[2]
            await query.message.delete()
            asyncio.create_task(_approve_payment_flow(client, user_id, p_id))

        elif cmd == "reject":
            p_id = data[2]
            await query.message.delete()
            asyncio.create_task(_reject_payment_flow(client, user_id, p_id))
    except MessageNotModified:
        pass  # User tapped same button — silently ignore
    except PyMongoError as db_err:
        logger.error(f"Mongo operation failed in market_callback: {db_err}")
        await _safe_answer(query, "Database connection issue. Please try again.", show_alert=True)
        try:
            await client.send_message(query.from_user.id, "⚠️ Database is temporarily unreachable (SSL/connection issue). Please retry in a moment.")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"market_callback error: {e}")
        if "query" in locals() and query:
            return await query.answer("Something went wrong. Please retry.", show_alert=True)

async def _reject_request_flow(client, user_id, req_id):
    from bson.objectid import ObjectId
    try: r = await db.db.premium_requests.find_one({"_id": ObjectId(req_id)})
    except: r = None
    if not r: return await client.send_message(user_id, "Request not found.")
    
    msg = await native_ask(client, user_id, f"<b>❌ REJECT REQUEST</b>\n\nStory: {r.get('story_name')}\n\nEnter the reason for rejection (this will be sent to the user):", reply_markup=ReplyKeyboardMarkup([["⛔ Cancel"]], resize_keyboard=True))
    if getattr(msg, 'text', None) and "Cancel" in msg.text:
         return await client.send_message(user_id, "<i>Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
         
    reason = (getattr(msg, 'text', '') or 'Not specified.').strip()
    
    from datetime import datetime
    await db.db.premium_requests.update_one({"_id": r['_id']}, {"$set": {"status": f"Rejected: {reason}", "updated_at": datetime.now()}})
    
    bot_id_str = str(r.get('bot_id'))
    from plugins.userbot.market_seller import market_clients
    if bot_id_str in market_clients:
        t_cli = market_clients[bot_id_str]
        try:
            u_doc = await db.get_user(r.get('user_id'))
            t_lang = u_doc.get("lang", "en")
            if t_lang == "hi":
                alert = f"<b>⚠️ कहानी अनुरोध अस्वीकृत</b>\n\n<b>कहानी:</b> {r.get('story_name')}\n<b>कारण:</b> {reason}"
            else:
                alert = f"<b>⚠️ STORY REQUEST REJECTED</b>\n\n<b>Story:</b> {r.get('story_name')}\n<b>Reason:</b> {reason}"
            await t_cli.send_message(r.get('user_id'), alert)
        except Exception: pass
        
    await client.send_message(user_id, f"✅ Request rejected and user notified.\nReason: {reason}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« " + utils.to_smallcap("Back to List"), callback_data="mk#reqs_0")]]))

async def _settings_flow(client, user_id, cmd):
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Back", callback_data="ask_cancel")]])
    if cmd == "set_upi":
        msg = await native_ask(client, user_id, "<b>❪ SET UPI ID ❫</b>\n\nEnter your UPI ID (e.g. <code>heyjeetx@naviaxis</code>):", reply_markup=cancel_kb)
        from pyrogram.types import CallbackQuery as _CQ
        if isinstance(msg, _CQ) or not getattr(msg, 'text', None):
            return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>")
        await db.set_config("upi_id", msg.text.strip())
        await client.send_message(user_id, f"✅ UPI ID updated to <code>{msg.text.strip()}</code>", reply_markup=ReplyKeyboardRemove())
        
    elif cmd == "set_db":
        await client.send_message(user_id, "Channels are now managed from the Channels panel.", reply_markup=ReplyKeyboardRemove())


async def _add_store_bot_flow(client, user_id):
    msg = await native_ask(client, user_id, "<b>❪ ADD CONNECTED BOT ❫</b>\n\nForward the Bot Token from @BotFather:", reply_markup=ReplyKeyboardMarkup([["⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True))
    from pyrogram.types import CallbackQuery as _CQ
    txt = getattr(msg, 'text', '') or ''
    if isinstance(msg, _CQ) or "Cancel" in txt or not txt:
        return await client.send_message(user_id, "<i>Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())

    import re
    msg_text = txt
    # Extract Bot Token format safely
    match = re.search(r'\d{8,11}:[a-zA-Z0-9_-]{35,}', msg_text)
    if not match:
        return await client.send_message(user_id, "❌ **Error:** No valid Bot Token found in your message. Provide just the token or forward it directly.", reply_markup=ReplyKeyboardRemove())
        
    token = match.group(0)
    
    try:
        from config import Config
        test_cli = Client(name=f"test_{user_id}", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, in_memory=True)
        await test_cli.start()
        me = await test_cli.get_me()
        await test_cli.stop()
    except Exception as e:
        # Graceful error
        msg_err = str(e)
        if "ACCESS_TOKEN_INVALID" in msg_err: msg_err = "The Token you provided is invalid or revoked. Please get a fresh token from @BotFather."
        return await client.send_message(user_id, f"❌ **Bot Token Error:**\n{msg_err}", reply_markup=ReplyKeyboardRemove())

    await db.db.premium_bots.update_one({"id": me.id}, {"$set": {"id": me.id, "username": me.username, "name": me.first_name, "token": token}}, upsert=True)
    
    # Live Boot logic to prevent needing a restart
    from plugins.userbot.market_seller import _process_start, _process_callback, _process_screenshot, _process_text, market_clients
    try:
        from pyrogram.handlers import MessageHandler, CallbackQueryHandler
        from utils import setup_ask_router
        new_cli = Client(name=f"market_{me.id}", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, in_memory=False)
        setup_ask_router(new_cli)
        new_cli.add_handler(MessageHandler(_process_start, filters.command("start") & filters.private))
        new_cli.add_handler(CallbackQueryHandler(_process_callback, filters.regex(r'^mb#')))
        new_cli.add_handler(MessageHandler(_process_screenshot, filters.photo & filters.private))
        new_cli.add_handler(MessageHandler(_process_text, filters.text & filters.private))
        await new_cli.start()
        market_clients[str(me.id)] = new_cli
    except Exception as e:
        logger.error(f"Failed to auto-boot Premium bot: {e}")
        
    await client.send_message(user_id, f"✅ Premium Bot @{me.username} successfully added and **Live Booted!**\nIt is now actively listening for buyers.", reply_markup=ReplyKeyboardRemove())


async def _add_story_flow(client, user_id):
    try:
        sj = {}
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Back", callback_data="ask_cancel")]])

        # Select Store Bot
        bots = await db.db.premium_bots.find().to_list(length=None)
        if not bots:
            return await client.send_message(user_id, "<b>‣ No Connected Bots available. Please Add Connected Bot first.</b>")

        bot_kb = [[f"@{b['username']}"] for b in bots] + [["⛔ Cᴀɴᴄᴇʟ"]]
        while True:
            msg_bot = await native_ask(
                client,
                user_id,
                "<b>❪ STEP 1: SELECT STORE BOT ❫</b>\n\nChoose the bot to sell this via:",
                reply_markup=ReplyKeyboardMarkup(bot_kb, resize_keyboard=True)
            )
            if getattr(msg_bot, "text", None) and "Cᴀɴᴄᴇʟ" in msg_bot.text:
                return await client.send_message(user_id, "<i>Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
            usr = (msg_bot.text or "").replace("@", "").strip()
            sel_bot = next((b for b in bots if b["username"] == usr), None)
            if sel_bot:
                sj["bot_id"] = sel_bot["id"]
                sj["bot_username"] = sel_bot["username"]
                break
            await client.send_message(user_id, "❌ Invalid bot selection. Please choose from keyboard.")

        # Source channel preference from Channels registry
        db_channels = await db.db.premium_channels.find({"type": "db"}).to_list(length=None)
        source_chat = None
        if db_channels:
            src_kb = [[f"{c.get('name', c['channel_id'])} ({c['channel_id']})"] for c in db_channels]
            src_kb += [["Manual / Forward / Link"], ["⛔ Cᴀɴᴄᴇʟ"]]
            msg_src = await native_ask(
                client,
                user_id,
                "<b>❪ STEP 2: SOURCE DB CHANNEL ❫</b>\n\nSelect a source channel or choose Manual mode:",
                reply_markup=ReplyKeyboardMarkup(src_kb, resize_keyboard=True),
            )
            if getattr(msg_src, "text", None) and "Cᴀɴᴄᴇʟ" in msg_src.text:
                return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>")
            picked = (msg_src.text or "").strip()
            if picked != "Manual / Forward / Link":
                for ch in db_channels:
                    key = f"{ch.get('name', ch['channel_id'])} ({ch['channel_id']})"
                    if key == picked:
                        source_chat = ch["channel_id"]
                        break

        # Start message
        while True:
            msg_s = await native_ask(
                client,
                user_id,
                "<b>❪ STEP 3: START MESSAGE ❫</b>\n\nForward the first message of the story (or send its link / message id):",
                reply_markup=cancel_kb,
            )
            if getattr(msg_s, "text", None) and "Cᴀɴᴄᴇʟ" in msg_s.text:
                return await client.send_message(user_id, "<i>Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
            try:
                sj["start_id"] = parse_id(msg_s)
                if getattr(msg_s, "forward_from_chat", None):
                    source_chat = msg_s.forward_from_chat.id
                elif getattr(msg_s, "text", None):
                    ch, _mid = parse_chat_from_link(msg_s.text)
                    if ch:
                        source_chat = ch
                break
            except Exception:
                await client.send_message(user_id, "❌ Invalid start message. Please forward a message or send a valid link/id.")

        # End message
        while True:
            msg_e = await native_ask(
                client,
                user_id,
                "<b>❪ STEP 4: LAST MESSAGE ❫</b>\n\nForward the last message of the story (or send its link / message id):",
                reply_markup=cancel_kb,
            )
            if getattr(msg_e, "text", None) and "Cᴀɴᴄᴇʟ" in msg_e.text:
                return await client.send_message(user_id, "<i>Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
            try:
                sj["end_id"] = parse_id(msg_e)
                if getattr(msg_e, "forward_from_chat", None) and not source_chat:
                    source_chat = msg_e.forward_from_chat.id
                elif getattr(msg_e, "text", None) and not source_chat:
                    ch, _mid = parse_chat_from_link(msg_e.text)
                    if ch:
                        source_chat = ch
                break
            except Exception:
                await client.send_message(user_id, "❌ Invalid end message. Please forward a message or send a valid link/id.")

        if sj["start_id"] > sj["end_id"]:
            sj["start_id"], sj["end_id"] = sj["end_id"], sj["start_id"]

        if isinstance(source_chat, str):
            try:
                source_chat = (await client.get_chat(source_chat)).id
            except Exception:
                source_chat = None

        if not source_chat:
            return await client.send_message(
                user_id,
                "❌ Could not detect source channel. Forward at least one episode message from the source channel.",
                reply_markup=ReplyKeyboardRemove(),
            )
        sj["source"] = source_chat

        # Meta Data
        # Meta Data
        msg_name_en = await native_ask(client, user_id, "<b>❪ STEP 5: STORY NAME ❫</b>\n\nEnter the story name:\n<i>(It will be translated automatically)</i>", reply_markup=cancel_kb)
        if getattr(msg_name_en, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_name_en.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        
        name_input = (msg_name_en.text or "").strip()
        waiting_msg = await client.send_message(user_id, "⏳ <i>Automatically translating name...</i>")
        sj['story_name_en'] = utils.translate_to_english(name_input)
        sj['story_name_hi'] = utils.translate_to_hindi(name_input)
        await waiting_msg.delete()

        msg_img = await native_ask(client, user_id, f"<b>❪ STEP 6: STORY IMAGE ❫</b>\n\n<b>EN:</b> {sj['story_name_en']}\n<b>HI:</b> {sj['story_name_hi']}\n\nSend the cover image for this story:", reply_markup=cancel_kb)
        if getattr(msg_img, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_img.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_img, 'photo', None):
            await client.send_message(user_id, "<i>Uploading image to store bot...</i>")
            try:
                from plugins.userbot.market_seller import market_clients
                store_cli = market_clients.get(str(sj["bot_id"]))
                dl = await client.download_media(msg_img.photo.file_id)
                ul = await store_cli.send_photo(user_id, photo=dl)
                sj['image'] = ul.photo.file_id
                import os; os.remove(dl)
            except Exception as e:
                sj['image'] = msg_img.photo.file_id # fallback
        else:
            sj['image'] = None

        msg_desc = await native_ask(
            client,
            user_id,
            "<b>❪ STEP 7: STORY DESCRIPTION ❫</b>\n\n"
            "<blockquote expandable='true'>"
            "Enter the description/synopsis of the story.\n\n"
            "Tip: It will be automatically translated to both English and Hindi."
            "</blockquote>",
            reply_markup=cancel_kb
        )
        if getattr(msg_desc, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_desc.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        
        desc_input = (msg_desc.text or "None").strip()
        waiting_msg = await client.send_message(user_id, "⏳ <i>Automatically translating description...</i>")
        sj['description'] = utils.translate_to_english(desc_input)
        sj['description_hi'] = utils.translate_to_hindi(desc_input)
        await waiting_msg.delete()

        msg_eps = await native_ask(client, user_id, "<b>❪ STEP 6.3: EPISODES ❫</b>\n\nHow many episodes? e.g. '595 / 595' or '100+':", reply_markup=cancel_kb)
        if getattr(msg_eps, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_eps.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['episodes'] = (msg_eps.text or "N/A").strip()

        kb_status = ReplyKeyboardMarkup([["Completed", "Ongoing"], ["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True)
        msg_status = await native_ask(client, user_id, "<b>❪ STEP 6.4: STATUS ❫</b>\n\nIs the story Completed or Ongoing?", reply_markup=kb_status)
        if getattr(msg_status, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_status.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['status'] = (msg_status.text or "Unknown").strip()

        # reset to normal cancel kb
        msg_genre = await native_ask(client, user_id, "<b>❪ STEP 6.5: GENRE ❫</b>\n\nEnter genre e.g. 'Romance', 'Thriller':", reply_markup=cancel_kb)
        if getattr(msg_genre, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_genre.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['genre'] = (msg_genre.text or "Unknown").strip()


        while True:
            msg_price = await native_ask(client, user_id, "<b>❪ STEP 7: PRICE IN INR ❫</b>\n\nEnter price (e.g. 100):", reply_markup=cancel_kb)
            if getattr(msg_price, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_price.text:
                return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
            try:
                price = int((msg_price.text or "").strip())
                if price < 1:
                    raise ValueError("price")
                sj["price"] = price
                break
            except Exception:
                await client.send_message(user_id, "❌ Price must be a positive number.")

        pf_kb = ReplyKeyboardMarkup([["Pocket FM", "Kuku FM"], ["Kuku TV", "Other"], ["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True)
        msg_plat = await native_ask(client, user_id, "<b>❪ STEP 8: PLATFORM FILTER ❫</b>\n\nWhich platform?", reply_markup=pf_kb)
        if getattr(msg_plat, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_plat.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['platform'] = (msg_plat.text or "Other").strip()

        # Delivery channel strategy (supports 100-200 channels via pool/rotation)
        sj["delivery_mode"] = "pool"
        sj["channel_id"] = None
        sj["channel_pool"] = []

        delivery_channels = await db.db.premium_channels.find({"type": "delivery"}).to_list(length=300)
        kb_mode = ReplyKeyboardMarkup(
            [["Use GLOBAL Pool (Auto-Rotate)"], ["Single Delivery Channel"], ["DM Only"], ["⛔ Cᴀɴᴄᴇʟ"]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        msg_mode = await native_ask(
            client,
            user_id,
            "<b>❪ STEP 9: DELIVERY MODE ❫</b>\n\n"
            "Choose how buyers will receive the one-time channel link:",
            reply_markup=kb_mode,
        )
        if getattr(msg_mode, "text", None) and "Cᴀɴᴄᴇʟ" in msg_mode.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        picked_mode = (msg_mode.text or "").strip()

        if picked_mode == "DM Only":
            sj["delivery_mode"] = "dm_only"
        elif picked_mode == "Single Delivery Channel":
            sj["delivery_mode"] = "single"
            if delivery_channels:
                # Search-based selection (scales)
                ask = await native_ask(
                    client,
                    user_id,
                    "<b>❪ DELIVERY CHANNEL PICKER ❫</b>\n\n"
                    "Send a delivery channel ID, or type part of its saved name to search.\n"
                    "<i>Tip: Use Channels → Bulk Add Delivery to import many channels fast.</i>",
                    reply_markup=cancel_kb,
                )
                if getattr(ask, "text", None) and "Cᴀɴᴄᴇʟ" in ask.text:
                    return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
                q = (ask.text or "").strip()
                chosen = None
                if q.lstrip("-").isdigit():
                    chosen = int(q)
                else:
                    ql = q.lower()
                    matches = []
                    for ch in delivery_channels:
                        nm = (ch.get("name") or "").lower()
                        if ql and (ql in nm):
                            matches.append(ch)
                    if matches:
                        # pick first match
                        chosen = matches[0]["channel_id"]
                sj["channel_id"] = chosen
            else:
                # No saved channels yet, accept custom
                custom = await native_ask(
                    client,
                    user_id,
                    "<b>❪ CUSTOM DELIVERY CHANNEL ❫</b>\n\nForward any message from the delivery channel or send its numeric chat id:",
                    reply_markup=cancel_kb,
                )
                if getattr(custom, "text", None) and "Cᴀɴᴄᴇʟ" in custom.text:
                    return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
                try:
                    sj["channel_id"] = custom.forward_from_chat.id if getattr(custom, "forward_from_chat", None) else int((custom.text or "").strip())
                except Exception:
                    sj["channel_id"] = None
        else:
            # GLOBAL pool (default). Optional per-story pool selection from saved list.
            sj["delivery_mode"] = "pool"
            sj["channel_pool"] = [c["channel_id"] for c in delivery_channels] if delivery_channels else []

        # Save
        result = await db.db.premium_stories.insert_one(sj)
        story_id = str(result.inserted_id)
        deep_link = f"https://t.me/{sj['bot_username']}?start=buy_{story_id}"
        
        await client.send_message(user_id, f"✅ **Story successfully added to Storefront!**\n\nThe Connected bot `@{(sj['bot_username'])}` is now actively selling `{sj['story_name_en']}` for ₹{sj['price']}!\n\n🔗 **Direct Purchase Link:**\n`{deep_link}`", reply_markup=ReplyKeyboardRemove())

    except Exception as e:
        logger.error(f"Story creation error: {e}")
        await client.send_message(user_id, f"<b>⚠️ Error adding story:</b> Please ensure all settings (like Store Bots) are correctly configured first.", reply_markup=ReplyKeyboardRemove())

async def _edit_story_flow(client, user_id, s_id, action):
    from bson.objectid import ObjectId
    s_id_obj = ObjectId(s_id)
    story = await db.db.premium_stories.find_one({"_id": s_id_obj})
    if not story: return await client.send_message(user_id, "Story missing.")
    
    label = action.capitalize()
    msg = await native_ask(client, user_id, f"<b>✏️ Edit Story {label}</b>\n\nSend the new {label} for <code>{story.get('story_name_en')}</code>.", reply_markup=ReplyKeyboardMarkup([["⛔ Cancel"]], resize_keyboard=True))
    from pyrogram.types import CallbackQuery as _CQ
    _txt = getattr(msg, 'text', '') or ''
    if isinstance(msg, _CQ) or "Cancel" in _txt or not _txt:
        return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

    try:
        if action == "price":
            try:
                new_price = int(msg.text)
            except ValueError:
                return await client.send_message(user_id, "❌ Valid integer price required.", reply_markup=ReplyKeyboardRemove())
                
            old_price = int(story.get("price", 0))
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"price": new_price}})
            
            if old_price > 0 and old_price != new_price:
                diff_type = "Drop 📉" if new_price < old_price else "Hike 📈"
                kb_notify = ReplyKeyboardMarkup([["Yes, Send Notification"], ["No"]], resize_keyboard=True, one_time_keyboard=True)
                ask_notif = await native_ask(
                    client, user_id, 
                    f"<b>📢 Send Price {diff_type} Notification?</b>\n\n"
                    f"Old Price: ₹{old_price}\n"
                    f"New Price: ₹{new_price}\n\n"
                    "Do you want to send a broadcast to all buyers and users about this change?", 
                    reply_markup=kb_notify
                )
                
                if getattr(ask_notif, 'text', None) and "Yes" in ask_notif.text:
                    await client.send_message(user_id, "<i>Broadcasting price update...</i>", reply_markup=ReplyKeyboardRemove())
                    try:
                        from plugins.userbot.market_seller import market_clients
                        store_cli = market_clients.get(str(story.get("bot_id")))
                        if store_cli:
                            # To maximize reach for marketing, send to all global system users 
                            # (Marketplace bots use the main `users` collection)
                            bot_users = await db.db.users.find({}, {"id": 1}).to_list(length=None)
                            
                            story_name = story.get('story_name_en', 'Premium Story')
                            trend = "DECREASED 📉" if new_price < old_price else "INCREASED 📈"
                            msg_text = (
                                f"<b>📢 Price Update Alert!</b>\n\n"
                                f"📖 <b>Story:</b> {story_name}\n"
                                f"💸 <b>Price {trend}</b>\n\n"
                                f"❌ Old Price: ₹{old_price}\n"
                                f"✅ <b>New Price: ₹{new_price}</b>\n\n"
                                f"<i>Go to the main menu to grab it now!</i>"
                            )
                            
                            sent = 0
                            for u in bot_users:
                                uid_int = u.get("id")
                                if uid_int:
                                    try:
                                        await store_cli.send_message(uid_int, msg_text)
                                        sent += 1
                                        await asyncio.sleep(0.05)
                                    except Exception:
                                        pass
                            await client.send_message(user_id, f"✅ Broadcasting completed. Sent to {sent} users.")
                        else:
                            await client.send_message(user_id, "⚠️ Store bot is offline. Could not send broadcast.", reply_markup=ReplyKeyboardRemove())
                    except Exception as e:
                        logger.error(f"Broadcast error: {e}")
                        await client.send_message(user_id, "⚠️ Error during broadcast.")
                else:
                    await client.send_message(user_id, f"✅ **Story {label} Updated!** (No broadcast sent)", reply_markup=ReplyKeyboardRemove())
                return
        elif action == "name":
            name_en = utils.translate_to_english(msg.text)
            name_hi = utils.translate_to_hindi(msg.text)
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"story_name_en": name_en, "story_name_hi": name_hi}})
        elif action == "image":
            if getattr(msg, 'photo', None):
                await client.send_message(user_id, "<i>Uploading image to store bot...</i>")
                try:
                    from plugins.userbot.market_seller import market_clients
                    store_cli = market_clients.get(str(story["bot_id"]))
                    dl = await client.download_media(msg.photo.file_id)
                    ul = await store_cli.send_photo(user_id, photo=dl)
                    await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"image": ul.photo.file_id}})
                    import os; os.remove(dl)
                except Exception as e:
                    await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"image": msg.photo.file_id}})
            else:
                return await client.send_message(user_id, "❌ Valid Photo required.", reply_markup=ReplyKeyboardRemove())
        elif action == "desc":
            desc_en = utils.translate_to_english(msg.text)
            desc_hi = utils.translate_to_hindi(msg.text)
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"description": desc_en, "description_hi": desc_hi}})
        elif action == "genre":
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"genre": msg.text}})
        elif action == "episodes":
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"episodes": msg.text}})
        await client.send_message(user_id, f"✅ **Story {label} Updated!**", reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        await client.send_message(user_id, f"❌ Invalid format. Please try again.", reply_markup=ReplyKeyboardRemove())

async def _approve_payment_flow(client, user_id, p_id):
    from bson.objectid import ObjectId
    from datetime import datetime
    checkout = await db.db.premium_checkout.find_one({"_id": ObjectId(p_id)})
    if not checkout: return await client.send_message(user_id, "Ticket not found.")
    
    if checkout.get("status") in ("approved", "rejected"):
        return await client.send_message(user_id, f"Ticket is already {checkout.get('status')}.")
        
    await db.db.premium_checkout.update_one(
        {"_id": ObjectId(p_id)},
        {"$set": {"status": "approved", "updated_at": datetime.utcnow(), "reviewed_by": user_id}}
    )

    import random, string
    order_id = f"OD-{checkout['user_id']}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"

    await db.add_purchase(checkout['user_id'], str(checkout['story_id']))
    await db.db.premium_purchases.insert_one({
        "user_id": checkout['user_id'],
        "story_id": checkout['story_id'],
        "bot_id": checkout['bot_id'],
        "purchased_at": datetime.utcnow(),
        "source": checkout.get("method", "upi"),
        "amount": checkout.get("amount", 0),
        "order_id": order_id
    })

    # Log payment
    from utils import log_payment, log_arya_event
    story = await db.db.premium_stories.find_one({"_id": checkout["story_id"]})
    user_info = await db.get_user(checkout['user_id'])
    s_name = story.get("story_name_en") if story else "Unknown"
    
    asyncio.create_task(log_arya_event(
        event_type="MANUAL UPI VERIFIED",
        user_id=checkout['user_id'],
        user_info=user_info,
        details=f"Story: {s_name}\nOrder ID: <code>{order_id}</code>\nAmount: ₹{checkout.get('amount', 0)}\nApproved by Admin ID: {user_id}"
    ))
    
    asyncio.create_task(log_payment(
        user_id=checkout['user_id'],
        user_first_name=user_info.get("first_name", "User"),
        username=user_info.get('username', ''),
        s_name=s_name,
        amount=checkout.get("amount", 0),
        method=checkout.get("method", "upi"),
        receipt_id=str(checkout["_id"]),
        photo_path=checkout.get("proof_path"),
        order_id=order_id,
        user_last_name=user_info.get("last_name", "")
    ))

    await client.send_message(user_id, "✅ Payment Approved successfully!")
    from plugins.userbot.market_seller import market_clients, dispatch_delivery_choice
    u_cli = market_clients.get(str(checkout['bot_id']))
    if u_cli:
        try: await u_cli.delete_messages(checkout['user_id'], checkout.get('status_msg_id', 0))
        except: pass
        story = await db.db.premium_stories.find_one({"_id": checkout["story_id"]})
        if story:
            asyncio.create_task(dispatch_delivery_choice(u_cli, checkout['user_id'], story))
            

async def _reject_payment_flow(client, user_id, p_id):
    msg = await native_ask(
        client, user_id, 
        f"<b>❌ Reject Payment</b>\n\nPlease enter the reason for rejecting this payment (this will be sent to the user).\n<i>Tip: You can send an image with caption as the reason!</i>", 
        reply_markup=ReplyKeyboardMarkup([["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True)
    )
    from pyrogram.types import CallbackQuery as _CQ
    _rtxt = getattr(msg, 'text', '') or ''
    if isinstance(msg, _CQ) or "Cancel" in _rtxt:
        return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

    reason = msg.text or getattr(msg, 'caption', None) or "No reason provided."
    photo_id = msg.photo.file_id if getattr(msg, 'photo', None) else None

    from bson.objectid import ObjectId
    checkout = await db.db.premium_checkout.find_one({"_id": ObjectId(p_id)})
    if not checkout: return await client.send_message(user_id, "Ticket not found.")
    
    if checkout.get("status") in ("approved", "rejected"):
        return await client.send_message(user_id, f"Ticket is already {checkout.get('status')}.")
    
    from datetime import datetime
    await db.db.premium_checkout.update_one(
        {"_id": ObjectId(p_id)},
        {"$set": {"status": "rejected", "reject_reason": reason, "updated_at": datetime.utcnow(), "reviewed_by": user_id}}
    )
    await client.send_message(user_id, f"✅ Payment Rejected. User has been notified.", reply_markup=ReplyKeyboardRemove())
    
    from plugins.userbot.market_seller import market_clients
    u_cli = market_clients.get(str(checkout['bot_id']))
    if u_cli:
        try: await u_cli.delete_messages(checkout['user_id'], checkout.get('status_msg_id', 0))
        except: pass
        user_msg = f"<b>❌ Payment Rejected</b>\n\nYour recent payment could not be verified.\n<b>Reason from Admin:</b>\n{reason}\n\n<i>If this is a mistake, please try again with a clear screenshot.</i>"
        if photo_id:
            try:
                # Need to download from mgmt bot and send via store bot 
                import os
                dl = await client.download_media(msg)
                await u_cli.send_photo(checkout['user_id'], photo=dl, caption=user_msg)
                os.remove(dl)
            except Exception:
                await u_cli.send_message(checkout['user_id'], user_msg)
        else:
            await u_cli.send_message(checkout['user_id'], user_msg)

async def _premium_bot_set(client, user_id, b_id, key, label):
    # Keep this minimal: no reply-keyboard "selection menu" unless absolutely required.
    if key == "logo":
        note = "<i>Upload a Photo to be used as your Bot Logo.</i>"
    elif key == "delivery_report":
        note = "<b>Available Variables:</b>\n<code>{story_name}</code>\n<code>{sent}</code>\n<code>{failed}</code>\n<code>{time}</code>"
    elif key == "caption":
        note = "<b>Available Variables:</b>\n<code>{story}</code>, <code>{price}</code>, <code>{original_caption}</code>, <code>{file_name}</code>\n<i>Allows standard HTML (e.g. &lt;b&gt;bold&lt;/b&gt;)</i>"
    else:
        note = "<i>Allows standard HTML syntax formatting.</i>"

    pretty_label = label.replace('_', ' ').title()

    extra_note = ""
    if key in ["welcome", "about", "quote", "quote_author"]:
        extra_note = "Send <code>disable</code> to completely hide this section.\n"

    msg = await native_ask(
        client,
        user_id,
        f"<b>❪ SET: {utils.to_smallcap(pretty_label)} ❫</b>\n\n"
        f"Send the new {pretty_label} for your Store Bot.\n"
        f"{extra_note}"
        f"Send <code>/reset</code> to revert to default.\n\n"
        f"{note}",
        reply_markup=ReplyKeyboardMarkup([["❮ Cancel"]], resize_keyboard=True)
    )
    
    txt = getattr(msg, 'text', "") or ""
    if "Cancel" in txt or "/cancel" in txt:
        back_target = f"mk#bot_view_{b_id}"
        if key in ["welcome", "about", "quote", "quote_author", "upi_redirect", "upi_name", "logo", "menu_media"]:
            back_target = f"mk#p_wa_{b_id}"
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton(utils.to_smallcap("Back"), callback_data=back_target)]])
        return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=back_kb)
    
    if getattr(msg, 'text', None) == "/reset":
        await db.db.premium_bots.update_one({"id": int(b_id)}, {"$unset": {f"config.{key}": ""}})
        tmp_rm = await client.send_message(user_id, "...", reply_markup=ReplyKeyboardRemove())
        await tmp_rm.delete()
        back_target = f"mk#bot_view_{b_id}"
        if key in ["welcome", "about", "quote", "quote_author", "upi_redirect", "upi_name", "logo", "menu_media"]:
            back_target = f"mk#p_wa_{b_id}"
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton(utils.to_smallcap("Back"), callback_data=back_target)]])
        return await client.send_message(user_id, f"<i>✅ {label} has been Reset to default.</i>", reply_markup=back_kb)
        
    val = msg.text or msg.caption or ""
    if key in ["logo", "fetching_media"]:
        media_type = None
        if getattr(msg, "photo", None): media_type = "photo"
        elif getattr(msg, "animation", None): media_type = "animation"
        elif getattr(msg, "video", None): media_type = "video"

        if not media_type:
            return await client.send_message(user_id, f"❌ Valid Photo/GIF/Video required for {pretty_label}.", reply_markup=ReplyKeyboardRemove())

        # Re-upload via the target store bot (file_id is bot-specific).
        from plugins.userbot.market_seller import market_clients
        store_cli = market_clients.get(str(b_id))
        if not store_cli:
            return await client.send_message(
                user_id,
                "❌ Store bot is not running right now.\n\nRestart the ecosystem and try again.",
                reply_markup=ReplyKeyboardRemove()
            )

        import os
        if not os.path.exists("downloads"):
            os.makedirs("downloads")

        ext = ".jpg" if media_type == "photo" else (".gif" if media_type == "animation" else ".mp4")
        tmp_path = await client.download_media(msg, file_name=f"downloads/{key}_{b_id}_{int(time.time())}{ext}")
        sent = None
        try:
            # Upload via store bot to get store-bot-specific file_id; then delete immediately.
            if media_type == "photo":
                sent = await store_cli.send_photo(user_id, photo=tmp_path)
                val = {"type": "photo", "file_id": sent.photo.file_id if sent.photo else sent.document.file_id}
            elif media_type == "animation":
                sent = await store_cli.send_animation(user_id, animation=tmp_path)
                # Fallback in case Telegram sees it as document/video
                fid = (sent.animation.file_id if sent.animation else 
                      (sent.document.file_id if sent.document else (sent.video.file_id if sent.video else None)))
                if not fid:
                    raise AttributeError("Could not retrieve file_id from sent animation.")
                val = {"type": "animation", "file_id": fid}
            else:
                sent = await store_cli.send_video(user_id, video=tmp_path)
                val = {"type": "video", "file_id": sent.video.file_id if sent.video else sent.document.file_id}

        finally:
            try:
                if sent:
                    await store_cli.delete_messages(user_id, sent.id)
            except Exception:
                pass
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    if key == "upi_redirect":
        from plugins.userbot.market_seller import sanitize_https_redirect_base
        val = sanitize_https_redirect_base(val)
        if not val:
            return await client.send_message(
                user_id,
                "❌ Invalid redirect URL.\n\nSend only the HTTPS base, e.g.\n<code>https://aryastoriesupi.vercel.app</code>\n\n(no spaces, no hidden characters after paste).",
                reply_markup=ReplyKeyboardRemove(),
            )
        
    await db.db.premium_bots.update_one({"id": int(b_id)}, {"$set": {f"config.{key}": val}})
    
    tmp_m = await client.send_message(user_id, "...", reply_markup=ReplyKeyboardRemove())
    await tmp_m.delete()

    back_target = f"mk#bot_view_{b_id}"
    if key in ["welcome", "about", "quote", "quote_author", "upi_redirect", "upi_name", "logo", "menu_media"]:
        back_target = f"mk#p_wa_{b_id}"
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton(utils.to_smallcap("Back"), callback_data=back_target)]])

    await client.send_message(user_id, f"<i>✅ {label} successfully updated!</i>", reply_markup=back_kb)


async def _menu_media_add_flow(client, user_id: int, b_id: str):
    bot = await db.db.premium_bots.find_one({"id": int(b_id)})
    if not bot:
        return await client.send_message(user_id, "❌ Bot not found.", reply_markup=ReplyKeyboardRemove())

    cfg = bot.get("config", {}) or {}
    items = _cfg_list(cfg, "menu_media")
    if len([x for x in items if isinstance(x, dict)]) >= 30:
        return await client.send_message(user_id, "⚠️ Max 30 menu media items reached. Delete one first.", reply_markup=ReplyKeyboardRemove())

    msg = await native_ask(
        client,
        user_id,
        "<b>➕ Add Menu Media</b>\n\nSend a <b>Photo</b>, <b>GIF</b>, or <b>Video</b>.\n\n<i>This will be shown randomly to users in the main menu.</i>",
        reply_markup=ReplyKeyboardRemove(),
    )

    media_type = None
    if getattr(msg, "photo", None):
        media_type = "photo"
    elif getattr(msg, "animation", None):
        media_type = "animation"
    elif getattr(msg, "video", None):
        media_type = "video"
    else:
        return await client.send_message(user_id, "❌ Please send a valid Photo / GIF / Video.", reply_markup=ReplyKeyboardRemove())

    from plugins.userbot.market_seller import market_clients
    store_cli = market_clients.get(str(b_id))
    if not store_cli:
        return await client.send_message(
            user_id,
            "❌ Store bot is not running right now.\n\nRestart the ecosystem and try again.",
            reply_markup=ReplyKeyboardRemove()
        )

    import os
    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    ext = ".jpg" if media_type == "photo" else (".gif" if media_type == "animation" else ".mp4")
    tmp_path = await client.download_media(msg, file_name=f"downloads/menu_media_{b_id}_{int(time.time())}{ext}")
    sent = None
    try:
        # Upload via store bot to get store-bot-specific file_id; then delete immediately (no "set" message).
        if media_type == "photo":
            sent = await store_cli.send_photo(user_id, photo=tmp_path)
            file_id = getattr(sent.photo, "file_id", None) or ""
        elif media_type == "animation":
            sent = await store_cli.send_animation(user_id, animation=tmp_path)
            file_id = getattr(sent.animation, "file_id", None) or ""
        else:
            sent = await store_cli.send_video(user_id, video=tmp_path)
            file_id = getattr(sent.video, "file_id", None) or ""
    finally:
        try:
            if sent:
                await store_cli.delete_messages(user_id, sent.id)
        except Exception:
            pass
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    if not file_id:
        return await client.send_message(user_id, "❌ Failed to capture media file_id. Try again.", reply_markup=ReplyKeyboardRemove())

    items = [x for x in items if isinstance(x, dict)]
    items.append({"type": media_type, "file_id": file_id})
    await db.db.premium_bots.update_one({"id": int(b_id)}, {"$set": {"config.menu_media": items}, "$unset": {"config.menuimg": ""}})

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Preview", callback_data=f"mk#menu_media_prev_{b_id}_{len(items)}")],
        [InlineKeyboardButton("« Back", callback_data=f"mk#menu_media_{b_id}")]
    ])
    await client.send_message(user_id, "✅ Menu media added.", reply_markup=kb)


async def _menu_media_preview_flow(client, user_id: int, b_id: str, idx: int):
    bot = await db.db.premium_bots.find_one({"id": int(b_id)})
    if not bot:
        return await client.send_message(user_id, "❌ Bot not found.", reply_markup=ReplyKeyboardRemove())
    cfg = bot.get("config", {}) or {}
    items = [x for x in _cfg_list(cfg, "menu_media") if isinstance(x, dict)]
    if idx < 1 or idx > len(items):
        return await client.send_message(user_id, "❌ Invalid item index.", reply_markup=ReplyKeyboardRemove())

    it = items[idx - 1]
    t = it.get("type")
    fid = it.get("file_id")
    txt = f"<b>Preview</b>\n\n<b>Item:</b> {idx}/{len(items)}\n<b>Type:</b> <code>{t}</code>"

    nav = []
    if idx > 1:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"mk#menu_media_prev_{b_id}_{idx-1}"))
    if idx < len(items):
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"mk#menu_media_prev_{b_id}_{idx+1}"))

    kb_rows = []
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🗑 Delete this", callback_data=f"mk#menu_media_del_{b_id}_{idx}")])
    kb_rows.append([InlineKeyboardButton("« Back", callback_data=f"mk#menu_media_{b_id}")])
    kb = InlineKeyboardMarkup(kb_rows)

    try:
        if t == "photo":
            await client.send_photo(user_id, photo=fid, caption=txt, reply_markup=kb)
        elif t == "animation":
            await client.send_animation(user_id, animation=fid, caption=txt, reply_markup=kb)
        else:
            await client.send_video(user_id, video=fid, caption=txt, reply_markup=kb)
    except Exception:
        await client.send_message(user_id, txt + "\n\n⚠️ Failed to preview this media.", reply_markup=kb)


async def _add_channel_flow(client, user_id, ch_type):
    """Wizard to add a DB or Delivery channel by forwarding a message from it."""
    label = "DB Source" if ch_type == "db" else "Delivery"
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Back", callback_data="ask_cancel")]])

    msg = await native_ask(
        client, user_id,
        f"<b>❪ ADD {label.upper()} CHANNEL ❫</b>\n\n"
        f"Forward <b>any message</b> from the {'source story' if ch_type == 'db' else 'private delivery'} channel, "
        f"OR type its Chat ID directly (e.g. <code>-100123456789</code>):",
        reply_markup=cancel_kb
    )
    if not msg or (getattr(msg, 'text', None) and "Cᴀɴᴄᴇʟ" in msg.text):
        return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>")

    try:
        if getattr(msg, 'forward_from_chat', None):
            cid = msg.forward_from_chat.id
            name = msg.forward_from_chat.title or str(cid)
        else:
            cid = int(msg.text.strip())
            # Try to get info
            try:
                chat = await client.get_chat(cid)
                name = chat.title or str(cid)
            except Exception:
                name = str(cid)

        # Check for duplicates
        existing = await db.db.premium_channels.find_one({"channel_id": cid, "type": ch_type})
        if existing:
            return await client.send_message(user_id, f"⚠️ This channel is already in the {label} list.", reply_markup=ReplyKeyboardRemove())

        await db.db.premium_channels.insert_one({"channel_id": cid, "name": name, "type": ch_type})
        await client.send_message(user_id, f"✅ <b>{label} Channel Added!</b>\n\n<b>Name:</b> {name}\n<b>ID:</b> <code>{cid}</code>", reply_markup=ReplyKeyboardRemove())

    except ValueError:
        await client.send_message(user_id, "❌ Invalid Chat ID. Please forward a message or type a valid numeric ID.", reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        await client.send_message(user_id, f"❌ Error: {e}", reply_markup=ReplyKeyboardRemove())


async def _bulk_add_delivery_channels(client, user_id: int):
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Back", callback_data="ask_cancel")]])
    msg = await native_ask(
        client,
        user_id,
        "<b>❪ BULK ADD DELIVERY CHANNELS ❫</b>\n\n"
        "Send delivery channel IDs separated by spaces/new lines.\n\n"
        "Example:\n<code>-100111...\n-100222...\n-100333...</code>\n\n"
        "<i>Tip: bot must be admin in those channels.</i>",
        reply_markup=cancel_kb,
    )
    if not msg or (getattr(msg, "text", None) and "Cᴀɴᴄᴇʟ" in msg.text):
        return await client.send_message(user_id, "<i>Process Cancelled Successfully!</i>")

    raw = (msg.text or "").replace(",", " ").replace("\t", " ")
    parts = [p.strip() for p in raw.split() if p.strip()]
    ids = []
    for p in parts:
        if p.lstrip("-").isdigit():
            ids.append(int(p))

    if not ids:
        return await client.send_message(user_id, "❌ No valid numeric channel IDs found.", reply_markup=ReplyKeyboardRemove())

    added = exists = failed = 0
    for cid in ids:
        try:
            ex = await db.db.premium_channels.find_one({"channel_id": cid, "type": "delivery"})
            if ex:
                exists += 1
                continue
            try:
                chat = await client.get_chat(cid)
                name = getattr(chat, "title", None) or str(cid)
            except Exception:
                name = str(cid)
            await db.db.premium_channels.insert_one({"channel_id": cid, "name": name, "type": "delivery"})
            added += 1
        except Exception:
            failed += 1

    await client.send_message(
        user_id,
        f"✅ Bulk add complete.\n\n"
        f"• Added: <b>{added}</b>\n"
        f"• Already existed: <b>{exists}</b>\n"
        f"• Failed: <b>{failed}</b>",
        reply_markup=ReplyKeyboardRemove(),
    )


async def _menu_media_bulk_add_flow(client, user_id: int, b_id: str):
    bot = await db.db.premium_bots.find_one({"id": int(b_id)})
    if not bot:
        return await client.send_message(user_id, "❌ Bot not found.", reply_markup=ReplyKeyboardRemove())

    from plugins.userbot.market_seller import market_clients
    store_cli = market_clients.get(str(b_id))
    if not store_cli:
        return await client.send_message(user_id, "❌ Store bot is not running right now.", reply_markup=ReplyKeyboardRemove())

    import os
    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    done_kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done Adding", callback_data="ask_cancel")]])
    await client.send_message(
        user_id,
        "<b>📥 BULK MEDIA ADDER</b>\n\n"
        "Send <b>Photos</b>, <b>GIFs</b>, or <b>Videos</b> one by one.\n"
        "I will automatically process and save them to your menu media list.\n\n"
        "<i>Click the button below when you are finished.</i>",
        reply_markup=done_kb
    )

    count = 0
    while True:
        # Wait for next input without sending a new text message every time
        # We use native_ask with a special check or just a small status message
        msg = await native_ask(
            client, 
            user_id, 
            f"<b>📥 Bulk Adding... (#{count})</b>\n\nSend the next item or click 'Done' below.", 
            reply_markup=done_kb,
            timeout=600
        )
        if not msg:
            break
        
        if isinstance(msg, CallbackQuery):
            if msg.data == "ask_cancel":
                break
            continue
            
        if getattr(msg, "text", None) and (msg.text.lower() == "/start" or "done" in msg.text.lower()):
            break

        media_type = None
        if getattr(msg, "photo", None): media_type = "photo"
        elif getattr(msg, "animation", None): media_type = "animation"
        elif getattr(msg, "video", None): media_type = "video"
        
        if not media_type:
            if not isinstance(msg, CallbackQuery):
                await client.send_message(user_id, "❌ Please send a Photo, GIF, or Video.\nOr click <b>'Done Adding'</b>.", reply_markup=done_kb)
            continue

        # Re-fetch bot to check current count
        bot = await db.db.premium_bots.find_one({"id": int(b_id)})
        items = _cfg_list(bot.get("config", {}), "menu_media")
        if len([x for x in items if isinstance(x, dict)]) >= 30:
            await client.send_message(user_id, "⚠️ Limit of 30 media items reached. Stopping bulk upload.")
            break

        ext = ".jpg" if media_type == "photo" else (".gif" if media_type == "animation" else ".mp4")
        tmp_path = await client.download_media(msg, file_name=f"downloads/bulk_{b_id}_{int(time.time())}{ext}")
        
        try:
            sent = None
            f_id = None
            if media_type == "photo":
                sent = await store_cli.send_photo(user_id, photo=tmp_path)
                f_id = getattr(sent.photo, "file_id", None)
            elif media_type == "animation":
                sent = await store_cli.send_animation(user_id, animation=tmp_path)
                f_id = getattr(sent.animation, "file_id", None)
            else:
                sent = await store_cli.send_video(user_id, video=tmp_path)
                f_id = getattr(sent.video, "file_id", None)

            if f_id:
                await db.db.premium_bots.update_one(
                    {"id": int(b_id)}, 
                    {"$push": {"config.menu_media": {"type": media_type, "file_id": f_id}}, "$unset": {"config.menuimg": ""}}
                )
                count += 1
                await client.send_message(user_id, f"✅ Media #{count} added! Send more or click 'Done'.", reply_markup=done_kb)
            
            if sent:
                await store_cli.delete_messages(user_id, sent.id)
        except Exception as e:
            await client.send_message(user_id, f"❌ Error processing item: {e}")
        finally:
            if os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except: pass

    await client.send_message(user_id, f"<b>🏁 Bulk Add Finished!</b>\n\nTotal media items added: <b>{count}</b>", reply_markup=ReplyKeyboardRemove())




async def _bot_broadcast_flow(client, user_id: int, b_id: str):
    from plugins.userbot.market_seller import market_clients
    from pyrogram.errors import UserIsBlocked, FloodWait, PeerIdInvalid, InputUserDeactivated
    import time
    from pyrogram.types import ReplyKeyboardMarkup, ReplyKeyboardRemove
    
    bt = await db.db.premium_bots.find_one({"id": int(b_id)})
    if not bt:
        return await client.send_message(user_id, "❌ Bot not found in Database.")
    
    seller_cli = market_clients.get(str(b_id))
    if not seller_cli:
        return await client.send_message(user_id, "❌ Bot is not active or not started. Please ensure the delivery bot is running.")

    prompt_kb = ReplyKeyboardMarkup([[utils.to_smallcap("Back")], [utils.to_smallcap("Cancel Transaction")]], resize_keyboard=True)
    
    try:
        ans1 = await native_ask(client, user_id, 
            f"<b>📢 BROADCAST SYSTEM — {bt.get('bot_username', 'Bot')}</b>\n\n"
            f"Please send the message you want to broadcast.\n"
            f"It can be <b>Text, Photo, Video, Document, or even a Forwarded Post</b>.\n\n"
            f"<i>Send anything now, or click Cancel below.</i>",
            reply_markup=prompt_kb
        )
        
        if _is_cancel(ans1):
            return await client.send_message(user_id, "<i>❌ Broadcast Cancelled.</i>", reply_markup=ReplyKeyboardRemove())

        # Start Implementation
        status_msg = await client.send_message(user_id, "<b>⏳ Initializing Broadcast...</b>", reply_markup=ReplyKeyboardRemove())
        
        # Determine Users: Users who have started THIS bot (bot_ids contains b_id)
        # We need to handle the case where b_id is passed as a string but stored as int/vice versa
        try: target_bot_id = int(b_id)
        except: target_bot_id = b_id

        users_cursor = db.db.users.find({"bot_ids": target_bot_id})
        total_users = await db.db.users.count_documents({"bot_ids": target_bot_id})
        
        if total_users == 0:
            return await status_msg.edit_text("❌ No users found for this bot.\n\nNote: User tracking for broadcasts started just now. Please wait for users to interact with the bot first.")

        sent_count = 0
        delivered = 0
        failed = 0
        blocked = 0
        
        start_time = time.time()
        
        # Iterate users
        async for user_doc in users_cursor:
            target_id = user_doc['id']
            try:
                await seller_cli.copy_message(chat_id=target_id, from_chat_id=user_id, message_id=ans1.id)
                delivered += 1
            except FloodWait as e:
                await asyncio.sleep(e.value)
                # Retry once
                try: 
                    await seller_cli.copy_message(chat_id=target_id, from_chat_id=user_id, message_id=ans1.id)
                    delivered += 1
                except: failed += 1
            except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
                blocked += 1
            except Exception as e:
                failed += 1
            
            sent_count += 1
            
            # Periodic update every 15 users
            if sent_count % 15 == 0 or sent_count == total_users:
                elapsed = time.time() - start_time
                speed = sent_count / elapsed if elapsed > 0 else 1
                rem_users = total_users - sent_count
                eta_s = rem_users / speed if speed > 0 else 0
                
                prog_txt = (
                    f"<b>📢 BROADCASTING IN PROGRESS...</b>\n\n"
                    f"<b>👤 Progress:</b> {sent_count}/{total_users}\n"
                    f"<b>✅ Success:</b> {delivered}\n"
                    f"<b>🚫 Blocked:</b> {blocked}\n"
                    f"<b>❌ Failed:</b> {failed}\n\n"
                    f"<b>⏱️ Speed:</b> {speed:.1f} users/sec\n"
                    f"<b>⏳ ETA:</b> {int(eta_s // 60)}m {int(eta_s % 60)}s"
                )
                try: await status_msg.edit_text(prog_txt)
                except: pass
        
        final_txt = (
            f"<b>🏁 BROADCAST COMPLETED!</b>\n\n"
            f"<b>📊 Final Stats:</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>👤 Total Targeted:</b> {total_users}\n"
            f"<b>✅ Delivered:</b> {delivered}\n"
            f"<b>🚫 Blocked/Inactive:</b> {blocked}\n"
            f"<b>❌ Other Failures:</b> {failed}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>⏱️ Total Time:</b> {int((time.time() - start_time) // 60)}m {int((time.time() - start_time) % 60)}s"
        )
        await status_msg.edit_text(final_txt)

    except asyncio.TimeoutError:
        await client.send_message(user_id, "⏳ Broadcast session timed out.", reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        logger.error(f"Broadcast Flow Error: {e}")
        await client.send_message(user_id, f"❌ Broadcast failed: {e}", reply_markup=ReplyKeyboardRemove())
