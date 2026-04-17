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
from pymongo.errors import PyMongoError
from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from database import db
from config import Config
import utils
from utils import native_ask
from pyrogram.types import CallbackQuery
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
        f"<blockquote expandable>"
        f"<b>⧉ ʙᴏᴛꜱ        ⟶</b> <code>{bots}</code>\n"
        f"<b>⧉ ꜱᴛᴏʀɪᴇꜱ     ⟶</b> <code>{stories}</code>\n"
        f"<b>⧉ ᴘᴇɴᴅɪɴɢ     ⟶</b> <code>{pendings}</code>\n"
        f"<b>⧉ ʙᴜʏᴇʀꜱ      ⟶</b> <code>{buyers}</code>\n"
        f"<b>⧉ ᴅʙ ᴄʜᴀɴɴᴇʟꜱ ⟶</b> <code>{db_ch}</code>\n"
        f"<b>⧉ ᴅᴇʟɪᴠᴇʀʏ    ⟶</b> <code>{dl_ch}</code>"
        f"</blockquote>\n"
        f"<blockquote expandable><i>💡 <b>𝗧𝗶𝗽:</b> Use 'Channels → Bulk Add' for large delivery pools.</i></blockquote>"
    )


    kb = [
        [InlineKeyboardButton("🛒 " + utils.to_smallcap("Add Story"), callback_data="mk#add_story"),
         InlineKeyboardButton("💸 " + utils.to_smallcap("Pending"), callback_data="mk#pending")],
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
        return await edit_message.edit_text(txt, reply_markup=markup)
    return await client.send_message(chat_id, txt, reply_markup=markup)


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
            await _safe_answer(query)
            return await _render_home(client, user_id, edit_message=query.message)
        
        elif cmd == "settings":
            await _safe_answer(query)
            kb = [
                [InlineKeyboardButton("💳 Set UPI ID", callback_data="mk#set_upi")],
                [InlineKeyboardButton("« Back", callback_data="mk#back")]
            ]
            await query.message.edit_text("<b>⚙️ Ecosystem Settings</b>\n\nConfigure your global payment settings here. Channel management is available in Channels.", reply_markup=InlineKeyboardMarkup(kb))

        elif cmd == "back":
            await _safe_answer(query)
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
            from datetime import datetime
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
            lines.append("<i>Shown randomly to users on /start. Supports Photo, GIF, Video. Max 10 items.</i>\n")
            if items:
                for i, it in enumerate(items, start=1):
                    t = (it or {}).get("type", "media")
                    legacy = " (legacy)" if (it or {}).get("legacy") else ""
                    lines.append(f"<b>{i}.</b> <code>{t}</code>{legacy}")
            else:
                lines.append("<blockquote>No media added yet.</blockquote>")

            kb = []
            if len([x for x in items if not (x or {}).get("legacy")]) < 10:
                kb.append([InlineKeyboardButton("➕ Add Media", callback_data=f"mk#menu_media_add_{b_id}")])
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
        await _safe_answer(query, "Something went wrong. Please retry.", show_alert=True)

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
        msg_name_en = await native_ask(client, user_id, "<b>❪ STEP 5: STORY NAME (ENGLISH) ❫</b>\n\nEnter clean English name:", reply_markup=cancel_kb)
        if getattr(msg_name_en, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_name_en.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['story_name_en'] = (msg_name_en.text or "").strip()

        msg_name_hi = await native_ask(client, user_id, "<b>❪ STEP 6: STORY NAME (HINDI) ❫</b>\n\nEnter Hindi/localized name:", reply_markup=cancel_kb)
        if getattr(msg_name_hi, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_name_hi.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['story_name_hi'] = (msg_name_hi.text or "").strip() or sj["story_name_en"]
        msg_img = await native_ask(client, user_id, "<b>❪ STEP 6.1: STORY IMAGE ❫</b>\n\nSend the cover image for this story:", reply_markup=cancel_kb)
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
            "<b>❪ STEP 6.2: STORY DESCRIPTION ❫</b>\n\n"
            "<blockquote expandable='true'>"
            "Enter the description/synopsis of the story.\n\n"
            "Tip: You can send a long paragraph; it will be shown in expandable quote style in buyer preview."
            "</blockquote>",
            reply_markup=cancel_kb
        )
        if getattr(msg_desc, 'text', None) and "Cᴀɴᴄᴇʟ" in msg_desc.text:
            return await client.send_message(user_id, "<i>Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
        sj['description'] = (msg_desc.text or "None").strip()

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
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"story_name_en": msg.text, "story_name_hi": msg.text}})
        elif action == "eps":
            parts = msg.text.strip().split("-")
            sid, eid = int(parts[0]), int(parts[1])
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"start_id": sid, "end_id": eid}})
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
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"description": msg.text}})
        elif action == "status":
            await db.db.premium_stories.update_one({"_id": s_id_obj}, {"$set": {"status": msg.text}})
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

    msg = await native_ask(
        client,
        user_id,
        f"<b>❪ SET: {utils.to_smallcap(pretty_label)} ❫</b>\n\n"
        f"Send the new {pretty_label} for your Store Bot.\n"
        f"Send <code>/reset</code> to remove it.\n\n"
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
        return await client.send_message(user_id, f"✅ {label} has been **Reset**.", reply_markup=ReplyKeyboardRemove())
        
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
                val = {"type": "photo", "file_id": sent.photo.file_id}
            elif media_type == "animation":
                sent = await store_cli.send_animation(user_id, animation=tmp_path)
                val = {"type": "animation", "file_id": sent.animation.file_id}
            else:
                sent = await store_cli.send_video(user_id, video=tmp_path)
                val = {"type": "video", "file_id": sent.video.file_id}
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
    await client.send_message(user_id, f"✅ **{label}** successfully updated!", reply_markup=ReplyKeyboardRemove())


async def _menu_media_add_flow(client, user_id: int, b_id: str):
    bot = await db.db.premium_bots.find_one({"id": int(b_id)})
    if not bot:
        return await client.send_message(user_id, "❌ Bot not found.", reply_markup=ReplyKeyboardRemove())

    cfg = bot.get("config", {}) or {}
    items = _cfg_list(cfg, "menu_media")
    if len([x for x in items if isinstance(x, dict)]) >= 10:
        return await client.send_message(user_id, "⚠️ Max 10 menu media items reached. Delete one first.", reply_markup=ReplyKeyboardRemove())

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

