"""
Share Bot — Delivery Agent
==========================
Handles deep-link delivery of batched episodes to users.
Features: Multi-channel FSub (up to 6), join-request mode, global auto-delete, content protection.
"""
import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import UserNotParticipant
from database import db
from config import Config

def format_msg(text: str, user) -> str:
    if not text: return ""
    return text.format(
        first_name=user.first_name or "",
        last_name=user.last_name or "",
        mention=user.mention
    )

logger = logging.getLogger(__name__)

share_clients = {}  # global reference dict: { bot_id: Client }
active_downloads = set()

# ── Helpers ──────────────────────────────────────────────────────────────────

async def delete_later(client, chat_id, msg_ids: list, notice_id: int, delay_secs: int):
    """Delete delivered files after delay_secs seconds."""
    await asyncio.sleep(delay_secs)
    for mid in msg_ids:
        try:
            await client.delete_messages(chat_id, mid)
        except Exception:
            pass
    try:
        if notice_id:
            await client.delete_messages(chat_id, notice_id)
    except Exception:
        pass


async def check_all_subscriptions(client, user_id: int, fsub_channels: list) -> list:
    """Returns list of channel dicts the user has NOT joined."""
    not_joined = []
    for ch in fsub_channels:
        chat_id = ch.get('chat_id')
        if not chat_id: continue
        
        # Inject peer access_hash so Share Bot doesn't get CHANNEL_INVALID
        ah = ch.get('access_hash', 0)
        if ah and str(chat_id).startswith("-100"):
            try:
                raw_id = abs(int(chat_id)) - 1000000000000
                await client.storage.update_peers([(raw_id, ah, "channel", None, None)])
            except Exception: pass
        try:
            member = await client.get_chat_member(int(chat_id), user_id)
            from pyrogram import enums
            if member.status in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED):
                not_joined.append(ch)
        except UserNotParticipant:
            not_joined.append(ch)
        except Exception as e:
            logger.error(f"FSub check failed (admin needed?): {e}")
            not_joined.append(ch)  # Fail secure: if we can't check, require it.
    return not_joined


async def start_share_bot():
    """Start all Share Bot clients from DB."""
    global share_clients
    
    # 1. Stop existing clients safely
    for b_id, cl in share_clients.items():
        try:
            await cl.stop()
        except Exception:
            pass
    share_clients.clear()
    
    # 2. Fetch configured bots
    bots = await db.get_share_bots()
    if not bots:
        logger.warning("No Share Bots configured — skipping startup.")
        return
        
    for index, b in enumerate(bots):
        try:
            sc = Client(
                name=f"share_bot_{b['id']}_{index}",
                bot_token=b['token'],
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                in_memory=True,
            )
            await sc.start()
            # Register handlers AFTER start — required for in_memory sessions in Pyrogram 2.x
            register_share_handlers(sc)
            share_clients[b['id']] = sc
            logger.info(f"Share Bot started: @{sc.me.username} ({b['name']})")
        except Exception as e:
            logger.error(f"Failed to start Share Bot {b['name']}: {e}")

# ── Handlers ─────────────────────────────────────────────────────────────────

def register_share_handlers(app: Client):
    """Attach all /start handlers to the given Client instance."""

    @app.on_chat_join_request()
    async def fsub_auto_approve(client, request):
        # Auto-Approve FSub join requests seamlessly
        fsub_chs = await db.get_share_fsub_channels()
        for ch in fsub_chs:
            if str(request.chat.id) == ch.get('chat_id') and ch.get('join_request'):
                try:
                    await request.approve()
                except Exception as e:
                    logger.error(f"FSub auto-approve failed for {request.chat.id}: {e}")

    @app.on_message(filters.private & filters.command("start"))
    async def process_start(client, message):
        user_id = message.from_user.id
        args    = message.command

        # Plain /start — welcome message
        if len(args) < 2:
            custom_wel = await db.get_share_text("welcome_msg", "")
            if custom_wel:
                await message.reply_text(format_msg(custom_wel, message.from_user))
            else:
                bot_name = client.me.first_name
                await message.reply_text(
                    f"<b>👋 Welcome to {bot_name}!</b>\n\n"
                    "I'm a secure file-delivery bot. Click a link button from the channel "
                    "to receive your episodes directly here in DM.\n\n"
                    "<i>If you ended up here by mistake, go back to the channel and click a button.</i>"
                )
            return

        uuid_str = args[1].strip()

        # 1. Fetch link record
        link_data = await db.get_share_link(uuid_str)
        if not link_data:
            await message.reply_text(
                "<b>❌ Link Expired or Invalid</b>\n\n"
                "This batch link no longer exists. Go back to the channel and click the latest link."
            )
            return

        msg_ids      = link_data.get('message_ids', [])
        source_chat  = link_data.get('source_chat')
        protect_flag = await db.get_share_protect_global()

        if not msg_ids or not source_chat:
            await message.reply_text("<b>❌ Database Error:</b> Missing file references.")
            return

        # 2. Multi-channel Force-Subscribe check
        fsub_channels = await db.get_share_fsub_channels()
        if fsub_channels:
            not_joined = await check_all_subscriptions(client, user_id, fsub_channels)
            if not_joined:
                # 2x2 grid for FSub buttons natively built with titles
                from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
                
                f_buttons = []
                for ch in not_joined:
                    label = ch.get('title') or "📢 Join Channel"
                    if ch.get('join_request'):
                        label = f"📨 {label}"
                    invite = ch.get('invite_link', '')
                    if invite:
                        f_buttons.append(InlineKeyboardButton(label, url=invite))
                        
                # Arrange FSub buttons in rows of 2
                buttons = []
                for i in range(0, len(f_buttons), 2):
                    buttons.append(f_buttons[i:i+2])
                    
                buttons.append([
                    InlineKeyboardButton(
                        "✅ I've Joined — Try Again!",
                        url=f"https://t.me/{client.me.username}?start={uuid_str}"
                    )
                ])

                fsub_msg = await db.get_share_text("fsub_msg", "")
                if fsub_msg:
                    txt = format_msg(fsub_msg, message.from_user)
                else:
                    txt = (
                        f"<b>🔒 Join Required!</b>\n\n"
                        f"Hey {message.from_user.first_name or 'User'},\n"
                        f"Please join all my update channels to use me!\n\n"
                        "<i>After joining, click <b>Try Again</b> below.</i>"
                    )

                await message.reply_text(
                    txt,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                return

        # 3. Resolve source channel peer — Share Bot uses in_memory so it has no peer cache.
        #    We must call get_chat() once so Pyrogram stores the peer, or copy_message will fail.
        try:
            await client.get_chat(source_chat)
        except Exception as peer_err:
            logger.warning(f"Peer resolution failed, trying access_hash inject: {peer_err}")
            access_hash = link_data.get('access_hash', 0)
            if access_hash and source_chat < 0:
                try:
                    from pyrogram.raw.types import InputPeerChannel as _IPC
                    raw_channel_id = abs(source_chat) - 1000000000000
                    await client.storage.update_peers([
                        (raw_channel_id, access_hash, "channel", None, None)
                    ])
                except Exception as inject_err:
                    logger.warning(f"Peer inject also failed: {inject_err}")


        # 4. Read auto-delete from GLOBAL config (not per-link) so setting changes apply everywhere
        auto_delete_mins = await db.get_share_autodelete_global()

        # 5. Deliver files one by one with copy_message
        dl_id = f"{user_id}_{uuid_str}"
        active_downloads.add(dl_id)
        
        sts = await message.reply_text(
            "<i>⏳ Fetching your files securely, please wait...</i>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_dl_{uuid_str}")
            ]])
        )

        sent_ids   = []
        fail_count = 0
        cap_tpl    = await db.get_share_text("custom_caption", "")
        formatted_cap = format_msg(cap_tpl, message.from_user) if cap_tpl else None

        try:
            for msg_id in msg_ids:
                if dl_id not in active_downloads:
                    await sts.edit_text("<b>🚫 Download Cancelled.</b>")
                    return
                try:
                    kwargs = {
                        "chat_id": user_id,
                        "from_chat_id": source_chat,
                        "message_id": msg_id,
                        "protect_content": protect_flag
                    }
                    if formatted_cap:
                        kwargs["caption"] = formatted_cap
                        
                    sent = await client.copy_message(**kwargs)
                    sent_ids.append(sent.id)
                except Exception as copy_err:
                    logger.warning(f"Failed to copy msg {msg_id}: {copy_err}")
                    fail_count += 1
                    
                await asyncio.sleep(0.3)  # Rate limit safety

            active_downloads.discard(dl_id)
            try:
                await sts.delete()
            except Exception:
                pass

            total = len(sent_ids)
            if total == 0:
                await message.reply_text(
                    "<b>❌ Delivery Failed</b>\n\n"
                    "Could not copy any files. "
                    "Ensure the Share Bot is an <b>admin</b> in the Database Channel."
                )
                return

            fail_note = f"\n<i>({fail_count} file(s) could not be copied)</i>" if fail_count else ""

            if auto_delete_mins > 0:
                hrs   = auto_delete_mins // 60
                mins_r= auto_delete_mins % 60
                del_str = (f"{hrs}h {mins_r}m" if hrs and mins_r
                           else (f"{hrs} hours" if hrs else f"{auto_delete_mins} minutes"))
                
                custom_del = await db.get_share_text("delete_msg", "")
                if custom_del:
                    txt = format_msg(custom_del, message.from_user).replace("{time}", del_str)
                else:
                    txt = (
                        f"<b>✅ {total} file(s) delivered!</b>\n\n"
                        f"⚠️ <b>Important:</b>\n"
                        f"Listen from here only. Due to copyright, the content will auto-delete after {del_str}.\n"
                        f"If your episodes get auto-deleted, then repeat the same process next time — "
                        f"you only need to click 'Try Again' once.{fail_note}"
                    )
                notice = await message.reply_text(txt)
                asyncio.create_task(
                    delete_later(client, user_id, sent_ids, notice.id, auto_delete_mins * 60)
                )
            else:
                custom_suc = await db.get_share_text("success_msg", "")
                if custom_suc:
                    txt = format_msg(custom_suc, message.from_user)
                else:
                    txt = f"<b>✅ {total} file(s) delivered!</b>{fail_note}"
                await message.reply_text(txt)

        except Exception as e:
            active_downloads.discard(dl_id)
            try: await sts.delete()
            except Exception: pass
            await message.reply_text(
                f"<b>❌ Delivery Failed</b>\n\n<code>{e}</code>\n\n"
                f"<i>The Share Bot must be an admin in the Database Channel to deliver files.</i>"
            )

    @app.on_callback_query(filters.regex(r'^cancel_dl_'))
    async def process_delivery_cancel(client, query):
        uuid_str = query.data.split('_')[-1]
        dl_id = f"{query.from_user.id}_{uuid_str}"
        if dl_id in active_downloads:
            active_downloads.discard(dl_id)
            await query.answer("Action aborted.", show_alert=True)
            try:
                await query.message.edit_text("<b>🚫 Download Cancelled.</b>")
            except Exception: pass
        else:
            await query.answer("This task is already cancelled or finished.", show_alert=True)
