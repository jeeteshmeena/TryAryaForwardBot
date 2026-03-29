"""
Share Bot — Delivery Agent
==========================
Handles deep-link delivery of batched episodes to users.
Handler functions are defined at module level so they can be passed to
add_handler() after the client is started (Pyrogram 2.x requirement).
"""
import logging
import asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import UserNotParticipant
from pyrogram.handlers import MessageHandler, CallbackQueryHandler, ChatJoinRequestHandler
from database import db
from config import Config

logger = logging.getLogger(__name__)

share_clients: dict = {}   # { bot_id_str: Client }
active_downloads: set = set()

# ─────────────────────────────────────────────────────────────────────────────
# Arya Bot Font constants
# ─────────────────────────────────────────────────────────────────────────────
ARYA_VERSION = "V1.0"
UPDATE_LINK   = "https://t.me/MeJeetX"
SUPPORT_LINK  = "https://t.me/LightchatX"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def format_msg(text: str, user) -> str:
    if not text:
        return ""
    try:
        full = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
        return text.format(
            first_name=user.first_name or "",
            last_name=user.last_name or "",
            full_name=full.strip(),
            mention=user.mention or user.first_name or "User",
        )
    except Exception:
        return text


async def delete_later(client, chat_id, msg_ids: list, notice_id: int, delay_secs: int):
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


async def check_all_subscriptions(client, user_id: int, fsub_channels: list, bot_id: str = None) -> list:
    """
    Returns list of channel dicts the user has NOT joined.
    For Join-Request channels: if the user already has a PENDING join request
    (detected by the auto-approve handler), they are treated as joined.
    """
    not_joined = []
    for ch in fsub_channels:
        chat_id = ch.get('chat_id')
        if not chat_id:
            continue
        is_jr = ch.get('join_request', False)
        try:
            # Pre-warm peer cache (in_memory client)
            try:
                await client.get_chat(int(chat_id))
            except Exception:
                pass
            member = await client.get_chat_member(int(chat_id), user_id)
            if member.status in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED):
                not_joined.append(ch)
            # MEMBER / ADMINISTRATOR / OWNER / RESTRICTED = they're in → allow
        except UserNotParticipant:
            if is_jr:
                # For JR channels: check if this user was already recorded
                # (JR handler added them to the pending set for instant access)
                uid_key = f"{chat_id}_{user_id}"
                if uid_key in _jr_approved:
                    # Already sent join request → treat as joined
                    pass
                else:
                    ch_copy = dict(ch)
                    ch_copy['needs_request'] = True
                    not_joined.append(ch_copy)
            else:
                not_joined.append(ch)
        except Exception as e:
            logger.warning(f"FSub check skipped for {chat_id}: {e}")
            # Can't verify — fail-open (don't block)
    return not_joined


# In-memory set: tracks users who have sent join requests (to JR channels)
# Format: "{chat_id}_{user_id}"
_jr_approved: set = set()


# ─────────────────────────────────────────────────────────────────────────────
# Module-level handler functions (required for add_handler to work)
# ─────────────────────────────────────────────────────────────────────────────

async def _fsub_record_jr(client, request):
    """
    Record that a user has sent a join request to a JR channel.
    This grants them instant access to files WITHOUT auto-approving their request.
    """
    bot_id = str(client.me.id) if client.me else None
    fsub_chs = await db.get_bot_fsub_channels(bot_id) if bot_id else []
    if not fsub_chs:
        fsub_chs = await db.get_share_fsub_channels()

    for ch in fsub_chs:
        if str(request.chat.id) == ch.get('chat_id') and ch.get('join_request'):
            try:
                # Mark user as having requested to join so FSub check knows they're cleared
                uid_key = f"{request.chat.id}_{request.from_user.id}"
                _jr_approved.add(uid_key)
                logger.info(f"Recorded JR for instant access: user {request.from_user.id} in {request.chat.id}")
            except Exception as e:
                logger.error(f"FSub JR record failed: {e}")


async def _process_start(client, message):
    """Handle /start [uuid] deep-link — deliver files to user."""
    user_id = message.from_user.id
    args = message.command
    bot_id = str(client.me.id) if client.me else None

    # Track user for stats and broadcast
    await db.add_share_bot_user(bot_id, user_id)

    # Plain /start — show welcome
    if len(args) < 2:
        await _send_welcome(client, message, bot_id)
        return

    uuid_str = args[1].strip()

    # Help command via deep-link (start=help)
    if uuid_str == "help":
        await _send_help(client, message, bot_id)
        return

    # 1. Fetch link record from DB
    link_data = await db.get_share_link(uuid_str)
    if not link_data:
        await message.reply_text(
            "<b>❌ Link Expired or Invalid</b>\n\n"
            "This batch link no longer exists. Go back to the channel and click the button again."
        )
        return

    msg_ids     = link_data.get('message_ids', [])
    source_chat = link_data.get('source_chat')
    protect_flag = await db.get_share_protect_global()

    if not msg_ids or not source_chat:
        await message.reply_text("<b>❌ Database Error:</b> Missing file references.")
        return

    # 2. Force-Subscribe check (per-bot fsub)
    fsub_channels = await db.get_bot_fsub_channels(bot_id) if bot_id else []
    if not fsub_channels:
        fsub_channels = await db.get_share_fsub_channels()  # fallback global

    if fsub_channels:
        not_joined = await check_all_subscriptions(client, user_id, fsub_channels, bot_id)
        if not_joined:
            f_buttons = []
            channel_num = 1
            for ch in not_joined:
                invite  = ch.get('invite_link', '')
                is_jr   = ch.get('join_request', False)
                label   = f"Jᴏɪɴ Cʜᴀɴɴᴇʟ {channel_num}"  # Never show channel name
                channel_num += 1
                if invite:
                    emoji = "📨" if is_jr else "📢"
                    f_buttons.append(InlineKeyboardButton(f"{emoji} {label}", url=invite))

            rows = []
            for i in range(0, len(f_buttons), 2):
                rows.append(f_buttons[i:i+2])
            rows.append([
                InlineKeyboardButton(
                    "Tʀʏ Aɢᴀɪɴ",
                    url=f"https://t.me/{client.me.username}?start={uuid_str}"
                )
            ])

            # FSub message from DB or default
            fsub_msg = await db.get_share_bot_text(bot_id, "fsub_msg") if bot_id else ""
            if not fsub_msg:
                fsub_msg = await db.get_share_text("fsub_msg", "")
            if fsub_msg:
                txt = format_msg(fsub_msg, message.from_user)
            else:
                has_jr = any(ch.get('join_request') for ch in not_joined)
                user_name = message.from_user.first_name or "User"
                if has_jr:
                    txt = (
                        f"<b>🔒 Jᴏɪɴ Rᴇϙᴜɪʀᴇᴅ!</b>\n\n"
                        f"Hᴇʏ {user_name} 👋\n"
                        "Pʟᴇᴀsᴇ sᴇɴᴅ ᴀ <b>ᴊᴏɪɴ ʀᴇϙᴜᴇsᴛ</b> ᴛᴏ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ʙᴇʟᴏᴡ.\n"
                        "<i>After tapping each button and sending the request, click <b>Tʀʏ Aɢᴀɪɴ</b> — you'll get instant access!</i>"
                    )
                else:
                    txt = (
                        f"<b>🔒 Jᴏɪɴ Rᴇϙᴜɪʀᴇᴅ!</b>\n\n"
                        f"Hᴇʏ {user_name} 👋\n"
                        "Pʟᴇᴀsᴇ ᴊᴏɪɴ ᴀʟʟ ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟs ᴛᴏ ᴜsᴇ ᴍᴇ!\n\n"
                        "<i>After joining, click <b>Tʀʏ Aɢᴀɪɴ</b> below.</i>"
                    )
            await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(rows))
            return

    # 3. Resolve source channel in this bot's peer cache via get_chat()
    try:
        await client.get_chat(source_chat)
    except Exception as peer_err:
        logger.warning(f"get_chat peer resolution failed: {peer_err}")

    # 4. Global auto-delete setting
    auto_delete_mins = await db.get_share_autodelete_global()

    # 5. Deliver
    dl_id = f"{user_id}_{uuid_str}"
    active_downloads.add(dl_id)

    sts = await message.reply_text(
        "<i>⏳ Fᴇᴛᴄʜɪɴɢ ʏᴏᴜʀ ꜰɪʟᴇs sᴇᴄᴜʀᴇʟʏ, ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ...</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"cancel_dl_{uuid_str}")
        ]])
    )

    sent_ids   = []
    fail_count = 0
    cap_tpl    = (await db.get_share_bot_text(bot_id, "custom_caption") if bot_id else "") or \
                 await db.get_share_text("custom_caption", "")
    formatted_cap = format_msg(cap_tpl, message.from_user) if cap_tpl else None

    try:
        for msg_id in msg_ids:
            if dl_id not in active_downloads:
                return  # cancel handler already edited the status
            try:
                kwargs = {
                    "chat_id": user_id,
                    "from_chat_id": source_chat,
                    "message_id": msg_id,
                    "protect_content": protect_flag,
                }
                if formatted_cap:
                    kwargs["caption"] = formatted_cap
                sent = await client.copy_message(**kwargs)
                sent_ids.append(sent.id)
            except Exception as copy_err:
                logger.warning(f"copy_message failed for msg {msg_id}: {copy_err}")
                fail_count += 1
            await asyncio.sleep(0.3)

        active_downloads.discard(dl_id)
        try:
            await sts.delete()
        except Exception:
            pass

        total = len(sent_ids)
        if total == 0:
            await message.reply_text(
                "<b>❌ Dᴇʟɪᴠᴇʀʏ Fᴀɪʟᴇᴅ</b>\n\n"
                "Could not copy any files. "
                "Ensure the Share Bot is an <b>admin</b> in the Database Channel."
            )
            return

        fail_note = f"\n<i>({fail_count} file(s) could not be copied)</i>" if fail_count else ""

        if auto_delete_mins > 0:
            hrs    = auto_delete_mins // 60
            mins_r = auto_delete_mins % 60
            del_str = (f"{hrs}h {mins_r}m" if hrs and mins_r
                       else (f"{hrs} hours" if hrs else f"{auto_delete_mins} minutes"))
            del_tpl = (await db.get_share_bot_text(bot_id, "delete_msg") if bot_id else "") or \
                      await db.get_share_text("delete_msg", "")
            if del_tpl:
                txt = format_msg(del_tpl, message.from_user).replace("{time}", del_str)
            else:
                txt = (
                    f"<i>⚠️ Iᴍᴘᴏʀᴛᴀɴᴛ: {total} file(s) delivered! Due to copyright, all messages "
                    f"will auto-delete after {del_str}. "
                    f"To re-access, simply click the same link button again.{fail_note}</i>"
                )
            notice = await message.reply_text(txt)
            asyncio.create_task(
                delete_later(client, user_id, sent_ids, notice.id, auto_delete_mins * 60)
            )
        else:
            suc_tpl = (await db.get_share_bot_text(bot_id, "success_msg") if bot_id else "") or \
                      await db.get_share_text("success_msg", "")
            txt = (format_msg(suc_tpl, message.from_user) if suc_tpl
                   else f"<b>✅ {total} ꜰɪʟᴇ(s) ᴅᴇʟɪᴠᴇʀᴇᴅ!</b>{fail_note}")
            await message.reply_text(txt)

    except Exception as e:
        active_downloads.discard(dl_id)
        try:
            await sts.delete()
        except Exception:
            pass
        await message.reply_text(
            f"<b>❌ Dᴇʟɪᴠᴇʀʏ Eʀʀᴏʀ:</b> <code>{e}</code>\n\n"
            "<i>The Share Bot must be an admin in the Database Channel to deliver files.</i>"
        )


async def _send_welcome(client, message, bot_id: str = None):
    """Send the welcome message + Help/About buttons."""
    user = message.from_user
    bot_name = client.me.first_name if client.me else "Delivery Bot"

    # Full name mention (clickable)
    full_name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
    mention = f'<a href="tg://user?id={user.id}">{full_name.strip()}</a>'

    # Bot-specific welcome text or global
    custom_wel = (await db.get_share_bot_text(bot_id, "welcome_msg") if bot_id else "") or \
                 await db.get_share_text("welcome_msg", "")

    # Image is SHARED between Welcome and About — always pull from About section
    bot_about = await db.get_share_bot_about(bot_id) if bot_id else {}
    about_img = bot_about.get('image_id') if bot_about else None

    if custom_wel:
        txt = format_msg(custom_wel, user).replace("{mention}", mention)
    else:
        txt = (
            f"<b>👋 Wᴇʟᴄᴏᴍᴇ ᴛᴏ {bot_name}!</b>\n\n"
            f"Hᴇʟʟᴏ {mention} ✨\n\n"
            "<i>I am a permanent file store bot — users can access stored messages "
            "by using a shareable link created for them.</i>\n\n"
            "Click a link button from the channel to receive your files directly here.\n"
            "<i>To know more, click the Help button below.</i>"
        )

    buttons = [
        [
            InlineKeyboardButton("ʜᴇʟᴘ", callback_data="sbd#help"),
            InlineKeyboardButton("ᴀʙᴏᴜᴛ", callback_data="sbd#about"),
        ],
        [
            InlineKeyboardButton("🔔 Uᴘᴅᴀᴛᴇ Cʜᴀɴɴᴇʟ", url=UPDATE_LINK)
        ]
    ]

    try:
        if about_img:
            await client.send_photo(
                user.id, photo=about_img,
                caption=txt,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))


async def _send_help(client, message, bot_id: str = None):
    """Send the Help menu."""
    txt = (
        "<b>🌵 Hᴇʟᴘ Mᴇɴᴜ</b>\n\n"
        "<i>I am a permanent file store bot. You can access stored files by using "
        "a shareable link given by me from the channel.</i>\n\n"
        "<b>📚 How to Get Files:</b>\n"
        "<i>➜ Open the channel and tap a link button\n"
        "➜ I will send the files directly to your DM\n"
        "➜ If force-subscribe is enabled, join required channels first\n"
        "➜ If your files are deleted, tap the same button again</i>\n\n"
        "<b>📚 Available Commands:</b>\n"
        "<i>➜ /start — check if I'm alive\n"
        "➜ Click any episode link button in the channel to receive files</i>\n\n"
        "<b>🛡️ Bot Info:</b>\n"
        "<i>➜ All file deliveries are encrypted and protected\n"
        "➜ Files may auto-delete after a set time (copyright protection)\n"
        "➜ Simply click your link button again to re-download</i>"
    )
    buttons = [
        [InlineKeyboardButton("◀️ Bᴀᴄᴋ", callback_data="sbd#back")],
        [InlineKeyboardButton("🔔 Uᴘᴅᴀᴛᴇ Cʜᴀɴɴᴇʟ", url=UPDATE_LINK)]
    ]
    try:
        await message.edit_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))


async def _send_about(client, query_or_msg, bot_id: str = None, edit: bool = True):
    """Send or edit the About section inline — always edits the same message."""
    bot_name = client.me.first_name if client.me else "Delivery Bot"
    about = await db.get_share_bot_about(bot_id) if bot_id else {}

    owner_name   = about.get('owner_name', 'JeetX')
    owner_link   = about.get('owner_link', 'https://t.me/MeJeetX')
    update_chan  = about.get('update_chan', 'JeetX')
    update_link  = about.get('update_link', UPDATE_LINK)
    support_chan = about.get('support_chan', 'Light Chat')
    support_link = about.get('support_link', SUPPORT_LINK)
    from plugins.commands import get_bot_version
    version      = about.get('version', get_bot_version())
    about_img    = about.get('image_id', None)   # SHARED with Welcome
    about_text   = about.get('custom_text', None)

    if about_text:
        txt = about_text
    else:
        txt = (
            f"✨ <b>ᴀʙᴏᴜᴛ ᴍᴇ</b>\n\n"
            f"✰ <b>ᴍʏ ɴᴀᴍᴇ:</b> {bot_name}\n"
            f"✰ <b>ᴏᴘᴇʀᴀᴛᴇᴅ ʙʏ:</b> Arya Bot\n"
            f"✰ <b>ᴍʏ ᴏᴡɴᴇʀ:</b> <a href=\"{owner_link}\">{owner_name}</a>\n"
            f"✰ <b>ᴜᴘᴅᴀᴛᴇs:</b> <a href=\"{update_link}\">{update_chan}</a>\n"
            f"✰ <b>sᴜᴘᴘᴏʀᴛ:</b> <a href=\"{support_link}\">{support_chan}</a>\n"
            f"✰ <b>ᴠᴇʀsɪᴏɴ:</b> {version}"
        )

    buttons = [[InlineKeyboardButton("◀️ Bᴀᴄᴋ", callback_data="sbd#back")]]
    markup  = InlineKeyboardMarkup(buttons)

    # Determine if current message is a photo (sent with image) or text
    msg = query_or_msg if hasattr(query_or_msg, 'photo') else getattr(query_or_msg, 'message', query_or_msg)
    is_photo_msg = bool(getattr(msg, 'photo', None))

    try:
        if is_photo_msg:
            # Edit caption inline — preserves the welcome image
            await msg.edit_caption(caption=txt, reply_markup=markup)
        else:
            # Edit text inline — no deletion, no new message
            await msg.edit_text(txt, reply_markup=markup, disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"_send_about edit failed: {e}")
        try:
            await msg.reply_text(txt, reply_markup=markup, disable_web_page_preview=True)
        except Exception:
            pass



async def _process_delivery_button(client, query):
    """Handle inline buttons on the welcome/help/about messages."""
    cmd = query.data.split('#')[1] if '#' in query.data else ''
    bot_id = str(client.me.id) if client.me else None
    msg = query.message
    is_photo = bool(getattr(msg, 'photo', None))

    if cmd == "help":
        await query.answer()
        txt = (
            "<b>🌵 Hᴇʟᴘ Mᴇɴᴜ</b>\n\n"
            "<i>I am a permanent file store bot. You can access stored files by using "
            "a shareable link given by me from the channel.</i>\n\n"
            "<b>📚 How to Get Files:</b>\n"
            "<i>➜ Open the channel and tap a link button\n"
            "➜ I will send the files directly to your DM\n"
            "➜ If force-subscribe is enabled, join required channels first\n"
            "➜ If your files are deleted, tap the same button again</i>\n\n"
            "<b>📚 Available Commands:</b>\n"
            "<i>➜ /start — check if I'm alive\n"
            "➜ Click any episode link button in the channel to receive files</i>\n\n"
            "<b>🛡️ Bot Info:</b>\n"
            "<i>➜ All file deliveries are encrypted and protected\n"
            "➜ Files may auto-delete after a set time (copyright protection)\n"
            "➜ Simply click your link button again to re-download</i>"
        )
        buttons = [
            [InlineKeyboardButton("◀️ Bᴀᴄᴋ", callback_data="sbd#back")],
            [InlineKeyboardButton("🔔 Uᴘᴅᴀᴛᴇ Cʜᴀɴɴᴇʟ", url=UPDATE_LINK)]
        ]
        markup = InlineKeyboardMarkup(buttons)
        try:
            if is_photo:
                await msg.edit_caption(caption=txt, reply_markup=markup)
            else:
                await msg.edit_text(txt, reply_markup=markup)
        except Exception:
            await msg.reply_text(txt, reply_markup=markup)

    elif cmd == "about":
        await query.answer()
        await _send_about(client, query, bot_id)

    elif cmd == "back":
        await query.answer()
        # Go back to welcome — edit current message in-place
        bot_name = client.me.first_name if client.me else "Delivery Bot"
        user = query.from_user
        full_name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
        mention = f'<a href="tg://user?id={user.id}">{full_name.strip()}</a>'

        bot_about = await db.get_share_bot_about(bot_id) if bot_id else {}
        about_img = bot_about.get('image_id') if bot_about else None

        custom_wel = (await db.get_share_bot_text(bot_id, "welcome_msg") if bot_id else "") or \
                     await db.get_share_text("welcome_msg", "")
        if custom_wel:
            txt = format_msg(custom_wel, user).replace("{mention}", mention)
        else:
            txt = (
                f"<b>👋 Wᴇʟᴄᴏᴍᴇ ᴛᴏ {bot_name}!</b>\n\n"
                f"Hᴇʟʟᴏ {mention} ✨\n\n"
                "<i>I am a permanent file store bot — users can access stored messages "
                "by using a shareable link created for them.</i>\n\n"
                "Click a link button from the channel to receive your files directly here.\n"
                "<i>To know more, click the Help button below.</i>"
            )
        buttons = [
            [
                InlineKeyboardButton("ʜᴇʟᴘ", callback_data="sbd#help"),
                InlineKeyboardButton("ᴀʙᴏᴜᴛ", callback_data="sbd#about"),
            ],
            [InlineKeyboardButton("🔔 Uᴘᴅᴀᴛᴇ Cʜᴀɴɴᴇʟ", url=UPDATE_LINK)]
        ]
        markup = InlineKeyboardMarkup(buttons)
        try:
            if is_photo:
                await msg.edit_caption(caption=txt, reply_markup=markup)
            else:
                await msg.edit_text(txt, reply_markup=markup)
        except Exception:
            pass
    else:
        await query.answer()


async def _process_delivery_cancel(client, query):
    """Handle cancel button during file delivery."""
    uuid_str = query.data.replace("cancel_dl_", "", 1)
    dl_id = f"{query.from_user.id}_{uuid_str}"
    if dl_id in active_downloads:
        active_downloads.discard(dl_id)
        await query.answer("Download cancelled.", show_alert=True)
        try:
            await query.message.edit_text("<b>🚫 Dᴏᴡɴʟᴏᴀᴅ Cᴀɴᴄᴇʟʟᴇᴅ.</b>")
        except Exception:
            pass
    else:
        await query.answer("Already finished or cancelled.", show_alert=True)


# ─────────────────────────────────────────────────────────────────────────────
# Registration & Startup
# ─────────────────────────────────────────────────────────────────────────────

def register_share_handlers(app: Client):
    """Register all handlers on a started Client instance."""
    # Auto-approve join requests for JR channels so users get instant access
    app.add_handler(ChatJoinRequestHandler(_fsub_record_jr))
    app.add_handler(MessageHandler(
        _process_start,
        filters.private & filters.command("start")
    ))
    app.add_handler(CallbackQueryHandler(
        _process_delivery_button,
        filters.regex(r'^sbd#')
    ))
    app.add_handler(CallbackQueryHandler(
        _process_delivery_cancel,
        filters.regex(r'^cancel_dl_')
    ))
    logger.info(f"Handlers registered on {app.name}")


async def start_share_bot():
    """Start all Share Bot clients from DB."""
    global share_clients

    # Stop existing clients first
    for cl in list(share_clients.values()):
        try:
            await cl.stop()
        except Exception:
            pass
    share_clients.clear()

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
            sc.is_initialized = True
            register_share_handlers(sc)
            share_clients[b['id']] = sc
            logger.info(f"Share Bot started: @{sc.me.username} [{b['name']}]")
        except Exception as e:
            logger.error(f"Failed to start Share Bot '{b['name']}': {e}")
