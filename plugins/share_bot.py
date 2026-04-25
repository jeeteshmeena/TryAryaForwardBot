"""
Share Bot — Delivery Agent
==========================
Handles deep-link delivery of batched episodes to users.
Handler functions are defined at module level so they can be passed to
add_handler() after the client is started (Pyrogram 2.x requirement).
"""
import logging
import asyncio
import random
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import UserNotParticipant
from pyrogram.handlers import MessageHandler, CallbackQueryHandler, ChatJoinRequestHandler
from database import db
from config import Config

logger = logging.getLogger(__name__)

share_clients: dict = {}   # { bot_id_str: Client }
active_downloads: set = set()

# Peer cache: tracks already-resolved chat_ids per client session.
# Avoids redundant get_chat() calls on every delivery request.
_peer_cache: dict = {}    # { (client_id, chat_id): timestamp }
_PEER_CACHE_TTL = 3600    # 1 hour — re-warm after this long

# Join-request tracking: records that a user has a pending join request.
# Format: "{chat_id}_{user_id}": timestamp_of_first_request
# TTL extended to 10 days because admin approval can take days.
_jr_approved: dict = {}
_JR_TTL = 864000          # 10 days

# 
# Arya Bot Font constants
# 
ARYA_VERSION = "V1.0"
UPDATE_LINK   = "https://t.me/MeJeetX"
SUPPORT_LINK  = "https://t.me/LightchatX"

# 
# Helpers
# 

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

def _sc(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
    ))

def _get_base_header(user) -> str:
    # Only first name, no last name — used in non-welcome contexts (About, Help)
    u_name = user.first_name or "User"
    return f"›› ʜᴇʏ, <a href='tg://user?id={user.id}'>{u_name}</a>\n\n"

def _get_welcome_text(user, bot_name, custom_wel=None) -> str:
    if custom_wel:
        return format_msg(custom_wel, user)
    first = user.first_name or "User"
    return (
        # Block 1: Greeting with first name only
        f"<blockquote expandable>›› ʜᴇʏ, <a href='tg://user?id={user.id}'>{first}</a>❣️</blockquote>\n"
        # Block 2: Welcome line
        f"<blockquote expandable><b>»  {_sc('Welcome to')} {bot_name}!</b></blockquote>\n"
        # Block 3: Description
        f"<blockquote expandable>{_sc('I am a file delivery bot. Tap any link button from the channel and I will send you the files directly here.')}</blockquote>\n"
        # Block 4: Help hint
        f"<blockquote expandable>{_sc('Click Help for more info.')}</blockquote>"
    )


def _get_help_text(user) -> str:
    return _get_base_header(user) + _sc(
        "Help Menu\n\n"
        "I am a permanent file store bot. You can access stored files by using "
        "a shareable link given by me from the channel.\n\n"
        "How to Get Files:\n"
        "➲  Open the channel and tap a link button\n"
        "➲  I will send the files directly to your DM\n"
        "➲  If force-subscribe is enabled, join required channels first\n"
        "➲  If your files are deleted, tap the same button again\n\n"
        "Available Commands:\n"
        "➲  /start — check if I'm alive\n"
        "➲  Click any episode link button in the channel to receive files\n\n"
        "Bot Info:\n"
        "➲  All deliveries are encrypted and protected\n"
        "➲  Files may auto-delete after a set time (copyright protection)\n"
        "➲  Simply click your link button again to re-download"
    )


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


async def _warm_peer(client, chat_id) -> None:
    """
    Resolve chat_id in the client's peer cache.
    Skips the network call if we've resolved it in the past hour.
    This avoids the 200-400ms latency spike on every delivery request.
    """
    import time
    client_id = getattr(client, 'me', None)
    client_id = client_id.id if client_id else id(client)
    key = (client_id, int(chat_id))
    if key in _peer_cache and (time.time() - _peer_cache[key]) < _PEER_CACHE_TTL:
        return   # already warm — skip the network call
    
    from bot import BOT_INSTANCE
    from plugins.utils import safe_resolve_peer
    try:
        await safe_resolve_peer(client, int(chat_id), bot=BOT_INSTANCE)
    except Exception:
        pass
        
    try:
        await client.get_chat(int(chat_id))
        _peer_cache[key] = time.time()
    except Exception:
        pass


_fsub_user_cache = {}  # { "uid_chatid": expiration_timestamp }

async def check_all_subscriptions(client, user_id: int, fsub_channels: list, bot_id: str = None) -> list:
    """
    Returns list of channel dicts the user has NOT joined.
    
    Verifies all channels in parallel for maximum speed.
    Normal channels are ALWAYS verified live with no caching (per user request).
    JR Channels are cached for 2 mins upon successful join request DB match.
    """
    import time
    import asyncio
    now = time.time()
    
    async def _check_single(ch):
        chat_id = ch.get('chat_id')
        if not chat_id:
            return None

        is_jr = ch.get('join_request', False)

        # Resolve numeric chat_id
        from bot import BOT_INSTANCE
        from plugins.utils import safe_resolve_peer
        
        ch_id_int = int(chat_id) if str(chat_id).lstrip('-').isdigit() else chat_id
        cache_key = f"{user_id}_{ch_id_int}"
        if cache_key in _fsub_user_cache and _fsub_user_cache[cache_key] > now:
            return None

        try:
            await safe_resolve_peer(client, chat_id, bot=BOT_INSTANCE)
        except Exception:
            pass

        try:
            member = await client.get_chat_member(ch_id_int, user_id)
            if getattr(member, 'status', None) in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED):
                raise UserNotParticipant()
            else:
                # Aggressively Cache SUCCESS for 2 hours to prevent FloodWaits across bulk link-clicks!
                _fsub_user_cache[cache_key] = now + 7200
                return None
        except UserNotParticipant:
            _fsub_user_cache.pop(cache_key, None)
            
            if is_jr:
                try:
                    ch_id_for_query = int(ch_id_int)
                except (ValueError, TypeError):
                    ch_id_for_query = ch_id_int
                    
                jr_query = {"user_id": int(user_id)}
                if isinstance(ch_id_for_query, int):
                    jr_query["$or"] = [{"chat_id": ch_id_for_query}, {"chat_id": str(ch_id_for_query)}]
                else:
                    cln = str(ch_id_for_query).lstrip("@").lower()
                    jr_query["$or"] = [{"chat_id": ch_id_for_query}, {"chat_id": str(ch_id_for_query)}, {"username": cln}]

                jr_doc = await db.db["pending_jrs"].find_one(jr_query)
                
                if jr_doc and (now - jr_doc.get("timestamp", 0) < _JR_TTL):
                    _fsub_user_cache[cache_key] = now + 120
                    logger.info(f"FSub: JR grant for user {user_id} in {ch_id_int}")
                    return None
                else:
                    ch_copy = dict(ch)
                    ch_copy['needs_request'] = True
                    return ch_copy
            else:
                ch_copy = dict(ch)
                ch_copy['never_joined'] = True
                return ch_copy
        except Exception as e:
            logger.warning(f"FSub check skipped for {chat_id}: {e}")
            return None

    tasks = [_check_single(ch) for ch in fsub_channels]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]



# 
# Module-level handler functions (required for add_handler to work)
# 

async def _fsub_record_jr(client, request):
    """
    Record that a user has sent a join request to a JR channel in persistent DB.
    Stores chat_id as INT to ensure consistent type for later lookups.
    """
    import time
    bot_id = str(client.me.id) if client.me else None
    fsub_chs = await db.get_bot_fsub_channels(bot_id) if bot_id else []
    if not fsub_chs:
        fsub_chs = await db.get_share_fsub_channels()

    req_ch_id = request.chat.id    # integer from Telegram
    req_user_id = request.from_user.id  # integer

    for ch in fsub_chs:
        ch_id = ch.get('chat_id')
        # Normalize for comparison
        try:
            ch_id_cmp = int(ch_id)
        except (ValueError, TypeError):
            ch_id_cmp = str(ch_id).lstrip('@').lower()

        req_username = str(getattr(request.chat, 'username', '') or '').lower()
        ch_username  = str(ch_id).lstrip('@').lower()

        matched = (
            req_ch_id == ch_id_cmp
            or (req_username and req_username == ch_username)
        )
        if matched and ch.get('join_request'):
            # Always store as int so the lookup in check_all_subscriptions matches
            await db.db["pending_jrs"].update_one(
                {"user_id": int(req_user_id), "chat_id": int(req_ch_id)},
                {"$set": {"timestamp": time.time(), "username": req_username}},
                upsert=True
            )
            # Also evict the FSub cache so next check hits DB fresh
            cache_key = f"{req_user_id}_{req_ch_id}"
            _fsub_user_cache.pop(cache_key, None)
            logger.info(f"JR recorded for user {req_user_id} in {req_ch_id} (TTL: 10d)")
            return


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
            "<b>‣  Link Expired or Invalid</b>\n\n"
            "This batch link no longer exists. Go back to the channel and click the button again."
        )
        return

    msg_ids     = link_data.get('message_ids', [])
    source_chat = link_data.get('source_chat')
    protect_flag = await db.get_share_protect_global()

    if not msg_ids or not source_chat:
        await message.reply_text("<b>‣  Database Error:</b> Missing file references.")
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
            _ordinal_sfx = ['ꜱᴛ','ɴᴅ','ʀᴅ','ᴛʜ','ᴛʜ','ᴛʜ','ᴛʜ','ᴛʜ']
            for ch in not_joined:
                invite  = ch.get('invite_link', '')
                is_jr   = ch.get('join_request', False)
                sfx = _ordinal_sfx[min(channel_num - 1, 7)]
                label = f"{channel_num}{sfx} Cʜᴀɴɴᴇʟ"
                channel_num += 1
                if invite:
                    f_buttons.append(InlineKeyboardButton(label, url=invite))

            rows = []
            for i in range(0, len(f_buttons), 2):
                rows.append(f_buttons[i:i+2])
            rows.append([
                InlineKeyboardButton(
                    "Tʀʏ Aɢᴀɪɴ",
                    callback_data=f"fsub_chk_{uuid_str}"
                )
            ])

            # FSub message: custom DB text or auto-generated based on situation
            fsub_msg = await db.get_share_bot_text(bot_id, "fsub_msg") if bot_id else ""
            if not fsub_msg:
                fsub_msg = await db.get_share_text("fsub_msg", "")
            if fsub_msg:
                txt = format_msg(fsub_msg, message.from_user)
            else:
                user_name = message.from_user.first_name or "User"
                has_jr       = any(ch.get('needs_request') for ch in not_joined)
                never_joined = any(ch.get('never_joined') for ch in not_joined)

                if has_jr:
                    # JR channel — join request already pending or needs to be sent
                    txt = (
                        f"<b>🔒  Aᴄᴄᴇss Dᴇɴɪᴇᴅ</b>\n\n"
                        f"Hey <b>{user_name}</b>,\n"
                        f"You must send a <b>Jᴏɪɴ Rᴇǫᴜᴇsᴛ</b> to the channel(s) below."
                        f" Once your request is approved by the admin you will get access automatically.\n\n"
                        f"<i>Already sent a request? Tap <b>Tʀʏ Aɢᴀɪɴ</b> — your request is being reviewed!</i>"
                    )
                else:
                    # Normal channel — never joined
                    txt = (
                        f"<b>🔒  Aᴄᴄᴇss Dᴇɴɪᴇᴅ</b>\n\n"
                        f"Hey <b>{user_name}</b>,\n"
                        f"You must join our update channel(s) below to access these files.\n\n"
                        f"<b>Steps:</b>\n"
                        f"① Tap the channel button → Join\n"
                        f"② Tap <b>Tʀʏ Aɢᴀɪɴ</b> below to unlock your files instantly!"
                    )
            await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(rows))
            return

    # 3. Warm peer cache for source channel (cached — near-instant on repeat requests)
    await _warm_peer(client, source_chat)

    # Send actual files
    sent_ids = []
    auto_delete_mins = (await db.get_share_bot_about(bot_id)).get('auto_delete', 0) if bot_id else 0
    if not auto_delete_mins:
        auto_delete_mins = await db.get_share_autodelete_global()

    # 5. Deliver
    dl_id = f"{user_id}_{uuid_str}"
    active_downloads.add(dl_id)

    # Show configurable fetching media (GIF / Photo / Video) or fallback to text
    fetching_media = await db.get_bot_fetching_media(bot_id) if bot_id else []
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"cancel_dl_{uuid_str}")]])
    fetch_text = "<i>»  Fᴇᴛᴄʜɪɴɢ ʏᴏᴜʀ ꜰɪʟᴇs sᴇᴄᴜʀᴇʟʏ, ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ...</i>"
    sts = None

    if fetching_media:
        import random
        fm = random.choice(fetching_media)
        fid  = fm.get('file_id')
        ftyp = fm.get('media_type', 'photo')
        try:
            if ftyp == 'animation':
                sts = await client.send_animation(
                    user_id, animation=fid, caption=fetch_text,
                    reply_markup=cancel_kb
                )
            elif ftyp == 'video':
                sts = await client.send_video(
                    user_id, video=fid, caption=fetch_text,
                    reply_markup=cancel_kb
                )
            else:
                sts = await client.send_photo(
                    user_id, photo=fid, caption=fetch_text,
                    reply_markup=cancel_kb
                )
            logger.info(f"[Fetch] Sent {ftyp} to user {user_id} via bot {bot_id}")
        except Exception as _fe:
            # Log the exact error so we know WHY it failed
            logger.warning(
                f"[Fetch] Media send FAILED for bot={bot_id} user={user_id} "
                f"type={ftyp} file_id={fid[:30]}... error: {_fe}"
            )
            # Do NOT clear the DB — just fall back to text for this request.
            # File references can expire; the admin can re-upload to refresh.
            sts = None

    if sts is None:
        # Fallback: plain text status
        sts = await message.reply_text(fetch_text, reply_markup=cancel_kb)

    sent_ids   = []
    fail_count = 0
    cap_tpl    = (await db.get_share_bot_text(bot_id, "custom_caption") if bot_id else "") or \
                 await db.get_share_text("custom_caption", "")
    formatted_cap = format_msg(cap_tpl, message.from_user) if cap_tpl else None

    for msg_id in msg_ids:
        if dl_id not in active_downloads:
            break  # cancel handler already edited the status
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
        await asyncio.sleep(0.02)

    active_downloads.discard(dl_id)
    try:
        await sts.delete()
    except Exception:
        pass

    total = len(sent_ids)
    if total == 0:
        await message.reply_text(
            "<b>‣  Dᴇʟɪᴠᴇʀʏ Fᴀɪʟᴇᴅ</b>\n\n"
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
                f"<i>‣  Important: {total} file(s) delivered! Due to copyright, all messages "
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
               else f"<i>‣  Important: {total} file(s) delivered! Due to copyright, all messages "
                    f"will auto-delete after 3 hours. "
                    f"To re-access, simply click the same link button again.{fail_note}</i>")
        await message.reply_text(txt)

    # ── Increment global delivery counter + Enhanced bilingual Thank-You ──
    if bot_id:
        await db.increment_bot_delivery_count(bot_id, total)
    grand_total = (await db.get_bot_delivery_count(bot_id)) if bot_id else total

    u_name = message.from_user.first_name or "you"
    last   = (" " + message.from_user.last_name) if getattr(message.from_user, "last_name", None) else ""
    full_name = f"{u_name}{last}"
    
    b_name = client.me.first_name if getattr(client, "me", None) else "this bot"

    thank_txt = (
        f"<b>»</b> <a href='tg://user?id={message.from_user.id}'>{full_name}</a>\n\n"
        f"<b>‣ {total} file(s) sent successfully!</b>\n"
        f"<b>‣</b> Total delivered by {b_name}: <b>{grand_total:,}</b> files\n\n"
        f"<blockquote expandable>"
        f"Thank you for using our service! Your files have been successfully delivered. "
        f"These links are permanent and never expire — you can simply tap the same button anytime "
        f"to re-access your files instantly.\n\n"
        f"If you enjoy our platform and want us to keep delivering amazing stories, "
        f"please consider supporting us with a small donation. Every contribution helps us maintain "
        f"our servers and expand our library."
        f"</blockquote>"
        f"<blockquote expandable>"
        f"हमारी सेवा का उपयोग करने के लिए आपका धन्यवाद! आपकी फाइलें सुगमता से डिलीवर हो गई हैं। "
        f"ये लिंक कभी expire नहीं होते — आप भविष्य में कभी भी उसी बटन पर क्लिक करके अपनी फाइलें "
        f"दोबारा प्राप्त कर सकते हैं।\n\n"
        f"अगर आपको हमारी सेवा पसंद आई है और आप चाहते हैं कि हम निरंतर उत्कृष्ट कहानियाँ "
        f"लाते रहें, तो कृपया हमें donation देकर support करें। आपका सहयोग हमारे सर्वर "
        f"और सेवाओं को बेहतर बनाने में अत्यंत सहायक है।"
        f"</blockquote>"
    )
    donate_btn = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💳 " + _sc("Support via UPI"), callback_data="sbd#donate"),
            InlineKeyboardButton("🔗 " + _sc("Razorpay"), url="https://razorpay.me/@SusJeetX")
        ]
    ])
    try:
        await message.reply_text(thank_txt, reply_markup=donate_btn)
    except Exception as _te:
        logger.warning(f"[ThankYou] send failed: {_te}")
    except Exception as e:
        active_downloads.discard(dl_id)
        try:
            await sts.delete()
        except Exception:
            pass
        await message.reply_text(
            f"<b>‣  Dᴇʟɪᴠᴇʀʏ Eʀʀᴏʀ:</b> <code>{e}</code>\n\n"
            "<i>The Share Bot must be an admin in the Database Channel to deliver files.</i>"
        )

async def _send_welcome(client, message, bot_id: str = None):
    """Send the welcome message + Help/About buttons."""
    user = message.from_user
    bot_name = client.me.first_name if client.me else "Delivery Bot"

    # Bot-specific welcome text or global
    custom_wel = (await db.get_share_bot_text(bot_id, "welcome_msg") if bot_id else "") or \
                 await db.get_share_text("welcome_msg", "")

    txt = _get_welcome_text(user, bot_name, custom_wel)

    bot_about = await db.get_share_bot_about(bot_id) if bot_id else {}
    # menu_image_id is set by admin via "🖼 Menu Image" in per-bot settings
    welcome_img = random.choice(bot_about.get('menu_image_ids', [])) if bot_about and bot_about.get('menu_image_ids') else None

    buttons = [
        [
            InlineKeyboardButton(_sc("Help"), callback_data="sbd#help"),
            InlineKeyboardButton(_sc("About"), callback_data="sbd#about"),
        ],
        [InlineKeyboardButton("»  " + _sc("Update Channel"), url=UPDATE_LINK)]
    ]
    markup = InlineKeyboardMarkup(buttons)

    try:
        if welcome_img:
            # Handle new dict format {"file_id": ..., "media_type": ...} vs old string format (photo)
            wid  = welcome_img.get('file_id') if isinstance(welcome_img, dict) else welcome_img
            wtyp = welcome_img.get('media_type', 'photo') if isinstance(welcome_img, dict) else 'photo'

            try:
                if wtyp == 'animation':
                    await client.send_animation(user.id, animation=wid, caption=txt, reply_markup=markup)
                elif wtyp == 'video':
                    await client.send_video(user.id, video=wid, caption=txt, reply_markup=markup)
                else:
                    await client.send_photo(user.id, photo=wid, caption=txt, reply_markup=markup)
                return  # success — skip text fallback
            except Exception as _media_err:
                logger.warning(f"[Welcome] Media send failed ({_media_err}), auto-clearing bad image and falling back to text")
                # Auto-clear stale/expired file_ids from DB so the error won't repeat
                try:
                    if bot_id:
                        about = await db.get_share_bot_about(bot_id) or {}
                        img_ids = about.get('menu_image_ids', [])
                        bad_fid = wid
                        cleaned = [x for x in img_ids if (x.get('file_id') if isinstance(x, dict) else x) != bad_fid]
                        await db.db.share_config.update_one(
                            {'_id': f'bot_{bot_id}_about'},
                            {'$set': {'menu_image_ids': cleaned}},
                            upsert=True
                        )
                except Exception:
                    pass

        # Reached here either because welcome_img is None or media send failed
        await message.reply_text(txt, reply_markup=markup)
    except Exception as _wel_err:
        logger.warning(f"[Welcome] Text fallback also failed: {_wel_err}")
        pass


async def _send_help(client, message, bot_id: str = None):
    """Send the Help menu for /start help."""
    txt = _get_help_text(message.from_user)
    buttons = [
        [InlineKeyboardButton("«  " + _sc("Back"), callback_data="sbd#back")],
        [InlineKeyboardButton("»  " + _sc("Update Channel"), url=UPDATE_LINK)]
    ]
    try:
        await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        pass


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
    about_text   = about.get('custom_text', None)
    
    msg = query_or_msg if hasattr(query_or_msg, 'photo') else getattr(query_or_msg, 'message', query_or_msg)
    user = getattr(query_or_msg, 'from_user', getattr(msg, 'from_user', None))

    if about_text:
        # Custom text: do NOT apply _sc — user may have hand-crafted formatting/links
        txt = _get_base_header(user) + about_text
    else:
        # Build the body with clickable HTML links — do NOT pass through _sc()
        # _sc() converts every ASCII char to Unicode small-caps, which destroys href URLs
        txt = (
            f"{_get_base_header(user)}"
            f"<b>»  ᴀʙᴏᴜᴛ ᴍᴇ</b>\n\n"
            f"<b>‣  ɴᴀᴍᴇ:</b>  {bot_name}\n"
            f"<b>‣  ᴏᴘᴇʀᴀᴛᴇᴅ ʙʏ:</b>  Arya Bot\n"
            f"<b>‣  ᴏᴡɴᴇʀ:</b>  <a href=\"{owner_link}\">{owner_name}</a>\n"
            f"<b>‣  ᴜᴘᴅᴀᴛᴇꜱ:</b>  <a href=\"{update_link}\">{update_chan}</a>\n"
            f"<b>‣  ꜱᴜᴘᴘᴏʀᴛ:</b>  <a href=\"{support_link}\">{support_chan}</a>\n"
            f"<b>‣  ᴠᴇʀꜱɪᴏɴ:</b>  {version}"
        )

    buttons = [[InlineKeyboardButton("«  " + _sc("Back"), callback_data="sbd#back")]]
    markup  = InlineKeyboardMarkup(buttons)

    is_media_msg = bool(getattr(msg, 'photo', None) or getattr(msg, 'animation', None) or getattr(msg, 'video', None))
    try:
        if is_media_msg:
            await msg.edit_caption(caption=txt, reply_markup=markup)
        else:
            await msg.edit_text(txt, reply_markup=markup,
                                disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"_send_about edit failed: {e}")

async def _process_delivery_button(client, query):
    """Handle inline buttons on the welcome/help/about messages."""
    cmd = query.data.split('#')[1] if '#' in query.data else ''
    bot_id = str(client.me.id) if client.me else None
    msg = query.message
    is_media_msg = bool(getattr(msg, 'photo', None) or getattr(msg, 'animation', None) or getattr(msg, 'video', None))

    if cmd == "help":
        await query.answer()
        txt = _get_help_text(query.from_user)
        buttons = [
            [InlineKeyboardButton("«  " + _sc("Back"), callback_data="sbd#back")],
            [InlineKeyboardButton("»  " + _sc("Update Channel"), url=UPDATE_LINK)]
        ]
        markup = InlineKeyboardMarkup(buttons)
        try:
            if is_media_msg: await msg.edit_caption(caption=txt, reply_markup=markup)
            else: await msg.edit_text(txt, reply_markup=markup)
        except Exception: pass

    elif cmd == "about":
        await query.answer()
        await _send_about(client, query, bot_id=bot_id, edit=True)

    elif cmd == "donate":
        await query.answer()
        sup_text = (
            f"<b>💖 " + _sc("support arya bot") + "</b>\n\n"
            f"<i>Your support keeps our servers running and allows us to deliver uninterrupted, high-quality content.</i>\n\n"
            f"<b>💳 " + _sc("direct upi details:") + "</b>\n"
            f"<b>‣  " + _sc("upi id:") + "</b>  <code>heyjeetx@naviaxis</code>\n"
            f"<b>‣  " + _sc("name:") + "</b>  Jeetesh Meena\n\n"
            f"<b>" + _sc("please choose an amount below to generate a direct payment qr code!") + "</b>"
        )
        buttons = [
            [
                InlineKeyboardButton("💸 ₹50", callback_data="sbd#pay_upi#50"),
                InlineKeyboardButton("💸 ₹100", callback_data="sbd#pay_upi#100"),
                InlineKeyboardButton("💸 ₹200", callback_data="sbd#pay_upi#200")
            ],
            [
                InlineKeyboardButton("💸 ₹500", callback_data="sbd#pay_upi#500"),
                InlineKeyboardButton("📝 " + _sc("custom amount"), callback_data="sbd#pay_upi#custom")
            ],
            [
                InlineKeyboardButton("🌍 " + _sc("non-upi / intl (razorpay)"), url="https://razorpay.me/@SusJeetX")
            ]
        ]
        try:
            await client.send_message(query.from_user.id, sup_text, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            pass

    elif cmd == "pay_upi":
        parts = query.data.split('#')
        am = parts[2] if len(parts) > 2 else "custom"
        await query.answer()
        
        if am == "custom":
            upi_uri = "upi://pay?pa=heyjeetx@naviaxis&pn=Jeetesh%20Meena&cu=INR"
            am_txt = "<code>Any Custom Amount</code>"
            instruction_txt = (
                f"<i>Scan the QR Code above, or use the direct deep-link below.\n"
                f"For Custom Amounts, your Payment App will automatically prompt you to enter the amount you wish to contribute!</i>"
            )
        else:
            upi_uri = f"upi://pay?pa=heyjeetx@naviaxis&pn=Jeetesh%20Meena&am={am}&cu=INR"
            am_txt = f"<code>₹{am}</code>"
            instruction_txt = f"<i>Scan the exact ₹{am} QR Code above, or use the direct deep-link below:</i>"
            
        caption = (
            f"<b>📱 {_sc('scan or tap to support')}</b>\n\n"
            f"<b>‣  {_sc('amount:')}</b>  {am_txt}\n"
            f"<b>‣  {_sc('upi id:')}</b>  <code>heyjeetx@naviaxis</code>\n"
            f"<b>‣  {_sc('name:')}</b>  Jeetesh Meena\n\n"
            f"{instruction_txt}"
        )
        
        import urllib.parse
        encoded_uri = urllib.parse.quote(upi_uri)
        qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=500x500&margin=2&data={encoded_uri}"
        
        buttons = [
            [InlineKeyboardButton("🌍 " + _sc("pay via razorpay instead"), url="https://razorpay.me/@SusJeetX")]
        ]
        
        try:
            await msg.delete()
            await client.send_photo(query.from_user.id, photo=qr_url, caption=caption, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception as e:
            logger.error(f"Support QR Error: {e}")

    elif cmd == "back":
        await query.answer()
        bot_name = client.me.first_name if client.me else "Delivery Bot"
        custom_wel = (await db.get_share_bot_text(bot_id, "welcome_msg") if bot_id else "") or await db.get_share_text("welcome_msg", "")
        txt = _get_welcome_text(query.from_user, bot_name, custom_wel)
        
        buttons = [
            [
                InlineKeyboardButton(_sc("Help"), callback_data="sbd#help"),
                InlineKeyboardButton(_sc("About"), callback_data="sbd#about"),
            ],
            [InlineKeyboardButton("»  " + _sc("Update Channel"), url=UPDATE_LINK)]
        ]
        markup = InlineKeyboardMarkup(buttons)
        try:
            if is_media_msg: await msg.edit_caption(caption=txt, reply_markup=markup)
            else: await msg.edit_text(txt, reply_markup=markup)
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

async def _process_fsub_check(client, query):
    """Handle Try Again callback for Force Subscribe."""
    uuid_str = query.data.replace("fsub_chk_", "", 1)
    
    # 1. Animation Step 1
    await query.message.edit_text("Lᴇᴛ ᴍᴇ ᴄʜᴇᴄᴋ ꜰᴏʀ ʏᴏᴜ...")
    
    bot_id = str(client.me.id) if client.me else None
    user_id = query.from_user.id
    
    # 2. Re-check FSub
    fsub_channels = await db.get_bot_fsub_channels(bot_id) if bot_id else []
    if not fsub_channels:
        fsub_channels = await db.get_share_fsub_channels()
        
    not_joined = []
    if fsub_channels:
        not_joined = await check_all_subscriptions(client, user_id, fsub_channels, bot_id)
        
    if not_joined:
        # Animation Step 2: Failed
        f_buttons = []
        channel_num = 1
        for ch in not_joined:
            invite  = ch.get('invite_link', '')
            is_jr   = ch.get('join_request', False)
            label   = f"Jᴏɪɴ Cʜᴀɴɴᴇʟ {channel_num}"
            channel_num += 1
            if invite:
                emoji = "» " if is_jr else "» "
                f_buttons.append(InlineKeyboardButton(f"{emoji} {label}", url=invite))

        rows = []
        for i in range(0, len(f_buttons), 2):
            rows.append(f_buttons[i:i+2])
        rows.append([
            InlineKeyboardButton(
                "Tʀʏ Aɢᴀɪɴ",
                callback_data=f"fsub_chk_{uuid_str}"
            )
        ])
        
        await query.message.edit_text(
            "I ᴄᴀɴɴᴏᴛ ɢɪᴠᴇ ʏᴏᴜ ᴀᴄᴄᴇꜱꜱ ʙᴇᴄᴀᴜꜱᴇ ʏᴏᴜ ʜᴀᴠᴇ ɴᴏᴛ ꜰᴜʟꜰɪʟʟᴇᴅ ᴛʜᴇ ʀᴇQᴜɪʀᴇᴍᴇɴᴛꜱ. Tʀʏ ᴀɢᴀɪɴ.",
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    # Animation Step 2: Success
    # Delete the check message and hand off to _process_start by spoofing a message
    try:
        await query.message.delete()
    except Exception: pass
    
    msg = query.message
    msg.from_user = query.from_user
    msg.command = ["start", uuid_str]
    await _process_start(client, msg)




# 
# Registration & Startup
# 

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
    app.add_handler(CallbackQueryHandler(
        _process_fsub_check,
        filters.regex(r'^fsub_chk_')
    ))

    # Add AI Enhancer support to Delivery Bot seamlessly
    try:
        from plugins.enhancer import enhance_offer_handler, enhance_execute_cb
        app.add_handler(MessageHandler(
            enhance_offer_handler,
            filters.private & (filters.photo | filters.document) & ~filters.forwarded
        ))
        app.add_handler(CallbackQueryHandler(
            enhance_execute_cb,
            filters.regex(r'^enh#do$')
        ))
    except ImportError: pass
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
            import os
            os.makedirs("sessions", exist_ok=True)
            sc = Client(
                name=f"share_bot_{b['id']}_{index}",
                bot_token=b['token'],
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                workdir="sessions"
            )
            await sc.start()
            sc.is_initialized = True
            register_share_handlers(sc)
            # ← Always store as STRING so live_batch / other lookups via str() always match
            share_clients[str(b['id'])] = sc
            logger.info(f"Share Bot started: @{sc.me.username} [{b['name']}]")
        except Exception as e:
            logger.error(f"Failed to start Share Bot '{b['name']}': {e}")

