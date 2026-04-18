import asyncio
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

# ── Shared waiting futures store ──
_waiting_futures: dict = {}

# ── Smallcap font converter (shared helper) ──
def to_smallcap(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
    ))

def _ask_key(bot: Client, user_id: int):
    return (getattr(getattr(bot, "me", None), "id", 0), int(user_id))

async def _input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    key = _ask_key(bot, uid) if uid else None
    if key and key in _waiting_futures:
        fut = _waiting_futures.pop(key)
        if not fut.done():
            fut.set_result(message)
            from pyrogram import StopPropagation
            raise StopPropagation
    message.continue_propagation()

async def _cb_input_router(bot, query):
    uid = query.from_user.id
    if query.data in ["ask_cancel", "ask_skip"]:
        key = _ask_key(bot, uid)
        if key and key in _waiting_futures:
            fut = _waiting_futures.pop(key)
            if not fut.done():
                fut.set_result(query)
            try: await query.answer()
            except: pass
            return
    query.continue_propagation()

def setup_ask_router(bot: Client):
    bot.add_handler(MessageHandler(_input_router, filters.private), group=-100)
    bot.add_handler(CallbackQueryHandler(_cb_input_router), group=-100)

async def native_ask(bot, user_id: int, text: str, reply_markup=None, timeout: int = 300):
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    key = _ask_key(bot, user_id)

    old = _waiting_futures.pop(key, None)
    if old and not old.done():
        old.cancel()

    _waiting_futures[key] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _waiting_futures.pop(key, None)
        raise

async def _deliver_purchased_story(bot_id: str, user_id: int, story: dict):
    """Delegates to the market_seller delivery engine after payment approval."""
    from plugins.userbot.market_seller import market_clients, dispatch_delivery_choice
    import logging
    logger = logging.getLogger(__name__)

    seller_cli = market_clients.get(str(bot_id))
    if not seller_cli:
        logger.error(f"Cannot deliver: store bot {bot_id} not running.")
        return

    # Show the delivery choice screen to the user (DM vs Channel as inline buttons)
    await dispatch_delivery_choice(seller_cli, user_id, story)

async def log_payment(user_id: int, user_first_name: str, s_name: str, amount, method: str,
                      receipt_id: str = "", photo_path: str = None, username: str = "", pay_link: str = "", order_id: str = "", user_last_name: str = ""):
    from config import Config
    from database import db
    if not getattr(Config, "PAYMENT_LOGS_CHANNEL", None) or not db.mgmt_client: return
    try:
        from datetime import datetime, timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        time_str = datetime.now(ist).strftime('%d %b %Y, %I:%M %p IST')
        
        method_badge = {
            "razorpay":  "💳 Razorpay (Automatic)",
            "easebuzz":  "💸 Easebuzz (Automatic)",
            "upi":       "🏦 Manual UPI",
            "manual_upi":"🏦 Manual UPI",
        }.get(method.lower(), method.capitalize())

        uname_line = f"@{username}" if username else "—"
        tg_link = f"tg://user?id={user_id}"
        full_name = f"{user_first_name} {user_last_name}".strip()

        link_line = ""
        if pay_link and "razorpay" in method.lower():
            link_line = f"\n<b>Payment Link:</b> <a href=\"{pay_link}\">View Receipt</a>"

        caption = (
            f"<b>✅ PAYMENT CONFIRMED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>❖ Order ID:</b> <code>{order_id or 'N/A'}</code>\n"
            f"<b>❖ User:</b> <a href=\"{tg_link}\">{full_name}</a> ({uname_line})\n"
            f"<b>❖ Telegram ID:</b> <code>{user_id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>❖ Story:</b> {s_name}\n"
            f"<b>❖ Amount Paid:</b> ₹{amount}\n"
            f"<b>❖ Method:</b> {method_badge}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>❖ Receipt / Gateway ID:</b>\n<code>{receipt_id or 'N/A'}</code>"
            f"{link_line}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>❖ Time:</b> {time_str}"
        )
        if photo_path:
            await db.mgmt_client.send_photo(int(Config.PAYMENT_LOGS_CHANNEL), photo=photo_path, caption=caption)
        else:
            await db.mgmt_client.send_message(int(Config.PAYMENT_LOGS_CHANNEL), text=caption)
    except Exception as e:
        import logging; logging.getLogger(__name__).error(f"Payment log error: {e}")

async def log_delivery(bot_username: str, user_id: int, user_first_name: str, s_name: str, d_type: str, status: str, username: str = "", order_id: str = "", user_last_name: str = ""):
    from config import Config
    from database import db
    if not getattr(Config, "DELIVERY_LOGS_CHANNEL", None) or not db.mgmt_client: return
    try:
        from datetime import datetime, timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        time_str = datetime.now(ist).strftime('%d %b %Y, %I:%M %p IST')
        
        uname_line = f"@{username}" if username else "—"
        full_name = f"{user_first_name} {user_last_name}".strip()
        tg_link = f"tg://user?id={user_id}"

        text = (
            f"<b>📦 DELIVERY EVENT</b>\n"
            f"────────────────────\n"
            f"<b>Order ID:</b> <code>{order_id or 'N/A'}</code>\n"
            f"<b>Store Bot:</b> @{bot_username or 'Unknown'}\n"
            f"<b>User:</b> <a href=\"{tg_link}\">{full_name}</a> ({uname_line})\n"
            f"<b>Telegram ID:</b> <code>{user_id}</code>\n"
            f"<b>Story:</b> {s_name}\n"
            f"<b>Method:</b> {d_type.upper()}\n"
            f"<b>Status:</b> {status}\n"
            f"<b>Date:</b> {time_str}"
        )
        await db.mgmt_client.send_message(int(Config.DELIVERY_LOGS_CHANNEL), text=text)
    except Exception as e:
        import logging; logging.getLogger(__name__).error(f"Delivery log error: {e}")

async def log_arya_event(event_type: str, user_id: int, user_info: dict, details: str):
    from config import Config
    from database import db
    if not getattr(Config, "ARYA_LOGS_CHANNEL", None) or not db.mgmt_client: return
    try:
        from datetime import datetime, timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        time_str = datetime.now(ist).strftime('%d %b %Y, %I:%M %p IST')

        username = user_info.get("username", "")
        uname_line = f"@{username}" if username else "—"
        full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip() or "Unknown"
        tg_link = f"tg://user?id={user_id}"

        joined = user_info.get("joined_date", time_str)
        if isinstance(joined, datetime):
            joined = joined.astimezone(ist).strftime('%d %b %Y, %I:%M %p IST')
        elif not isinstance(joined, str):
            joined = "N/A"

        text = (
            f"<b>🛡️ ARYA CORE LOG | {event_type}</b>\n"
            f"────────────────────\n"
            f"<b>User:</b> <a href=\"{tg_link}\">{full_name}</a> ({uname_line})\n"
            f"<b>Telegram ID:</b> <code>{user_id}</code>\n"
            f"<b>Joined:</b> {joined}\n"
            f"────────────────────\n"
            f"<b>Details:</b>\n{details}\n"
            f"────────────────────\n"
            f"<b>Time:</b> {time_str}"
        )
        channel_id = int(Config.ARYA_LOGS_CHANNEL)
        # resolve_peer warms Pyrogram's internal InputPeer cache for channels
        try:
            await db.mgmt_client.resolve_peer(channel_id)
        except Exception:
            pass
        await db.mgmt_client.send_message(channel_id, text=text)
    except Exception as e:
        import logging; logging.getLogger(__name__).error(f"Arya core log error: {e}")



