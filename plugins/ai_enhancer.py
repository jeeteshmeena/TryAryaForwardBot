import asyncio
import base64
import logging
import traceback
import aiohttp
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

import database as db
from utils import _sc

logger = logging.getLogger(__name__)

# Constants
REPLICATE_MODEL_VERSION = "42fed1c4974146d4d2414e2be2c5277c7fcf05fcc3a73abf41610695738c1d7b"  # Real-ESRGAN

_ai_waiter: dict[int, asyncio.Future] = {}

@Client.on_message(filters.private, group=-9)
async def _ai_input_router(bot, message: Message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _ai_waiter:
        fut = _ai_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation

async def _ai_ask(bot, user_id: int):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    if user_id in _ai_waiter and not _ai_waiter[user_id].done():
        _ai_waiter[user_id].cancel()
    _ai_waiter[user_id] = fut
    try:
        return await asyncio.wait_for(fut, timeout=120)
    except asyncio.TimeoutError:
        _ai_waiter.pop(user_id, None)
        raise

def _get_ai_markup(cfg):
    has_key = bool(cfg.get("replicate_key"))
    key_text = _sc("api key set") if has_key else _sc("api key not set")
    buttons = [
        [InlineKeyboardButton(f"🔑 {key_text}", callback_data="ai_set_key")],
        [InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="settings#main")]
    ]
    if has_key:
        buttons.insert(0, [InlineKeyboardButton("🗑 Rᴇᴍᴏᴠᴇ Kᴇʏ", callback_data="ai_remove_key")])
    return InlineKeyboardMarkup(buttons)

def _get_ai_text(cfg):
    has_key = bool(cfg.get("replicate_key"))
    status = "READY" if has_key else "NOT CONFIGURED"
    return (
        f"<b>❪ A I   E N H A N C E R ❫</b>\n\n"
        f"<b>Status:</b> <code>{status}</code>\n\n"
        "This tool uses Replicate's Real-ESRGAN model to upscale and improve the quality of images and covers automatically. "
        "A button will magically appear beneath visual media while delivering batch links if you have configured an API key."
    )

@Client.on_callback_query(filters.regex(r"^settings#enhancer$"))
async def ai_menu_cb(bot, query: CallbackQuery):
    cfg = await db.get_enhancer_config()
    await query.message.edit_text(_get_ai_text(cfg), reply_markup=_get_ai_markup(cfg))

@Client.on_callback_query(filters.regex(r"^ai_(set_key|remove_key)$"))
async def ai_settings_actions(bot, query: CallbackQuery):
    action = query.data.split("_", 1)[1]
    uid = query.from_user.id
    
    if action == "remove_key":
        await db.update_enhancer_config(replicate_key="")
        cfg = await db.get_enhancer_config()
        return await query.message.edit_text(_get_ai_text(cfg), reply_markup=_get_ai_markup(cfg))

    if action == "set_key":
        m1 = await query.message.reply_text("<b>🔑 Send your Replicate API Key:</b>\n<i>(Or send /cancel)</i>")
        try:
            resp = await _ai_ask(bot, uid)
            if resp.text and not resp.text.startswith("/cancel"):
                await db.update_enhancer_config(replicate_key=resp.text.strip())
                await resp.delete()
                await m1.delete()
                cfg = await db.get_enhancer_config()
                return await query.message.edit_text(_get_ai_text(cfg), reply_markup=_get_ai_markup(cfg))
        except asyncio.TimeoutError:
            pass
        await m1.delete()

@Client.on_callback_query(filters.regex(r"^ai_enh#"))
async def trigger_ai_enhance(bot, query: CallbackQuery):
    uid = query.from_user.id
    cfg = await db.get_enhancer_config()
    api_key = cfg.get("replicate_key", "")
    if not api_key:
        return await query.answer("❌ Replicate API Key is not set in settings!", show_alert=True)
        
    msg_id_pair = query.data.split("#")[1]  # "source_chat_id:original_msg_id"
    if ":" not in msg_id_pair:
        return await query.answer("❌ Invalid media payload.", show_alert=True)
    
    from_chat, origin_msg_id = msg_id_pair.split(":")
    await query.answer("Processing via AI... please wait.")
    
    # Send a quick pending text or just let it spin? Let's just do it directly.
    # We must retrieve the media from the message that query was clicked on, IF it exists.
    # Wait, the query is clicked on the bot's own delivered message!
    tmsg = query.message
    if not tmsg.photo and not tmsg.document:
        return await query.answer("❌ No media found in this message.", show_alert=True)

    prog = await query.message.reply_text("<i>✨ Enhancing media via Replicate...</i>")
    try:
        media_bytes = await bot.download_media(tmsg, in_memory=True)
        if not media_bytes:
            raise Exception("Download empty.")
            
        b64 = base64.b64encode(media_bytes.getbuffer()).decode("utf-8")
        uri = f"data:image/jpeg;base64,{b64}"
        
        payload = {
            "version": REPLICATE_MODEL_VERSION,
            "input": {"image": uri, "scale": 2}
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.replicate.com/v1/predictions", json=payload, headers=headers) as r:
                if r.status != 201:
                    err = await r.text()
                    raise Exception(f"API Error {r.status}")
                pred = await r.json()
                poll_url = pred["urls"]["get"]
                
            out_url = None
            for _ in range(30):
                await asyncio.sleep(2)
                async with session.get(poll_url, headers=headers) as r2:
                    if r2.status == 200:
                        data = await r2.json()
                        status = data.get("status")
                        if status == "succeeded":
                            out_url = data.get("output")
                            break
                        elif status == "failed":
                            raise Exception("Replicate model failed to process.")
            
            if not out_url:
                raise Exception("Timed out waiting for Replicate.")
                
            if isinstance(out_url, list): out_url = out_url[0]
            
            async with session.get(out_url) as r3:
                if r3.status != 200:
                    raise Exception("Failed to fetch enhanced image.")
                enhanced_bytes = await r3.read()
                
        # Now update the message media
        from io import BytesIO
        enhanced_io = BytesIO(enhanced_bytes)
        enhanced_io.name = "enhanced.jpg"
        
        from pyrogram.types import InputMediaPhoto
        await tmsg.edit_media(InputMediaPhoto(enhanced_io, caption=tmsg.caption.html if tmsg.caption else None))
        await prog.delete()
        
    except Exception as e:
        logger.error(f"AI Enhance Error: {e}", exc_info=True)
        await prog.edit_text(f"<b>❌ AI Enhancement failed:</b> <code>{e}</code>")
        await asyncio.sleep(3)
        await prog.delete()
