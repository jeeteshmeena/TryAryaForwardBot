"""
AI Image Enhancer via Replicate API
Supports Real-ESRGAN, GFPGAN, CodeFormer
Settings UI integrated into /settings -> AI Enhancer
"""
import os
import io
import asyncio
import base64
import logging
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

import database as db
from config import Config
from pyrogram import ContinuePropagation

logger = logging.getLogger(__name__)

# ── Model registry ────────────────────────────────────────────────────────────
MODELS = {
    "esrgan": {
        "name": "Real-ESRGAN",
        "desc": "Best for general upscaling, scenery, objects",
        "version": "42fed1c4974146d4d2414e2be2c5277c7fcf05fcc3a73abf41610695738c1d7b",
        "input_key": "image",
    },
    "gfpgan": {
        "name": "GFPGAN (Faces)",
        "desc": "Targets blurred or damaged facial structures",
        "version": "9283608cb6f7eec596760e2586209b5aa811a4362b66236b2d1840de6bb17f8b",
        "input_key": "img",
    },
    "codeformer": {
        "name": "CodeFormer",
        "desc": "Powerful face restoration and pixel repair",
        "version": "7de2ea26c616d5bf2245ad0d5e24f0c1a403d17ed687df2a0f88eabf83c18c94",
        "input_key": "image",
    },
}

# ── Waiting state dict (reuses the sj pattern) ───────────────────────────────
_enh_waiting: dict[int, asyncio.Future] = {}


@Client.on_message(filters.private, group=-9)
async def _enh_input_router(bot, message: Message):
    """Route private text to any pending _enh_ask() futures."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _enh_waiting:
        fut = _enh_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    from pyrogram.handlers import ContinuePropagation
    raise ContinuePropagation


async def _enh_ask(bot, user_id: int, timeout: int = 120):
    """Wait for user's next private message."""
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    old = _enh_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _enh_waiting[user_id] = fut
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _enh_waiting.pop(user_id, None)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# SETTINGS UI
# ══════════════════════════════════════════════════════════════════════════════

def _enh_markup(cfg: dict) -> InlineKeyboardMarkup:
    model_id   = cfg.get("model", "esrgan")
    model_info = MODELS.get(model_id, MODELS["esrgan"])
    scale      = cfg.get("scale", 2)
    enabled    = cfg.get("enabled", False)
    has_key    = bool(cfg.get("api_key", ""))

    status_btn = f"{'✅ Enabled' if enabled else '❌ Disabled'}  —  tap to toggle"
    key_btn    = f"API Key: {'✅ Set' if has_key else '❌ Not Set'}  —  tap to change"
    model_btn  = f"Model: {model_info['name']}  —  tap to cycle"
    scale_btn  = f"Scale: {scale}x  —  tap to toggle 2x / 4x"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(status_btn, callback_data="enh#toggle")],
        [InlineKeyboardButton(key_btn,    callback_data="enh#set_key")],
        [InlineKeyboardButton(model_btn,  callback_data="enh#cycle_model")],
        [InlineKeyboardButton(scale_btn,  callback_data="enh#toggle_scale")],
        [InlineKeyboardButton("❮ Bᴀᴄᴋ",  callback_data="settings#main")],
    ])


def _enh_text(cfg: dict) -> str:
    model_id   = cfg.get("model", "esrgan")
    model_info = MODELS.get(model_id, MODELS["esrgan"])
    scale      = cfg.get("scale", 2)
    enabled    = cfg.get("enabled", False)
    has_key    = bool(cfg.get("api_key", ""))

    return (
        "<b><u>✨ AI Image Enhancer</u></b>\n\n"
        f"<b>Status:</b> {'<b>Active</b>' if enabled else 'Disabled'}\n"
        f"<b>API Key:</b> {'Set ✅' if has_key else '❌ Missing — tap button below to add'}\n"
        f"<b>Model:</b> {model_info['name']}\n"
        f"<i>{model_info['desc']}</i>\n"
        f"<b>Scale:</b> {scale}x upscaling\n\n"
        "<i>When enabled, users can send any image (photo or document) "
        "and tap <b>✨ Enhance Image</b> to upscale it using Replicate's GPU.</i>"
    )


@Client.on_callback_query(filters.regex(r"^settings#enhancer$"))
async def enhancer_settings_cb(bot, update: CallbackQuery):
    if update.from_user.id not in Config.BOT_OWNER_ID:
        return await update.answer("Owners only.", show_alert=True)
    cfg = await db.get_enhancer_config()
    await update.message.edit_text(_enh_text(cfg), reply_markup=_enh_markup(cfg))


@Client.on_callback_query(filters.regex(r"^enh#(toggle|set_key|cycle_model|toggle_scale)$"))
async def enhancer_action_cb(bot, update: CallbackQuery):
    if update.from_user.id not in Config.BOT_OWNER_ID:
        return await update.answer("Owners only.", show_alert=True)
    await update.answer()

    action = update.data.split("#")[1]
    uid    = update.from_user.id
    cfg    = await db.get_enhancer_config()

    if action == "toggle":
        if not cfg.get("api_key"):
            return await update.answer("⚠️ Set an API key before enabling!", show_alert=True)
        await db.update_enhancer_config(enabled=not cfg.get("enabled", False))

    elif action == "set_key":
        sent = await bot.send_message(
            uid,
            "<b>Send your Replicate API Token now:</b>\n"
            "<i>Get one free at replicate.com/account/api-tokens</i>\n\n"
            "Send /cancel to abort."
        )
        try:
            resp = await _enh_ask(bot, uid, timeout=120)
            if not resp:
                raise asyncio.TimeoutError
            txt = (resp.text or "").strip()
            if txt.lower() == "/cancel":
                await sent.edit_text("<i>Cancelled.</i>")
            elif txt:
                await db.update_enhancer_config(api_key=txt)
                await sent.edit_text("✅ <b>API Key saved successfully!</b>")
                try: await resp.delete()
                except: pass
        except asyncio.TimeoutError:
            await sent.edit_text("<i>Timed out. Try again.</i>")
        return  # Don't edit the settings message — user can tap back

    elif action == "cycle_model":
        keys       = list(MODELS.keys())
        curr_idx   = keys.index(cfg.get("model", "esrgan"))
        next_model = keys[(curr_idx + 1) % len(keys)]
        await db.update_enhancer_config(model=next_model)

    elif action == "toggle_scale":
        next_scale = 4 if cfg.get("scale", 2) == 2 else 2
        await db.update_enhancer_config(scale=next_scale)

    cfg = await db.get_enhancer_config()
    try:
        await update.message.edit_text(_enh_text(cfg), reply_markup=_enh_markup(cfg))
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# REPLICATE ENGINE
# ══════════════════════════════════════════════════════════════════════════════

async def process_replicate(image_bytes: bytes, cfg: dict):
    """
    Submit image to Replicate, poll until done, return (bytes, None) on success
    or (None, error_str) on failure.
    """
    api_key  = cfg.get("api_key", "")
    if not api_key:
        return None, "Replicate API key is not set."

    model_id  = cfg.get("model", "esrgan")
    model_cfg = MODELS.get(model_id, MODELS["esrgan"])
    version   = model_cfg["version"]
    input_key = model_cfg["input_key"]
    scale     = cfg.get("scale", 2)

    # Encode image as data URI (avoids file upload complexity)
    b64  = base64.b64encode(image_bytes).decode("utf-8")
    uri  = f"data:image/jpeg;base64,{b64}"

    payload = {
        "version": version,
        "input": {
            input_key: uri,
            "scale": scale,
        }
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    async with aiohttp.ClientSession() as session:
        # 1. Start prediction
        async with session.post(
            "https://api.replicate.com/v1/predictions",
            json=payload, headers=headers
        ) as r:
            if r.status != 201:
                body = await r.text()
                return None, f"Replicate API error ({r.status}): {body[:300]}"
            pred     = await r.json()
            poll_url = pred["urls"]["get"]

        # 2. Poll (max 3 minutes = 90 × 2 s)
        for _ in range(90):
            await asyncio.sleep(2)
            async with session.get(poll_url, headers=headers) as r2:
                if r2.status != 200:
                    continue
                data   = await r2.json()
                status = data.get("status")

                if status == "succeeded":
                    out = data.get("output")
                    if isinstance(out, list):
                        out = out[0]
                    if not out:
                        return None, "Replicate returned no output URL."
                    async with session.get(out) as img_r:
                        if img_r.status == 200:
                            return await img_r.read(), None
                        return None, f"Could not download result image (HTTP {img_r.status})"

                elif status == "failed":
                    return None, f"Replicate job failed: {data.get('error', 'unknown error')}"

                elif status == "canceled":
                    return None, "Replicate job was canceled."

        return None, "Timed out waiting for Replicate (> 3 minutes)."


# ══════════════════════════════════════════════════════════════════════════════
# USER-FACING HANDLER
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(
    filters.private & (filters.photo | filters.document) & ~filters.forwarded,
    group=2
)
async def enhance_offer_handler(bot, message: Message):
    """When enhancer is ON, offer to enhance any incoming image."""
    cfg = await db.get_enhancer_config()
    if not cfg.get("enabled"):
        return

    # Gate: document must be an image
    if message.document:
        if message.document.file_size > 5 * 1024 * 1024:
            return  # too large
        mime = getattr(message.document, "mime_type", "") or ""
        if "image" not in mime.lower():
            return  # not an image

    await message.reply_text(
        "<b>✨ AI Image Enhancer</b>\n\n"
        f"Upscale this image using <b>{MODELS[cfg.get('model','esrgan')]['name']}</b> "
        f"at <b>{cfg.get('scale', 2)}x</b> quality?\n\n"
        "<i>Tap the button below to start. Processing takes ~30–60 seconds.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✨ Enhance Image", callback_data="enh#do")]
        ]),
        quote=True,
    )


@Client.on_callback_query(filters.regex(r"^enh#do$"))
async def enhance_execute_cb(bot, update: CallbackQuery):
    cfg = await db.get_enhancer_config()
    if not cfg.get("enabled"):
        return await update.answer("Enhancer is currently disabled.", show_alert=True)
    if not cfg.get("api_key"):
        return await update.answer("No API key configured. Ask the bot owner.", show_alert=True)

    src_msg = update.message.reply_to_message
    if not src_msg:
        return await update.answer("Original image not found.", show_alert=True)

    await update.answer()
    sts = await update.message.edit_text("<i>Downloading image...</i>")

    try:
        raw = await bot.download_media(src_msg, in_memory=True)
        img_bytes = raw.getbuffer().tobytes()
    except Exception as e:
        return await sts.edit_text(f"<b>❌ Download failed:</b> <code>{e}</code>")

    model_name = MODELS[cfg.get("model", "esrgan")]["name"]
    scale      = cfg.get("scale", 2)
    await sts.edit_text(
        f"<i>Enhancing via <b>{model_name}</b> ({scale}x)…\n"
        "This may take 30–90 seconds on Replicate's GPU.</i>"
    )

    result_bytes, err = await process_replicate(img_bytes, cfg)

    if err:
        return await sts.edit_text(f"<b>❌ Enhancement failed:</b>\n<code>{err}</code>")

    await sts.edit_text("<i>Uploading enhanced image…</i>")
    out_bio      = io.BytesIO(result_bytes)
    out_bio.name = "enhanced.png"

    try:
        await bot.send_document(
            chat_id=update.from_user.id,
            document=out_bio,
            caption=f"✨ <b>Enhanced — {model_name} {scale}x</b>",
            reply_to_message_id=src_msg.id,
        )
        await sts.delete()
    except Exception as e:
        await sts.edit_text(f"<b>❌ Upload failed:</b> <code>{e}</code>")
