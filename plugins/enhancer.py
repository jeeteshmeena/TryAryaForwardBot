import os
import io
import asyncio
import base64
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

import database as db
from config import Config
from plugins.settings import main_buttons
from helper.utils import _ask

# Models available on Replicate
MODELS = {
    "esrgan": {
        "name": "Real-ESRGAN",
        "version": "42fed1c4974146d4d2414e2be2c5277c7fcf05fcc3a73abf41610695738c1d7b"
    },
    "gfpgan": {
        "name": "GFPGAN (Faces)",
        "version": "9283608cb6f7eec596760e2586209b5aa811a4362b66236b2d1840de6bb17f8b"
    },
    "codeformer": {
        "name": "CodeFormer",
        "version": "7de2ea26c616d5bf2245ad0d5e24f0c1a403d17ed687df2a0f88eabf83c18c94"
    }
}

# ══════════════════════════════════════════════════════════════════════════════
# SETTINGS UI
# ══════════════════════════════════════════════════════════════════════════════

def enhancer_markup(cfg: dict):
    model = cfg.get("model", "esrgan")
    model_name = MODELS.get(model, MODELS["esrgan"])["name"]
    scale = cfg.get("scale", 2)
    enabled = "✅ ON" if cfg.get("enabled") else "❌ OFF"
    has_key = "✅ Set" if cfg.get("api_key") else "❌ Not Set"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Status: {enabled}", callback_data="enhancer_toggle")],
        [InlineKeyboardButton(f"API Key: {has_key}", callback_data="enhancer_key")],
        [InlineKeyboardButton(f"Model: {model_name}", callback_data="enhancer_model")],
        [InlineKeyboardButton(f"Scale: {scale}x", callback_data="enhancer_scale")],
        [InlineKeyboardButton("❮ Bᴀᴄᴋ", callback_data="settings#main")]
    ])

@Client.on_callback_query(filters.regex(r"^settings#enhancer$"))
async def enhancer_settings_cb(bot, update: CallbackQuery):
    if update.from_user.id not in Config.BOT_OWNER_ID:
        return await update.answer("Owners only.", show_alert=True)
    cfg = await db.get_enhancer_config()
    txt = (
        "<b>✨ AI Image Enhancer Settings</b>\n\n"
        f"<b>Status:</b> {'Active' if cfg.get('enabled') else 'Disabled'}\n"
        f"<b>API Key:</b> {'Set' if cfg.get('api_key') else 'Missing'}\n"
        f"<b>Model:</b> {MODELS.get(cfg.get('model', 'esrgan'))['name']}\n"
        f"<b>Scale:</b> {cfg.get('scale', 2)}x\n\n"
        "<i>When enabled, users sending images directly to the bot will be prompted to automatically enhance them using the Replicate API.</i>"
    )
    await update.message.edit_text(txt, reply_markup=enhancer_markup(cfg))

@Client.on_callback_query(filters.regex(r"^enhancer_(toggle|key|model|scale)$"))
async def enhancer_action_cb(bot, update: CallbackQuery):
    if update.from_user.id not in Config.BOT_OWNER_ID:
        return await update.answer("Owners only.", show_alert=True)
    
    action = update.data.split("_")[1]
    cfg = await db.get_enhancer_config()

    if action == "toggle":
        if not cfg.get("api_key"):
            return await update.answer("Set an API key first!", show_alert=True)
        await db.update_enhancer_config(enabled=not cfg.get("enabled", False))

    elif action == "key":
        ask_msg = await update.message.reply_text("<b>Send your Replicate API Token:</b>\n<i>(Or send /cancel to abort)</i>")
        try:
            resp = await bot.listen(update.from_user.id, filters=filters.text, timeout=120)
            if resp.text and not resp.text.startswith("/"):
                await db.update_enhancer_config(api_key=resp.text.strip())
                await update.message.reply_text("✅ Replicate API Key saved.")
            await resp.delete()
            await ask_msg.delete()
        except:
            await ask_msg.delete()

    elif action == "model":
        keys = list(MODELS.keys())
        idx = keys.index(cfg.get("model", "esrgan"))
        next_model = keys[(idx + 1) % len(keys)]
        await db.update_enhancer_config(model=next_model)

    elif action == "scale":
        curr = cfg.get("scale", 2)
        next_scale = 4 if curr == 2 else 2
        await db.update_enhancer_config(scale=next_scale)

    cfg = await db.get_enhancer_config()
    try:
        await update.message.edit_reply_markup(reply_markup=enhancer_markup(cfg))
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
# ENHANCEMENT ENGINE (Works on both Main Bot and Share Bot)
# ══════════════════════════════════════════════════════════════════════════════

async def process_replicate(image_bytes: bytes, cfg: dict) -> bytes | str:
    """Sends image to Replicate, polls for result, downloads enhanced image, returns bytes or error string."""
    api_key = cfg.get("api_key")
    if not api_key: return "API key missing."
    
    b64_img = base64.b64encode(image_bytes).decode('utf-8')
    data_uri = f"data:image/jpeg;base64,{b64_img}"
    
    model_id = cfg.get("model", "esrgan")
    version = MODELS.get(model_id)["version"]
    scale = cfg.get("scale", 2)
    
    # Payload format differs slightly per model, but 'image' and 'scale' are generally common
    payload = {
        "version": version,
        "input": {
            "image": data_uri,
            "scale": scale
        }
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    async with aiohttp.ClientSession() as session:
        # 1. Start prediction
        async with session.post("https://api.replicate.com/v1/predictions", json=payload, headers=headers) as resp:
            if resp.status != 201:
                return f"Replicate API Error: {await resp.text()}"
            pred = await resp.json()
            
        poll_url = pred["urls"]["get"]
        
        # 2. Poll until finished
        for _ in range(60): # 120 seconds max
            await asyncio.sleep(2)
            async with session.get(poll_url, headers=headers) as stat_resp:
                if stat_resp.status != 200: continue
                stat_data = await stat_resp.json()
                status = stat_data.get("status")
                
                if status == "succeeded":
                    out_url = stat_data.get("output")
                    if isinstance(out_url, list): out_url = out_url[0] # some models return a list
                    if not out_url: return "No output returned from API."
                    
                    # 3. Download enhanced image
                    async with session.get(out_url) as img_resp:
                        if img_resp.status == 200:
                            return await img_resp.read()
                        return f"Failed to download output image (Status {img_resp.status})"
                        
                elif status == "failed":
                    return f"Enhancement failed: {stat_data.get('error')}"
                elif status == "canceled":
                    return "Job was canceled."
                    
        return "Enhancement timed out (took > 120s)."

# Main Bot & Share Bot photo handler (Triggered securely via button)
@Client.on_message(filters.private & (filters.photo | filters.document) & ~filters.forwarded, group=2)
async def auto_enhance_listener(bot, message: Message):
    # Only active if globally enabled
    cfg = await db.get_enhancer_config()
    if not cfg.get("enabled"): return
    
    # Ignore oversized files (5MB max for base64 safety)
    if message.document and message.document.file_size > 5 * 1024 * 1024:
        return
        
    mime = getattr(message.document, "mime_type", "") if message.document else ""
    if message.document and "image" not in mime.lower():
        return # Not an image document
        
    # Ask the user if they want to enhance it (prevents messing up other flows)
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Enhance Image", callback_data="do_enhance")]
    ])
    await message.reply_text("<b>✨ AI Image Enhancer</b>\nDo you want to enhance and upscale this image?", reply_markup=btns, quote=True)


@Client.on_callback_query(filters.regex(r"^do_enhance$"))
async def trigger_enhance_cb(bot, update: CallbackQuery):
    cfg = await db.get_enhancer_config()
    if not cfg.get("enabled"):
        return await update.answer("Enhancer is currently disabled.", show_alert=True)
        
    msg = update.message.reply_to_message
    if not msg:
        return await update.answer("Original image not found.", show_alert=True)
        
    sts = await update.message.edit_text("<i>Downloading original image...</i>")
    
    try:
        # Download strictly to memory
        img_io = await bot.download_media(msg, in_memory=True)
        img_bytes = img_io.getbuffer().tobytes()
        
        await sts.edit_text(f"<i>Enhancing image via {MODELS.get(cfg.get('model', 'esrgan'))['name']} (Scale: {cfg.get('scale', 2)}x)... Please wait.</i>")
        
        result = await process_replicate(img_bytes, cfg)
        
        if isinstance(result, str):
            # It's an error message
            await sts.edit_text(f"<b>❌ Enhancement Failed:</b>\n<code>{result}</code>")
        else:
            # We got the enhanced bytes
            await sts.edit_text("<i>Uploading enhanced image...</i>")
            out_bio = io.BytesIO(result)
            out_bio.name = "Enhanced_Arya.png"
            
            await bot.send_document(
                chat_id=update.from_user.id,
                document=out_bio,
                caption=f"✨ <b>Enhanced via {MODELS.get(cfg.get('model', 'esrgan'))['name']}</b>",
                reply_to_message_id=msg.id
            )
            await sts.delete()
            
    except Exception as e:
        await sts.edit_text(f"<b>❌ Error:</b> <code>{str(e)}</code>")
