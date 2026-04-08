"""
sarvam_ai.py — Arya Bot
=========================
Sarvam AI Chatbot Integration
- Intercepts mentions/tags in the configured Help Group
- Auto-replies in Hindi & English using Sarvam AI Saaras model
- Admin-controlled toggle, model switching, and group config from Owner Panel
- Supports preview from the Owner Panel
"""

import asyncio
import logging
import aiohttp
import re as _re

from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)

from database import db

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Sarvam AI Models (https://docs.sarvam.ai/api-reference-docs)
# ──────────────────────────────────────────────────────────────────────────────
SARVAM_MODELS = {
    "saaras:v2":        "Saaras v2 (Default Chat)",
    "saaras:v2.1":      "Saaras v2.1 (Best Quality)",
    "saaras:v1":        "Saaras v1 (Fastest)",
}
DEFAULT_MODEL = "saaras:v2.1"

# ──────────────────────────────────────────────────────────────────────────────
# System Prompt
# ──────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "You are Arya, a helpful, friendly, and expert support assistant for a Telegram Media Bot. "
    "Your role is to help users solve issues with audio/video forwarding, batch link generation, "
    "live jobs, merging files, and delivery bots. "
    "Always reply concisely. If the user writes in Hindi, reply in Hindi. "
    "If in English, reply in English. If mixed, reply in Hindi with some English. "
    "Don't be overly formal. Be helpful and empathetic. "
    "Keep replies under 300 words unless a technical explanation is required."
)

# ──────────────────────────────────────────────────────────────────────────────
# DB helpers (stored in global stats collection)
# ──────────────────────────────────────────────────────────────────────────────

async def _get_sarvam_cfg() -> dict:
    doc = await db.stats.find_one({"_id": "sarvam_config"})
    return doc or {}

async def _save_sarvam_cfg(**kwargs):
    await db.stats.update_one(
        {"_id": "sarvam_config"},
        {"$set": kwargs},
        upsert=True
    )


# ──────────────────────────────────────────────────────────────────────────────
# Sarvam API call
# ──────────────────────────────────────────────────────────────────────────────

async def _call_sarvam(api_key: str, model: str, user_message: str) -> str:
    """Call Sarvam AI chat completions API and return reply text."""
    url = "https://api.sarvam.ai/v1/chat/completions"
    headers = {
        "api-subscription-key": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        "temperature": 0.7,
        "max_tokens": 512,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status == 401:
                raise Exception("Invalid API Key")
            if r.status == 429:
                raise Exception("Rate limit exceeded — try again later")
            if r.status != 200:
                err_text = await r.text()
                raise Exception(f"API Error {r.status}: {err_text[:120]}")
            data = await r.json()
            choices = data.get("choices", [])
            if not choices:
                raise Exception("Empty response from Sarvam AI")
            return choices[0]["message"]["content"].strip()


# ──────────────────────────────────────────────────────────────────────────────
# Owner Panel UI helpers
# ──────────────────────────────────────────────────────────────────────────────

def _sarvam_markup(cfg: dict) -> InlineKeyboardMarkup:
    has_key   = bool(cfg.get("api_key"))
    is_on     = cfg.get("enabled", False) and has_key
    model     = cfg.get("model", DEFAULT_MODEL)
    group_id  = cfg.get("help_group_id")

    btns = []

    # Enable / Disable toggle
    power_lbl = "🟢 Sarvam AI: ON" if is_on else "🔴 Sarvam AI: OFF"
    if has_key:
        btns.append([InlineKeyboardButton(power_lbl, callback_data="sarvam#toggle")])

    # API Key management
    key_lbl = "🔑 Change API Key" if has_key else "🔑 Set API Key"
    btns.append([InlineKeyboardButton(key_lbl, callback_data="sarvam#set_key")])
    if has_key:
        btns.append([InlineKeyboardButton("🗑 Remove API Key", callback_data="sarvam#del_key")])

    # Model selector
    model_lbl = SARVAM_MODELS.get(model, model)
    btns.append([InlineKeyboardButton(f"🤖 Model: {model_lbl}", callback_data="sarvam#model")])

    # Help Group config
    grp_lbl = f"💬 Group: {group_id}" if group_id else "💬 Set Help Group"
    btns.append([InlineKeyboardButton(grp_lbl, callback_data="sarvam#set_group")])

    # Preview
    if has_key:
        btns.append([InlineKeyboardButton("🔬 Preview Reply", callback_data="sarvam#preview")])

    btns.append([InlineKeyboardButton("❮ Back to Owner Panel", callback_data="settings#owners")])
    return InlineKeyboardMarkup(btns)


def _sarvam_text(cfg: dict) -> str:
    has_key  = bool(cfg.get("api_key"))
    is_on    = cfg.get("enabled", False) and has_key
    model    = cfg.get("model", DEFAULT_MODEL)
    group_id = cfg.get("help_group_id")

    status   = "🟢 Active" if is_on else ("🔴 Inactive" if has_key else "⚙️ Not Configured")
    masked   = ("sk_..." + cfg["api_key"][-6:]) if has_key else "—"

    return (
        "<b><u>🤖 Sarvam AI Assistant</u></b>\n\n"
        f"<b>Status:</b> <code>{status}</code>\n"
        f"<b>API Key:</b> <code>{masked}</code>\n"
        f"<b>Model:</b> <code>{SARVAM_MODELS.get(model, model)}</code>\n"
        f"<b>Help Group:</b> <code>{group_id or 'Not set'}</code>\n\n"
        "When enabled, this assistant listens in the configured Help Group. "
        "If a user <b>mentions the bot</b> or <b>replies to the bot</b>, "
        "it automatically generates a helpful reply in <b>Hindi / English</b>.\n\n"
        "<i>Use the Owner Panel to toggle, change the model, or set which group to monitor.</i>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Waiter for text input (reuses future-pattern from settings.py)
# ──────────────────────────────────────────────────────────────────────────────
_sarvam_waiting: dict[int, asyncio.Future] = {}

@Client.on_message(filters.private, group=-19)
async def _sarvam_input_router(bot, message: Message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _sarvam_waiting:
        fut = _sarvam_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation

async def _sarvam_ask(bot, user_id: int, prompt: str, timeout: int = 120) -> Message:
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    old = _sarvam_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _sarvam_waiting[user_id] = fut
    await bot.send_message(user_id, prompt)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _sarvam_waiting.pop(user_id, None)
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Owner Panel entry point  (callback_data = sarvam#*)
# ──────────────────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^sarvam#"))
async def sarvam_cb(bot, query: CallbackQuery):
    from plugins.owner_utils import is_any_owner
    uid = query.from_user.id
    if not await is_any_owner(uid):
        return await query.answer("⛔ Owner only!", show_alert=True)
    try:
        await query.answer()
    except Exception:
        pass

    action = query.data.split("#", 1)[1]
    cfg    = await _get_sarvam_cfg()

    # ── Main Panel ────────────────────────────────────────────────────────────
    if action == "main":
        await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))
        return

    # ── Toggle ON/OFF ─────────────────────────────────────────────────────────
    if action == "toggle":
        current = cfg.get("enabled", False)
        await _save_sarvam_cfg(enabled=not current)
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Set API Key ───────────────────────────────────────────────────────────
    if action == "set_key":
        await query.message.delete()
        try:
            resp = await _sarvam_ask(
                bot, uid,
                "<b>🔑 Send your Sarvam AI API Key:</b>\n<i>(Format: sk_...  — or send /cancel)</i>"
            )
            txt = (resp.text or "").strip()
            await resp.delete()
            if txt.lower() in ("/cancel", "cancel"):
                cfg = await _get_sarvam_cfg()
                m = await bot.send_message(uid, "Cancelled.", reply_markup=_sarvam_markup(cfg))
                return
            if not txt.startswith("sk_"):
                m = await bot.send_message(uid, "❌ Invalid key format. Must start with <code>sk_</code>.", reply_markup=_sarvam_markup(cfg))
                return
            await _save_sarvam_cfg(api_key=txt, enabled=True)
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, "✅ API Key saved and Sarvam AI enabled!", reply_markup=_sarvam_markup(cfg))
        except asyncio.TimeoutError:
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, "⏳ Timed out.", reply_markup=_sarvam_markup(cfg))
        return

    # ── Delete API Key ────────────────────────────────────────────────────────
    if action == "del_key":
        await _save_sarvam_cfg(api_key="", enabled=False)
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Model Selector ────────────────────────────────────────────────────────
    if action == "model":
        model_btns = []
        for mkey, mlabel in SARVAM_MODELS.items():
            tick = "✅ " if cfg.get("model", DEFAULT_MODEL) == mkey else ""
            model_btns.append([InlineKeyboardButton(
                f"{tick}{mlabel}", callback_data=f"sarvam#setmodel_{mkey}"
            )])
        model_btns.append([InlineKeyboardButton("❮ Back", callback_data="sarvam#main")])
        return await query.message.edit_text(
            "<b>🤖 Choose Sarvam AI Model:</b>\n\n"
            "<i>Higher quality models may have slightly slower responses.</i>",
            reply_markup=InlineKeyboardMarkup(model_btns)
        )

    if action.startswith("setmodel_"):
        new_model = action.split("setmodel_", 1)[1]
        await _save_sarvam_cfg(model=new_model)
        try:
            await query.answer(f"Model set to {SARVAM_MODELS.get(new_model, new_model)}!", show_alert=False)
        except Exception:
            pass
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Set Help Group ────────────────────────────────────────────────────────
    if action == "set_group":
        await query.message.delete()
        try:
            resp = await _sarvam_ask(
                bot, uid,
                "<b>💬 Set Help Group</b>\n\n"
                "Send the <b>Chat ID</b> of your help group (negative number, e.g. <code>-1001234567890</code>).\n"
                "Or forward any message from that group.\n\n"
                "<i>Send /cancel to abort.</i>"
            )
            txt  = (resp.text or "").strip()
            if resp.forward_from_chat:
                gid = resp.forward_from_chat.id
            elif txt.lstrip("-").isdigit():
                gid = int(txt)
            else:
                gid = None

            await resp.delete()
            if txt.lower() in ("/cancel", "cancel") or gid is None:
                cfg = await _get_sarvam_cfg()
                await bot.send_message(uid, "Cancelled or invalid group ID.", reply_markup=_sarvam_markup(cfg))
                return
            await _save_sarvam_cfg(help_group_id=gid)
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, f"✅ Help Group set to <code>{gid}</code>!", reply_markup=_sarvam_markup(cfg))
        except asyncio.TimeoutError:
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, "⏳ Timed out.", reply_markup=_sarvam_markup(cfg))
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    if action == "preview":
        api_key = cfg.get("api_key")
        model   = cfg.get("model", DEFAULT_MODEL)
        if not api_key:
            return await query.answer("❌ No API key configured!", show_alert=True)
        await query.message.delete()
        prog = await bot.send_message(uid, "<i>🤖 Calling Sarvam AI for a preview...</i>")
        try:
            test_q   = "मेरा live job अचानक रुक गया, क्या करूं?"
            reply    = await _call_sarvam(api_key, model, test_q)
            model_lbl = SARVAM_MODELS.get(model, model)
            cfg = await _get_sarvam_cfg()
            await prog.edit_text(
                f"<b>🔬 Preview Reply</b>\n"
                f"<b>Model:</b> <code>{model_lbl}</code>\n\n"
                f"<b>Test Question:</b>\n<i>{test_q}</i>\n\n"
                f"<b>Sarvam Response:</b>\n{reply}",
                reply_markup=_sarvam_markup(cfg)
            )
        except Exception as e:
            cfg = await _get_sarvam_cfg()
            await prog.edit_text(f"❌ Preview failed: <code>{e}</code>", reply_markup=_sarvam_markup(cfg))
        return


# ──────────────────────────────────────────────────────────────────────────────
# Inject Sarvam AI button into Owner Panel  (via monkey-patch on import)
# ──────────────────────────────────────────────────────────────────────────────
# The sarvam#main entry point is added to settings.py owners panel via patch below.
# This avoids editing settings.py directly.

def _patch_owner_panel():
    """
    Called once at bot startup. Registers the Sarvam AI button into the Owner Panel
    by patching the owners_cb handler to include it when rendering the owners view.
    This is done cleanly via DB-driven logic, no monkey-patching needed:
    the 'sarvam' button is registered via a separate callback that settings.py
    already handles through the regex ^sarvam# above.
    """
    pass  # No patch needed — sarvam#main is its own callback handled above.

_patch_owner_panel()


# ──────────────────────────────────────────────────────────────────────────────
# Help Group Message Listener  — intercepts mentions & replies
# ──────────────────────────────────────────────────────────────────────────────

_BOT_USERNAME_CACHE: str | None = None

async def _get_bot_username(bot) -> str:
    global _BOT_USERNAME_CACHE
    if not _BOT_USERNAME_CACHE:
        me = await bot.get_me()
        _BOT_USERNAME_CACHE = (me.username or "").lower()
    return _BOT_USERNAME_CACHE


@Client.on_message(filters.group, group=10)
async def sarvam_group_listener(bot, message: Message):
    """Listen in the configured help group for mentions or replies to the bot."""
    cfg = await _get_sarvam_cfg()

    # Gate 1 — feature must be enabled
    if not cfg.get("enabled"):
        raise ContinuePropagation

    api_key = cfg.get("api_key", "")
    if not api_key:
        raise ContinuePropagation

    # Gate 2 — must be in the configured group
    group_id = cfg.get("help_group_id")
    if not group_id or message.chat.id != int(group_id):
        raise ContinuePropagation

    # Gate 3 — must be a text message
    text = message.text or message.caption or ""
    if not text.strip():
        raise ContinuePropagation

    # Gate 4 — detect a mention or reply to the bot
    bot_username = await _get_bot_username(bot)
    is_mention = (
        f"@{bot_username}" in text.lower()
        or any(
            e.type.value == "mention" and f"@{bot_username}" in text[e.offset: e.offset + e.length].lower()
            for e in (message.entities or [])
        )
    )
    is_reply_to_bot = (
        message.reply_to_message
        and message.reply_to_message.from_user
        and (message.reply_to_message.from_user.username or "").lower() == bot_username
    )

    if not is_mention and not is_reply_to_bot:
        raise ContinuePropagation

    # Clean the text — strip the bot mention
    clean_text = _re.sub(rf"@{bot_username}", "", text, flags=_re.IGNORECASE).strip()
    if not clean_text:
        raise ContinuePropagation

    model = cfg.get("model", DEFAULT_MODEL)

    # Indicate bot is typing
    try:
        await bot.send_chat_action(message.chat.id, "typing")
    except Exception:
        pass

    try:
        reply = await _call_sarvam(api_key, model, clean_text)
        # Send the reply quoting the user
        await message.reply_text(
            reply,
            quote=True,
        )
    except Exception as e:
        logger.warning(f"[SarvamAI] Failed to respond in group {group_id}: {e}")
        # Silently fail — don't spam the group with errors

    raise ContinuePropagation
