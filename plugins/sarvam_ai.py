"""
sarvam_ai.py — Arya Bot
=========================
Sarvam AI Chatbot Integration
- Intercepts mentions/tags in the configured Help Groups (supports multiple group IDs)
- Auto-replies in Hindi & English using Sarvam AI chat models
- Optional voice reply using Sarvam TTS (male/female voices)
- Controlled from Main Settings (Settings → 🤖 Sarvam AI)
- Toggle, model switching, voice switching, group config, API key, and preview
"""

import asyncio
import logging
import aiohttp
import re as _re
import base64
import os

from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)

from database import db

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Sarvam AI Chat Models (https://docs.sarvam.ai/api-reference-docs)
# ──────────────────────────────────────────────────────────────────────────────
SARVAM_MODELS = {
    "sarvam-m":    "Sarvam-M (Balanced — Recommended)",
    "sarvam-30b":  "Sarvam-30B (Fast & Lightweight)",
    "sarvam-105b": "Sarvam-105B (Highest Quality)",
}
DEFAULT_MODEL = "sarvam-m"

# ──────────────────────────────────────────────────────────────────────────────
# Sarvam TTS Voices (https://docs.sarvam.ai/api-reference-docs/text-to-speech)
# ──────────────────────────────────────────────────────────────────────────────
SARVAM_VOICES = {
    "meera":   "Meera 👩 (Female, Hindi)",
    "pavithra": "Pavithra 👩 (Female, Hindi)",
    "maitreyi": "Maitreyi 👩 (Female, Hindi)",
    "arvind":  "Arvind 👨 (Male, Hindi)",
    "amol":    "Amol 👨 (Male, Hindi)",
    "amartya": "Amartya 👨 (Male, Hindi)",
}
DEFAULT_VOICE = "meera"

# ──────────────────────────────────────────────────────────────────────────────
# Chatbot identity & system prompt
# ──────────────────────────────────────────────────────────────────────────────
CHATBOT_NAME = "Aarya"  # Separate identity from the main bot "Arya"

SYSTEM_PROMPT = (
    f"You are {CHATBOT_NAME}, a friendly and highly knowledgeable support assistant "
    "for a Telegram Media Bot. Your job is to help users solve problems with "
    "audio/video forwarding, batch link generation, live jobs, merging files, "
    "delivery bots, and episode management. "
    "Always give clear, step-by-step answers. "
    "If the user writes in Hindi, reply in Hindi. "
    "If in English, reply in English. If mixed, prefer Hindi with key terms in English. "
    "Be warm, helpful, and empathetic. Keep replies under 250 words unless technically required. "
    "Never say you are an AI or mention Sarvam. You are simply Aarya, a support bot."
)

# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
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
# Sarvam Chat API call
# ──────────────────────────────────────────────────────────────────────────────

async def _call_sarvam_chat(api_key: str, model: str, user_message: str) -> str:
    """Call Sarvam AI chat completions and return reply text."""
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
        "temperature": 0.65,
        "max_tokens": 600,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status == 401:
                raise Exception("Invalid API Key (401)")
            if r.status == 429:
                raise Exception("Rate limit exceeded — try again later (429)")
            if r.status != 200:
                err_text = await r.text()
                raise Exception(f"API Error {r.status}: {err_text[:200]}")
            data = await r.json()
            choices = data.get("choices", [])
            if not choices:
                raise Exception("Empty response from Sarvam AI")
            return choices[0]["message"]["content"].strip()


# ──────────────────────────────────────────────────────────────────────────────
# Sarvam TTS API call
# ──────────────────────────────────────────────────────────────────────────────

async def _call_sarvam_tts(api_key: str, voice: str, text: str) -> bytes:
    """Call Sarvam TTS and return raw WAV/MP3 audio bytes."""
    url = "https://api.sarvam.ai/text-to-speech"
    headers = {
        "api-subscription-key": api_key,
        "Content-Type": "application/json",
    }
    # Truncate to Sarvam TTS limit
    text = text[:500]
    payload = {
        "inputs": [text],
        "target_language_code": "hi-IN",
        "speaker": voice,
        "pitch": 0,
        "pace": 1.0,
        "loudness": 1.5,
        "speech_sample_rate": 22050,
        "enable_preprocessing": True,
        "model": "bulbul:v1",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                err_text = await r.text()
                raise Exception(f"TTS Error {r.status}: {err_text[:150]}")
            data = await r.json()
            # Response: {"audios": ["<base64>", ...]}
            b64 = data.get("audios", [None])[0]
            if not b64:
                raise Exception("TTS returned no audio data")
            return base64.b64decode(b64)


# ──────────────────────────────────────────────────────────────────────────────
# Settings panel UI
# ──────────────────────────────────────────────────────────────────────────────

def _sarvam_markup(cfg: dict) -> InlineKeyboardMarkup:
    has_key   = bool(cfg.get("api_key"))
    is_on     = cfg.get("enabled", False) and has_key
    model     = cfg.get("model", DEFAULT_MODEL)
    voice     = cfg.get("voice", DEFAULT_VOICE)
    voice_on  = cfg.get("voice_reply", False)
    group_ids = cfg.get("help_group_ids", [])

    btns = []

    # ON/OFF toggle
    if has_key:
        power_lbl = "🟢 Chatbot: ON" if is_on else "🔴 Chatbot: OFF"
        btns.append([InlineKeyboardButton(power_lbl, callback_data="sarvam#toggle")])

    # API Key
    key_lbl = "🔑 Change API Key" if has_key else "🔑 Set API Key"
    btns.append([InlineKeyboardButton(key_lbl, callback_data="sarvam#set_key")])
    if has_key:
        btns.append([InlineKeyboardButton("🗑 Remove API Key", callback_data="sarvam#del_key")])

    # Chat model
    model_lbl = SARVAM_MODELS.get(model, model)
    btns.append([InlineKeyboardButton(f"🤖 Model: {model_lbl[:28]}", callback_data="sarvam#model")])

    # Voice reply toggle
    if has_key:
        voice_lbl = f"🔊 Voice Reply: {'ON  (' + SARVAM_VOICES.get(voice, voice)[:15] + ')' if voice_on else 'OFF'}"
        btns.append([
            InlineKeyboardButton(voice_lbl, callback_data="sarvam#voice_toggle"),
            InlineKeyboardButton("🎙 Change Voice", callback_data="sarvam#voice"),
        ])

    # Help Groups
    grp_count = len(group_ids)
    grp_lbl = f"💬 Help Groups: {grp_count} set" if grp_count else "💬 Add Help Group"
    btns.append([
        InlineKeyboardButton(grp_lbl, callback_data="sarvam#list_groups"),
        InlineKeyboardButton("➕ Add Group", callback_data="sarvam#add_group"),
    ])

    # Preview
    if has_key:
        btns.append([InlineKeyboardButton("🔬 Preview Reply", callback_data="sarvam#preview")])

    btns.append([InlineKeyboardButton("❮ Back to Settings", callback_data="settings#main")])
    return InlineKeyboardMarkup(btns)


def _sarvam_text(cfg: dict) -> str:
    has_key  = bool(cfg.get("api_key"))
    is_on    = cfg.get("enabled", False) and has_key
    model    = cfg.get("model", DEFAULT_MODEL)
    voice    = cfg.get("voice", DEFAULT_VOICE)
    voice_on = cfg.get("voice_reply", False)
    group_ids = cfg.get("help_group_ids", [])

    status  = "🟢 Active" if is_on else ("🔴 Inactive" if has_key else "⚙️ Not Configured")
    masked  = ("sk_...●●●●●" + cfg["api_key"][-4:]) if has_key else "—"
    grp_str = "\n".join(f"  • <code>{g}</code>" for g in group_ids) if group_ids else "  —"
    voice_str = f"🔊 {SARVAM_VOICES.get(voice, voice)} ({'Voice replies ON' if voice_on else 'Text only'})"

    return (
        f"<b><u>🤖 {CHATBOT_NAME} — Sarvam AI Assistant</u></b>\n\n"
        f"<b>Status:</b> <code>{status}</code>\n"
        f"<b>API Key:</b> <code>{masked}</code>\n"
        f"<b>Model:</b> <code>{SARVAM_MODELS.get(model, model)}</code>\n"
        f"<b>Voice:</b> {voice_str}\n"
        f"<b>Help Groups:</b>\n{grp_str}\n\n"
        f"When enabled, <b>{CHATBOT_NAME}</b> listens in the configured Help Groups. "
        f"If a user <b>mentions the bot</b> or <b>replies to a bot message</b>, "
        f"it automatically generates a helpful reply in <b>Hindi / English</b>.\n\n"
        f"<i>Use the buttons below to configure.</i>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Waiter pattern for text input
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
# Settings Callback Handler  (callback_data = sarvam#*)
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
        await _save_sarvam_cfg(enabled=not cfg.get("enabled", False))
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
                await bot.send_message(uid, "Cancelled.", reply_markup=_sarvam_markup(cfg))
                return
            if not txt.startswith("sk_"):
                cfg = await _get_sarvam_cfg()
                await bot.send_message(uid, "❌ Invalid key — must start with <code>sk_</code>.", reply_markup=_sarvam_markup(cfg))
                return
            await _save_sarvam_cfg(api_key=txt, enabled=True)
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, "✅ API Key saved and chatbot enabled!", reply_markup=_sarvam_markup(cfg))
        except asyncio.TimeoutError:
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, "⏳ Timed out.", reply_markup=_sarvam_markup(cfg))
        return

    # ── Remove API Key ────────────────────────────────────────────────────────
    if action == "del_key":
        await _save_sarvam_cfg(api_key="", enabled=False)
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Chat Model Selector ───────────────────────────────────────────────────
    if action == "model":
        cur_model = cfg.get("model", DEFAULT_MODEL)
        model_btns = []
        for mkey, mlabel in SARVAM_MODELS.items():
            tick = "✅ " if cur_model == mkey else ""
            model_btns.append([InlineKeyboardButton(f"{tick}{mlabel}", callback_data=f"sarvam#setmodel_{mkey}")])
        model_btns.append([InlineKeyboardButton("❮ Back", callback_data="sarvam#main")])
        return await query.message.edit_text(
            "<b>🤖 Choose Chat Model:</b>\n\n"
            "<i>Higher quality models give better answers but may be slightly slower.</i>",
            reply_markup=InlineKeyboardMarkup(model_btns)
        )

    if action.startswith("setmodel_"):
        new_model = action.split("setmodel_", 1)[1]
        await _save_sarvam_cfg(model=new_model)
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Voice Reply Toggle ────────────────────────────────────────────────────
    if action == "voice_toggle":
        await _save_sarvam_cfg(voice_reply=not cfg.get("voice_reply", False))
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Voice Selector ────────────────────────────────────────────────────────
    if action == "voice":
        cur_voice = cfg.get("voice", DEFAULT_VOICE)
        voice_btns = []
        for vkey, vlabel in SARVAM_VOICES.items():
            tick = "✅ " if cur_voice == vkey else ""
            voice_btns.append([InlineKeyboardButton(f"{tick}{vlabel}", callback_data=f"sarvam#setvoice_{vkey}")])
        voice_btns.append([InlineKeyboardButton("❮ Back", callback_data="sarvam#main")])
        return await query.message.edit_text(
            "<b>🎙 Choose TTS Voice:</b>\n\n"
            "<i>Voice replies send audio messages in the help group.</i>",
            reply_markup=InlineKeyboardMarkup(voice_btns)
        )

    if action.startswith("setvoice_"):
        new_voice = action.split("setvoice_", 1)[1]
        await _save_sarvam_cfg(voice=new_voice)
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── List Help Groups ──────────────────────────────────────────────────────
    if action == "list_groups":
        group_ids = cfg.get("help_group_ids", [])
        if not group_ids:
            return await query.answer("No groups set yet. Use ➕ Add Group.", show_alert=True)
        grp_btns = []
        for gid in group_ids:
            grp_btns.append([InlineKeyboardButton(f"🗑 Remove {gid}", callback_data=f"sarvam#rmgroup_{gid}")])
        grp_btns.append([InlineKeyboardButton("➕ Add Another Group", callback_data="sarvam#add_group")])
        grp_btns.append([InlineKeyboardButton("❮ Back", callback_data="sarvam#main")])
        return await query.message.edit_text(
            "<b>💬 Help Groups</b>\n\nCurrently monitoring:\n" +
            "\n".join(f"• <code>{g}</code>" for g in group_ids) +
            "\n\n<i>Tap a group to remove it.</i>",
            reply_markup=InlineKeyboardMarkup(grp_btns)
        )

    if action.startswith("rmgroup_"):
        gid_str = action.split("rmgroup_", 1)[1]
        try:
            gid = int(gid_str)
        except ValueError:
            gid = gid_str
        group_ids = cfg.get("help_group_ids", [])
        group_ids = [g for g in group_ids if g != gid]
        await _save_sarvam_cfg(help_group_ids=group_ids)
        cfg = await _get_sarvam_cfg()
        return await query.message.edit_text(_sarvam_text(cfg), reply_markup=_sarvam_markup(cfg))

    # ── Add Help Group ────────────────────────────────────────────────────────
    if action == "add_group":
        await query.message.delete()
        try:
            resp = await _sarvam_ask(
                bot, uid,
                "<b>💬 Add Help Group</b>\n\n"
                "Send the <b>Chat ID</b> of your help group (negative number, e.g. <code>-1001234567890</code>).\n"
                "Or forward any message from that group.\n\n"
                "<i>Send /cancel to abort.</i>"
            )
            txt = (resp.text or "").strip()
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
            group_ids = cfg.get("help_group_ids", [])
            if gid not in group_ids:
                group_ids.append(gid)
            await _save_sarvam_cfg(help_group_ids=group_ids)
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, f"✅ Group <code>{gid}</code> added!", reply_markup=_sarvam_markup(cfg))
        except asyncio.TimeoutError:
            cfg = await _get_sarvam_cfg()
            await bot.send_message(uid, "⏳ Timed out.", reply_markup=_sarvam_markup(cfg))
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    if action == "preview":
        api_key = cfg.get("api_key")
        model   = cfg.get("model", DEFAULT_MODEL)
        voice   = cfg.get("voice", DEFAULT_VOICE)
        voice_on = cfg.get("voice_reply", False)
        if not api_key:
            return await query.answer("❌ No API key configured!", show_alert=True)
        await query.message.delete()
        prog = await bot.send_message(uid, "<i>🤖 Calling Sarvam AI for a preview...</i>")
        try:
            test_q  = "मेरा live job अचानक रुक गया है, क्या करूं?"
            reply   = await _call_sarvam_chat(api_key, model, test_q)
            model_lbl = SARVAM_MODELS.get(model, model)
            cfg = await _get_sarvam_cfg()
            preview_text = (
                f"<b>🔬 Preview — {CHATBOT_NAME}</b>\n"
                f"<b>Model:</b> <code>{model_lbl}</code>\n\n"
                f"<b>Test Question:</b>\n<i>{test_q}</i>\n\n"
                f"<b>Response:</b>\n{reply}"
            )
            await prog.edit_text(preview_text, reply_markup=_sarvam_markup(cfg))

            # Also send voice preview if enabled
            if voice_on:
                try:
                    audio_bytes = await _call_sarvam_tts(api_key, voice, reply)
                    tmp_path = f"temp_sarvam_preview_{uid}.wav"
                    with open(tmp_path, "wb") as f_out:
                        f_out.write(audio_bytes)
                    await bot.send_voice(uid, tmp_path, caption=f"🎙 Voice preview — {SARVAM_VOICES.get(voice, voice)}")
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                except Exception as ve:
                    await bot.send_message(uid, f"⚠️ Voice preview failed: <code>{ve}</code>")
        except Exception as e:
            cfg = await _get_sarvam_cfg()
            await prog.edit_text(f"❌ Preview failed: <code>{e}</code>", reply_markup=_sarvam_markup(cfg))
        return


# ──────────────────────────────────────────────────────────────────────────────
# Help Group Listener — intercepts mentions & replies
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
    """Listen in configured help groups for mentions or replies to the bot."""
    cfg = await _get_sarvam_cfg()

    # Gate 1: feature enabled and key set
    if not cfg.get("enabled"):
        raise ContinuePropagation
    api_key = cfg.get("api_key", "")
    if not api_key:
        raise ContinuePropagation

    # Gate 2: must be in one of the configured groups (supports multiple)
    group_ids = cfg.get("help_group_ids", [])
    # Legacy single group_id support
    legacy_gid = cfg.get("help_group_id")
    if legacy_gid and legacy_gid not in group_ids:
        group_ids = group_ids + [legacy_gid]
    if not group_ids:
        raise ContinuePropagation
    if message.chat.id not in [int(g) for g in group_ids]:
        raise ContinuePropagation

    # Gate 3: must be a text message
    text = message.text or message.caption or ""
    if not text.strip():
        raise ContinuePropagation

    # Gate 4: detect mention or reply to bot
    bot_username = await _get_bot_username(bot)
    is_mention = (
        f"@{bot_username}" in text.lower()
        or any(
            e.type.value == "mention" and
            f"@{bot_username}" in text[e.offset: e.offset + e.length].lower()
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

    # Clean: strip bot mention from text
    clean_text = _re.sub(rf"@{bot_username}", "", text, flags=_re.IGNORECASE).strip()
    if not clean_text:
        raise ContinuePropagation

    model   = cfg.get("model", DEFAULT_MODEL)
    voice   = cfg.get("voice", DEFAULT_VOICE)
    voice_on = cfg.get("voice_reply", False)

    try:
        await bot.send_chat_action(message.chat.id, "typing")
    except Exception:
        pass

    try:
        reply_text = await _call_sarvam_chat(api_key, model, clean_text)

        if voice_on:
            # Send voice message (TTS)
            try:
                audio_bytes = await _call_sarvam_tts(api_key, voice, reply_text)
                tmp_path = f"temp_sarvam_{message.id}.wav"
                with open(tmp_path, "wb") as f_out:
                    f_out.write(audio_bytes)
                await message.reply_voice(tmp_path, quote=True)
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            except Exception as ve:
                logger.warning(f"[SarvamAI] TTS failed, falling back to text: {ve}")
                await message.reply_text(reply_text, quote=True)
        else:
            await message.reply_text(reply_text, quote=True)

    except Exception as e:
        logger.warning(f"[SarvamAI] Failed to respond in group {message.chat.id}: {e}")
        # Silently fail — don't spam the group with error messages

    raise ContinuePropagation
