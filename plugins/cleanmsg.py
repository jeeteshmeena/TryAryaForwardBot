"""
Clean MSG Plugin — Proper Implementation using bot.ask()
Flow:
  1. Select Account  (bot/userbot)
  2. Toggle-select Target Chats (multi-step ask loop)
  3. Select Message Type  (All / Audio / Video / Document / Photo / etc.)
  4. Confirm → Execute → Report

Key fix: Channels attribute messages to the channel, not the bot.
So we NEVER filter by sender — we iterate ALL messages and filter by type only.
  • Bot client  → iterate via get_messages(IDs) batches (bot-safe, no GetHistory)
  • Userbot     → get_chat_history() then filter by type
"""
import asyncio
from database import db
from .test import CLIENT, start_clone_bot
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageDeleteForbidden, ChatAdminRequired
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)

_CLIENT = CLIENT()

# Pending confirm contexts  msg_id → dict
_pending_cleans: dict[int, dict] = {}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def __parse_link(link: str):
    link = link.strip().rstrip('/')
    if "t.me/" not in link:
        return None, None
    parts = link.split('/')
    if not parts[-1].isdigit():
        return None, None
    msg_id = int(parts[-1])
    
    if "t.me/c/" in link:
        chat_id = int(f"-100{parts[-2]}")
    else:
        chat_id = parts[-2]
    return chat_id, msg_id

def _type_matches(msg, wanted: str) -> bool:
    if msg.empty or msg.service:
        return False
    if wanted == "all":
        return True
    if wanted == "all_media":
        return bool(msg.media)
    if wanted == "commands":
        return bool(msg.text and msg.text.startswith("/"))
    mapping = {
        "audio":     lambda m: bool(m.audio),
        "voice":     lambda m: bool(m.voice),
        "video":     lambda m: bool(m.video),
        "document":  lambda m: bool(m.document),
        "photo":     lambda m: bool(m.photo),
        "animation": lambda m: bool(m.animation),
        "sticker":   lambda m: bool(m.sticker),
        "text":      lambda m: bool(m.text and not m.media),
    }
    check = mapping.get(wanted)
    return check(msg) if check else False


async def _safe_delete(client, chat_id: int, ids: list) -> int:
    if not ids:
        return 0
    try:
        await client.delete_messages(chat_id, ids)
        return len(ids)
    except (MessageDeleteForbidden, ChatAdminRequired):
        return 0
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
        try:
            await client.delete_messages(chat_id, ids)
            return len(ids)
        except Exception:
            return 0
    except Exception:
        return 0


async def _do_delete(client, chat_id, wanted: str, status_msg, is_bot: bool, check_range=None) -> int:
    """
    Find and delete messages of the given type in chat_id.
    Returns total deleted count.
    """
    total = [0]
    batch = []

    async def flush():
        nonlocal batch
        if batch:
            total[0] += await _safe_delete(client, chat_id, batch)
            batch = []
            try:
                await status_msg.edit_text(
                    f"<b>🗑 Cleaning… <code>{total[0]}</code> deleted so far.</b>"
                )
            except Exception:
                pass

    if check_range:
        start_id = min(check_range[0], check_range[1])
        end_id = max(check_range[0], check_range[1])
        all_ids = list(range(start_id, end_id + 1))
        
        for i in range(0, len(all_ids), 200):
            chunk = all_ids[i:i+200]
            try:
                msgs = await client.get_messages(chat_id, chunk)
                if not isinstance(msgs, list): msgs = [msgs]
                valid = [m for m in msgs if m and not m.empty and not m.service]
                for msg in valid:
                    if _type_matches(msg, wanted):
                        batch.append(msg.id)
                        if len(batch) >= 100:
                            await flush()
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
            except Exception:
                pass
        await flush()
        return total[0]

    if not is_bot:
        # USERBOT path: get_chat_history works here
        try:
            async for msg in client.get_chat_history(chat_id, limit=0):
                if _type_matches(msg, wanted):
                    batch.append(msg.id)
                    if len(batch) >= 100:
                        await flush()
        except Exception as e:
            print(f"[CleanMSG] get_chat_history error for {chat_id}: {e}")
    else:
        # BOT path: iterate by message IDs
        BATCH = 200
        current = 1
        consecutive_empty = 0
        MAX_EMPTY_RUNS = 5

        while True:
            ids_to_fetch = list(range(current, current + BATCH))
            try:
                msgs = await client.get_messages(chat_id, ids_to_fetch)
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except Exception as e:
                print(f"[CleanMSG] get_messages error: {e}")
                break

            if not isinstance(msgs, list):
                msgs = [msgs]

            valid = [m for m in msgs if m and not m.empty and not m.service]

            if not valid:
                consecutive_empty += 1
                if consecutive_empty >= MAX_EMPTY_RUNS:
                    break
                current += BATCH
                continue

            consecutive_empty = 0
            for msg in valid:
                if _type_matches(msg, wanted):
                    batch.append(msg.id)
                    if len(batch) >= 100:
                        await flush()

            current += BATCH

    await flush()
    return total[0]


# ──────────────────────────────────────────────────────────────────────────────
# Settings callback entry point
# ──────────────────────────────────────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r'^settings#cleanmsg$'))
async def clean_msg_from_settings(bot, query):
    await query.message.delete()
    await _cleanmsg_flow(bot, query.from_user.id)


# ──────────────────────────────────────────────────────────────────────────────
# Command entry point
# ──────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.private & filters.command("cleanmsg"))
async def cleanmsg_cmd(bot, message):
    await _cleanmsg_flow(bot, message.from_user.id)


# ──────────────────────────────────────────────────────────────────────────────
# Main interactive flow using bot.ask()
# ──────────────────────────────────────────────────────────────────────────────
async def _cleanmsg_flow(bot, user_id: int):

    # ── Step 1: Choose account ─────────────────────────────────────────────
    accounts = await db.get_bots(user_id)
    if not accounts:
        return await bot.send_message(
            user_id,
            "<b>❌ No accounts found. Add one in /settings → Accounts first.</b>"
        )

    acc_buttons = []
    for acc in accounts:
        label = "🤖 Bot" if acc.get('is_bot', True) else "👤 Userbot"
        name  = acc.get('username') or acc.get('name', 'Unknown')
        acc_buttons.append([KeyboardButton(f"{label}: {name} [{acc['id']}]")])
    acc_buttons.append([KeyboardButton("/cancel")])

    acc_reply = await bot.ask(
        user_id,
        "<b>🗑 Clean MSG — Step 1/3</b>\n\nChoose which account to use for deletion:",
        reply_markup=ReplyKeyboardMarkup(acc_buttons, resize_keyboard=True, one_time_keyboard=True)
    )
    if "/cancel" in acc_reply.text:
        return await acc_reply.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

    sel_acc = None
    if "[" in acc_reply.text and "]" in acc_reply.text:
        try:
            acc_id = int(acc_reply.text.split('[')[-1].split(']')[0])
            sel_acc = await db.get_bot(user_id, acc_id)
        except Exception:
            pass
    if not sel_acc:
        sel_acc = accounts[0]

    # ── Step 2: Choose target chat(s) — multi-select loop ─────────────────
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await bot.send_message(
            user_id,
            "<b>❌ No target channels found. Add channels via /settings → Channels.</b>",
            reply_markup=ReplyKeyboardRemove()
        )

    selected_chats: list[int] = []
    sel_names: list[str]      = []
    id_map = {ch['title']: ch['chat_id'] for ch in channels}

    while True:
        # Build current keyboard showing ✅ already selected chats
        ch_btns = []
        for ch in channels:
            tick = "✅ " if ch['chat_id'] in selected_chats else "⬜ "
            ch_btns.append([KeyboardButton(f"{tick}{ch['title']}")])
        ch_btns.append([KeyboardButton("✔ All / Clear All")])
        ch_btns.append([KeyboardButton("▶ Done"), KeyboardButton("/cancel")])

        hint = (
            f"\n\n<b>Selected ({len(selected_chats)}):</b> " +
            (", ".join(sel_names[:3]) + ("…" if len(sel_names) > 3 else "") if sel_names else "none")
        )
        ch_reply = await bot.ask(
            user_id,
            "<b>🗑 Step 2/3</b> — Select chats to clean.\n"
            "Tap a chat to toggle, or <b>send a Chat ID/Link to add it manually.</b>\n"
            "Tap <b>▶ Done</b> when finished." + hint,
            reply_markup=ReplyKeyboardMarkup(ch_btns, resize_keyboard=True, one_time_keyboard=True)
        )
        txt = ch_reply.text.strip()

        if "/cancel" in txt:
            return await ch_reply.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

        if "Done" in txt or "▶" in txt:
            break

        if "All / Clear All" in txt or "✔" in txt:
            if len(selected_chats) == len(channels):
                selected_chats.clear()
                sel_names.clear()
            else:
                selected_chats = [ch['chat_id'] for ch in channels]
                sel_names      = [ch['title']   for ch in channels]
            continue

        # Toggle individual
        clean_txt = txt.replace("✅ ", "").replace("⬜ ", "").strip()
        found = False
        for title, cid in id_map.items():
            if title in clean_txt or clean_txt in title:
                found = True
                if cid in selected_chats:
                    selected_chats.remove(cid)
                    sel_names.remove(title)
                else:
                    selected_chats.append(cid)
                    sel_names.append(title)
                break
                
        if not found:
            custom_id = None
            if clean_txt.startswith("-100") and clean_txt[1:].isdigit():
                custom_id = int(clean_txt)
            elif clean_txt.isdigit() or (clean_txt.startswith("-") and clean_txt[1:].isdigit()):
                custom_id = int(clean_txt)
            elif "t.me/c/" in clean_txt:
                parts = clean_txt.split("t.me/c/")[1].split("/")
                if parts[0].isdigit():
                    custom_id = int(f"-100{parts[0]}")
            elif "t.me/" in clean_txt:
                username = clean_txt.split("t.me/")[1].split("/")[0].split("?")[0]
                custom_id = username
            
            if custom_id:
                if custom_id in selected_chats:
                    selected_chats.remove(custom_id)
                    sel_names.remove(str(custom_id))
                else:
                    selected_chats.append(custom_id)
                    sel_names.append(str(custom_id))

    if not selected_chats:
        return await bot.send_message(
            user_id, "<b>❌ No chats selected. Cancelled.</b>",
            reply_markup=ReplyKeyboardRemove()
        )

    # ── Step 3: Choose message type ────────────────────────────────────────
    type_reply = await bot.ask(
        user_id,
        "<b>🗑 Step 3/4</b> — What type of messages to delete?",
        reply_markup=ReplyKeyboardMarkup([
            [KeyboardButton("🗂 All Messages"),   KeyboardButton("🎵 Audio")],
            [KeyboardButton("🎤 Voice"),          KeyboardButton("📹 Video")],
            [KeyboardButton("📄 Document"),       KeyboardButton("🖼 Photo")],
            [KeyboardButton("🎞 Animation"),      KeyboardButton("🖍 Text Only")],
            [KeyboardButton("📦 All Media"),      KeyboardButton("🤖 Commands")],
            [KeyboardButton("/cancel")],
        ], resize_keyboard=True, one_time_keyboard=True)
    )
    if "/cancel" in (type_reply.text or ""):
        return await type_reply.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())

    type_map = {
        "All Messages": "all",    "All Media": "all_media",
        "Audio": "audio",         "Voice":     "voice",
        "Video": "video",         "Document":  "document",
        "Photo": "photo",         "Animation": "animation",
        "Text Only": "text",      "Commands": "commands",
    }
    wanted = "all"
    for label, key in type_map.items():
        if label in (type_reply.text or ""):
            wanted = key
            break

    # ── Step 4: Choose Range ────────────────────────────────────────
    range_reply = await bot.ask(
        user_id,
        "<b>🗑 Step 4/4</b> — Process the <b>Entire Chat(s)</b>, or define a <b>Custom Link Range</b>?",
        reply_markup=ReplyKeyboardMarkup([
            [KeyboardButton("🌍 Entire Chat(s)")],
            [KeyboardButton("🔗 Custom Link Range (From-To)")],
            [KeyboardButton("/cancel")]
        ], resize_keyboard=True, one_time_keyboard=True)
    )
    if "/cancel" in (range_reply.text or ""):
        return await range_reply.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
        
    check_range = None
    if "Custom Link Range" in (range_reply.text or ""):
        msg_reply1 = await bot.ask(
            user_id,
            "Send the <b>FIRST message link</b> (from where deletion should start):",
            reply_markup=ReplyKeyboardRemove()
        )
        if "/cancel" in msg_reply1.text: return await msg_reply1.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
        chat_id_from, start_id = __parse_link(msg_reply1.text)
        
        msg_reply2 = await bot.ask(
            user_id,
            "Send the <b>LAST message link</b> (till where deletion should happen):"
        )
        if "/cancel" in msg_reply2.text: return await msg_reply2.reply("<b>Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
        chat_id_to, end_id = __parse_link(msg_reply2.text)
        
        if not chat_id_from or str(chat_id_from) != str(chat_id_to):
            return await bot.send_message(user_id, "<b>❌ Invalid link or chats do not match. Cancelled.</b>")
            
        selected_chats = [chat_id_from]
        sel_names = [str(chat_id_from)]
        check_range = (start_id, end_id)

    # ── Confirm ─────────────────────────────────────────────────────────────
    acc_label = "🤖 Bot" if sel_acc.get('is_bot', True) else "👤 Userbot"
    acc_name  = sel_acc.get('name', 'Unknown')
    chat_list = "\n".join(f"  • {n}" for n in sel_names)
    type_label = type_reply.text.strip()
    range_label = f"From ID <code>{start_id}</code> to <code>{end_id}</code>" if check_range else "Entire Chat(s)"

    confirm_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, Delete!", callback_data="cleanmsg#confirm"),
        InlineKeyboardButton("❌ Cancel",       callback_data="cleanmsg#abort")
    ]])
    confirm_msg = await bot.send_message(
        user_id,
        f"<b>⚠️ Confirm Deletion</b>\n\n"
        f"<b>Account:</b> {acc_label} — {acc_name}\n"
        f"<b>Delete type:</b> {type_label}\n"
        f"<b>Range:</b> {range_label}\n"
        f"<b>Chats:</b>\n{chat_list}\n\n"
        f"<i>⚠️ This action is irreversible. Continue?</i>",
        reply_markup=confirm_markup
    )
    if "Custom Link Range" not in (range_reply.text or ""):
        await range_reply.reply("<b>Confirm above ⬆️</b>", reply_markup=ReplyKeyboardRemove())
    else:
        await msg_reply2.reply("<b>Confirm above ⬆️</b>", reply_markup=ReplyKeyboardRemove())

    # Store context
    _pending_cleans[confirm_msg.id] = {
        "user_id": user_id,
        "account": sel_acc,
        "chats":   selected_chats,
        "wanted":  wanted,
        "check_range": check_range
    }


# ──────────────────────────────────────────────────────────────────────────────
# Confirm / Abort callbacks
# ──────────────────────────────────────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r'^cleanmsg#confirm$'))
async def cleanmsg_confirm(bot, query):
    ctx = _pending_cleans.pop(query.message.id, None)
    if not ctx:
        return await query.answer("Session expired. Run /cleanmsg again.", show_alert=True)

    sel_acc       = ctx["account"]
    selected_chats= ctx["chats"]
    wanted        = ctx["wanted"]
    user_id       = ctx["user_id"]

    status_msg = await query.message.edit_text("<b>🗑 Starting Clean MSG… please wait.</b>")

    # Start forwarding client
    try:
        client = await start_clone_bot(_CLIENT.client(sel_acc))
    except Exception as e:
        return await status_msg.edit_text(f"<b>❌ Could not start account:</b>\n<code>{e}</code>")

    me     = await client.get_me()
    is_bot = getattr(me, 'is_bot', False)

    grand_total  = 0
    failed_chats = []

    for chat_id in selected_chats:
        try:
            await status_msg.edit_text(
                f"<b>🗑 Cleaning…\n✅ Deleted so far: <code>{grand_total}</code></b>"
            )
            count = await _do_delete(client, chat_id, wanted, status_msg, is_bot, ctx.get("check_range"))
            grand_total += count
        except Exception as e:
            print(f"[CleanMSG] Chat {chat_id} error: {e}")
            failed_chats.append(str(chat_id))

    try:
        await client.stop()
    except Exception:
        pass

    from .settings import main_buttons
    result = (
        f"<b>✅ Clean MSG Complete!</b>\n\n"
        f"<b>Total deleted:</b> <code>{grand_total}</code> messages"
    )
    if failed_chats:
        result += f"\n\n<b>⚠️ Errors in:</b> {', '.join(failed_chats)}"

    await status_msg.edit_text(
        result,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("↩ Back to Settings", callback_data="settings#main")
        ]])
    )


@Client.on_callback_query(filters.regex(r'^cleanmsg#abort$'))
async def cleanmsg_abort(bot, query):
    _pending_cleans.pop(query.message.id, None)
    from .settings import main_buttons
    await query.message.edit_text(
        "<b>Cancelled.</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("↩ Back to Settings", callback_data="settings#main")
        ]])
    )
