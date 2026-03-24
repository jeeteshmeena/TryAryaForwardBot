import os
import sys
import asyncio
from database import db, mongodb_version
from config import Config, temp
from platform import python_version
from translation import Translation
from plugins.lang import t, _tx
from pyrogram import Client, filters, enums, __version__ as pyrogram_version
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaDocument


async def _main_buttons(user_id: int):
    lang = await db.get_language(user_id)
    return [
        [InlineKeyboardButton('📢 Main Channel',   url='https://t.me/MeJeetX')],
        [
            InlineKeyboardButton('💬 Support Group', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
            InlineKeyboardButton('📜 Sᴛᴀᴛᴜs',       callback_data='status'),
        ],
        [
            InlineKeyboardButton(_tx(lang, 'btn_help'),  callback_data='help'),
            InlineKeyboardButton(_tx(lang, 'btn_about'), callback_data='about'),
        ],
        [
            InlineKeyboardButton(_tx(lang, 'btn_settings'), callback_data='settings#main'),
            InlineKeyboardButton(_tx(lang, 'btn_jobs'),     callback_data='job#list'),
        ],
        [
            InlineKeyboardButton('⚡ Mᴜʟᴛɪ Jᴏʙ',    callback_data='mj#list'),
        ],
    ]

# ── static fallback used before user_id is available ──────────────────────
_STATIC_BUTTONS = [
    [InlineKeyboardButton('📢 Main Channel',   url='https://t.me/MeJeetX')],
    [
        InlineKeyboardButton('💬 Support Group', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
        InlineKeyboardButton('📜 Sᴛᴀᴛᴜs',       callback_data='status'),
    ],
    [
        InlineKeyboardButton('🙋‍♂️ Help',  callback_data='help'),
        InlineKeyboardButton('💁‍♂️ About', callback_data='about'),
    ],
    [
        InlineKeyboardButton('⚙️ Sᴇᴛᴛɪɴɢs ⚙️', callback_data='settings#main'),
        InlineKeyboardButton('📋 Lɪᴠᴇ Jᴏʙs',    callback_data='job#list'),
    ],
]

# ===================Start Function===================

@Client.on_message(filters.private & filters.command(['start']))
async def start(client, message):
    user = message.from_user
    if not await db.is_user_exist(user.id):
        await db.add_user(user.id, user.first_name)
    try:
        from .jobs import resume_live_jobs
        await resume_live_jobs(user.id)
    except Exception:
        pass
    try:
        from .taskjob import resume_task_jobs
        await resume_task_jobs(user.id)
    except Exception:
        pass
    try:
        from .multijob import resume_multi_jobs
        await resume_multi_jobs(user.id)
    except Exception:
        pass
    btns = await _main_buttons(user.id)
    await client.send_message(
        chat_id=message.chat.id,
        reply_markup=InlineKeyboardMarkup(btns),
        text=await t(user.id, 'START_TXT', user.first_name),
    )

# ==================Restart Function==================

@Client.on_message(filters.private & filters.command(['restart']) & filters.user(Config.BOT_OWNER_ID))
async def restart(client, message):
    msg = await message.reply_text(text="<i>Trying to restarting.....</i>")
    await asyncio.sleep(5)
    await msg.edit("<i>Server restarted successfully ✅</i>")
    os.execl(sys.executable, sys.executable, *sys.argv)

# ==================Callback Functions==================

@Client.on_callback_query(filters.regex(r'^help'))
async def helpcb(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    await query.message.edit_text(
        text=_tx(lang, 'HELP_TXT'),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('ʜᴏᴡ ᴛᴏ ᴜsᴇ ᴍᴇ ❓', callback_data='how_to_use')],
            [InlineKeyboardButton('⚙️ sᴇᴛᴛɪɴɢs', callback_data='settings#main')],
            [InlineKeyboardButton('↩ ʙᴀᴄᴋ', callback_data='back')],
        ])
    )

@Client.on_callback_query(filters.regex(r'^how_to_use'))
async def how_to_use(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    await query.message.edit_text(
        text=_tx(lang, 'HOW_USE_TXT'),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='help')]]),
        disable_web_page_preview=True,
    )

@Client.on_callback_query(filters.regex(r'^back'))
async def back(bot, query):
    user_id = query.from_user.id
    btns = await _main_buttons(user_id)
    await query.message.edit_text(
        reply_markup=InlineKeyboardMarkup(btns),
        text=await t(user_id, 'START_TXT', query.from_user.first_name),
    )

@Client.on_callback_query(filters.regex(r'^about'))
async def about(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    await query.message.edit_text(
        text=_tx(lang, 'ABOUT_TXT', python_version=python_version()),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='back')]]),
        disable_web_page_preview=True,
        parse_mode=enums.ParseMode.HTML,
    )

@Client.on_callback_query(filters.regex(r'^status'))
async def status(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    users_count, bots_count = await db.total_users_bots_count()
    total_channels = await db.total_channels()
    await query.message.edit_text(
        text=_tx(lang, 'STATUS_TXT',
                 users_count, bots_count, temp.forwardings, total_channels, temp.BANNED_USERS),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='back')]]),
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# /stats  — Owner only: detailed bot statistics
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("stats") & filters.user(Config.BOT_OWNER_ID))
async def owner_stats(bot, message):
    import time as _time
    from main import START_TIME

    total_users        = await db.get_total_users_count()
    active_forwarding  = await db.get_active_forwardings_count()
    active_jobs        = await db.get_active_jobs_count()
    total_channels_cnt = await db.total_channels()
    _, bots_count      = await db.total_users_bots_count()

    elapsed = _time.time() - START_TIME
    d, rem  = divmod(int(elapsed), 86400)
    h, rem  = divmod(rem, 3600)
    m, s    = divmod(rem, 60)
    uptime  = f"{d}d {h}h {m}m {s}s"

    try:
        from .jobs import _job_tasks
        in_memory_tasks = len([tk for tk in _job_tasks.values() if not tk.done()])
    except Exception:
        in_memory_tasks = "N/A"

    text = (
        "<b>╭─────❰ 📊 Owner Stats ❱─────╮</b>\n"
        "<b>┃</b>\n"
        f"<b>┣⊸ 👥 Total Users     :</b> <code>{total_users}</code>\n"
        f"<b>┣⊸ 📡 Active Forwards  :</b> <code>{active_forwarding}</code>\n"
        f"<b>┣⊸ 🟢 Active Live Jobs :</b> <code>{active_jobs}</code>  <i>(tasks: {in_memory_tasks})</i>\n"
        f"<b>┣⊸ 🤖 Bot Accounts     :</b> <code>{bots_count}</code>\n"
        f"<b>┣⊸ 📢 Channels Saved   :</b> <code>{total_channels_cnt}</code>\n"
        f"<b>┣⊸ 🚫 Banned Users     :</b> <code>{len(temp.BANNED_USERS)}</code>\n"
        "<b>┃</b>\n"
        f"<b>┣⊸ ⏱ Uptime            :</b> <code>{uptime}</code>\n"
        "<b>┃</b>\n"
        "<b>╰──────────────────────────╯</b>"
    )
    await message.reply_text(text)

# ══════════════════════════════════════════════════════════════════════════════
# /replace  — Add a Find & Replace string for captions
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("replace"))
async def replace_strings(bot, message):
    user_id = message.from_user.id
    if len(message.command) < 3:
        usage = (
            "<b>Usage:</b> <code>/replace old_text new_text</code>\n\n"
            "This will replace all instances of <code>old_text</code> with <code>new_text</code> in forwarded captions.\n"
            "Use <code>/replace clear</code> to remove all replacements."
        )
        if len(message.command) == 2 and message.command[1].lower() == 'clear':
            configs = await db.get_configs(user_id)
            configs['replacements'] = {}
            await db.update_configs(user_id, configs)
            return await message.reply_text("✅ All text replacements cleared!")
        return await message.reply_text(usage)

    old_text = message.command[1]
    new_text = " ".join(message.command[2:])

    configs = await db.get_configs(user_id)
    replacements = configs.get('replacements', {})
    if old_text in replacements:
        del replacements[old_text]
    else:
        replacements[old_text] = new_text

    configs['replacements'] = replacements
    await db.update_configs(user_id, configs)
    await message.reply_text(f"✅ Replacement added:\n\n<code>{old_text}</code> ➔ <code>{new_text}</code>")
