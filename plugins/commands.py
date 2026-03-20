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
            InlineKeyboardButton('🔔 Updates',       url='https://t.me/MeJeetX'),
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
            InlineKeyboardButton('🚀 Task Jobs',            callback_data='tj#list'),
        ]
    ]

# ── static fallback used before user_id is available ──────────────────────
_STATIC_BUTTONS = [
    [InlineKeyboardButton('📢 Main Channel',   url='https://t.me/MeJeetX')],
    [
        InlineKeyboardButton('💬 Support Group', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
        InlineKeyboardButton('🔔 Updates',       url='https://t.me/MeJeetX'),
    ],
    [
        InlineKeyboardButton('🙋‍♂️ Help',  callback_data='help'),
        InlineKeyboardButton('💁‍♂️ About', callback_data='about'),
    ],
    [
        InlineKeyboardButton('⚙️ Settings ⚙️', callback_data='settings#main'),
        InlineKeyboardButton('📋 Live Jobs',    callback_data='job#list'),
    ],
    [
        InlineKeyboardButton('🚀 Task Jobs',    callback_data='tj#list'),
    ]
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
    btns = await _main_buttons(user.id)
    await client.send_message(
        chat_id=message.chat.id,
        reply_markup=InlineKeyboardMarkup(btns),
        text=await t(user.id, 'START_TXT', user.first_name),
    )

# ══════════════════════════════════════════════════════════════════════════════
# Restart / Update
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command(['restart']) & filters.user(Config.BOT_OWNER_ID))
async def restart(client, message):
    msg = await message.reply_text(
        "<b>╭──────❰ 🔄 ʀᴇsᴛᴀʀᴛɪɴɢ ❱──────╮\n"
        "┃\n"
        "┣⊸ sᴀᴠɪɴɢ ᴊᴏʙ sᴛᴀᴛᴇ ᴛᴏ ᴅʙ...\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    )
    await asyncio.sleep(2)
    await msg.edit(
        "<b>╭──────❰ ✅ ʀᴇsᴛᴀʀᴛᴇᴅ ❱──────╮\n"
        "┃\n"
        "┣⊸ ʙᴏᴛ ɪs ʙᴀᴄᴋ ᴏɴʟɪɴᴇ ✅\n"
        "┣⊸ ᴊᴏʙs ᴡɪʟʟ ʀᴇsᴜᴍᴇ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    )
    os.execl(sys.executable, sys.executable, *sys.argv)


@Client.on_message(filters.private & filters.command(['update']) & filters.user(Config.BOT_OWNER_ID))
async def update_bot(client, message):
    """Pull latest code from GitHub and instantly restart the bot."""
    import subprocess, shutil

    msg = await message.reply_text(
        "<b>╭──────❰ 🔄 ᴜᴘᴅᴀᴛᴇ ❱──────╮\n"
        "┃\n"
        "┣⊸ ᴘᴜʟʟɪɴɢ ʟᴀᴛᴇsᴛ ᴄʜᴀɴɢᴇs ғʀᴏᴍ ɢɪᴛ...\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    )

    # -- git pull -------------------------------------------------------
    git  = shutil.which("git") or "git"
    cwd  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = await asyncio.create_subprocess_exec(
        git, "pull", "origin", "main",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await proc.communicate()
    out = (stdout or b"").decode().strip()
    err = (stderr or b"").decode().strip()

    # -- Already up to date? -------------------------------------------
    if "Already up to date" in out:
        return await msg.edit(
            "<b>╭──────❰ ✅ ᴜᴘ ᴛᴏ ᴅᴀᴛᴇ ❱──────╮\n"
            "┃\n"
            "┣⊸ ɴᴏ ɴᴇᴡ ᴄʜᴀɴɢᴇs ᴏɴ ɢɪᴛ.\n"
            "┣⊸ ɴᴏ ʀᴇsᴛᴀʀᴛ ɴᴇᴇᴅᴇᴅ ✅\n"
            "┃\n"
            "╰────────────────────────────────╯</b>"
        )

    # -- Error? ---------------------------------------------------------
    if proc.returncode != 0:
        snippet = (err or out)[:500]
        return await msg.edit(
            f"<b>╭──────❰ ❌ ᴜᴘᴅᴀᴛᴇ ғᴀɪʟᴇᴅ ❱──────╮\n"
            f"┃\n"
            f"┣⊸ ɢɪᴛ ᴇxɪᴛ ᴄᴏᴅᴇ: {proc.returncode}\n"
            f"┃\n"
            f"╰────────────────────────────────╯</b>\n"
            f"<code>{snippet}</code>"
        )

    # -- Parse changed files -------------------------------------------
    changed_files = [
        ln.strip() for ln in out.splitlines()
        if ln.strip() and not ln.startswith(("From ", "remote:", "Updating", "Fast-forward"))
        and "|" not in ln and "file" not in ln
    ]
    files_str = "\n".join(f"┣⊸ ◈ {f}" for f in changed_files[:10]) or "┣⊸ ◈ (see git log)"

    await msg.edit(
        f"<b>╭──────❰ ✅ ᴜᴘᴅᴀᴛᴇᴅ ❱──────╮\n"
        f"┃\n"
        f"┣⊸ 𝐂𝐡𝐚𝐧𝐠𝐞𝐝 𝐅𝐢𝐥𝐞𝐬:\n"
        f"{files_str}\n"
        f"┃\n"
        f"┣⊸ ʀᴇsᴛᴀʀᴛɪɴɢ ɪɴ 3s...\n"
        f"┣⊸ ᴊᴏʙs ᴡɪʟʟ ʀᴇsᴜᴍᴇ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ\n"
        f"┃\n"
        f"╰────────────────────────────────╯</b>"
    )
    await asyncio.sleep(3)
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
            [
                InlineKeyboardButton('⚙️ sᴇᴛᴛɪɴɢs', callback_data='settings#main'),
                InlineKeyboardButton('📜 sᴛᴀᴛᴜs',   callback_data='status'),
            ],
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
    import time as _time
    from main import START_TIME
    user_id = query.from_user.id

    users_count        = await db.get_total_users_count()
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
        in_memory_tasks = "0"

    try:
        from .taskjob import _pause_events
        in_memory_taskjobs = len(_pause_events)
    except Exception:
        in_memory_taskjobs = "0"

    text = (
        "<b>╭─────❰ 📊 sʏsᴛᴇᴍ sᴛᴀᴛᴜs ❱─────╮</b>\n"
        "<b>┃</b>\n"
        f"<b>┣⊸ ⏱ ᴜᴘᴛɪᴍᴇ :</b> <code>{uptime}</code>\n"
        f"<b>┣⊸ 🟢 ᴀᴄᴛɪᴠᴇ ʟɪᴠᴇ ᴊᴏʙs :</b> <code>{active_jobs}</code> <i>(Tasks: {in_memory_tasks})</i>\n"
        f"<b>┣⊸ 🚀 ᴀᴄᴛɪᴠᴇ ᴛᴀsᴋ ᴊᴏʙs :</b> <code>{in_memory_taskjobs}</code>\n"
        f"<b>┣⊸ 📡 ɴᴏʀᴍᴀʟ ғᴏʀᴡᴀʀᴅs :</b> <code>{active_forwarding}</code>\n"
        "<b>┃</b>\n"
        f"<b>┣⊸ 👥 ᴛᴏᴛᴀʟ ᴜsᴇʀs :</b> <code>{users_count}</code>\n"
        f"<b>┣⊸ 🤖 ʙᴏᴛ/ᴜsᴇʀʙᴏᴛs ᴀᴄᴛɪᴠᴇ :</b> <code>{bots_count}</code>\n"
        f"<b>┣⊸ 📢 ᴄʜᴀɴɴᴇʟs sᴀᴠᴇᴅ :</b> <code>{total_channels_cnt}</code>\n"
        "<b>┃</b>\n"
        "<b>╰───────────────────────────╯</b>"
    )

    await query.message.edit_text(
        text=text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='help')]]),
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
