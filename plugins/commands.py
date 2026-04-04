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



async def _safe_edit(bot, query, **kwargs):
    if getattr(query.message, 'photo', None):
        await query.message.delete()
        kwargs['chat_id'] = query.message.chat.id
        return await bot.send_message(**kwargs)
    else:
        return await query.message.edit_text(**kwargs)

async def _main_buttons(user_id: int):
    lang = await db.get_language(user_id)
    return [
        [
            InlineKeyboardButton(_tx(lang, 'btn_settings'), callback_data='settings#main'),
            InlineKeyboardButton(_tx(lang, 'btn_jobs'),     callback_data='job#list'),
        ],
        [
            InlineKeyboardButton('»  Mᴜʟᴛɪ Jᴏʙ',    callback_data='mj#list'),
            InlineKeyboardButton('»  Mᴇʀɢᴇʀ Jᴏʙ',   callback_data='mg#main'),
        ],
        [
            InlineKeyboardButton('»  Cʟᴇᴀɴᴇʀ Jᴏʙ', callback_data='cl#main'),
            InlineKeyboardButton('»  Cʟᴇᴀɴ MSG',    callback_data='settings#cleanmsg'),
        ],
        [
            InlineKeyboardButton('»  Bᴀᴛᴄʜ Lɪɴᴋs',  callback_data='sl#start'),
            InlineKeyboardButton('Sᴛᴀᴛᴜs',         callback_data='status'),
        ],
        [
            InlineKeyboardButton('Aʙᴏᴜᴛ',           callback_data='about'),
        ],
    ]

#  static fallback used before user_id is available 
_STATIC_BUTTONS = [
    [InlineKeyboardButton('📢 Main Channel',   url='https://t.me/MeJeetX')],
    [
        InlineKeyboardButton('💬 Support Group', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
    ],
    [
        InlineKeyboardButton('⚙️ Settings', callback_data='settings#main'),
        InlineKeyboardButton('📋 Live Jobs', callback_data='job#list'),
    ],
    [
        InlineKeyboardButton('»  Mᴜʟᴛɪ Jᴏʙ',   callback_data='mj#list'),
        InlineKeyboardButton('»  Bᴀᴛᴄʜ Lɪɴᴋs', callback_data='sl#start'),
    ],
    [
        InlineKeyboardButton('»  Cʟᴇᴀɴᴇʀ Jᴏʙ', callback_data='cl#main'),
        InlineKeyboardButton('»  Bᴀᴛᴄʜ Lɪɴᴋs', callback_data='sl#start'),
    ],
    [
        InlineKeyboardButton('Sᴛᴀᴛᴜs',         callback_data='status'),
        InlineKeyboardButton('Aʙᴏᴜᴛ',           callback_data='about'),
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
    configs = await db.get_configs(user.id)
    menu_image_id = configs.get('menu_image_id')
    btns = await _main_buttons(user.id)

    full_name = f"{user.first_name} {user.last_name}" if getattr(user, 'last_name', None) else user.first_name
    txt = await t(user.id, 'START_TXT', user.id, full_name)
    markup = InlineKeyboardMarkup(btns)

    if menu_image_id:
        await client.send_photo(
            chat_id=message.chat.id,
            photo=menu_image_id,
            caption=txt,
            reply_markup=markup,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            reply_markup=markup,
            text=txt,
        )

# ==================Restart Function==================

@Client.on_message(filters.private & filters.command(['restart']) & filters.user(Config.BOT_OWNER_ID))
async def restart(client, message):
    msg = await message.reply_text(text="<i>Trying to restarting.....</i>")
    await asyncio.sleep(5)
    await msg.edit("<i>Server restarted successfully » </i>")
    os.execl(sys.executable, sys.executable, *sys.argv)

# ==================Callback Functions==================

@Client.on_callback_query(filters.regex(r'^help'))
async def helpcb(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    await _safe_edit(bot, query, 
        text=_tx(lang, 'HELP_TXT'),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('ʜᴏᴡ ᴛᴏ ᴜꜱᴇ ᴍᴇ » ', callback_data='how_to_use')],
            [InlineKeyboardButton('»  ꜱᴇᴛᴛɪɴɢꜱ', callback_data='settings#main')],
            [InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='back')],
        ])
    )

@Client.on_callback_query(filters.regex(r'^how_to_use'))
async def how_to_use(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    await _safe_edit(bot, query, 
        text=_tx(lang, 'HOW_USE_TXT'),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='help')]]),
        disable_web_page_preview=True,
    )

@Client.on_callback_query(filters.regex(r'^back'))
async def back(bot, query):
    user_id = query.from_user.id
    configs = await db.get_configs(user_id)
    menu_image_id = configs.get('menu_image_id')
    btns = await _main_buttons(user_id)
    
    full_name = f"{query.from_user.first_name} {query.from_user.last_name}" if getattr(query.from_user, 'last_name', None) else query.from_user.first_name
    txt = await t(user_id, 'START_TXT', user_id, full_name)
    markup = InlineKeyboardMarkup(btns)

    if menu_image_id:
        if getattr(query.message, "photo", None):
            await query.message.edit_caption(caption=txt, reply_markup=markup)
        else:
            await query.message.delete()
            await bot.send_photo(chat_id=query.message.chat.id, photo=menu_image_id, caption=txt, reply_markup=markup)
    else:
        if getattr(query.message, "photo", None):
            await query.message.delete()
            await bot.send_message(chat_id=query.message.chat.id, text=txt, reply_markup=markup)
        else:
            await query.message.edit_text(text=txt, reply_markup=markup, disable_web_page_preview=True)

def get_bot_version():
    try:
        import subprocess
        r = subprocess.run(["git", "log", "-1", "--format=%h (%cs)"], capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return "Unknown"

def _simplify_commit(msg: str) -> str:
    """Convert a raw git commit message into a simple, user-friendly sentence."""
    import re as _re
    # Strip conventional commit prefixes like fix:, feat:, chore:, refactor: etc.
    msg = _re.sub(r'^(fix|feat|chore|refactor|style|docs|perf|test|build|ci|revert|hotfix|add|update|remove|merge|wip)[:(\[].*?[)\]]?:\s*', '', msg, flags=_re.IGNORECASE).strip()
    # Common technical patterns → plain words
    replacements = [
        (_re.compile(r'\[?[A-Z]+-\d+\]?'), ''),           # Jira ticket refs
        (_re.compile(r'\battr\b', _re.I), 'attribute'),
        (_re.compile(r'\bdb\b', _re.I), 'database'),
        (_re.compile(r'\bsts\b', _re.I), 'status object'),
        (_re.compile(r'\bregex\b', _re.I), 'pattern matching'),
        (_re.compile(r'\binit\b', _re.I), 'initialize'),
        (_re.compile(r'defensive programming', _re.I), 'crash prevention'),
        (_re.compile(r'\bfwd\b', _re.I), 'forwarding'),
        (_re.compile(r'\bundle\b', _re.I), 'topic message'),
        (_re.compile(r'→|->'), 'to'),
    ]
    for pattern, replacement in replacements:
        msg = pattern.sub(replacement, msg)
    msg = msg.strip(' .-,')
    if msg and not msg[0].isupper():
        msg = msg[0].upper() + msg[1:]
    if msg and not msg.endswith('.'):
        msg += '.'
    return msg if len(msg) > 4 else None

def get_whats_new():
    try:
        import subprocess
        r = subprocess.run(
            ["git", "log", "-15", "--format=%s|%cs"],
            capture_output=True, text=True
        )
        if r.returncode == 0 and r.stdout.strip():
            lines = []
            for entry in r.stdout.strip().splitlines():
                parts = entry.split('|', 1)
                raw_msg = parts[0].strip()
                date_str = parts[1].strip() if len(parts) > 1 else ''
                # Format date
                try:
                    import datetime
                    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
                    date_label = dt.strftime('%d %b %Y')
                except Exception:
                    date_label = date_str
                simplified = _simplify_commit(raw_msg)
                if simplified:
                    lines.append(f"🔸 <b>{date_label}</b> — {simplified}")
            if lines:
                return '\n'.join(lines)
    except Exception:
        pass
    return "No recent updates found."

@Client.on_callback_query(filters.regex(r'^about'))
async def about(bot, query):
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    await _safe_edit(bot, query, 
        text=_tx(lang, 'ABOUT_TXT', python_version=python_version(), bot_version=get_bot_version()),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('📢 Mᴀɪɴ Cʜᴀɴɴᴇʟ',   url='https://t.me/MeJeetX')],
            [
                InlineKeyboardButton('💬 Sᴜᴘᴘᴏʀᴛ Gʀᴏᴜᴘ', url='https://t.me/+1p2hcQ4ZaupjNjI1'),
                InlineKeyboardButton('🙋 Hᴇʟᴘ',  callback_data='help'),
            ],
            [InlineKeyboardButton('»  ᴡʜᴀᴛ\'s Nᴇᴡ', callback_data='whatsnew')],
            [InlineKeyboardButton('❮ Bᴀᴄᴋ', callback_data='back')]
        ]),
        disable_web_page_preview=True,
        parse_mode=enums.ParseMode.HTML,
    )

@Client.on_callback_query(filters.regex(r'^whatsnew'))
async def whats_new(bot, query):
    text = f"<b><u>»  WHAT'S NEW (Latest Updates)</u></b>\n\n{get_whats_new()}"
    await _safe_edit(bot, query, 
        text=text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='about')]]),
        disable_web_page_preview=True,
        parse_mode=enums.ParseMode.HTML,
    )

def humanbytes(size):
    if not size: return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0: break
        size /= 1024.0
    return f"{size:.2f} {unit}"

def get_readable_time(seconds: int) -> str:
    count = 0
    ping_time = ""
    time_list = []
    time_suffix_list = ["s", "m", "h", "days"]
    while count < 4:
        count += 1
        curr_time = seconds % 60
        time_list.append(int(curr_time))
        seconds = int(seconds / 60)
        if seconds == 0: break
    for x in range(len(time_list)):
        time_list[x] = str(time_list[x]) + time_suffix_list[x]
    if len(time_list) == 4:
        ping_time += time_list.pop() + " "
    time_list.reverse()
    ping_time += ":".join(time_list)
    return ping_time

@Client.on_callback_query(filters.regex(r'^status'))
async def status(bot, query):
    import psutil, time, asyncio
    user_id = query.from_user.id
    lang = await db.get_language(user_id)
    
    # Send a quick response to clear spinning wheel while computing speed
    await query.answer()
    
    users_count, bots_count = await db.total_users_bots_count()
    total_channels = await db.total_channels()
    
    # Calculate real-time speed in one second
    old_net = psutil.net_io_counters()
    await asyncio.sleep(1)
    new_net = psutil.net_io_counters()
    dl_speed = humanbytes(new_net.bytes_recv - old_net.bytes_recv) + "/s"
    ul_speed = humanbytes(new_net.bytes_sent - old_net.bytes_sent) + "/s"
    
    stats = await db.get_global_stats()
    live_fwd = stats.get('live_forward', 0)
    batch_fwd = stats.get('batch_forward', 0)
    normal_fwd = stats.get('normal_forward', 0)
    total_fwd = live_fwd + batch_fwd + normal_fwd
    
    dl_files = stats.get('total_files_downloaded', 0)
    ul_files = stats.get('total_files_uploaded', 0)
    data_usage = humanbytes(stats.get('total_data_usage_bytes', 0))
    
    uptime = get_readable_time(int(time.time() - stats.get('bot_start_time', time.time())))
    
    kwargs = {
        'users_count': users_count,
        'bots_count': bots_count,
        'total_channels': total_channels,
        'banned_users': temp.BANNED_USERS,
        'current_forwards': temp.forwardings,
        'live_forward': live_fwd,
        'batch_forward': batch_fwd,
        'normal_forward': normal_fwd,
        'total_forward': total_fwd,
        'total_files_downloaded': dl_files,
        'total_files_uploaded': ul_files,
        'total_data_usage_bytes': data_usage,
        'dl_speed': dl_speed,
        'ul_speed': ul_speed,
        'uptime': uptime
    }

    await _safe_edit(bot, query, 
        text=_tx(lang, 'STATUS_TXT', **kwargs),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='back')]]),
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# /stats  — Owner only: detailed bot statistics
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("resetstats") & filters.user(Config.BOT_OWNER_ID))
async def reset_stats(bot, message):
    await db.reset_global_stats()
    await message.reply_text("»  Global Stats successfully reset.")

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
        "<b> »  Owner Stats </b>\n"
        "<b></b>\n"
        f"<b>  👥 Total Users     :</b> <code>{total_users}</code>\n"
        f"<b>  📡 Active Forwards  :</b> <code>{active_forwarding}</code>\n"
        f"<b>  🟢 Active Live Jobs :</b> <code>{active_jobs}</code>  <i>(tasks: {in_memory_tasks})</i>\n"
        f"<b>  »  Bot Accounts     :</b> <code>{bots_count}</code>\n"
        f"<b>  »  Channels Saved   :</b> <code>{total_channels_cnt}</code>\n"
        f"<b>  🚫 Banned Users     :</b> <code>{len(temp.BANNED_USERS)}</code>\n"
        "<b></b>\n"
        f"<b>  »  Uptime            :</b> <code>{uptime}</code>\n"
        "<b></b>\n"
        "<b></b>"
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
            return await message.reply_text("»  All text replacements cleared!")
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
    await message.reply_text(f"»  Replacement added:\n\n<code>{old_text}</code> ➔ <code>{new_text}</code>")
