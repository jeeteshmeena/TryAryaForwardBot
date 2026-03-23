import os
from config import Config

class Translation(object):
  START_TXT = """<b>╭──────❰ ✦ 𝐀𝐮𝐭𝐨 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐫 ✦ ❱──────╮
┃
┣⊸ 𝐇𝐞𝐥𝐥𝐨 {}
┃
┣⊸ 🤖 Aryᴀ Bᴏᴛ [ ᴩᴏwᴇʀғᴜʟ Fᴏʀᴡᴀʀᴅ Tᴏᴏʟ ]
┃
┣⊸ <i>ɪ ᴄᴀɴ ғᴏʀᴡᴀʀᴅ ᴀʟʟ ᴍᴇssᴀɢᴇs ғʀᴏᴍ ᴏɴᴇ
┃  ᴄʜᴀɴɴᴇʟ ᴛᴏ ᴀɴᴏᴛʜᴇʀ ᴄʜᴀɴɴᴇʟ ᴡɪᴛʜ
┃  ᴍᴏʀᴇ ғᴇᴀᴛᴜʀᴇs.</i>
┃
╰────────────────────────────────╯</b>
"""


  HELP_TXT = """<b><u>🔆 ʜᴇʟᴘ — Aryᴀ Bᴏᴛ</u></b>

<b>📌 ᴄᴏᴍᴍᴀɴᴅs:</b>
<code>/start</code>     — ᴄʜᴇᴄᴋ ɪғ ɪ'ᴍ ᴀʟɪᴠᴇ
<code>/forward</code>   — sᴛᴀʀᴛ ʙᴀᴛᴄʜ ғᴏʀᴡᴀʀᴅɪɴɢ
<code>/jobs</code>      — ᴍᴀɴᴀɢᴇ ʟɪᴠᴇ ᴊᴏʙs (ʙᴀᴄᴋɢʀᴏᴜɴᴅ ғᴡᴅ)
<code>/batchjobs</code>  — ᴍᴀɴᴀɢᴇ ʙᴀᴛᴄʜ ᴊᴏʙs (ʙᴜʟᴋ ᴄᴏᴘʏ, ᴘᴀᴜsᴇ/ʀᴇsᴜᴍᴇ)
<code>/cleanmsg</code>  — ʙᴜʟᴋ ᴅᴇʟᴇᴛᴇ ᴍᴇssᴀɢᴇs
<code>/settings</code>  — ᴄᴏɴғɪɢᴜʀᴇ ᴀʟʟ sᴇᴛᴛɪɴɢs
<code>/reset</code>     — ʀᴇsᴇᴛ sᴇᴛᴛɪɴɢs ᴛᴏ ᴅᴇғᴀᴜʟᴛ

<b>⚡ ғᴇᴀᴛᴜʀᴇs:</b>
<b>►</b> ғᴏʀᴡᴀʀᴅ ғʀᴏᴍ ᴘᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟs — ɴᴏ ᴀᴅᴍɪɴ ɴᴇᴇᴅᴇᴅ
<b>►</b> ғᴏʀᴡᴀʀᴅ ғʀᴏᴍ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟs — ᴠɪᴀ ʙᴏᴛ/ᴜsᴇʀʙᴏᴛ ᴀᴅᴍɪɴ
<b>►</b> ᴍᴜʟᴛɪ-ᴀᴄᴄᴏᴜɴᴛ: ᴜᴘ ᴛᴏ 2 ʙᴏᴛs + 2 ᴜsᴇʀʙᴏᴛs
<b>►</b> ʟɪᴠᴇ ᴊᴏʙs — ʙᴀᴄᴋɢʀᴏᴜɴᴅ ᴛᴀsᴋs, ᴘᴀʀᴀʟʟᴇʟ ᴛᴏ ʙᴀᴛᴄʜ
<b>►</b> ʙᴀᴛᴄʜ ᴊᴏʙs — ʙᴜʟᴋ ᴄᴏᴘʏ ᴡɪᴛʜ ᴘᴀᴜsᴇ/ʀᴇsᴜᴍᴇ, ᴍᴜʟᴛɪᴘʟᴇ sɪᴍᴜʟᴛᴀɴᴇᴏᴜsʟʏ
<b>►</b> ᴅᴜᴀʟ ᴅᴇsᴛɪɴᴀᴛɪᴏɴs — sᴇɴᴅ ᴛᴏ 2 ᴄʜᴀɴɴᴇʟs ᴀᴛ ᴏɴᴄᴇ
<b>►</b> ɢʀᴏᴜᴘ ᴛᴏᴘɪᴄ sᴜᴘᴘᴏʀᴛ — ᴘᴏsᴛ ɪɴᴛᴏ ᴀ sᴘᴇᴄɪғɪᴄ ᴛʜʀᴇᴀᴅ
<b>►</b> ɴᴇᴡ→ᴏʟᴅ & ᴏʟᴅ→ɴᴇᴡ ғᴏʀᴡᴀʀᴅɪɴɢ ᴏʀᴅᴇʀ
<b>►</b> ғɪʟᴛᴇʀs — sᴋɪᴘ ᴀᴜᴅɪᴏ/ᴠɪᴅᴇᴏ/ᴘʜᴏᴛᴏ/ᴛᴇxᴛ/sᴛɪᴄᴋᴇʀ/ᴘᴏʟʟ ᴇᴛᴄ.
<b>►</b> ᴄᴜsᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ / ʀᴇᴍᴏᴠᴇ ᴄᴀᴘᴛɪᴏɴ / ᴀᴅᴅ ʙᴜᴛᴛᴏɴs
<b>►</b> sᴋɪᴘ ᴅᴜᴘʟɪᴄᴀᴛᴇ ᴍᴇssᴀɢᴇs
<b>►</b> ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴏᴅᴇ — ʙʏᴘᴀssᴇs ᴄᴏɴᴛᴇɴᴛ ʀᴇsᴛʀɪᴄᴛɪᴏɴs
<b>►</b> ᴄʟᴇᴀɴ ᴍsɢ — ʙᴜʟᴋ ᴅᴇʟᴇᴛᴇ ғʀᴏᴍ ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟs
"""

  HOW_USE_TXT = """<b><u>📍 ʜᴏᴡ ᴛᴏ ᴜsᴇ — Aryᴀ Bᴏᴛ</u></b>

<b>1️⃣ ᴀᴅᴅ ᴀɴ ᴀᴄᴄᴏᴜɴᴛ</b>
  ‣ ɢᴏ ᴛᴏ /settings → ⚙️ ᴀᴄᴄᴏᴜɴᴛs
  ‣ ᴀᴅᴅ ᴀ ʙᴏᴛ (sᴇɴᴅ ᴛᴏᴋᴇɴ) ᴏʀ ᴜsᴇʀʙᴏᴛ (sᴇɴᴅ sᴇssɪᴏɴ sᴛʀɪɴɢ)
  ‣ ᴜᴘ ᴛᴏ 2 ʙᴏᴛs + 2 ᴜsᴇʀʙᴏᴛs

<b>2️⃣ ᴀᴅᴅ ᴀ ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟ</b>
  ‣ ɢᴏ ᴛᴏ /settings → 📣 ᴄʜᴀɴɴᴇʟs
  ‣ ʏᴏᴜʀ ʙᴏᴛ/ᴜsᴇʀʙᴏᴛ ᴍᴜsᴛ ʙᴇ <b>ᴀᴅᴍɪɴ</b> ɪɴ ᴛʜᴇ ᴛᴀʀɢᴇᴛ

<b>3️⃣ ᴄᴏɴғɪɢᴜʀᴇ sᴇᴛᴛɪɴɢs</b>
  ‣ <b>ғɪʟᴛᴇʀs</b> — ᴄʜᴏᴏsᴇ ᴡʜᴀᴛ ᴛʏᴘᴇs ᴛᴏ sᴋɪᴘ
  ‣ <b>ᴄᴀᴘᴛɪᴏɴ</b> — ᴄᴜsᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ᴏʀ ʀᴇᴍᴏᴠᴇ ɪᴛ
  ‣ <b>ғᴏʀᴡᴀʀᴅ ᴛᴀɢ</b> — sʜᴏᴡ ᴏʀ ʜɪᴅᴇ ғᴏʀᴡᴀʀᴅᴇᴅ-ғʀᴏᴍ ʟᴀʙᴇʟ
  ‣ <b>ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴏᴅᴇ</b> — ʀᴇ-ᴜᴘʟᴏᴀᴅ ғɪʟᴇs (ʙʏᴘᴀssᴇs ʀᴇsᴛʀɪᴄᴛɪᴏɴs)
  ‣ <b>ᴅᴜᴘʟɪᴄᴀᴛᴇ sᴋɪᴘ</b> — ᴀᴠᴏɪᴅ ʀᴇ-ғᴏʀᴡᴀʀᴅɪɴɢ sᴀᴍᴇ ᴄᴏɴᴛᴇɴᴛ

<b>4️⃣ ʙᴀᴛᴄʜ ғᴏʀᴡᴀʀᴅ (/forward)</b>
  ‣ ᴄʜᴏᴏsᴇ ᴀᴄᴄᴏᴜɴᴛ → sᴇʟᴇᴄᴛ ᴛᴀʀɢᴇᴛ → sᴇɴᴅ sᴏᴜʀᴄᴇ ʟɪɴᴋ/ɪᴅ
  ‣ ᴄʜᴏᴏsᴇ ᴏʀᴅᴇʀ (ᴏʟᴅ→ɴᴇᴡ / ɴᴇᴡ→ᴏʟᴅ) → sᴇᴛ sᴋɪᴘ ᴄᴏᴜɴᴛ

<b>5️⃣ ʟɪᴠᴇ ᴊᴏʙs (/jobs)</b>
  ‣ ᴀᴜᴛᴏ-ғᴏʀᴡᴀʀᴅs ɴᴇᴡ ᴍsɢs ɪɴ ᴛʜᴇ ʙᴀᴄᴋɢʀᴏᴜɴᴅ
  ‣ ᴏᴘᴛɪᴏɴᴀʟ ʙᴀᴛᴄʜ ᴘʜᴀsᴇ: ᴄᴏᴘɪᴇs ᴏʟᴅ ᴍsɢs ғɪʀsᴛ, ᴛʜᴇɴ ɢᴏᴇs ʟɪᴠᴇ
  ‣ ᴅᴜᴀʟ ᴅᴇsᴛɪɴᴀᴛɪᴏɴs: sᴇɴᴅ ᴛᴏ 2 ᴄʜᴀɴɴᴇʟs ᴀᴛ ᴛʜᴇ sᴀᴍᴇ ᴛɪᴍᴇ
  ‣ ɢʀᴏᴜᴘ ᴛᴏᴘɪᴄ sᴜᴘᴘᴏʀᴛ — ᴘᴏsᴛ ɪɴᴛᴏ ᴀ sᴘᴇᴄɪғɪᴄ ᴛʜʀᴇᴀᴅ
  ‣ ᴘᴇʀ-ᴊᴏʙ sɪᴢᴇ/ᴅᴜʀᴀᴛɪᴏɴ ʟɪᴍɪᴛ

<b>6️⃣ ʙᴀᴛᴄʜ ᴊᴏʙs (/batchjobs)</b>
  ‣ ʙᴜʟᴋ-ᴄᴏᴘɪᴇs ᴀʟʟ ᴇxɪsᴛɪɴɢ ᴍsɢs ɪɴ ᴛʜᴇ ʙᴀᴄᴋɢʀᴏᴜɴᴅ
  ‣ ᴘᴀᴜsᴇ / ʀᴇsᴜᴍᴇ ғʀᴏᴍ ᴇxᴀᴄᴛ ᴘᴏsɪᴛɪᴏɴ
  ‣ ᴍᴜʟᴛɪᴘʟᴇ ᴊᴏʙs ᴄᴀɴ ʀᴜɴ ᴀᴛ ᴛʜᴇ sᴀᴍᴇ ᴛɪᴍᴇ
  ‣ ᴄᴜsᴛᴏᴍ ᴍᴇssᴀɢᴇ ɪᴅ ʀᴀɴɢᴇ (ᴇ.ɢ. 500:2000)

<b>7️⃣ ᴄʟᴇᴀɴ ᴍsɢ (/cleanmsg)</b>
  ‣ sᴇʟᴇᴄᴛ ᴀᴄᴄᴏᴜɴᴛ + ᴄʜᴀᴛ + ᴍᴇssᴀɢᴇ ᴛʏᴘᴇ → ʙᴜʟᴋ ᴅᴇʟᴇᴛᴇ

<b>⚠️ ɴᴏᴛᴇs:</b>
  ‣ ʙᴏᴛ ᴀᴄᴄᴏᴜɴᴛ: ɴᴇᴇᴅs ᴀᴅᴍɪɴ ɪɴ ᴛᴀʀɢᴇᴛ (ᴀɴᴅ sᴏᴜʀᴄᴇ ɪғ ᴘʀɪᴠᴀᴛᴇ)
  ‣ ᴜsᴇʀʙᴏᴛ: ɴᴇᴇᴅs ᴍᴇᴍʙᴇʀsʜɪᴘ ɪɴ sᴏᴜʀᴄᴇ + ᴀᴅᴍɪɴ ɪɴ ᴛᴀʀɢᴇᴛ
  ‣ ғᴏʀ ᴘᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟs, ᴀ ɴᴏʀᴍᴀʟ ʙᴏᴛ ᴡᴏʀᴋs ғɪɴᴇ
  ‣ ғᴏʀ ᴘʀɪᴠᴀᴛᴇ/ʀᴇsᴛʀɪᴄᴛᴇᴅ sᴏᴜʀᴄᴇs, ᴜsᴇ ᴀ ᴜsᴇʀʙᴏᴛ
"""

  ABOUT_TXT = """<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐃𝐞𝐭𝐚𝐢𝐥𝐬 ❱──────╮
┃ 
┣⊸ 🤖 Mʏ Nᴀᴍᴇ   : <a href=https://t.me/MeJeetX>Aryᴀ Bᴏᴛ</a>
┣⊸ 👨‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ : <a href=https://t.me/MeJeetX>MeJeetX</a>
┣⊸ 📢 ᴄʜᴀɴɴᴇʟ   : <a href=https://t.me/MeJeetX>Updates</a>
┣⊸ 💬 sᴜᴘᴘᴏʀᴛ   : <a href=https://t.me/+1p2hcQ4ZaupjNjI1>Support Group</a>
┃ 
┣⊸ 🗣️ ʟᴀɴɢᴜᴀɢᴇ  : ᴘʏᴛʜᴏɴ 3 
┃  {python_version}
┣⊸ 📚 ʟɪʙʀᴀʀʏ   : ᴘʏʀᴏɢʀᴀᴍ  
┃
╰─────────────────────────────╯</b>"""

  STATUS_TXT = """<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐒𝐭𝐚𝐭𝐮𝐬 ❱──────╮
┃
┣⊸ 👨 ᴜsᴇʀs   : <code>{}</code>
┣⊸ 🤖 ʙᴏᴛs    : <code>{}</code>
┣⊸ 📡 ғᴏʀᴡᴀʀᴅ : <code>{}</code>
┣⊸ 📣 ᴄʜᴀɴɴᴇʟ : <code>{}</code>
┣⊸ 🚫 ʙᴀɴɴᴇᴅ  : <code>{}</code>
┃
╰─────────────────────────────╯</b>"""

  FROM_MSG = "<b>❪ SET SOURCE CHAT ❫\n\nForward the last message or link.\nType username/ID (e.g. <code>@somebot</code> or <code>123456</code>) for bot/private chat.\nType <code>me</code> for Saved Messages.\n/cancel - to cancel</b>"
  TO_MSG = "<b>❪ CHOOSE TARGET CHAT ❫\n\nChoose your target chat from the given buttons.\n/cancel - Cancel this process</b>"
  SAVED_MSG_MODE = "<b>❪ SELECT MODE ❫\n\nChoose forwarding mode:\n1. <code>batch</code> - Forward existing messages.\n2. <code>live</code> - Continuous (wait for new messages).</b>"
  SAVED_MSG_LIMIT = "<b>❪ NUMBER OF MESSAGES ❫\n\nHow many messages to forward?\nEnter a number or <code>all</code>.</b>"
  SKIP_MSG = "<b>❪ SET MESSAGE SKIPING NUMBER ❫</b>\n\n<b>Skip the message as much as you enter the number and the rest of the message will be forwarded\nDefault Skip Number =</b> <code>0</code>\n<code>eg: You enter 0 = 0 message skiped\n You enter 5 = 5 message skiped</code>\n/cancel <b>- cancel this process</b>"
  CANCEL = "<b>Process Cancelled Succefully !</b>"
  BOT_DETAILS = "<b><u>📄 BOT DETAILS</b></u>\n\n<b>➣ NAME:</b> <code>{}</code>\n<b>➣ BOT ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}"
  USER_DETAILS = "<b><u>📄 USERBOT DETAILS</b></u>\n\n<b>➣ NAME:</b> <code>{}</code>\n<b>➣ USER ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}"

  TEXT = """<b>╭──────❰ ✦ 𝐀𝐮𝐭𝐨 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐫 ✦ ❱──────╮
┃
┣⊸ ◈ 𝐅𝐞𝐭𝐜𝐡𝐞𝐝     : <code>{}</code>
┣⊸ ◈ 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐝   : <code>{}</code>
┣⊸ ◈ 𝐃𝐮𝐩𝐥𝐢𝐜𝐚𝐭𝐞   : <code>{}</code>
┣⊸ ◈ 𝐒𝐤𝐢𝐩𝐩𝐞𝐝     : <code>{}</code>
┣⊸ ◈ 𝐃𝐞𝐥𝐞𝐭𝐞𝐝     : <code>{}</code>
┃
┣⊸ ◈ 𝐒𝐭𝐚𝐭𝐮𝐬      : <code>{}</code>
┣⊸ ◈ 𝐄𝐓𝐀         : <code>{}</code>
┃
╰────────────────────────────────╯</b>"""

  TEXT1 = TEXT

  DUPLICATE_TEXT = """<b>╭──────❰ ✦ 𝐔𝐧𝐞𝐪𝐮𝐢𝐟𝐲 𝐒𝐭𝐚𝐭𝐮𝐬 ✦ ❱──────╮
┃
┣⊸ ◈ 𝐅𝐞𝐭𝐜𝐡𝐞𝐝     : <code>{}</code>
┣⊸ ◈ 𝐃𝐮𝐩𝐥𝐢𝐜𝐚𝐭𝐞𝐬  : <code>{}</code>
┃
╰───────────────── {} ────╯</b>"""
