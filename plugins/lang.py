"""
Language Selection Plugin
=========================
Allows users to pick English / Hindi / Hinglish as their preferred language.
All key bot responses will be returned in the selected language.

Usage:
  /lang  — open language picker (also accessible from Settings)
  from .lang import t   — use t(user_id, key) in any plugin for translated text

Supported languages:
  en        — English  (default)
  hi        — Hindi (Devanagari)
  hinglish  — Hinglish (Hindi written in English)
"""
from database import db
from config import Config
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ══════════════════════════════════════════════════════════════════════════════
# Translation strings – ALL multi-line entries use triple-quoted strings
# ══════════════════════════════════════════════════════════════════════════════

_S = {}   # populated below; we use a plain dict for clarity

# ── START_TXT ──────────────────────────────────────────────────────────────
_S["START_TXT"] = {
    "en": (
        "<b>╭──────❰ ✦ 𝐀𝐮𝐭𝐨 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐫 ✦ ❱──────╮\n"
        "┃\n"
        "┣⊸ 𝐇𝐞𝐥𝐥𝐨 {}\n"
        "┃\n"
        "┣⊸ 🤖 Aryᴀ Bᴏᴛ [ ᴩᴏwᴇʀғᴜʟ Fᴏʀᴡᴀʀᴅ Tᴏᴏʟ ]\n"
        "┃\n"
        "┣⊸ <i>ɪ ᴄᴀɴ ғᴏʀᴡᴀʀᴅ ᴀʟʟ ᴍᴇssᴀɢᴇs ғʀᴏᴍ ᴏɴᴇ\n"
        "┃  ᴄʜᴀɴɴᴇʟ ᴛᴏ ᴀɴᴏᴛʜᴇʀ ᴄʜᴀɴɴᴇʟ ᴡɪᴛʜ\n"
        "┃  ᴍᴏʀᴇ ғᴇᴀᴛᴜʀᴇs.</i>\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    ),
    "hi": (
        "<b>╭──────❰ ✦ 𝐀𝐮𝐭𝐨 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐫 ✦ ❱──────╮\n"
        "┃\n"
        "┣⊸ 𝐇𝐞𝐥𝐥𝐨 {}\n"
        "┃\n"
        "┣⊸ 🤖 Aryᴀ Bᴏᴛ [ ᴩᴏwᴇʀғᴜʟ Fᴏʀᴡᴀʀᴅ Tᴏᴏʟ ]\n"
        "┃\n"
        "┣⊸ <i>मैं एक चैनल से दूसरे चैनल में सभी संदेश\n"
        "┃  फॉरवर्ड कर सकता हूँ - कई एडवांस\n"
        "┃  फीचर्स के साथ।</i>\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    ),
    "hinglish": (
        "<b>╭──────❰ ✦ 𝐀𝐮𝐭𝐨 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐞𝐫 ✦ ❱──────╮\n"
        "┃\n"
        "┣⊸ 𝐇𝐞𝐥𝐥𝐨 {}\n"
        "┃\n"
        "┣⊸ 🤖 Aryᴀ Bᴏᴛ [ ᴩᴏwᴇʀғᴜʟ Fᴏʀᴡᴀʀᴅ Tᴏᴏʟ ]\n"
        "┃\n"
        "┣⊸ <i>Main ek channel se doosre channel mein\n"
        "┃  sab messages forward kar sakta hoon -\n"
        "┃  bahut saare features ke saath.</i>\n"
        "┃\n"
        "╰────────────────────────────────╯</b>"
    ),
}

# ── HELP_TXT ───────────────────────────────────────────────────────────────
_S["HELP_TXT"] = {
    "en": (
        "<b><u>🔆 HELP — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>📌 Commands:</b>\n"
        "<code>/start</code>  — Check if I'm alive\n"
        "<code>/forward</code>  — Start batch forwarding\n"
        "<code>/jobs</code>  — Manage Live Jobs (background forwarding)\n"
        "<code>/cleanmsg</code>  — Bulk delete messages from chats\n"
        "<code>/settings</code>  — Configure all settings\n"
        "<code>/reset</code>  — Reset settings to default\n\n"
        "<b>⚡ Features:</b>\n"
        "<b>►</b> Forward from public channels — no admin needed\n"
        "<b>►</b> Forward from private channels — via bot/userbot admin\n"
        "<b>►</b> Multi-Account: up to 2 Bots + 2 Userbots\n"
        "<b>►</b> Live Jobs — background tasks, run parallel to batch forwards\n"
        "<b>►</b> New→Old &amp; Old→New forwarding order\n"
        "<b>►</b> Filters — skip audio/video/photo/text/sticker/poll etc.\n"
        "<b>►</b> Custom caption / remove caption / add buttons\n"
        "<b>►</b> Skip duplicate messages\n"
        "<b>►</b> Extension / Keyword / Size filters\n"
        "<b>►</b> Download mode — bypasses forward restrictions\n"
        "<b>►</b> Clean MSG — bulk delete from target channels"
    ),
    "hi": (
        "<b><u>🔆 संसथान (HELP) — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>📌 Commands:</b>\n"
        "<code>/start</code>  — मैं चालू हूँ या नहीं चेक करें\n"
        "<code>/forward</code>  — फॉरवर्डिंग शुरू करें\n"
        "<code>/jobs</code>  — लाइव जॉब मैनेज करें (बैकग्राउंड फॉरवर्डिंग)\n"
        "<code>/cleanmsg</code>  — एक साथ अनेक संदेश डिलीट करें\n"
        "<code>/settings</code>  — सेटिंग्स बदलिए\n"
        "<code>/reset</code>  — सेटिंग्स डिफ़ॉल्ट करें\n\n"
        "<b>⚡ Features:</b>\n"
        "<b>►</b> पब्लिक चैनल्स से फॉरवर्ड — एडमिन होने की जरूरत नहीं\n"
        "<b>►</b> प्राइवेट चैनल्स से फॉरवर्ड — बोट/यूज़रबोट एडमिन द्वारा\n"
        "<b>►</b> मल्टी-अकाउंट: 2 बोट्स + 2 यूज़रबोट्स\n"
        "<b>►</b> लाइव जॉब्स — बैकग्राउंड में चलने वाले काम\n"
        "<b>►</b> नया→पुराना और पुराना→नया दोनों तरीके\n"
        "<b>►</b> फिल्टर्स — ऑडियो/टेक्स्ट/वीडियो/फ़ोटो इत्यादि इग्नोर करें\n"
        "<b>►</b> कस्टम कैप्शन या कैप्शन हटायें\n"
        "<b>►</b> डुप्लीकेट संदेश इग्नोर करें\n"
        "<b>►</b> एक्सटेंशन / कीवर्ड / साइज फिल्टर्स\n"
        "<b>►</b> डाउनलोड मोड — डाउनलोड कर अपलोड करें\n"
        "<b>►</b> Clean MSG — चैनल्स से बल्क में संदेश डिलीट करें"
    ),
    "hinglish": (
        "<b><u>🔆 HELP — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>📌 Commands:</b>\n"
        "<code>/start</code>  — Check karo main zinda hu ya nahi\n"
        "<code>/forward</code>  — Forwarding chalu karo\n"
        "<code>/jobs</code>  — Live Jobs manage karo\n"
        "<code>/cleanmsg</code>  — Chats se ek saath messages udaao\n"
        "<code>/settings</code>  — Saari settings yahan milegi\n"
        "<code>/reset</code>  — Settings default pe kar do\n\n"
        "<b>⚡ Features:</b>\n"
        "<b>►</b> Public channel se forward — no admin required\n"
        "<b>►</b> Private channel se forward — bot/userbot admin zaruri h\n"
        "<b>►</b> Multi-Account: 2 Bots + 2 Userbots add kar sakte ho\n"
        "<b>►</b> Live Jobs — peeche background me chalte rahenge\n"
        "<b>►</b> New→Old aur Old→New dono support\n"
        "<b>►</b> Filters — audio/video/photo/sticker/text skip karo\n"
        "<b>►</b> Custom caption / caption hata do / buttons lagao\n"
        "<b>►</b> Duplicate posts skip ho jayenge automatically\n"
        "<b>►</b> Extension / Keyword / Size filters\n"
        "<b>►</b> Download mode — block bypass karke upload karega\n"
        "<b>►</b> Clean MSG — Target channel se bulk m messages delete karo"
    ),
}

# ── HOW_USE_TXT ────────────────────────────────────────────────────────────
_S["HOW_USE_TXT"] = {
    "en": (
        "<b><u>📍 How to Use — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>1️⃣ Add an Account</b>\n"
        "  ‣ Go to /settings → ⚙️ Accounts\n"
        "  ‣ Add a Bot (send its token) or a Userbot (send session string)\n"
        "  ‣ You can add up to 2 Bots + 2 Userbots\n\n"
        "<b>2️⃣ Add a Target Channel</b>\n"
        "  ‣ Go to /settings → 📣 Channels\n"
        "  ‣ Your Bot/Userbot must be <b>admin</b> in the target\n\n"
        "<b>3️⃣ Configure Settings</b>\n"
        "  ‣ <b>Filters</b> — choose what types of messages to skip\n"
        "  ‣ <b>Caption</b> — custom caption or remove it\n"
        "  ‣ <b>Forward Tag</b> — show or hide forwarded-from label\n"
        "  ‣ <b>Download Mode</b> — re-upload files (bypasses restrictions)\n"
        "  ‣ <b>Duplicate Skip</b> — avoid re-forwarding same content\n\n"
        "<b>4️⃣ Batch Forward (/forward)</b>\n"
        "  ‣ Choose account → select target → send source link/ID\n"
        "  ‣ Choose order (Old→New / New→Old) → set skip count\n"
        "  ‣ Verify DOUBLE CHECK → click Yes\n\n"
        "<b>5️⃣ Live Jobs (/jobs)</b>\n"
        "  ‣ Creates a <b>background job</b> that auto-forwards new messages\n"
        "  ‣ Works alongside batch forwarding simultaneously\n"
        "  ‣ Supports channels, groups, bot private chats, saved messages\n"
        "  ‣ Respects your Filters settings\n"
        "  ‣ Stop/Start/Delete any job anytime from /jobs\n\n"
        "<b>6️⃣ Clean MSG (/cleanmsg)</b>\n"
        "  ‣ Select account + target chat(s) + message type\n"
        "  ‣ Bulk deletes messages in one go\n\n"
        "<b>⚠️ Notes:</b>\n"
        "  ‣ Bot account: needs admin in TARGET (and SOURCE if private)\n"
        "  ‣ Userbot: needs membership in SOURCE + admin in TARGET\n"
        "  ‣ For public channels, a normal Bot works fine\n"
        "  ‣ For private/restricted sources, use a Userbot"
    ),
    "hi": (
        "<b><u>📍 इस्तमाल कैसे करें — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>1️⃣ अकाउंट जोड़ें</b>\n"
        "  ‣ /settings पर जाएं → ⚙️ Accounts\n"
        "  ‣ बोट (टोकन) या यूज़रबोट (सेशन स्ट्रिंग) जोड़े\n\n"
        "<b>2️⃣ टारगेट चैनल जोड़ें</b>\n"
        "  ‣ /settings पर जाएं → 📣 Channels\n"
        "  ‣ आपका अकाउंट टारगेट चैनल में एडमिन होना चाहिए\n\n"
        "<b>3️⃣ सेटिंग्स कॉन्फ़िगर करें</b>\n"
        "  ‣ <b>फ़िल्टर्स</b> — अनावश्यक संदेश हटाएँ\n"
        "  ‣ <b>कैप्शन</b> — खुद की कैप्शन डालें या हटाएँ\n"
        "  ‣ <b>फॉरवर्ड टैग</b> — असली चैनल का नाम छुपाएँ\n"
        "  ‣ <b>डाउनलोड मोड</b> — रिस्ट्रिक्टेड मीडिया को डाउनलोड/अपलोड करें\n\n"
        "<b>4️⃣ फॉरवर्ड शुरू करें (/forward)</b>\n"
        "  ‣ अकाउंट चुनें → स्रोत लिंक भेजें → आगे बढ़ें\n\n"
        "<b>5️⃣ लाइव जॉब्स (/jobs)</b>\n"
        "  ‣ बैकग्राउंड में लगातार चलने वाले टास्क बनाएँ\n\n"
        "<b>⚠️ याद रखें:</b>\n"
        "  ‣ प्राइवेट सोर्स के लिए आपको यूज़रबोट चाहिए या बोट एडमिन हो"
    ),
    "hinglish": (
        "<b><u>📍 Istemal Kaise Karein — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>1️⃣ Account Add Karo</b>\n"
        "  ‣ /settings mein jao → ⚙️ Accounts\n"
        "  ‣ Bot ka token do ya Userbot ki session string lagao\n\n"
        "<b>2️⃣ Target Channel Add Karo</b>\n"
        "  ‣ /settings pe → 📣 Channels\n"
        "  ‣ Tumhara account wahan admin hona zaruri hai\n\n"
        "<b>3️⃣ Settings Theek Karo</b>\n"
        "  ‣ <b>Filters</b> — kya kya skip karna h wo tick karo\n"
        "  ‣ <b>Caption</b> — khud ka caption do ya uda do\n"
        "  ‣ <b>Forward Tag</b> — Asli channel ka nam chupao\n"
        "  ‣ <b>Download Mode</b> — restricted source file nikal lega\n\n"
        "<b>4️⃣ Forward Chalu Karo (/forward)</b>\n"
        "  ‣ Account chuno → source link send karo aur OK karo\n\n"
        "<b>5️⃣ Live Jobs (/jobs)</b>\n"
        "  ‣ Background me forward lagao, chalta rahega rozana\n\n"
        "<b>⚠️ Yaad Rakhein:</b>\n"
        "  ‣ Private channels m userbot bhetar kaam karta hai"
    ),
}

# ── ABOUT_TXT ──────────────────────────────────────────────────────────────
_S["ABOUT_TXT"] = {
    "en": (
        "<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐃𝐞𝐭𝐚𝐢𝐥𝐬 ❱──────╮\n"
        "┃ \n"
        "┣⊸ 🤖 Mʏ Nᴀᴍᴇ   : <a href='https://t.me/MeJeetX'>Aryᴀ Bᴏᴛ</a>\n"
        "┣⊸ 👨‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ : <a href='https://t.me/MeJeetX'>MeJeetX</a>\n"
        "┣⊸ 📢 ᴄʜᴀɴɴᴇʟ   : <a href='https://t.me/MeJeetX'>Updates</a>\n"
        "┣⊸ 💬 sᴜᴘᴘᴏʀᴛ   : <a href='https://t.me/+1p2hcQ4ZaupjNjI1'>Support Group</a>\n"
        "┃ \n"
        "┣⊸ 🗣️ ʟᴀɴɢᴜᴀɢᴇ  : ᴘʏᴛʜᴏɴ 3 \n"
        "┃  {python_version}\n"
        "┣⊸ 📚 ʟɪʙʀᴀʀʏ   : ᴘʏʀᴏɢʀᴀᴍ  \n"
        "┃\n"
        "╰─────────────────────────────╯</b>"
    ),
    "hi": (
        "<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐃𝐞𝐭𝐚𝐢𝐥𝐬 ❱──────╮\n"
        "┃ \n"
        "┣⊸ 🤖 मेरा नाम   : <a href='https://t.me/MeJeetX'>Aryᴀ Bᴏᴛ</a>\n"
        "┣⊸ 👨‍💻 डेवलपर   : <a href='https://t.me/MeJeetX'>MeJeetX</a>\n"
        "┣⊸ 📢 चैनल      : <a href='https://t.me/MeJeetX'>Updates</a>\n"
        "┣⊸ 💬 सपोर्ट     : <a href='https://t.me/+1p2hcQ4ZaupjNjI1'>Support Group</a>\n"
        "┃ \n"
        "┣⊸ 🗣️ भाषा      : ᴘʏᴛʜᴏɴ 3 \n"
        "┃  {python_version}\n"
        "┣⊸ 📚 लाइब्रेरी   : ᴘʏʀᴏɢʀᴀᴍ  \n"
        "┃\n"
        "╰─────────────────────────────╯</b>"
    ),
    "hinglish": (
        "<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐃𝐞𝐭𝐚𝐢𝐥𝐬 ❱──────╮\n"
        "┃ \n"
        "┣⊸ 🤖 Mera Naam : <a href='https://t.me/MeJeetX'>Aryᴀ Bᴏᴛ</a>\n"
        "┣⊸ 👨‍💻 Developer : <a href='https://t.me/MeJeetX'>MeJeetX</a>\n"
        "┣⊸ 📢 Channel   : <a href='https://t.me/MeJeetX'>Updates</a>\n"
        "┣⊸ 💬 Support   : <a href='https://t.me/+1p2hcQ4ZaupjNjI1'>Support Group</a>\n"
        "┃ \n"
        "┣⊸ 🗣️ Language  : ᴘʏᴛʜᴏɴ 3 \n"
        "┃  {python_version}\n"
        "┣⊸ 📚 Library   : ᴘʏʀᴏɢʀᴀᴍ  \n"
        "┃\n"
        "╰─────────────────────────────╯</b>"
    ),
}

# ── STATUS_TXT ─────────────────────────────────────────────────────────────
_S["STATUS_TXT"] = {
    "en": (
        "<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐒𝐭𝐚𝐭𝐮𝐬 ❱──────╮\n"
        "┃\n"
        "┣⊸ 👨 ᴜsᴇʀs   : <code>{}</code>\n"
        "┣⊸ 🤖 ʙᴏᴛs    : <code>{}</code>\n"
        "┣⊸ 📡 ғᴏʀᴡᴀʀᴅ : <code>{}</code>\n"
        "┣⊸ 📣 ᴄʜᴀɴɴᴇʟ : <code>{}</code>\n"
        "┣⊸ 🚫 ʙᴀɴɴᴇᴅ  : <code>{}</code>\n"
        "┃\n"
        "╰─────────────────────────────╯</b>"
    ),
    "hi": (
        "<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐒𝐭𝐚𝐭𝐮𝐬 ❱──────╮\n"
        "┃\n"
        "┣⊸ 👨 यूज़र्स  : <code>{}</code>\n"
        "┣⊸ 🤖 बोट्स   : <code>{}</code>\n"
        "┣⊸ 📡 फॉरवर्ड  : <code>{}</code>\n"
        "┣⊸ 📣 चैनल्स  : <code>{}</code>\n"
        "┣⊸ 🚫 बैन     : <code>{}</code>\n"
        "┃\n"
        "╰─────────────────────────────╯</b>"
    ),
    "hinglish": (
        "<b>╭──────❰ 🤖 𝐁𝐨𝐭 𝐒𝐭𝐚𝐭𝐮𝐬 ❱──────╮\n"
        "┃\n"
        "┣⊸ 👨 Users   : <code>{}</code>\n"
        "┣⊸ 🤖 Bots    : <code>{}</code>\n"
        "┣⊸ 📡 Forward : <code>{}</code>\n"
        "┣⊸ 📣 Channel : <code>{}</code>\n"
        "┣⊸ 🚫 Banned  : <code>{}</code>\n"
        "┃\n"
        "╰─────────────────────────────╯</b>"
    ),
}

# ── FROM_MSG ───────────────────────────────────────────────────────────────
_S["FROM_MSG"] = {
    "en": (
        "<b>❪ SET SOURCE CHAT ❫\n\n"
        "Forward the last message or link.\n"
        "Type username/ID (e.g. <code>@somebot</code> or <code>123456</code>) for bot/private chat.\n"
        "Type <code>me</code> for Saved Messages.\n"
        "/cancel - to cancel</b>"
    ),
    "hi": (
        "<b>❪ स्रोत चैट सेट करें ❫\n\n"
        "अंतिम संदेश या लिंक फॉरवर्ड करें।\n"
        "बोट/प्राइवेट चैट के लिए यूज़रनेम/ID टाइप करें।\n"
        "सेव्ड मैसेज के लिए <code>me</code> टाइप करें।\n"
        "रद्द करने के लिए /cancel</b>"
    ),
    "hinglish": (
        "<b>❪ SOURCE CHAT BATAO ❫\n\n"
        "Last message ya link forward karo.\n"
        "Bot/private chat ke liye username ya ID bhejo.\n"
        "Saved messages ke liye <code>me</code> likho.\n"
        "Cancel karne ke liye /cancel</b>"
    ),
}

# ── TO_MSG ─────────────────────────────────────────────────────────────────
_S["TO_MSG"] = {
    "en": "<b>❪ CHOOSE TARGET CHAT ❫\n\nChoose your target chat from the given buttons.\n/cancel - Cancel this process</b>",
    "hi": "<b>❪ टारगेट चैट चुनें ❫\n\nनीचे दिए गए बटन से अपनी टारगेट चैट चुनें।\n/cancel - इस प्रक्रिया को रद्द करें</b>",
    "hinglish": "<b>❪ TARGET CHAT CHUNO ❫\n\nNeeche diye gaye buttons se target chat select karo.\n/cancel - is process ko cancel karo</b>",
}

# ── SAVED_MSG_MODE ─────────────────────────────────────────────────────────
_S["SAVED_MSG_MODE"] = {
    "en": "<b>❪ SELECT MODE ❫\n\nChoose forwarding mode:\n1. <code>batch</code> - Forward existing messages.\n2. <code>live</code> - Continuous (wait for new messages).</b>",
    "hi": "<b>❪ मोड चुनें ❫\n\nफॉरवर्डिंग मोड चुनें:\n1. <code>batch</code> - मौजूदा संदेश फॉरवर्ड करें।\n2. <code>live</code> - लाइव (नए संदेशों का इंतजार करें)।</b>",
    "hinglish": "<b>❪ MODE SELECT KARO ❫\n\nForwarding mode chuno:\n1. <code>batch</code> - Purane messages forward karo.\n2. <code>live</code> - Naye messages ka wait karega.</b>",
}

# ── SAVED_MSG_LIMIT ────────────────────────────────────────────────────────
_S["SAVED_MSG_LIMIT"] = {
    "en": "<b>❪ NUMBER OF MESSAGES ❫\n\nHow many messages to forward?\nEnter a number or <code>all</code>.</b>",
    "hi": "<b>❪ संदेशों की संख्या ❫\n\nकितने संदेश फॉरवर्ड करने हैं?\nकोई संख्या डालें या <code>all</code> लिखें।</b>",
    "hinglish": "<b>❪ KITNE MESSAGES ❫\n\nKitne messages forward karne hain?\nNumber likho ya <code>all</code> bhejo.</b>",
}

# ── SKIP_MSG ───────────────────────────────────────────────────────────────
_S["SKIP_MSG"] = {
    "en": (
        "<b>❪ SET MESSAGE SKIPING NUMBER ❫</b>\n\n"
        "<b>Skip the message as much as you enter the number and the rest of the message will be forwarded\n"
        "Default Skip Number =</b> <code>0</code>\n"
        "<code>eg: You enter 0 = 0 message skiped\n"
        " You enter 5 = 5 message skiped</code>\n"
        "/cancel <b>- cancel this process</b>"
    ),
    "hi": (
        "<b>❪ संदेश छोड़ें ❫</b>\n\n"
        "<b>जितनी संख्या डालेंगे उतने संदेश छोड़कर बाकी फॉरवर्ड होंगे।\n"
        "डिफ़ॉल्ट =</b> <code>0</code>\n"
        "<code>उदा: 0 = 0 छोड़े गए\n"
        " 5 = 5 छोड़े गए</code>\n"
        "रद्द करने के लिए /cancel"
    ),
    "hinglish": (
        "<b>❪ SKIP MESSAGES ❫</b>\n\n"
        "<b>Jitna number bataoge utne shuru ke messages chutt jayenge\n"
        "Default skip =</b> <code>0</code>\n"
        "<code>eg: 0 likhne par = 0 skip honge\n"
        " 5 likhne par = 5 skip honge</code>\n"
        "Cancel ke liye /cancel"
    ),
}

# ── CANCEL ─────────────────────────────────────────────────────────────────
_S["CANCEL"] = {
    "en": "<b>Process Cancelled Succefully !</b>",
    "hi": "<b>प्रक्रिया सफलतापूर्वक रद्द की गई!</b>",
    "hinglish": "<b>Process Cancel ho gaya!</b>",
}

# ── BOT_DETAILS ────────────────────────────────────────────────────────────
_S["BOT_DETAILS"] = {
    "en": "<b><u>📄 BOT DETAILS</u></b>\n\n<b>➣ NAME:</b> <code>{}</code>\n<b>➣ BOT ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
    "hi": "<b><u>📄 बोट विवरण</u></b>\n\n<b>➣ नाम:</b> <code>{}</code>\n<b>➣ बोट ID:</b> <code>{}</code>\n<b>➣ यूज़रनेम:</b> @{}",
    "hinglish": "<b><u>📄 BOT DETAILS</u></b>\n\n<b>➣ NAAM:</b> <code>{}</code>\n<b>➣ BOT ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
}

# ── USER_DETAILS ───────────────────────────────────────────────────────────
_S["USER_DETAILS"] = {
    "en": "<b><u>📄 USERBOT DETAILS</u></b>\n\n<b>➣ NAME:</b> <code>{}</code>\n<b>➣ USER ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
    "hi": "<b><u>📄 यूज़रबोट विवरण</u></b>\n\n<b>➣ नाम:</b> <code>{}</code>\n<b>➣ यूज़र ID:</b> <code>{}</code>\n<b>➣ यूज़रनेम:</b> @{}",
    "hinglish": "<b><u>📄 USERBOT DETAILS</u></b>\n\n<b>➣ NAAM:</b> <code>{}</code>\n<b>➣ USER ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
}

# ── TEXT (forwarding status box) ───────────────────────────────────────────
_S["TEXT"] = {
    "en": (
        "<b>Forwarding Progress</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hi": (
        "<b>फॉरवर्डिंग प्रोग्रेस</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hinglish": (
        "<b>Forwarding Progress</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
}

# ── DUPLICATE_TEXT ─────────────────────────────────────────────────────────
_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> {}"
    ),
    "hi": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> {}"
    ),
    "hinglish": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> {}"
    ),
}

# ── Simple one-liners ──────────────────────────────────────────────────────
_S["cancelled"] = {
    "en": "✅ Process cancelled.",
    "hi": "✅ प्रक्रिया रद्द की गई।",
    "hinglish": "✅ Process cancel ho gaya.",
}
_S["btn_settings"] = {"en": "⚙️ Settings", "hi": "⚙️ सेटिंग्स", "hinglish": "⚙️ Settings"}
_S["btn_jobs"] = {"en": "📋 Live Jobs", "hi": "📋 लाइव जॉब्स", "hinglish": "📋 Live Jobs"}
_S["btn_help"] = {"en": "🙋‍♂️ Help", "hi": "🙋‍♂️ संसथान", "hinglish": "🙋‍♂️ Help"}
_S["btn_about"] = {"en": "💁‍♂️ About", "hi": "💁‍♂️ बारे में", "hinglish": "💁‍♂️ About"}
_S["btn_close"] = {"en": "❌ Close", "hi": "❌ बंद करें", "hinglish": "❌ Close"}
_S["settings_title"] = {
    "en": "⚙️ Change your settings as you wish:",
    "hi": "⚙️ अपनी सेटिंग्स बदलें:",
    "hinglish": "⚙️ Apni settings apne hisaab se badlo:",
}
_S["select_lang"] = {
    "en": "🌐 Select your preferred language:",
    "hi": "🌐 अपनी भाषा चुनें:",
    "hinglish": "🌐 Apni language select karo:",
}
_S["lang_set"] = {
    "en": "✅ Language set to <b>English</b>.",
    "hi": "✅ भाषा <b>हिंदी</b> में सेट की गई।",
    "hinglish": "✅ Language <b>Hinglish</b> mein set ho gayi!",
}
_S["no_bot"] = {
    "en": "<code>You didn't add any bot. Please add a bot using /settings !</code>",
    "hi": "<code>आपने कोई बोट नहीं जोड़ा। /settings से बोट जोड़ें!</code>",
    "hinglish": "<code>Koi bot add nahi kiya. /settings se bot add karo!</code>",
}
_S["no_channel"] = {
    "en": "Please set a target channel in /settings before forwarding.",
    "hi": "फॉरवर्ड करने से पहले /settings में टारगेट चैनल सेट करें।",
    "hinglish": "Forward karne se pehle /settings mein target channel set karo.",
}
_S["choose_account"] = {
    "en": "<b>Choose Account for Forwarding:</b>",
    "hi": "<b>फॉरवर्डिंग के लिए अकाउंट चुनें:</b>",
    "hinglish": "<b>Forwarding ke liye account chuno:</b>",
}
_S["choose_order"] = {
    "en": "<b>Choose Forwarding Order:</b>",
    "hi": "<b>फॉरवर्डिंग का क्रम चुनें:</b>",
    "hinglish": "<b>Forwarding order chuno:</b>",
}
_S["order_old_new"] = {"en": "Old to New", "hi": "पुराना से नया", "hinglish": "Old to New"}
_S["order_new_old"] = {"en": "New to Old", "hi": "नया से पुराना", "hinglish": "New to Old"}

# ══════════════════════════════════════════════════════════════════════════════
# Core helpers
# ══════════════════════════════════════════════════════════════════════════════

def _tx(lang: str, key: str, *args, **kwargs) -> str:
    """Return translated string for given lang+key. Falls back to English."""
    lang_map = _S.get(key, {})
    text = lang_map.get(lang) or lang_map.get("en", f"[{key}]")
    if args or kwargs:
        try:
            text = text.format(*args, **kwargs)
        except (KeyError, IndexError):
            pass
    return text


async def t(user_id: int, key: str, *args, **kwargs) -> str:
    """Async helper: fetch user's language from DB then return translated string."""
    lang = await db.get_language(user_id)
    return _tx(lang, key, *args, **kwargs)


def t_sync(lang: str, key: str, *args, **kwargs) -> str:
    """Sync helper when you already know the lang string."""
    return _tx(lang, key, *args, **kwargs)


# ══════════════════════════════════════════════════════════════════════════════
# /lang command + callbacks
# ══════════════════════════════════════════════════════════════════════════════

def _lang_keyboard(current_lang: str) -> InlineKeyboardMarkup:
    def mark(code): return "✅ " if current_lang == code else ""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{mark('en')}🇺🇸 English",       callback_data="setlang#en"),
            InlineKeyboardButton(f"{mark('hi')}🇮🇳 हिंदी",         callback_data="setlang#hi"),
        ],
        [
            InlineKeyboardButton(f"{mark('hinglish')}🌐 Hinglish", callback_data="setlang#hinglish"),
        ],
        [
            InlineKeyboardButton("↩ Back to Settings",             callback_data="settings#main"),
        ]
    ])


@Client.on_message(filters.private & filters.command("lang"))
async def lang_cmd(bot, message):
    user_id = message.from_user.id
    current = await db.get_language(user_id)
    await message.reply_text(
        _tx(current, "select_lang"),
        reply_markup=_lang_keyboard(current)
    )


@Client.on_callback_query(filters.regex(r'^settings#lang$'))
async def lang_settings_cb(bot, query):
    user_id = query.from_user.id
    current = await db.get_language(user_id)
    await query.message.edit_text(
        _tx(current, "select_lang"),
        reply_markup=_lang_keyboard(current)
    )


@Client.on_callback_query(filters.regex(r'^setlang#'))
async def setlang_cb(bot, query):
    user_id = query.from_user.id
    lang    = query.data.split("#", 1)[1]
    if lang not in ("en", "hi", "hinglish"):
        return await query.answer("Invalid language!", show_alert=True)

    await db.set_language(user_id, lang)
    label_map = {"en": "English 🇺🇸", "hi": "हिंदी 🇮🇳", "hinglish": "Hinglish 🌐"}
    await query.answer(f"Language set to {label_map[lang]}!", show_alert=False)
    await query.message.edit_text(
        _tx(lang, "lang_set"),
        reply_markup=_lang_keyboard(lang)
    )
