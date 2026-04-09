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

#  START_TXT 
_S["START_TXT"] = {
    "en": (
        "<i>Hello </i><a href='tg://user?id={}'>{}</a>\n"
        "\n"
        "<i>I am </i>ᴀʀʏᴀ ʙᴏᴛ<i>, a multi-functional bot that </i>ᴄᴀɴ ғᴏʀᴡᴀʀᴅ<i> messages across channels and groups. I fully support normal forwarding, multi-job batches, live syncing, the media merger system (video & audio), batch link generation, and bulk message cleaning.</i>\n"
    ),
    "hi": (
        "<i>Hello </i><a href='tg://user?id={}'>{}</a>\n"
        "\n"
        "<i>I am </i>ᴀʀʏᴀ ʙᴏᴛ<i>, a multi-functional bot that </i>ᴄᴀɴ ғᴏʀᴡᴀʀᴅ<i> messages across channels and groups. I fully support normal forwarding, multi-job batches, live syncing, the media merger system (video & audio), batch link generation, and bulk message cleaning.</i>\n"
    ),
    "hinglish": (
        "<i>Hello </i><a href='tg://user?id={}'>{}</a>\n"
        "\n"
        "<i>I am </i>ᴀʀʏᴀ ʙᴏᴛ<i>, a multi-functional bot that </i>ᴄᴀɴ ғᴏʀᴡᴀʀᴅ<i> messages across channels and groups. I fully support normal forwarding, multi-job batches, live syncing, the media merger system (video & audio), batch link generation, and bulk message cleaning.</i>\n"
    ),
}

#  HELP_TXT 
_S["HELP_TXT"] = {
    "en": (
        "<b><u>»  ʜᴇʟᴘ — ᴀʀʏᴀ ʙᴏᴛ</u></b>\n\n"
        "<b>»  ᴄᴏᴍᴍᴀɴᴅꜱ:</b>\n"
        "<code>/ꜱᴛᴀʀᴛ</code>  — ᴄʜᴇᴄᴋ ɪꜰ ɪ'ᴍ ᴀʟɪᴠᴇ\n"
        "<code>/ꜰᴏʀᴡᴀʀᴅ</code>  — ꜱᴛᴀʀᴛ ʙᴀᴛᴄʜ ꜰᴏʀᴡᴀʀᴅɪɴɢ\n"
        "<code>/ᴊᴏʙꜱ</code>  — ᴍᴀɴᴀɢᴇ ʟɪᴠᴇ ᴊᴏʙꜱ (ʙᴀᴄᴋɢʀᴏᴜɴᴅ ꜰᴏʀᴡᴀʀᴅɪɴɢ)\n"
        "<code>/ᴄʟᴇᴀɴᴍꜱɢ</code>  — ʙᴜʟᴋ ᴅᴇʟᴇᴛᴇ ᴍᴇꜱꜱᴀɢᴇꜱ ꜰʀᴏᴍ ᴄʜᴀᴛꜱ\n"
        "<code>/ꜱᴇᴛᴛɪɴɢꜱ</code>  — ᴄᴏɴꜰɪɢᴜʀᴇ ᴀʟʟ ꜱᴇᴛᴛɪɴɢꜱ\n"
        "<code>/ʀᴇꜱᴇᴛ</code>  — ʀᴇꜱᴇᴛ ꜱᴇᴛᴛɪɴɢꜱ ᴛᴏ ᴅᴇꜰᴀᴜʟᴛ\n\n"
        "<b>»  ꜰᴇᴀᴛᴜʀᴇꜱ:</b>\n"
        "<b>➲ </b> ꜰᴏʀᴡᴀʀᴅ ꜰʀᴏᴍ ᴘᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟꜱ — ɴᴏ ᴀᴅᴍɪɴ ɴᴇᴇᴅᴇᴅ\n"
        "<b>➲ </b> ꜰᴏʀᴡᴀʀᴅ ꜰʀᴏᴍ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟꜱ — ᴠɪᴀ ʙᴏᴛ/ᴜꜱᴇʀʙᴏᴛ ᴀᴅᴍɪɴ\n"
        "<b>➲ </b> ᴍᴜʟᴛɪ-ᴀᴄᴄᴏᴜɴᴛ: ᴜᴘ ᴛᴏ 2 ʙᴏᴛꜱ + 2 ᴜꜱᴇʀʙᴏᴛꜱ\n"
        "<b>➲ </b> ʟɪᴠᴇ ᴊᴏʙꜱ — ʙᴀᴄᴋɢʀᴏᴜɴᴅ ᴛᴀꜱᴋꜱ, ʀᴜɴ ᴘᴀʀᴀʟʟᴇʟ ᴛᴏ ʙᴀᴛᴄʜ ꜰᴏʀᴡᴀʀᴅꜱ\n"
        "<b>➲ </b> ɴᴇᴡ→ᴏʟᴅ &ᴀᴍᴘ; ᴏʟᴅ→ɴᴇᴡ ꜰᴏʀᴡᴀʀᴅɪɴɢ ᴏʀᴅᴇʀ\n"
        "<b>➲ </b> ꜰɪʟᴛᴇʀꜱ — ꜱᴋɪᴘ ᴀᴜᴅɪᴏ/ᴠɪᴅᴇᴏ/ᴘʜᴏᴛᴏ/ᴛᴇxᴛ/ꜱᴛɪᴄᴋᴇʀ/ᴘᴏʟʟ ᴇᴛᴄ.\n"
        "<b>➲ </b> ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ / ʀᴇᴍᴏᴠᴇ ᴄᴀᴘᴛɪᴏɴ / ᴀᴅᴅ ʙᴜᴛᴛᴏɴꜱ\n"
        "<b>➲ </b> ꜱᴋɪᴘ ᴅᴜᴘʟɪᴄᴀᴛᴇ ᴍᴇꜱꜱᴀɢᴇꜱ\n"
        "<b>➲ </b> ᴇxᴛᴇɴꜱɪᴏɴ / ᴋᴇʏᴡᴏʀᴅ / ꜱɪᴢᴇ ꜰɪʟᴛᴇʀꜱ\n"
        "<b>➲ </b> ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴏᴅᴇ — ʙʏᴘᴀꜱꜱᴇꜱ ꜰᴏʀᴡᴀʀᴅ ʀᴇꜱᴛʀɪᴄᴛɪᴏɴꜱ\n"
        "<b>➲ </b> ᴄʟᴇᴀɴ ᴍꜱɢ — ʙᴜʟᴋ ᴅᴇʟᴇᴛᴇ ꜰʀᴏᴍ ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟꜱ"
    ),
    "hi": (
        "<b><u>»  संसथान (HELP) — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>»  Commands:</b>\n"
        "<code>/start</code>  — मैं चालू हूँ या नहीं चेक करें\n"
        "<code>/forward</code>  — फॉरवर्डिंग शुरू करें\n"
        "<code>/jobs</code>  — लाइव जॉब मैनेज करें (बैकग्राउंड फॉरवर्डिंग)\n"
        "<code>/cleanmsg</code>  — एक साथ अनेक संदेश डिलीट करें\n"
        "<code>/settings</code>  — सेटिंग्स बदलिए\n"
        "<code>/reset</code>  — सेटिंग्स डिफ़ॉल्ट करें\n\n"
        "<b>»  Features:</b>\n"
        "<b>➲ </b> पब्लिक चैनल्स से फॉरवर्ड — एडमिन होने की जरूरत नहीं\n"
        "<b>➲ </b> प्राइवेट चैनल्स से फॉरवर्ड — बोट/यूज़रबोट एडमिन द्वारा\n"
        "<b>➲ </b> मल्टी-अकाउंट: 2 बोट्स + 2 यूज़रबोट्स\n"
        "<b>➲ </b> लाइव जॉब्स — बैकग्राउंड में चलने वाले काम\n"
        "<b>➲ </b> नया→पुराना और पुराना→नया दोनों तरीके\n"
        "<b>➲ </b> फिल्टर्स — ऑडियो/टेक्स्ट/वीडियो/फ़ोटो इत्यादि इग्नोर करें\n"
        "<b>➲ </b> कस्टम कैप्शन या कैप्शन हटायें\n"
        "<b>➲ </b> डुप्लीकेट संदेश इग्नोर करें\n"
        "<b>➲ </b> एक्सटेंशन / कीवर्ड / साइज फिल्टर्स\n"
        "<b>➲ </b> डाउनलोड मोड — डाउनलोड कर अपलोड करें\n"
        "<b>➲ </b> Clean MSG — चैनल्स से बल्क में संदेश डिलीट करें"
    ),
    "hinglish": (
        "<b><u>»  HELP — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>»  Commands:</b>\n"
        "<code>/start</code>  — Check karo main zinda hu ya nahi\n"
        "<code>/forward</code>  — Forwarding chalu karo\n"
        "<code>/jobs</code>  — Live Jobs manage karo\n"
        "<code>/cleanmsg</code>  — Chats se ek saath messages udaao\n"
        "<code>/settings</code>  — Saari settings yahan milegi\n"
        "<code>/reset</code>  — Settings default pe kar do\n\n"
        "<b>»  Features:</b>\n"
        "<b>➲ </b> Public channel se forward — no admin required\n"
        "<b>➲ </b> Private channel se forward — bot/userbot admin zaruri h\n"
        "<b>➲ </b> Multi-Account: 2 Bots + 2 Userbots add kar sakte ho\n"
        "<b>➲ </b> Live Jobs — peeche background me chalte rahenge\n"
        "<b>➲ </b> New→Old aur Old→New dono support\n"
        "<b>➲ </b> Filters — audio/video/photo/sticker/text skip karo\n"
        "<b>➲ </b> Custom caption / caption hata do / buttons lagao\n"
        "<b>➲ </b> Duplicate posts skip ho jayenge automatically\n"
        "<b>➲ </b> Extension / Keyword / Size filters\n"
        "<b>➲ </b> Download mode — block bypass karke upload karega\n"
        "<b>➲ </b> Clean MSG — Target channel se bulk m messages delete karo"
    ),
}

#  HOW_USE_TXT 
_S["HOW_USE_TXT"] = {
    "en": (
        "<b><u>»  ʜᴏᴡ ᴛᴏ ᴜꜱᴇ — ᴀʀʏᴀ ʙᴏᴛ</u></b>\n\n"
        "<b>»  ᴀᴅᴅ ᴀɴ ᴀᴄᴄᴏᴜɴᴛ</b>\n"
        "  ‣ ɢᴏ ᴛᴏ /ꜱᴇᴛᴛɪɴɢꜱ → »  ᴀᴄᴄᴏᴜɴᴛꜱ\n"
        "  ‣ ᴀᴅᴅ ᴀ ʙᴏᴛ (ꜱᴇɴᴅ ɪᴛꜱ ᴛᴏᴋᴇɴ) ᴏʀ ᴀ ᴜꜱᴇʀʙᴏᴛ (ꜱᴇɴᴅ ꜱᴇꜱꜱɪᴏɴ ꜱᴛʀɪɴɢ)\n"
        "  ‣ ʏᴏᴜ ᴄᴀɴ ᴀᴅᴅ ᴜᴘ ᴛᴏ 2 ʙᴏᴛꜱ + 2 ᴜꜱᴇʀʙᴏᴛꜱ\n\n"
        "<b>»  ᴀᴅᴅ ᴀ ᴛᴀʀɢᴇᴛ ᴄʜᴀɴɴᴇʟ</b>\n"
        "  ‣ ɢᴏ ᴛᴏ /ꜱᴇᴛᴛɪɴɢꜱ → 📣 ᴄʜᴀɴɴᴇʟꜱ\n"
        "  ‣ ʏᴏᴜʀ ʙᴏᴛ/ᴜꜱᴇʀʙᴏᴛ ᴍᴜꜱᴛ ʙᴇ <b>ᴀᴅᴍɪɴ</b> ɪɴ ᴛʜᴇ ᴛᴀʀɢᴇᴛ\n\n"
        "<b>»  ᴄᴏɴꜰɪɢᴜʀᴇ ꜱᴇᴛᴛɪɴɢꜱ</b>\n"
        "  ‣ <b>ꜰɪʟᴛᴇʀꜱ</b> — ᴄʜᴏᴏꜱᴇ ᴡʜᴀᴛ ᴛʏᴘᴇꜱ ᴏꜰ ᴍᴇꜱꜱᴀɢᴇꜱ ᴛᴏ ꜱᴋɪᴘ\n"
        "  ‣ <b>ᴄᴀᴘᴛɪᴏɴ</b> — ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ᴏʀ ʀᴇᴍᴏᴠᴇ ɪᴛ\n"
        "  ‣ <b>ꜰᴏʀᴡᴀʀᴅ ᴛᴀɢ</b> — ꜱʜᴏᴡ ᴏʀ ʜɪᴅᴇ ꜰᴏʀᴡᴀʀᴅᴇᴅ-ꜰʀᴏᴍ ʟᴀʙᴇʟ\n"
        "  ‣ <b>ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴏᴅᴇ</b> — ʀᴇ-ᴜᴘʟᴏᴀᴅ ꜰɪʟᴇꜱ (ʙʏᴘᴀꜱꜱᴇꜱ ʀᴇꜱᴛʀɪᴄᴛɪᴏɴꜱ)\n"
        "  ‣ <b>ᴅᴜᴘʟɪᴄᴀᴛᴇ ꜱᴋɪᴘ</b> — ᴀᴠᴏɪᴅ ʀᴇ-ꜰᴏʀᴡᴀʀᴅɪɴɢ ꜱᴀᴍᴇ ᴄᴏɴᴛᴇɴᴛ\n\n"
        "<b>»  ʙᴀᴛᴄʜ ꜰᴏʀᴡᴀʀᴅ (/ꜰᴏʀᴡᴀʀᴅ)</b>\n"
        "  ‣ ᴄʜᴏᴏꜱᴇ ᴀᴄᴄᴏᴜɴᴛ → ꜱᴇʟᴇᴄᴛ ᴛᴀʀɢᴇᴛ → ꜱᴇɴᴅ ꜱᴏᴜʀᴄᴇ ʟɪɴᴋ/ɪᴅ\n"
        "  ‣ ᴄʜᴏᴏꜱᴇ ᴏʀᴅᴇʀ (ᴏʟᴅ→ɴᴇᴡ / ɴᴇᴡ→ᴏʟᴅ) → ꜱᴇᴛ ꜱᴋɪᴘ ᴄᴏᴜɴᴛ\n"
        "  ‣ ᴠᴇʀɪꜰʏ ᴅᴏᴜʙʟᴇ ᴄʜᴇᴄᴋ → ᴄʟɪᴄᴋ ʏᴇꜱ\n\n"
        "<b>»  ʟɪᴠᴇ ᴊᴏʙꜱ (/ᴊᴏʙꜱ)</b>\n"
        "  ‣ ᴄʀᴇᴀᴛᴇꜱ ᴀ <b>ʙᴀᴄᴋɢʀᴏᴜɴᴅ ᴊᴏʙ</b> ᴛʜᴀᴛ ᴀᴜᴛᴏ-ꜰᴏʀᴡᴀʀᴅꜱ ɴᴇᴡ ᴍᴇꜱꜱᴀɢᴇꜱ\n"
        "  ‣ ᴡᴏʀᴋꜱ ᴀʟᴏɴɢꜱɪᴅᴇ ʙᴀᴛᴄʜ ꜰᴏʀᴡᴀʀᴅɪɴɢ ꜱɪᴍᴜʟᴛᴀɴᴇᴏᴜꜱʟʏ\n"
        "  ‣ ꜱᴜᴘᴘᴏʀᴛꜱ ᴄʜᴀɴɴᴇʟꜱ, ɢʀᴏᴜᴘꜱ, ʙᴏᴛ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛꜱ, ꜱᴀᴠᴇᴅ ᴍᴇꜱꜱᴀɢᴇꜱ\n"
        "  ‣ ʀᴇꜱᴘᴇᴄᴛꜱ ʏᴏᴜʀ ꜰɪʟᴛᴇʀꜱ ꜱᴇᴛᴛɪɴɢꜱ\n"
        "  ‣ ꜱᴛᴏᴘ/ꜱᴛᴀʀᴛ/ᴅᴇʟᴇᴛᴇ ᴀɴʏ ᴊᴏʙ ᴀɴʏᴛɪᴍᴇ ꜰʀᴏᴍ /ᴊᴏʙꜱ\n\n"
        "<b>»  ᴄʟᴇᴀɴ ᴍꜱɢ (/ᴄʟᴇᴀɴᴍꜱɢ)</b>\n"
        "  ‣ ꜱᴇʟᴇᴄᴛ ᴀᴄᴄᴏᴜɴᴛ + ᴛᴀʀɢᴇᴛ ᴄʜᴀᴛ(ꜱ) + ᴍᴇꜱꜱᴀɢᴇ ᴛʏᴘᴇ\n"
        "  ‣ ʙᴜʟᴋ ᴅᴇʟᴇᴛᴇꜱ ᴍᴇꜱꜱᴀɢᴇꜱ ɪɴ ᴏɴᴇ ɢᴏ\n\n"
        "<b>‣  ɴᴏᴛᴇꜱ:</b>\n"
        "  ‣ ʙᴏᴛ ᴀᴄᴄᴏᴜɴᴛ: ɴᴇᴇᴅꜱ ᴀᴅᴍɪɴ ɪɴ ᴛᴀʀɢᴇᴛ (ᴀɴᴅ ꜱᴏᴜʀᴄᴇ ɪꜰ ᴘʀɪᴠᴀᴛᴇ)\n"
        "  ‣ ᴜꜱᴇʀʙᴏᴛ: ɴᴇᴇᴅꜱ ᴍᴇᴍʙᴇʀꜱʜɪᴘ ɪɴ ꜱᴏᴜʀᴄᴇ + ᴀᴅᴍɪɴ ɪɴ ᴛᴀʀɢᴇᴛ\n"
        "  ‣ ꜰᴏʀ ᴘᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟꜱ, ᴀ ɴᴏʀᴍᴀʟ ʙᴏᴛ ᴡᴏʀᴋꜱ ꜰɪɴᴇ\n"
        "  ‣ ꜰᴏʀ ᴘʀɪᴠᴀᴛᴇ/ʀᴇꜱᴛʀɪᴄᴛᴇᴅ ꜱᴏᴜʀᴄᴇꜱ, ᴜꜱᴇ ᴀ ᴜꜱᴇʀʙᴏᴛ"
    ),
    "hi": (
        "<b><u>»  इस्तमाल कैसे करें — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>»  अकाउंट जोड़ें</b>\n"
        "  ‣ /settings पर जाएं → »  Accounts\n"
        "  ‣ बोट (टोकन) या यूज़रबोट (सेशन स्ट्रिंग) जोड़े\n\n"
        "<b>»  टारगेट चैनल जोड़ें</b>\n"
        "  ‣ /settings पर जाएं → 📣 Channels\n"
        "  ‣ आपका अकाउंट टारगेट चैनल में एडमिन होना चाहिए\n\n"
        "<b>»  सेटिंग्स कॉन्फ़िगर करें</b>\n"
        "  ‣ <b>फ़िल्टर्स</b> — अनावश्यक संदेश हटाएँ\n"
        "  ‣ <b>कैप्शन</b> — खुद की कैप्शन डालें या हटाएँ\n"
        "  ‣ <b>फॉरवर्ड टैग</b> — असली चैनल का नाम छुपाएँ\n"
        "  ‣ <b>डाउनलोड मोड</b> — रिस्ट्रिक्टेड मीडिया को डाउनलोड/अपलोड करें\n\n"
        "<b>»  फॉरवर्ड शुरू करें (/forward)</b>\n"
        "  ‣ अकाउंट चुनें → स्रोत लिंक भेजें → आगे बढ़ें\n\n"
        "<b>»  लाइव जॉब्स (/jobs)</b>\n"
        "  ‣ बैकग्राउंड में लगातार चलने वाले टास्क बनाएँ\n\n"
        "<b>‣  याद रखें:</b>\n"
        "  ‣ प्राइवेट सोर्स के लिए आपको यूज़रबोट चाहिए या बोट एडमिन हो"
    ),
    "hinglish": (
        "<b><u>»  Istemal Kaise Karein — Aryᴀ Bᴏᴛ</u></b>\n\n"
        "<b>»  Account Add Karo</b>\n"
        "  ‣ /settings mein jao → »  Accounts\n"
        "  ‣ Bot ka token do ya Userbot ki session string lagao\n\n"
        "<b>»  Target Channel Add Karo</b>\n"
        "  ‣ /settings pe → 📣 Channels\n"
        "  ‣ Tumhara account wahan admin hona zaruri hai\n\n"
        "<b>»  Settings Theek Karo</b>\n"
        "  ‣ <b>Filters</b> — kya kya skip karna h wo tick karo\n"
        "  ‣ <b>Caption</b> — khud ka caption do ya uda do\n"
        "  ‣ <b>Forward Tag</b> — Asli channel ka nam chupao\n"
        "  ‣ <b>Download Mode</b> — restricted source file nikal lega\n\n"
        "<b>»  Forward Chalu Karo (/forward)</b>\n"
        "  ‣ Account chuno → source link send karo aur OK karo\n\n"
        "<b>»  Live Jobs (/jobs)</b>\n"
        "  ‣ Background me forward lagao, chalta rahega rozana\n\n"
        "<b>‣  Yaad Rakhein:</b>\n"
        "  ‣ Private channels m userbot bhetar kaam karta hai"
    ),
}

#  ABOUT_TXT 
_S["ABOUT_TXT"] = {
    "en": (
        "<b> »  Bot Details \n"
        " \n"
        "  »  ᴍʏ ɴᴀᴍᴇ   : <a href='https://t.me/MeJeetX'>ᴀʀʏᴀ ʙᴏᴛ</a>\n"
        "  » ‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ : <a href='https://t.me/MeJeetX'>ᴍᴇᴊᴇᴇᴛx</a>\n"
        "  »  ᴄʜᴀɴɴᴇʟ   : <a href='https://t.me/MeJeetX'>ᴜᴘᴅᴀᴛᴇꜱ</a>\n"
        "  »  ꜱᴜᴘᴘᴏʀᴛ   : <a href='https://t.me/+1p2hcQ4ZaupjNjI1'>ꜱᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ</a>\n"
        " \n"
        "  »  ᴠᴇʀꜱɪᴏɴ   : <code>{bot_version}</code> \n"
        "  »  ʟᴀɴɢᴜᴀɢᴇ  : ᴘʏᴛʜᴏɴ 3 \n"
        "  {python_version}\n"
        "  »  ʟɪʙʀᴀʀʏ   : ᴘʏʀᴏɢʀᴀᴍ  \n"
        "\n"
        "</b>"
    ),
    "hi": (
        "<b> »  Bot Details \n"
        " \n"
        "  »  मेरा नाम   : <a href='https://t.me/MeJeetX'>Aryᴀ Bᴏᴛ</a>\n"
        "  » ‍💻 डेवलपर   : <a href='https://t.me/MeJeetX'>MeJeetX</a>\n"
        "  »  चैनल      : <a href='https://t.me/MeJeetX'>Updates</a>\n"
        "  »  सपोर्ट     : <a href='https://t.me/+1p2hcQ4ZaupjNjI1'>Support Group</a>\n"
        " \n"
        "  »  वर्ज़न     : <code>{bot_version}</code> \n"
        "  »  भाषा      : ᴘʏᴛʜᴏɴ 3 \n"
        "  {python_version}\n"
        "  »  लाइब्रेरी   : ᴘʏʀᴏɢʀᴀᴍ  \n"
        "\n"
        "</b>"
    ),
    "hinglish": (
        "<b> »  Bot Details \n"
        " \n"
        "  »  Mera Naam : <a href='https://t.me/MeJeetX'>Aryᴀ Bᴏᴛ</a>\n"
        "  » ‍💻 Developer : <a href='https://t.me/MeJeetX'>MeJeetX</a>\n"
        "  »  Channel   : <a href='https://t.me/MeJeetX'>Updates</a>\n"
        "  »  Support   : <a href='https://t.me/+1p2hcQ4ZaupjNjI1'>Support Group</a>\n"
        " \n"
        "  »  Version   : <code>{bot_version}</code> \n"
        "  »  Language  : ᴘʏᴛʜᴏɴ 3 \n"
        "  {python_version}\n"
        "  »  Library   : ᴘʏʀᴏɢʀᴀᴍ  \n"
        "\n"
        "</b>"
    ),
}

#  STATUS_TXT 
_S["STATUS_TXT"] = {
    "en": (
        "<b>╔════❰ Bot Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Users: <code>{users_count}</code>\n"
        "║┣⪼ Bots: <code>{bots_count}</code>\n"
        "║┣⪼ Channels: <code>{total_channels}</code>\n"
        "║┣⪼ Banned: <code>{banned_users}</code>\n"
        "║┣⪼ Current forwards: <code>{current_forwards}</code>\n"
        "║┣⪼ Live forwards: <code>{live_forward}</code>\n"
        "║┣⪼ Batch forwards: <code>{batch_forward}</code>\n"
        "║┣⪼ Normal forwards: <code>{normal_forward}</code>\n"
        "║┣⪼ Total forwards: <code>{total_forward}</code>\n"
        "║┣⪼ Total downloads: <code>{total_files_downloaded}</code> files\n"
        "║┣⪼ Total uploads: <code>{total_files_uploaded}</code> files\n"
        "║┣⪼ Total data: <code>{total_data_usage_bytes}</code>\n"
        "║┣⪼ DL speed: <code>{dl_speed}</code>\n"
        "║┣⪼ UP speed: <code>{ul_speed}</code>\n"
        "║┣⪼ Uptime: <code>{uptime}</code>\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Active ❱══❍⊱❁۪۪</b>"
    ),
    "hi": (
        "<b>╔════❰ Bot Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Users: <code>{users_count}</code>\n"
        "║┣⪼ Bots: <code>{bots_count}</code>\n"
        "║┣⪼ Channels: <code>{total_channels}</code>\n"
        "║┣⪼ Banned: <code>{banned_users}</code>\n"
        "║┣⪼ Current forwards: <code>{current_forwards}</code>\n"
        "║┣⪼ Live forwards: <code>{live_forward}</code>\n"
        "║┣⪼ Batch forwards: <code>{batch_forward}</code>\n"
        "║┣⪼ Normal forwards: <code>{normal_forward}</code>\n"
        "║┣⪼ Total forwards: <code>{total_forward}</code>\n"
        "║┣⪼ Total downloads: <code>{total_files_downloaded}</code> files\n"
        "║┣⪼ Total uploads: <code>{total_files_uploaded}</code> files\n"
        "║┣⪼ Total data: <code>{total_data_usage_bytes}</code>\n"
        "║┣⪼ DL speed: <code>{dl_speed}</code>\n"
        "║┣⪼ UP speed: <code>{ul_speed}</code>\n"
        "║┣⪼ Uptime: <code>{uptime}</code>\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Active ❱══❍⊱❁۪۪</b>"
    ),
    "hinglish": (
        "<b>╔════❰ Bot Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Users: <code>{users_count}</code>\n"
        "║┣⪼ Bots: <code>{bots_count}</code>\n"
        "║┣⪼ Channels: <code>{total_channels}</code>\n"
        "║┣⪼ Banned: <code>{banned_users}</code>\n"
        "║┣⪼ Current forwards: <code>{current_forwards}</code>\n"
        "║┣⪼ Live forwards: <code>{live_forward}</code>\n"
        "║┣⪼ Batch forwards: <code>{batch_forward}</code>\n"
        "║┣⪼ Normal forwards: <code>{normal_forward}</code>\n"
        "║┣⪼ Total forwards: <code>{total_forward}</code>\n"
        "║┣⪼ Total downloads: <code>{total_files_downloaded}</code> files\n"
        "║┣⪼ Total uploads: <code>{total_files_uploaded}</code> files\n"
        "║┣⪼ Total data: <code>{total_data_usage_bytes}</code>\n"
        "║┣⪼ DL speed: <code>{dl_speed}</code>\n"
        "║┣⪼ UP speed: <code>{ul_speed}</code>\n"
        "║┣⪼ Uptime: <code>{uptime}</code>\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Active ❱══❍⊱❁۪۪</b>"
    ),
}

#  FROM_MSG 
_S["FROM_MSG"] = {
    "en": (
        "<b>❪ ꜱᴇᴛ ꜱᴏᴜʀᴄᴇ ᴄʜᴀᴛ ❫\n\n"
        "ꜰᴏʀᴡᴀʀᴅ ᴛʜᴇ ʟᴀꜱᴛ ᴍᴇꜱꜱᴀɢᴇ ᴏʀ ʟɪɴᴋ.\n"
        "ᴛʏᴘᴇ ᴜꜱᴇʀɴᴀᴍᴇ/ɪᴅ (ᴇ.ɢ. <code>@ꜱᴏᴍᴇʙᴏᴛ</code> ᴏʀ <code>123456</code>) ꜰᴏʀ ʙᴏᴛ/ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛ.\n"
        "ᴛʏᴘᴇ <code>ᴍᴇ</code> ꜰᴏʀ ꜱᴀᴠᴇᴅ ᴍᴇꜱꜱᴀɢᴇꜱ.\n"
        "/ᴄᴀɴᴄᴇʟ - ᴛᴏ ᴄᴀɴᴄᴇʟ</b>"
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

#  TO_MSG 
_S["TO_MSG"] = {
    "en": "<b>❪ CHOOSE TARGET CHAT ❫\n\nChoose your target chat from the given buttons.\n/cancel - Cancel this process</b>",
    "hi": "<b>❪ टारगेट चैट चुनें ❫\n\nनीचे दिए गए बटन से अपनी टारगेट चैट चुनें।\n/cancel - इस प्रक्रिया को रद्द करें</b>",
    "hinglish": "<b>❪ TARGET CHAT CHUNO ❫\n\nNeeche diye gaye buttons se target chat select karo.\n/cancel - is process ko cancel karo</b>",
}

#  SAVED_MSG_MODE 
_S["SAVED_MSG_MODE"] = {
    "en": "<b>❪ SELECT MODE ❫\n\nChoose forwarding mode:\n1. <code>batch</code> - Forward existing messages.\n2. <code>live</code> - Continuous (wait for new messages).</b>",
    "hi": "<b>❪ मोड चुनें ❫\n\nफॉरवर्डिंग मोड चुनें:\n1. <code>batch</code> - मौजूदा संदेश फॉरवर्ड करें।\n2. <code>live</code> - लाइव (नए संदेशों का इंतजार करें)।</b>",
    "hinglish": "<b>❪ MODE SELECT KARO ❫\n\nForwarding mode chuno:\n1. <code>batch</code> - Purane messages forward karo.\n2. <code>live</code> - Naye messages ka wait karega.</b>",
}

#  SAVED_MSG_LIMIT 
_S["SAVED_MSG_LIMIT"] = {
    "en": "<b>❪ NUMBER OF MESSAGES ❫\n\nHow many messages to forward?\nEnter a number or <code>all</code>.</b>",
    "hi": "<b>❪ संदेशों की संख्या ❫\n\nकितने संदेश फॉरवर्ड करने हैं?\nकोई संख्या डालें या <code>all</code> लिखें।</b>",
    "hinglish": "<b>❪ KITNE MESSAGES ❫\n\nKitne messages forward karne hain?\nNumber likho ya <code>all</code> bhejo.</b>",
}

#  SKIP_MSG 
_S["SKIP_MSG"] = {
    "en": (
        "<b>❪ ꜱᴇᴛ ᴍᴇꜱꜱᴀɢᴇ ꜱᴋɪᴘɪɴɢ ɴᴜᴍʙᴇʀ ❫</b>\n\n"
        "<b>ꜱᴋɪᴘ ᴛʜᴇ ᴍᴇꜱꜱᴀɢᴇ ᴀꜱ ᴍᴜᴄʜ ᴀꜱ ʏᴏᴜ ᴇɴᴛᴇʀ ᴛʜᴇ ɴᴜᴍʙᴇʀ ᴀɴᴅ ᴛʜᴇ ʀᴇꜱᴛ ᴏꜰ ᴛʜᴇ ᴍᴇꜱꜱᴀɢᴇ ᴡɪʟʟ ʙᴇ ꜰᴏʀᴡᴀʀᴅᴇᴅ\n"
        "ᴅᴇꜰᴀᴜʟᴛ ꜱᴋɪᴘ ɴᴜᴍʙᴇʀ =</b> <code>0</code>\n"
        "<code>ᴇɢ: ʏᴏᴜ ᴇɴᴛᴇʀ 0 = 0 ᴍᴇꜱꜱᴀɢᴇ ꜱᴋɪᴘᴇᴅ\n"
        " ʏᴏᴜ ᴇɴᴛᴇʀ 5 = 5 ᴍᴇꜱꜱᴀɢᴇ ꜱᴋɪᴘᴇᴅ</code>\n"
        "/ᴄᴀɴᴄᴇʟ <b>- ᴄᴀɴᴄᴇʟ ᴛʜɪꜱ ᴘʀᴏᴄᴇꜱꜱ</b>"
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

#  CANCEL 
_S["CANCEL"] = {
    "en": "<b>Process Cancelled Succefully !</b>",
    "hi": "<b>प्रक्रिया सफलतापूर्वक रद्द की गई!</b>",
    "hinglish": "<b>Process Cancel ho gaya!</b>",
}

#  BOT_DETAILS 
_S["BOT_DETAILS"] = {
    "en": "<b><u>»  BOT DETAILS</u></b>\n\n<b>➣ NAME:</b> <code>{}</code>\n<b>➣ BOT ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
    "hi": "<b><u>»  बोट विवरण</u></b>\n\n<b>➣ नाम:</b> <code>{}</code>\n<b>➣ बोट ID:</b> <code>{}</code>\n<b>➣ यूज़रनेम:</b> @{}",
    "hinglish": "<b><u>»  BOT DETAILS</u></b>\n\n<b>➣ NAAM:</b> <code>{}</code>\n<b>➣ BOT ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
}

#  USER_DETAILS 
_S["USER_DETAILS"] = {
    "en": "<b><u>»  USERBOT DETAILS</u></b>\n\n<b>➣ NAME:</b> <code>{}</code>\n<b>➣ USER ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
    "hi": "<b><u>»  यूज़रबोट विवरण</u></b>\n\n<b>➣ नाम:</b> <code>{}</code>\n<b>➣ यूज़र ID:</b> <code>{}</code>\n<b>➣ यूज़रनेम:</b> @{}",
    "hinglish": "<b><u>»  USERBOT DETAILS</u></b>\n\n<b>➣ NAAM:</b> <code>{}</code>\n<b>➣ USER ID:</b> <code>{}</code>\n<b>➣ USERNAME:</b> @{}",
}

#  TEXT (forwarding status box) 
_S["TEXT"] = {
    "en": (
        "<b>╔════❰ Forward Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Fetched messages: <code>{}</code>\n"
        "║┣⪼ Successfully forwarded: <code>{}</code>\n"
        "║┣⪼ Duplicate messages: <code>{}</code>\n"
        "║┣⪼ Skipped messages: <code>{}</code>\n"
        "║┣⪼ Deleted messages: <code>{}</code>\n"
        "║┣⪼ Current status: <code>{}</code>\n"
        "║┣⪼ ETA: <code>{}</code>\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Auto Forwarder ❱══❍⊱❁۪۪</b>"
    ),
    "hi": (
        "<b>╔════❰ Forward Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Fetched messages: <code>{}</code>\n"
        "║┣⪼ Successfully forwarded: <code>{}</code>\n"
        "║┣⪼ Duplicate messages: <code>{}</code>\n"
        "║┣⪼ Skipped messages: <code>{}</code>\n"
        "║┣⪼ Deleted messages: <code>{}</code>\n"
        "║┣⪼ Current status: <code>{}</code>\n"
        "║┣⪼ ETA: <code>{}</code>\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Auto Forwarder ❱══❍⊱❁۪۪</b>"
    ),
    "hinglish": (
        "<b>╔════❰ Forward Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Fetched messages: <code>{}</code>\n"
        "║┣⪼ Successfully forwarded: <code>{}</code>\n"
        "║┣⪼ Duplicate messages: <code>{}</code>\n"
        "║┣⪼ Skipped messages: <code>{}</code>\n"
        "║┣⪼ Deleted messages: <code>{}</code>\n"
        "║┣⪼ Current status: <code>{}</code>\n"
        "║┣⪼ ETA: <code>{}</code>\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Auto Forwarder ❱══❍⊱❁۪۪</b>"
    ),
}

#  DUPLICATE_TEXT 
_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>╔════❰ Unequify Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Fetched messages: <code>{}</code>\n"
        "║┣⪼ Duplicate messages: <code>{}</code>\n"
        "║┣⪼ {} \n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Setup ❱══❍⊱❁۪۪</b>"
    ),
    "hi": (
        "<b>╔════❰ Unequify Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Fetched messages: <code>{}</code>\n"
        "║┣⪼ Duplicate messages: <code>{}</code>\n"
        "║┣⪼ {} \n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Setup ❱══❍⊱❁۪۪</b>"
    ),
    "hinglish": (
        "<b>╔════❰ Unequify Status ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        "║┣⪼ Fetched messages: <code>{}</code>\n"
        "║┣⪼ Duplicate messages: <code>{}</code>\n"
        "║┣⪼ {} \n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚═════❰ Setup ❱══❍⊱❁۪۪</b>"
    ),
}

#  Simple one-liners 
_S["<i>Process Cancelled Successfully!</i>"] = {
    "en": "»  Process cancelled.",
    "hi": "»  प्रक्रिया रद्द की गई।",
    "hinglish": "»  Process cancel ho gaya.",
}
_S["btn_settings"] = {"en": "⚙️ Sᴇᴛᴛɪɴɢs", "hi": "⚙️ Sᴇᴛᴛɪɴɢs", "hinglish": "⚙️ Sᴇᴛᴛɪɴɢs"}
_S["btn_jobs"] = {"en": "📋 Lɪᴠᴇ Jᴏʙs", "hi": "📋 Lɪᴠᴇ Jᴏʙs", "hinglish": "📋 Lɪᴠᴇ Jᴏʙs"}
_S["btn_help"] = {"en": "🙋 Hᴇʟᴘ", "hi": "🙋 Hᴇʟᴘ", "hinglish": "🙋 Hᴇʟᴘ"}
_S["btn_about"] = {"en": "💁 Aʙᴏᴜᴛ", "hi": "💁 Aʙᴏᴜᴛ", "hinglish": "💁 Aʙᴏᴜᴛ"}
_S["btn_close"] = {"en": "‣  Close", "hi": "‣  बंद करें", "hinglish": "‣  Close"}
_S["settings_title"] = {
    "en": "»  Change your settings as you wish:",
    "hi": "»  अपनी सेटिंग्स बदलें:",
    "hinglish": "»  Apni settings apne hisaab se badlo:",
}
_S["select_lang"] = {
    "en": "»  Select your preferred language:",
    "hi": "»  अपनी भाषा चुनें:",
    "hinglish": "»  Apni language select karo:",
}
_S["lang_set"] = {
    "en": "»  Language set to <b>English</b>.",
    "hi": "»  भाषा <b>हिंदी</b> में सेट की गई।",
    "hinglish": "»  Language <b>Hinglish</b> mein set ho gayi!",
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
    def mark(code): return "»  " if current_lang == code else ""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{mark('en')}🇺🇸 English",       callback_data="setlang#en"),
            InlineKeyboardButton(f"{mark('hi')}🇮🇳 हिंदी",         callback_data="setlang#hi"),
        ],
        [
            InlineKeyboardButton(f"{mark('hinglish')}»  Hinglish", callback_data="setlang#hinglish"),
        ],
        [
            InlineKeyboardButton("Bᴀᴄᴋ Tᴏ Sᴇᴛᴛɪɴɢs",             callback_data="settings#main"),
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
        return await query.answer("ɪɴᴠᴀʟɪᴅ ʟᴀɴɢᴜᴀɢᴇ!", show_alert=True)

    await db.set_language(user_id, lang)
    label_map = {"en": "English 🇺🇸", "hi": "हिंदी 🇮🇳", "hinglish": "Hinglish"}
    label = label_map.get(lang, lang)
    await query.answer(f"Language set to {label}!", show_alert=False)
    await query.message.edit_text(
        _tx(lang, "lang_set"),
        reply_markup=_lang_keyboard(lang)
    )