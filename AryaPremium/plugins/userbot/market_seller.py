"""
Marketplace Seller Bot
======================
Handles the customer UI for buying stories, T&C, and progressive delivery.
"""
import logging
import asyncio
import base64
import io
import re
import html
from datetime import datetime
from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, 
    ReplyKeyboardRemove, InputMediaPhoto, InputMediaVideo, InputMediaAnimation
)
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.errors import MessageNotModified
from database import db
from config import Config
from utils import native_ask, _deliver_purchased_story, to_smallcap
from plugins.userbot.razorpay_helpers import _create_rzp_link, _check_rzp_status
from plugins.userbot.easebuzz_helpers import _create_easebuzz_link, _check_easebuzz_status

from utils_upi import generate_upi_card

logger = logging.getLogger(__name__)
market_clients: dict = {}
dm_aborts = set()


def _clean_upi_note(s: str) -> str:
    # Keep it short + app-compatible.
    s = (s or "").strip()
    s = re.sub(r"[^a-zA-Z0-9 _:/#-]+", "", s)
    return s[:40]


def _build_upi_uri(*, upi_id: str, payee_name: str, amount: int, note: str) -> str:
    """
    Build a conservative UPI URI to maximize compatibility across apps.

    - Always include: pa, am, cu
    - Include pn only when explicitly configured (avoid branding mismatch like "Arya YT")
    - Keep tn generic and short to reduce fraud/risk heuristics
    """
    import urllib.parse
    pa = (upi_id or "").strip()
    am = str(amount)
    q = [f"pa={pa}", f"am={am}", "cu=INR"]

    pn_clean = (payee_name or "").strip()
    if pn_clean:
        q.append(f"pn={urllib.parse.quote_plus(pn_clean)}")

    return "upi://pay?" + "&".join(q)


# Telegram URL buttons only allow http/https; pasted URLs often include invisible Unicode (ZWSP, BOM) → BUTTON_URL_INVALID.
_INVISIBLE_URL_JUNK = re.compile(r"[\u200b-\u200f\u2060\ufeff\ufe0f\u200d\u200c\u00a0]+")


def sanitize_https_redirect_base(url: str) -> str:
    if not url:
        return ""
    s = _INVISIBLE_URL_JUNK.sub("", str(url)).strip()
    s = s.rstrip("/").strip()
    if not (s.lower().startswith("http://") or s.lower().startswith("https://")):
        return ""
    # Block accidental query strings on base (we append /r/...)
    if "?" in s:
        s = s.split("?", 1)[0].rstrip("/")
    return s


def build_open_upi_app_https_url(base: str, upi_uri: str) -> str:
    """
    Short HTTPS URL for Telegram buttons: https://host/r/<base64url(upi_uri)>
    Avoids huge ?uri=... links and invisible-char breakage.
    """
    base = sanitize_https_redirect_base(base)
    if not base or not upi_uri:
        return ""
    tok = base64.urlsafe_b64encode(upi_uri.encode("utf-8")).decode("ascii").rstrip("=")
    href = f"{base}/r/{tok}"
    # Telegram inline button URL limit ~2048 bytes
    if len(href) > 2040:
        return ""
    return href


def _can_sliceurl_shorten(url: str) -> bool:
    if not url:
        return False
    u = url.strip()
    if u.startswith("http://") or u.startswith("https://"):
        return True
    return u.lower().startswith("upi://pay?") and "pa=" in u and "am=" in u


async def _sliceurl_api_shorten(url: str) -> str:
    """
    SliceURL api-public ?action=shorten — supports https://… and (after your deploy) upi://pay?…

    Env: SLICEURL_API_URL, SLICEURL_API_KEY (slc_…)
    """
    if not _can_sliceurl_shorten(url):
        return ""

    api_base = (getattr(Config, "SLICEURL_API_URL", None) or "").strip()
    key = (getattr(Config, "SLICEURL_API_KEY", None) or "").strip()

    if api_base and key.startswith("slc_"):
        try:
            import aiohttp
            post_url = f"{api_base.rstrip('/')}?action=shorten"
            headers = {
                "X-API-Key": key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    post_url,
                    json={"long_url": url},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    status = resp.status
                    try:
                        data = await resp.json()
                    except Exception:
                        data = {}
            if status in (200, 201) and isinstance(data, dict) and data.get("success"):
                short = data.get("short_url")
                if isinstance(short, str) and short.startswith("http"):
                    return short
            logger.warning(f"SliceURL shorten rejected: status={status} body={data}")
        except Exception as e:
            logger.warning(f"SliceURL shorten failed: {e}")
        return ""

    # Strict mode: do not use legacy/generic shorteners.
    return ""


def _make_qr_png_bytes(data: str, *, logo_png_bytes: bytes | None = None) -> bytes:
    try:
        import qrcode
        from PIL import Image
    except Exception as e:
        # The runtime may not have optional deps installed; caller should fallback gracefully.
        raise RuntimeError("QR deps missing") from e

    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=12,
        border=2,
    )
    qr.add_data(data)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white").convert("RGBA")

    if logo_png_bytes:
        try:
            logo = Image.open(io.BytesIO(logo_png_bytes)).convert("RGBA")
            max_w = int(img.size[0] * 0.22)
            max_h = int(img.size[1] * 0.22)
            logo.thumbnail((max_w, max_h))

            pad = max(6, int(img.size[0] * 0.012))
            bg_w, bg_h = logo.size[0] + pad * 2, logo.size[1] + pad * 2
            bg = Image.new("RGBA", (bg_w, bg_h), (255, 255, 255, 255))
            pos = ((img.size[0] - bg_w) // 2, (img.size[1] - bg_h) // 2)
            img.alpha_composite(bg, pos)
            img.alpha_composite(logo, (pos[0] + pad, pos[1] + pad))
        except Exception:
            pass

    out = io.BytesIO()
    out.name = "upi_qr.png"
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()

def _sc(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
    ))

def _bs(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        "𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"
    ))

def _get_base_header(user) -> str:
    u_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "User"
    return f"<blockquote expandable><b>Hello {u_name}</b></blockquote>\n\n"

# Language Texts
T = {
    "en": {
        "welcome": "Welcome to",
        "store": "Store",
        "intro": "Browse our premium collection. Tap Marketplace to explore stories by platform.",
        "tc_accept": "✅ I Accept the Terms",
        "tc_reject": "❌ I Reject",
        "no_stories": "No stories currently available.",
        "pay_upi": "Pay via UPI",
        "back": "❮ Back",
        "qr_msg": """<b>💳 Complete Payment</b>

• Scan the QR code above.
• Amount: ₹{price}

<b>After paying, send the successful payment screenshot here.</b>""",
        "wait_ver": "⏳ Your payment is being verified, please wait (approx 5 minutes)...",
        "notify": "🔔 Notify Admin",
        "prof_title": "╔═⟦ 𝗣𝗥𝗢𝗙𝗜𝗟𝗘 ⟧═╗",
        "prof_name": "ɴᴀᴍᴇ",
        "prof_uname": "ᴜꜱᴇʀɴᴀᴍᴇ",
        "prof_id": "ᴛɢ ɪᴅ",
        "prof_bought": "ᴘᴜʀᴄʜᴀꜱᴇꜱ",
        "prof_lang": "ʟᴀɴɢᴜᴀɢᴇ",
        "prof_join": "ᴊᴏɪɴᴇᴅ",
        "my_reqs": "📝 MY REQUESTS",
        "set_lang": "⚙️ Settings",
        "set_prompt": """<b>⚙️ Settings</b>

Select your language:""",
        "req_main_title": "📝 My Story Requests",
        "req_click": "Click on any request to view its status:",
        "req_empty": "You haven't made any story requests yet.",
        "back_prof": "« BACK TO PROFILE",
        "back_reqs": "« BACK TO REQUESTS",
        "req_details": "📝 STORY REQUEST DETAILS",
        "req_name": "Name",
        "req_plat": "Platform",
        "req_type": "Type",
        "req_date": "Date",
        "req_status": "Status",
        "already_owned": "✅ You already own this story. Sending delivery options...",
        "wait_a_sec": "WAIT A SECOND...",
        "req_step1": "<b>📤 Story Request System</b>\n\n<i>(Note: The story you request will be a Paid service, please keep this in mind.)</i>\n\nPlease enter the exact name of the story you are looking for:",
        "req_step2": "Got it. Send me any sample files, links, or screenshots related to this story (to help us locate it). If you don’t have any, type /skip.",
        "req_done": "✅ <b>Request Submitted!</b>\nWe have received your request. You can track its status using the 'My Requests' button in your Profile.",
        "cant_find_btn": "🔍 CAN'T FIND? REQUEST NOW!",
        "req_search_prompt": """<b>🔍 SEARCH / REQUEST STORY</b>

Type the <b>Story Name</b> you want to search or request:""",
        "req_cancel": "Process Cancelled.",
        "req_success": """✅ <b>Request Submitted!</b>

Our team will search for this story and update you soon. Check status in <b>Profile -> My Requests</b>."""
    },
    "hi": {
        "welcome": "स्वागत है",
        "store": "स्टोर",
        "intro": "प्रीमियम कलेक्शन ब्राउज़ करें। Marketplace पर टैप करें।",
        "tc_accept": "✅ मुझे शर्तें मंजूर हैं",
        "tc_reject": "❌ मैं अस्वीकार करता हूँ",
        "no_stories": "वर्तमान में कोई स्टोरी उपलब्ध नहीं है।",
        "pay_upi": "UPI से पेमेंट करें",
        "back": "❮ वापस",
        "qr_msg": """<b>💳 पेमेंट पूरा करें</b>

• ऊपर QR स्कैन करें।
• राशि: ₹{price}

<b>पेमेंट के बाद स्क्रीनशॉट यहाँ भेजें।</b>""",
        "wait_ver": "⏳ आपके भुगतान का सत्यापन हो रहा है...",
        "notify": "🔔 एडमिन को सूचित करें",
        "prof_title": "╔═⟦ आपकी प्रोफाइल ⟧═╗",
        "prof_name": "नाम",
        "prof_uname": "यूज़रनेम",
        "prof_id": "आईडी",
        "prof_bought": "खरीदी गई स्टोरीज",
        "prof_lang": "भाषा",
        "prof_join": "जुड़े हुए",
        "my_reqs": "📝 मेरे अनुरोध (My Requests)",
        "set_lang": "⚙️ सेटिंग्स",
        "set_prompt": """<b>⚙️ सेटिंग्स</b>

अपनी पसंदीदा भाषा चुनें:""",
        "req_main_title": "📝 मेरे स्टोरी अनुरोध",
        "req_click": "किसी भी अनुरोध पर क्लिक करके उसका स्टेटस देखें:",
        "req_empty": "आपने अभी तक कोई स्टोरी अनुरोध नहीं किया है।",
        "back_prof": "« प्रोफाइल पर वापस",
        "back_reqs": "« अनुरोधों पर वापस",
        "req_details": "📝 स्टोरी अनुरोध विवरण",
        "req_name": "कहानी का नाम",
        "req_plat": "प्लेटफॉर्म",
        "req_type": "प्रकार (Type)",
        "req_date": "तारीख",
        "req_status": "स्टेटस",
        "already_owned": "✅ आप पहले ही इस स्टोरी को खरीद चुके हैं। डिलीवरी विकल्प भेजे जा रहे हैं...",
        "wait_a_sec": "कृपया प्रतीक्षा करें...",
        "cant_find_btn": "🔍 कहानी नहीं मिल रही? अनुरोध करें!",
        "req_search_prompt": """<b>🔍 स्टोरी खोजें / अनुरोध करें</b>

उस <b>कहानी का नाम</b> लिखें जिसे आप खोजना या अनुरोध करना चाहते हैं:""",
        "req_cancel": "प्रक्रिया रद्द कर दी गई।",
        "req_step1": """<b>स्टेप 1/3:</b>
कृपया उस <b>कहानी का नाम</b> लिखें जिसका आप अनुरोध करना चाहते हैं:
<i>(नोट: जो कहानी आप रीक्वेस्ट कर रहे हैं वह पेड (Paid) होगी, तो कृपया इस बात का ध्यान रखते हुए रीक्वेस्ट करें।)</i>""",
        "req_step2": """<b>स्टेप 2/3:</b>
<b>प्लेटफॉर्म</b> चुनें (जैसे: Ullu, AltBalaji):""",
        "req_step3": """<b>स्टेप 3/3:</b>
आपको यह कैसे चाहिए? (जैसे: केवल एपिसोड, पूरी फिल्म, आदि):""",
        "req_success": """✅ <b>अनुरोध जमा हो गया!</b>

हमारी टीम इस कहानी को खोजेगी और जल्द ही आपको अपडेट करेगी। स्टेटस देखने के लिए <b>प्रोफाइल -> मेरे अनुरोध</b> पर जाएं।"""
    }
}

def _get_main_menu(lang='en'):
    if lang == 'hi':
        kb = [
            [
                InlineKeyboardButton("ᴀ", callback_data="mb#about_arya_0"),
                InlineKeyboardButton("ʀ", callback_data="mb#about_arya_0"),
                InlineKeyboardButton("ʏ", callback_data="mb#about_arya_0"),
                InlineKeyboardButton("ᴀ", callback_data="mb#about_arya_0")
            ],
            [InlineKeyboardButton("• मार्केटप्लेस •", callback_data="mb#main_marketplace"),
             InlineKeyboardButton("• मेरी स्टोरीज •", callback_data="mb#my_buys")],
            [InlineKeyboardButton("प्रोफाइल", callback_data="mb#main_profile"),
             InlineKeyboardButton("सेटिंग्स", callback_data="mb#main_settings")],
            [InlineKeyboardButton("मदद / जानकारी", callback_data="mb#main_help")],
            [
                InlineKeyboardButton("ᴄ", callback_data="mb#main_close"),
                InlineKeyboardButton("ʟ", callback_data="mb#main_close"),
                InlineKeyboardButton("ᴏ", callback_data="mb#main_close"),
                InlineKeyboardButton("ꜱ", callback_data="mb#main_close"),
                InlineKeyboardButton("ᴇ", callback_data="mb#main_close")
            ]
        ]
    else:
        kb = [
            [
                InlineKeyboardButton("ᴀ", callback_data="mb#about_arya_0"),
                InlineKeyboardButton("ʀ", callback_data="mb#about_arya_0"),
                InlineKeyboardButton("ʏ", callback_data="mb#about_arya_0"),
                InlineKeyboardButton("ᴀ", callback_data="mb#about_arya_0")
            ],
            [InlineKeyboardButton(f"• {_bs('MARKETPLACE')} •", callback_data="mb#main_marketplace"),
             InlineKeyboardButton(f"• {_bs('MY STORIES')} •", callback_data="mb#my_buys")],
            [InlineKeyboardButton(f"{_sc('Profile')}", callback_data="mb#main_profile"),
             InlineKeyboardButton(f"{_sc('Settings')}", callback_data="mb#main_settings")],
            [InlineKeyboardButton(f"{_sc('Help')}", callback_data="mb#main_help")],
            [
                InlineKeyboardButton("ᴄ", callback_data="mb#main_close"),
                InlineKeyboardButton("ʟ", callback_data="mb#main_close"),
                InlineKeyboardButton("ᴏ", callback_data="mb#main_close"),
                InlineKeyboardButton("ꜱ", callback_data="mb#main_close"),
                InlineKeyboardButton("ᴇ", callback_data="mb#main_close")
            ]
        ]
    return InlineKeyboardMarkup(kb)


def _get_premium_menu_markup(bt_cfg: dict, lang: str):
    """
    Adds optional URL buttons (Updates/Support) like your reference UI.
    Stored in premium_bots.config as `updates_url` / `support_url`.
    """
    rows = []
    updates_url = (bt_cfg.get("updates_url") or "").strip()
    support_url = (bt_cfg.get("support_url") or "").strip()
    if updates_url or support_url:
        r = []
        if updates_url:
            label = "अपडेट्स" if lang == 'hi' else _sc("UPDATES")
            r.append(InlineKeyboardButton(label, url=updates_url))
        if support_url:
            label = "सपोर्ट" if lang == 'hi' else _sc("SUPPORT")
            r.append(InlineKeyboardButton(label, url=support_url))
        if r:
            rows.append(r)
    base = _get_main_menu(lang).inline_keyboard
    # Insert URL row above Close
    if rows:
        base = base[:2] + rows + base[2:]
    return InlineKeyboardMarkup(base)


def _menu_card_text(user, bt_cfg: dict, bot_name: str, lang: str = 'en') -> str:
    from utils import translate_to_hindi
    u_mention = f'<a href="tg://user?id={user.id}">{html.escape((user.first_name or "User").strip())}</a>'
    
    # --- DEFAULTS ---
    def _is_def(val, default_val):
        if not val or not default_val: return False
        import re
        return re.sub(r'\s+', '', val) == re.sub(r'\s+', '', default_val)

    DEFAULT_WELCOME_EN = """›› ʜᴇʏ, {name} | {bot_name}"""
    DEFAULT_ABOUT_EN = """ʙʀᴏᴡꜱᴇ ᴘʀᴇᴍɪᴜᴍ ꜱᴛᴏʀɪᴇꜱ ꜰʀᴏᴍ pocket fm, kuku fm, headphone & more.
ᴛᴀᴘ marketplace ᴛᴏ ᴇxᴘʟᴏʀᴇ ꜱᴛᴏʀɪᴇꜱ ʙʏ platform."""
    DEFAULT_QUOTE_EN = """ǫᴜᴀʟɪᴛʏ ꜱᴛᴏʀɪᴇꜱ • ɪɴꜱᴛᴀɴᴛ ᴅᴇʟɪᴠᴇʀʏ • ᴀᴜᴛᴏᴍᴀᴛᴇᴅ"""
    DEFAULT_AUTHOR_EN = """— ᴀʀʏᴀ ᴘʀᴇᴍɪᴜᴍ"""
    
    DEFAULT_WELCOME_HI = """नमस्ते {name}, आपका आर्या बोट में स्वागत है।"""
    DEFAULT_ABOUT_HI = """यहाँ आपको प्रसिद्ध ऐप्स की कहानियाँ मिलेंगी, जिन्हें आप “मार्केटप्लेस” पर जाकर खरीद सकते हैं।"""
    DEFAULT_QUOTE_HI = """अगर आप मुझे मुख्य भूमिका में रखकर कोई कहानी लिखेंगे... तो वह निश्चित रूप से एक त्रासदी होगी।"""
    DEFAULT_AUTHOR_HI = """— अज्ञात"""

    # --- 1. Welcome Section ---
    text_en = bt_cfg.get("welcome")
    if text_en and text_en.lower() == "disable":
        welcome = ""
    else:
        if not text_en:
            text_en = DEFAULT_WELCOME_EN
        
        if lang == 'hi':
            if _is_def(text_en, DEFAULT_WELCOME_EN):
                welcome = DEFAULT_WELCOME_HI
            else:
                welcome = translate_to_hindi(text_en)
        else:
            welcome = text_en
        
        welcome = welcome.replace("{name}", u_mention).replace("{bot_name}", bot_name).replace("{user}", u_mention).replace("{first_name}", u_mention)

    # --- 2. About Section ---
    text_en = bt_cfg.get("about")
    if text_en and text_en.lower() == "disable":
        about = ""
    else:
        if not text_en:
            text_en = DEFAULT_ABOUT_EN
        
        if lang == 'hi':
            if _is_def(text_en, DEFAULT_ABOUT_EN):
                about = DEFAULT_ABOUT_HI
            else:
                about = translate_to_hindi(text_en)
        else:
            about = text_en

    # --- 3. Quote Section ---
    text_en = bt_cfg.get("quote")
    if text_en and text_en.lower() == "disable":
        quote = ""
    else:
        if not text_en:
            text_en = DEFAULT_QUOTE_EN
        
        if lang == 'hi':
            if _is_def(text_en, DEFAULT_QUOTE_EN):
                quote = DEFAULT_QUOTE_HI
            else:
                quote = translate_to_hindi(text_en)
        else:
            quote = text_en

    # --- 4. Author Section ---
    text_en = bt_cfg.get("quote_author")
    if text_en and text_en.lower() == "disable":
        author = ""
    else:
        if not text_en:
            text_en = DEFAULT_AUTHOR_EN
        
        if lang == 'hi':
            if _is_def(text_en, DEFAULT_AUTHOR_EN):
                author = DEFAULT_AUTHOR_HI
            else:
                author = translate_to_hindi(text_en)
        else:
            author = text_en
        
    blocks = []
    
    if welcome.strip():
        blocks.append(f'<blockquote expandable="true">{welcome.strip()}</blockquote>')
    if about.strip():
        blocks.append(f'<blockquote expandable="true">{about.strip()}</blockquote>')
        
    if (welcome.strip() or about.strip()) and (quote.strip() or author.strip()):
        blocks.append("")
        
    if quote.strip():
        blocks.append(f'<blockquote expandable="true">{quote.strip()}</blockquote>')
    if author.strip():
        blocks.append(f'<blockquote expandable="true"><b>{author.strip()}</b></blockquote>')
        
    return "\n".join(blocks)




async def _edit_main_menu_in_place(client, query, user, lang: str):
    """
    Edit current message back to main menu when possible.
    Supports random media rotation on navigation.
    """
    bt = await db.db.premium_bots.find_one({"id": client.me.id})
    bt_cfg = bt.get("config", {}) if bt else {}
    bot_name = client.me.first_name
    msg_txt = _menu_card_text(user, bt_cfg, bot_name, lang)
    markup = _get_premium_menu_markup(bt_cfg, lang)

    # Media rotation on navigation
    items = [x for x in _cfg_list(bt_cfg, "menu_media") if isinstance(x, dict) and x.get("file_id")]
    if not items and (bt_cfg.get("menuimg") or "").strip():
        items = [{"type": "photo", "file_id": (bt_cfg.get("menuimg") or "").strip()}]
    
    is_media = bool(getattr(query.message, 'photo', None) or getattr(query.message, 'video', None) or getattr(query.message, 'animation', None))
    
    if items and is_media:
        import random
        media_item = random.choice(items)
        t = (media_item.get("type") or "photo").strip()
        fid = (media_item.get("file_id") or "").strip()
        
        try:
            input_media = None
            if t == "animation":
                input_media = InputMediaAnimation(fid, caption=msg_txt, parse_mode=enums.ParseMode.HTML)
            elif t == "video":
                input_media = InputMediaVideo(fid, caption=msg_txt, parse_mode=enums.ParseMode.HTML)
            else:
                input_media = InputMediaPhoto(fid, caption=msg_txt, parse_mode=enums.ParseMode.HTML)

            await query.message.edit_media(media=input_media, reply_markup=markup)
            return
        except Exception as e:
            # Fallback if media edit fails (e.g. invalid file_id)
            logger.warning(f"Failed to rotate media on edit: {e}")

    res = await _safe_edit(query.message, text=msg_txt, markup=markup)
    if not res:
        await _send_main_menu(client, query.from_user.id, user, lang)


async def _safe_edit(msg, *, text: str, markup: InlineKeyboardMarkup):
    is_media = bool(getattr(msg, 'photo', None) or getattr(msg, 'video', None) or getattr(msg, 'animation', None) or getattr(msg, 'document', None))
    try:
        if is_media:
            return await msg.edit_caption(caption=text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
        return await msg.edit_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
    except MessageNotModified:
        return None
    except Exception as e:
        logger.warning(f"Safe edit failed: {e}")
        return None


async def _send_main_menu(client, user_id: int, user, lang: str, reply_to_message_id: int = None):
    bt = await db.db.premium_bots.find_one({"id": client.me.id})
    bt_cfg = bt.get("config", {}) if bt else {}
    bot_name = client.me.first_name
    msg_txt = _menu_card_text(user, bt_cfg, bot_name, lang)
    markup = _get_premium_menu_markup(bt_cfg, lang)

    # Menu media rotation: supports Photo / GIF / Video.
    items = [x for x in _cfg_list(bt_cfg, "menu_media") if isinstance(x, dict) and x.get("file_id")]
    if not items and (bt_cfg.get("menuimg") or "").strip():
        # Backward compatible
        items = [{"type": "photo", "file_id": (bt_cfg.get("menuimg") or "").strip()}]

    if items:
        import random
        random.shuffle(items)
        for it in items[:30]:
            t = (it.get("type") or "photo").strip()
            fid = (it.get("file_id") or "").strip()
            if not fid:
                continue
            try:
                if t == "animation":

                    return await client.send_animation(
                        user_id,
                        animation=fid,
                        caption=msg_txt,
                        reply_markup=markup,
                        parse_mode=enums.ParseMode.HTML,
                        reply_to_message_id=reply_to_message_id
                    )
                if t == "video":
                    return await client.send_video(
                        user_id,
                        video=fid,
                        caption=msg_txt,
                        reply_markup=markup,
                        parse_mode=enums.ParseMode.HTML,
                        reply_to_message_id=reply_to_message_id
                    )
                return await client.send_photo(
                    user_id,
                    photo=fid,
                    caption=msg_txt,
                    reply_markup=markup,
                    parse_mode=enums.ParseMode.HTML,
                    reply_to_message_id=reply_to_message_id
                )
            except Exception as e:
                # Auto-heal: remove broken media entries (MEDIA_EMPTY / expired file_id)
                logger.warning(f"Menu media send failed; pruning. type={t} err={e}")
                try:
                    await db.db.premium_bots.update_one(
                        {"id": client.me.id},
                        {"$pull": {"config.menu_media": {"file_id": fid}}}
                    )
                except Exception:
                    pass
                return await client.send_message(user_id, msg_txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML, reply_to_message_id=reply_to_message_id)

    return await client.send_message(user_id, msg_txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML, reply_to_message_id=reply_to_message_id)


def _fmt_delivery_text(tpl: str, user, story, sent_count: int = 0, fail_count: int = 0) -> str:
    safe_tpl = tpl or ""
    return (
        safe_tpl
        .replace("{user_id}", str(user.id if user else ""))
        .replace("{user_name}", (user.first_name or "User") if user else "User")
        .replace("{story}", str(story.get("story_name_en", "Story")))
        .replace("{price}", str(story.get("price", 0)))
        .replace("{sent}", str(sent_count))
        .replace("{failed}", str(fail_count))
    )


async def _delete_later(client, user_id: int, msg_ids: list, wait_seconds: int):
    await asyncio.sleep(wait_seconds)
    for mid in msg_ids:
        try:
            await client.delete_messages(user_id, mid)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────
# Story Detail Preview (shown before T&C on deep links)
# ─────────────────────────────────────────────────────────────────
async def _show_story_preview(client, user_id, story, lang):
    """Show story name, image, description and episode count. User clicks Continue -> T&C."""
    name = story.get(f'story_name_{lang}', story.get('story_name_en', 'Unknown'))
    ep_count = abs(story.get('end_id', 0) - story.get('start_id', 0)) + 1 if story.get('end_id') else "?"
    platform = story.get('platform', 'Other')
    price = story.get('price', 0)
    desc = story.get('description', 'Premium audio story — exclusive content.')
    s_id = str(story['_id'])

    txt = (
        f"<b>📖 {_sc(name)}</b>\n\n"
        f"<b>{_sc('Platform')}:</b> {platform}\n"
        f"<b>{_sc('Episodes')}:</b> ~{ep_count}\n"
        f"<b>{_sc('Price')}:</b> ₹{price}\n\n"
        f"<blockquote expandable>{_sc(desc)}</blockquote>"
    )
    kb = [[InlineKeyboardButton(f"▶️ {_sc('CONTINUE TO PURCHASE')}", callback_data=f"mb#story_preview_continue_{s_id}")]]

    img = story.get('image_url')
    if img:
        try:
            await client.send_photo(
                user_id,
                photo=img,
                caption=txt,
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode=enums.ParseMode.HTML
            )
            return
        except Exception:
            pass
    await client.send_message(user_id, txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────────
# T&C (with Accept and Reject buttons - all inline, no native_ask)
# ─────────────────────────────────────────────────────────────────
def to_mathbold(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        "𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗"
    ))

def to_mathitalic(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "𝑎𝑏𝑐𝑑𝑒𝑓𝑔ℎ𝑖𝑗𝑘𝑙𝑚𝑛𝑜𝑝𝑞𝑟𝑠𝑡𝑢𝑣𝑤𝑥𝑦𝑧𝐴𝐵𝐶𝐷𝐸𝐹𝐺𝐻𝐼𝐽𝐾𝐿𝑀𝑁𝑂𝑃𝑄𝑅𝑆𝑇𝑈𝑉𝑊𝑋𝑌𝑍"
    ))

async def _show_story_profile(client, user_id, story, lang):
    name = story.get(f'story_name_{lang}', story.get('story_name_en', 'Unknown'))
    status = story.get('status', 'Unknown')
    platform = story.get('platform', 'Unknown')
    genre = story.get('genre', 'Unknown')
    episodes = story.get('episodes', 'Unknown')
    image = story.get('image')

    if lang == 'hi':
        status_lbl = "स्टेटस"
        plat_lbl = "प्लेटफॉर्म"
        genre_lbl = "जौनर"
        ep_lbl = "एपिसोड्स"
        desc_lbl = "कहानी का विवरण"
        confirm_btn = "✅ आगे बढ़ें"
        back_btn = "❮ वापस"
        loading_txt = "प्रोफाइल लोड हो रही है..."
    else:
        status_lbl = "Status"
        plat_lbl = "Platform"
        genre_lbl = "Genre"
        ep_lbl = "Episodes"
        desc_lbl = "Story Description"
        confirm_btn = f"✅ {_sc('CONFIRM')}"
        back_btn = f"❮ {_sc('BACK')}"
        loading_txt = _sc("LOADING PROFILE...")

    desc = story.get(f'description_{lang}', story.get('description', '')).strip()
    
    delivery_mode = story.get('delivery_mode', 'pool')
    del_hi = "केवल डायरेक्ट DM (कोई चैनल नहीं)" if delivery_mode == "dm_only" else "चैनल लिंक और DM"
    del_en = "Direct DM Only (No Channel Link)" if delivery_mode == "dm_only" else "Channel Invite + DM"
    del_lbl = "डिलीवरी" if lang == "hi" else "Delivery"
    del_val = del_hi if lang == "hi" else del_en
    
    price = int(story.get('price', 0))
    if price > 0:
        if price <= 50: mrp = 149
        elif price <= 100: mrp = 299
        elif price <= 200: mrp = 599
        elif price <= 300: mrp = 899
        else: mrp = int(price * 2.5)
        calc_off = int(((mrp - price) / mrp) * 100)
        p_lbl = "कीमत" if lang == "hi" else "Price"
        # Shopping app style: M.R.P: ̶₹̶̶1̶̶4̶̶9̶ Deal Price: ₹49
        price_line = f"<b>🏷 {p_lbl}:</b> <s>₹{mrp}</s>  <b>₹{price}</b> <i>({calc_off}% OFF)</i>\n"
    else:
        price_line = ""
    
    header_txt = (
        f"<b>♨️ Story:</b> {to_mathbold(name)}\n"
        f"<b>🔰 {status_lbl}:</b> <b>{status}</b>\n"
        f"<b>🖥 {plat_lbl}:</b> <b>{platform}</b>\n"
        f"<b>🧩 {genre_lbl}:</b> <b>{genre}</b>\n"
        f"{price_line}"
        f"<b>🎬 {ep_lbl}:</b> <b>{episodes}</b>\n"
        f"<b>📥 {del_lbl}:</b> <i>{del_val}</i>\n\n"
    )
    if desc and desc.lower() != "none":
        header_txt += (
            f"<b>{desc_lbl}</b>\n"
            f"<blockquote expandable>"
            f"-{to_mathbold(desc)}"
            f"</blockquote>\n"
        )
    txt = header_txt
        
    demo_btn = "👀 डेमो फ़ाइलें देखें" if lang == "hi" else "👀 View Demo Files"
    kb = [
        [InlineKeyboardButton(confirm_btn, callback_data=f"mb#show_tc#{str(story['_id'])}")],
        [InlineKeyboardButton(demo_btn, callback_data=f"mb#demo#{str(story['_id'])}")],
        [InlineKeyboardButton(back_btn, callback_data="mb#return_main")]
    ]
    markup = InlineKeyboardMarkup(kb)
    
    from pyrogram import enums
    tmp = await client.send_message(user_id, f"<b>› › ⏳ {loading_txt}</b>", reply_markup=ReplyKeyboardRemove(), parse_mode=enums.ParseMode.HTML)
    
    try:
        if image:
            try:
                await client.send_photo(user_id, photo=image, caption=txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
                await tmp.delete()
                return
            except Exception: pass
        await client.send_message(user_id, txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
        await tmp.delete()
    except Exception: pass

async def _show_tc(client, user_id, story_id, lang='en'):
    if lang == 'hi':
        tc_title = "<b>⟦ नियम और शर्तें ⟧</b>"
        tc_subtitle = "खरीदने से पहले, कृपया निम्नलिखित पढ़ें और सहमत हों:"
        missing_title = "• <b>गायब एपिसोड</b>"
        missing_desc = "यदि सार्वजनिक रूप से जारी नहीं किया गया तो 3-5 एपिसोड अनुपलब्ध हो सकते हैं। बाद में उपलब्ध होने पर, उन्हें अपने आप जोड़ दिया जाएगा। 5 से अधिक गायब? सहायता से संपर्क करें।"
        quality_title = "• <b>क्वालिटी</b>"
        quality_desc = "पुराने एपिसोड्स की क्वालिटी कम हो सकती है। हम 100% क्वालिटी की गारंटी नहीं दे सकते, लेकिन हमेशा सर्वश्रेष्ठ वर्जन प्रदान करेंगे।"
        order_title = "• <b>एपिसोड क्रम</b>"
        order_desc = "एपिसोड्स कभी-कभी क्रम से बाहर हो सकते हैं। सभी फाइलें आर्या बॉट द्वारा क्लीन और ब्रांडेड हैं।"
        refund_title = "• <b>कोई रिफंड नहीं</b>"
        refund_desc = "भुगतान की पुष्टि होने और डिलीवरी शुरू होने के बाद कोई रिफंड नहीं दिया जाएगा।"
        fake_title = "• <b>नकली स्क्रीनशॉट</b>"
        fake_desc = "नकली या अमान्य भुगतान प्रमाण भेजने पर स्थायी रूप से प्रतिबंध लगा दिया जाएगा।"
        accept_btn = "सहमत हूँ"
        reject_btn = "अस्वीकार"
        back_btn = "‹ वापस"
    else:
        tc_title = "<b>⟦ 𝗧𝗘𝗥𝗠𝗦 & 𝗖𝗢𝗡𝗗𝗜𝗧𝗜𝗢𝗡𝗦 ⟧</b>"
        tc_subtitle = "𝖡𝖾𝖿𝗈𝗋𝖾 𝗉𝗎𝗋𝖼𝗁𝖺𝗌𝗂𝗇𝗀, 𝗉𝗅𝖾𝖺𝗌𝖾 𝗋𝖾𝖺𝖽 𝖺𝗇𝖽 𝖺𝗀𝗋𝖾𝖾 𝗍𝗈 𝗍𝗁𝖾 𝖿𝗈𝗅𝗅𝗈𝗐𝗂𝗇𝗀:"
        missing_title = "• <b>𝗠𝗶𝘀𝘀𝗶𝗻𝗴 𝗘𝗽𝗶𝘀𝗼𝗱𝗲𝘀</b>"
        missing_desc = "𝟹–𝟻 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝖻𝖾 𝗎𝗇𝖺𝗏𝖺𝗂ʟ𝖺𝖻𝗅𝖾 𝗂𝖿 𝗇𝗈𝗍 𝗉𝗎𝖻𝗅𝗂𝖼𝗅𝗒 𝗋𝖾𝗅𝖾𝖺𝗌𝖾𝖽. 𝖨𝖿 𝖺𝗏𝖺𝗂ʟ𝖺𝖻𝗅𝖾 𝗅𝖺𝗍𝖾𝗋, 𝗍𝗁𝖾𝗒 𝗐𝗂𝗅𝗅 𝖻𝖾 𝖺𝖽𝖽𝖾𝖽 𝖺𝗎𝗍𝗈𝗆𝖺𝗍𝗂𝖼𝖺𝗅𝗅𝗒."
        quality_title = "• <b>𝗤𝘂𝗮𝗹𝗶𝘁𝘆</b>"
        quality_desc = "𝖲𝗈𝗆𝖾 𝗈𝗅𝖽𝖾𝗋 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝗁𝖺𝗏𝖾 𝗋𝖾𝖽𝗎𝖼𝖾𝖽 𝗊𝗎𝖺𝗅𝗂𝗍𝗒. 𝖶𝖾 𝖼𝖺𝗇𝗇𝗈𝗍 𝗀𝗎𝖺𝗋𝖺𝗇𝗍𝖾𝖾 𝟣𝟢𝟢% 𝗊𝗎𝖺𝗅𝗂𝗍𝗒, 𝖻𝗎𝗍 𝖺𝗅𝗐𝖺𝗒𝗌 𝗉𝗋𝗈𝗏𝗂𝖽𝖾 𝖻𝖾𝗌𝗍 𝗏𝖾𝗋𝗌𝗂𝗈𝗇."
        order_title = "• <b>𝗘𝗽𝗶𝘀𝗼𝗱𝗲 𝗢𝗿𝗱𝗲𝗿</b>"
        order_desc = "𝖤𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝗋𝖺𝗋𝖾𝗅𝗒 𝖻𝖾 𝗈𝗎𝗍 𝗈𝖿 𝗌𝖾𝗊𝗎𝖾𝗇𝖼𝖾. 𝖠𝗅𝗅 𝖿𝗂𝗅𝖾𝗌 𝖺𝗋𝖾 𝖼𝗅𝖾𝖺𝗇𝖾𝖽 𝖺𝗇𝖽 𝖻𝗋𝖺𝗇𝖽𝖾𝖽 𝖻𝗒 𝖠𝗋𝗒𝖺 𝖡𝗈𝗍."
        refund_title = "• <b>𝗡𝗼 𝗥𝗲𝗳𝘂𝗻𝗱𝘀</b>"
        refund_desc = "𝖭𝗈 𝗋𝖾𝖿𝗎𝗇𝖽𝗌 𝗈𝗇𝖼𝖾 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗂𝗌 𝖼𝗈𝗇𝖿𝗂𝗋𝗆𝖾𝖽 𝖺𝗇𝖽 𝖽𝖾𝗅𝗂𝗏𝖾𝗋𝗒 𝗌𝗍𝖺𝗋𝗍𝗌."
        fake_title = "• <b>𝗙𝗮𝗸𝗲 𝗦𝗰𝗿𝗲𝗲𝗻𝘀𝗵𝗼𝘁𝘀</b>"
        fake_desc = "𝖥𝖺𝗄𝖾 𝗈𝗋 𝗂𝗇𝗏𝖺𝗅𝗂𝖽 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗉𝗋𝗈𝗈𝖿𝗌 𝗐𝗂𝗅𝗅 𝗅𝖾𝖺𝖽 𝗍𝗈 𝗉𝖾𝗋𝗆𝖺𝗇𝖾𝗇𝗍 𝖻𝖺𝗇."
        accept_btn = "𝗜 𝗔𝗰𝗰𝗲𝗽𝘁"
        reject_btn = "𝗥𝗲𝗷𝗲𝗰𝘁"
        back_btn = "‹ Back"

    tc_text = (
        f"{tc_title}\n\n"
        f"{tc_subtitle}\n\n"
        f"<blockquote expandable>{missing_title}\n{missing_desc}</blockquote>\n"
        f"<blockquote expandable>{quality_title}\n{quality_desc}</blockquote>\n"
        f"<blockquote expandable>{order_title}\n{order_desc}</blockquote>\n"
        f"<blockquote expandable>{refund_title}\n{refund_desc}</blockquote>\n"
        f"<blockquote expandable>{fake_title}\n{fake_desc}</blockquote>"
    )
    
    kb = [
        [InlineKeyboardButton(accept_btn, callback_data=f"mb#tc_accept_{story_id}")],
        [InlineKeyboardButton(reject_btn, callback_data="mb#tc_reject"),
         InlineKeyboardButton(back_btn, callback_data=f"mb#view_{story_id}")]
    ]
    from pyrogram import enums
    await client.send_message(user_id, tc_text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=enums.ParseMode.HTML)

def _cfg_list(cfg: dict, key: str):
    v = (cfg or {}).get(key)
    return v if isinstance(v, list) else []


# ─────────────────────────────────────────────────────────────────
# Story Payment Detail
# ─────────────────────────────────────────────────────────────────
def _is_upi_restricted() -> bool:
    """Returns True if current IST time is between 9 PM (21:00) and 6 AM (06:00)."""
    from datetime import timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist)
    h = now_ist.hour
    return h >= 21 or h < 6


async def _show_story_details(client, msg_or_query, story, lang, bot_cfg: dict = None):
    from pyrogram.types import Message
    is_msg = isinstance(msg_or_query, Message)
    bot_cfg = bot_cfg or {}

    name = story.get(f'story_name_{lang}', story.get('story_name_en', 'Unknown'))
    price = int(story.get('price', 1))
    
    if price > 0:
        if price <= 50: mrp = 149
        elif price <= 100: mrp = 299
        elif price <= 200: mrp = 599
        elif price <= 300: mrp = 899
        else: mrp = int(price * 2.5)
        calc_off = int(((mrp - price) / mrp) * 100)
        p_str = f"<s>₹{mrp}</s> <b>₹{price}</b> <i>({calc_off}% OFF)</i>"
    else:
        p_str = f"<b>₹{price}</b>"
    
    if lang == 'hi':
        title = "⟦ सुरक्षित चेकआउट ⟧"
        item_lbl = "आइटम"
        price_lbl = "कुल कीमत"
        desc = "आप इस प्रीमियम कहानी को खरीदने जा रहे हैं। पेमेंट के बाद आपको तुरंत एक्सेस मिल जाएगा।"
        pay_gateway_btn = "पेमेंट गेटवे से भुगतान (Razorpay)"
        pay_upi_btn = "यूपीआई (Manual UPI)"
        unavailable_upi = "यूपीआई भुगतान अभी बंद है।"
        back_btn = "❮ वापस"
    else:
        title = "⟦ 𝗦𝗘𝗖𝗨𝗥𝗘 𝗖𝗛𝗘𝗖𝗞𝗢𝗨𝗧 ⟧"
        item_lbl = "Item"
        price_lbl = "Total Price"
        desc = "𝖠𝗀𝗋𝖾𝖾𝖽 𝖺𝗇𝖽 𝖼𝗈𝗇𝖿𝗂𝗋𝗆𝖾𝖽. 𝖯𝗅𝖾𝖺𝗌𝖾 𝗉𝗋𝗈𝖼𝖾𝖾𝖽 𝗐𝗂𝗍𝗁 𝗍𝗁𝖾 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗍𝗈 𝗎𝗇𝗅𝗈𝖼𝗄 𝗒𝗈𝗎𝗋 𝗌𝗍𝗈𝗋𝗒 𝗂𝗇𝗌𝗍𝖺𝗇𝗍𝗅𝗒."
        pay_gateway_btn = f"💳 {_sc('PAY VIA RAZORPAY')}"
        pay_upi_btn = f"🏦 {_sc('PAY VIA MANUAL UPI')}"
        unavailable_upi = "UPI Currently Unavailable"
        back_btn = f"❮ {_sc('BACK')}"

    txt = (
        f"<b>{title}</b>\n\n"
        f"<b>{item_lbl} :</b> <code>{name}</code>\n"
        f"<b>{price_lbl} :</b> {p_str}\n\n"
        f"{desc}"
    )

    kb = []
    # Razorpay row
    kb.append([InlineKeyboardButton(pay_gateway_btn, callback_data=f"mb#pay#razorpay#{str(story['_id'])}")])
    
    # UPI row
    from .market_seller import _is_upi_restricted
    upi_enabled = bot_cfg.get('upi_enabled', True)
    upi_restricted = _is_upi_restricted()
    if upi_enabled and not upi_restricted:
        kb.append([InlineKeyboardButton(pay_upi_btn, callback_data=f"mb#pay#upi#{str(story['_id'])}")])
    else:
        kb.append([InlineKeyboardButton(f"🚫 {unavailable_upi}", callback_data="mb#noop")])
        
    kb.append([InlineKeyboardButton(back_btn, callback_data="mb#return_main")])
    markup = InlineKeyboardMarkup(kb)

    if is_msg:
        await msg_or_query.reply_text(txt, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
    else:
        await _safe_edit(msg_or_query.message, text=txt, markup=markup)

async def _process_start(client, message):
    user_id = message.from_user.id
    from pyrogram import enums
    
    is_new = await db.db.users.count_documents({"id": int(user_id)}) == 0
    if is_new:
        from utils import log_arya_event
        asyncio.create_task(log_arya_event(
            event_type="NEW USER JOIN",
            user_id=user_id,
            user_info={"first_name": message.from_user.first_name, "last_name": getattr(message.from_user, "last_name", ""), "username": getattr(message.from_user, "username", "")},
            details="User started the Premium Store bot for the first time."
        ))
        
    user = await db.get_user(user_id)
    # Track which delivery bots this user has started
    await db.db.users.update_one({"id": int(user_id)}, {"$addToSet": {"bot_ids": client.me.id}}, upsert=True)
    
    args = message.command

    if 'lang' not in user:
        lang_prompt = (
            "<b>⟦ 𝗦𝗘𝗟𝗘𝗖𝗧 𝗟𝗔𝗡𝗚𝗨𝗔𝗚𝗘 ⟧</b>\n\n"
            "<blockquote expandable>"
            "<i>Choose your preferred language to continue.\n"
            "अपनी भाषा चुनें और आगे बढ़ें।</i>"
            "</blockquote>"
        )
        kb = [[InlineKeyboardButton("• English", callback_data="mb#lang#en"),
               InlineKeyboardButton("• हिंदी", callback_data="mb#lang#hi")]]
        return await message.reply_text(lang_prompt, reply_markup=InlineKeyboardMarkup(kb))

    lang = user.get('lang', 'en')

    # ── Force Join Logic (Unicode only, no emojis) ──
    INVITE_CHANNEL = "https://t.me/AryaPremiumTG"
    try:
        chat_member = await client.get_chat_member("@AryaPremiumTG", user_id)
        if chat_member.status in (enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT):
            raise Exception("Not joined")
    except Exception:
        if lang == 'hi':
            join_title = "𝗧𝗘𝗟𝗘𝗚𝗥𝗔𝗠 𝗖𝗛𝗔𝗡𝗡𝗘𝗟 𝗝𝗢𝗜𝗡 𝗞𝗔𝗥𝗘𝗡"
            join_txt = (
                "𝗕𝗼𝘁 𝗸𝗼 𝘂𝘀𝗲 𝗸𝗮𝗿𝗻𝗲 𝗸𝗲 𝗹𝗶𝘆𝗲 𝗮𝗮𝗽𝗸𝗼 𝗵𝘂𝗺𝗮𝗿𝗲 𝗰𝗵𝗮𝗻𝗻𝗲𝗹 𝗺𝗲𝗶𝗻 𝗷𝗼𝗶𝗻 𝗵𝗼𝗻𝗮 𝗵𝗼𝗴𝗮।\n\n"
                "<blockquote expandable>"
                "𝗝𝗼𝗶𝗻 𝗸𝗮𝗿𝗻𝗲 𝗸𝗲 𝗯𝗮𝗮𝗱 '𝗝𝗼𝗶𝗻𝗲𝗱' 𝗽𝗮𝗿 𝗰𝗹𝗶𝗰𝗸 𝗸𝗮𝗿𝗲𝗻। 𝗜𝘀𝘀𝗲 𝗮𝗮𝗽𝗸𝗼 𝘀𝗮𝗯𝗵𝗶 𝗮𝗱𝘃𝗮𝗻𝗰𝗲𝗱 𝗳𝗲𝗮𝘁𝘂𝗿𝗲𝘀 𝗮𝘂𝗿 𝘂𝗽𝗱𝗮𝘁𝗲𝘀 𝗺𝗶𝗹𝘁𝗲 𝗿𝗮𝗵𝗲𝗻𝗴𝗲।\n"
                "</blockquote>"
            )
            join_btn = "✓ 𝗝𝗢𝗜𝗡 𝗖𝗛𝗔𝗡𝗡𝗘𝗟"
            joined_btn = "✓ 𝗝𝗢𝗜𝗡 𝗞𝗔𝗥 𝗟𝗜𝗬𝗔"
        else:
            join_title = "𝗝𝗢𝗜𝗡 𝗢𝗨𝗥 𝗖𝗛𝗔𝗡𝗡𝗘𝗟"
            join_txt = (
                "𝗬𝗼𝘂 𝗺𝘂𝘀𝘁 𝗷𝗼𝗶𝗻 𝗼𝘂𝗿 𝗧𝗲𝗹𝗲𝗴𝗿𝗮𝗺 𝗰𝗵𝗮𝗻𝗻𝗲𝗹 𝘁𝗼 𝘂𝘀𝗲 𝘁𝗵𝗶𝘀 𝗯𝗼𝘁.\n\n"
                "<blockquote expandable>"
                "𝗔𝗳𝘁𝗲𝗿 𝗷𝗼𝗶𝗻𝗶𝗻𝗴, 𝗰𝗹𝗶𝗰𝗸 '𝗝𝗼𝗶𝗻𝗲𝗱' 𝘁𝗼 𝗰𝗼𝗻𝘁𝗶𝗻𝘂𝗲. 𝗬𝗼𝘂 𝘄𝗶𝗹𝗹 𝗴𝗲𝘁 𝗮𝗰𝗰𝗲𝘀𝘀 𝘁𝗼 𝗮𝗹𝗹 𝗽𝗿𝗲𝗺𝗶𝘂𝗺 𝘀𝘁𝗼𝗿𝗶𝗲𝘀 𝗮𝗻𝗱 𝗶𝗻𝘀𝘁𝗮𝗻𝘁 DELIVERY."
                "</blockquote>"
            )
            join_btn = "✓ 𝗝𝗢𝗜𝗡 𝗖𝗛𝗔𝗡𝗡𝗘𝗟"
            joined_btn = "✓ 𝗝𝗢𝗜𝗡𝗘𝗗"

        join_kb = [
            [InlineKeyboardButton(join_btn, url=INVITE_CHANNEL)],
            [InlineKeyboardButton(joined_btn, callback_data="mb#joined_check")]
        ]
        return await message.reply_text(f"<b>{join_title}</b>\n\n{join_txt}", reply_markup=InlineKeyboardMarkup(join_kb))

    # ── Deep Link Handler ──
    if len(args) > 1 and args[1].startswith("buy_"):
        story_id = args[1].replace("buy_", "")
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(story_id)})
        if story:
            has_paid = await db.has_purchase(user_id, story_id)
            if has_paid:
                msg = t["already_owned"]
                await message.reply_text(msg)
                return await dispatch_delivery_choice(client, user_id, story)
            return await _show_story_profile(client, user_id, story, lang)

    # Standard Main Menu
    wait_msg_txt = "WAIT A SECOND..." if lang == 'en' else "कृपया प्रतीक्षा करें..."
    wait_msg = await message.reply_text(f"<b>› › ⏳ {wait_msg_txt}</b>", parse_mode=enums.ParseMode.HTML)
    await asyncio.sleep(0.4)
    await wait_msg.delete()

    await _send_main_menu(client, user_id, message.from_user, lang, reply_to_message_id=message.id)

async def _process_my_stories(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    
    # Send a fresh "My Stories" menu
    purchases = user.get('purchases', [])
    from bson.objectid import ObjectId

    PAGE_SIZE = 5
    page = 0
    # Deduplicate for UI
    unique_p = []
    seen = set()
    for p in purchases:
        p_id = str(p)
        if p_id not in seen:
            unique_p.append(p)
            seen.add(p_id)
    purchases = unique_p
    total = len(purchases)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page_purchases = purchases[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    kb = []
    for pid in page_purchases:
        try:
            st = await db.db.premium_stories.find_one({"_id": ObjectId(pid)})
            if st:
                en_name = st.get('story_name_en', 'Story')
                hi_name = st.get('story_name_hi', en_name)
                # Display only the relevant language name
                s_name = f"📖 {hi_name if lang == 'hi' else en_name}"
                kb.append([InlineKeyboardButton(s_name, callback_data=f"mb#purchased_view_{pid}")])
        except Exception: pass

        except Exception:
            pass

    if lang == 'hi':
        title = "⟦ मेरी स्टोरीज ⟧"
        total_txt = "कुल स्टोरी ⟶"
        desc = "आपके अकाउंट में मौजूद सभी स्टोरीज नीचे दी गई हैं। किसी भी स्टोरी को देखने या दोबारा एक्सेस करने के लिए उसे चुनें।"
        next_btn = "आगे ❭"
        prev_btn = "❬ पीछे"
        back_btn = "« वापस मेनू"
        empty_txt = "कोई खरीद नहीं मिली। स्टोर देखें।"
        market_btn = "स्टोर खोलें"
    else:
        title = "⟦ 𝗠𝗬 𝗦𝗧𝗢𝗥𝗜𝗘𝗦 ⟧"
        total_txt = "ᴛᴏᴛᴀʟ ⟶"
        desc = "𝖠𝗅𝗅 𝗌𝗍𝗈𝗋𝗂𝖾𝗌 𝗅𝗂𝗌𝗍𝖾𝖽 𝖻𝖾𝗅𝗈𝗐 𝖺𝗋𝖾 𝖺𝗅𝗋𝖾𝖺𝖽𝗒 𝗈𝗇 𝗒𝗈𝗎𝗋 𝖺𝖼𝖼𝗈𝗎𝗇𝗍. 𝖲𝖾𝗅𝖾𝖼𝗍 𝖺𝗇𝗒 𝗌𝗍𝗈𝗋𝗒 𝗍𝗈 𝗏𝗂𝖾𝗐 𝖽𝖾𝗍𝖺𝗂𝗅𝗌."
        next_btn = "𝗡𝗲𝘅𝘁 ❭"
        prev_btn = "❬ 𝗣𝗿𝗲𝘃"
        back_btn = "Back to Menu"
        empty_txt = "ɴᴏ ᴘᴜʀᴄʜᴀꜱᴇꜱ ꜰᴏᴜɴᴅ."
        market_btn = "OPEN MARKETPLACE"

    if total_pages > 1:
        nav = []
        nav.append(InlineKeyboardButton(f"ᴘᴀɢᴇ 1/{total_pages}", callback_data="mb#noop"))
        nav.append(InlineKeyboardButton(next_btn, callback_data="mb#my_buys_page_1"))
        kb.append(nav)

    kb.append([InlineKeyboardButton(back_btn, callback_data="mb#main_back")])

    if total > 0:
        txt_b = (
            f"<b>{title}</b>\n\n"
            f"<b>{total_txt}</b> {total}\n\n"
            f"{desc}"
        )
    else:
        txt_b = (
            f"<b>{title}</b>\n\n"
            f"<b>{total_txt}</b> 0\n\n"
            f"{empty_txt}"
        )
        kb.insert(0, [InlineKeyboardButton(market_btn, callback_data="mb#main_marketplace")])

    await client.send_message(user_id, txt_b, reply_markup=InlineKeyboardMarkup(kb))

async def _process_text(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    txt = message.text.strip()

    # Intercept direct section commands
    cmd_text = txt.lower()
    if cmd_text in ["/marketplace", "/mystories", "/stories", "/arya", "/help", "/settings", "/profile"]:
        m = await message.reply_text("<i>⏳ Loading...</i>")
        
        class MockQuery:
            def __init__(self, msg, user, data):
                self.message = msg
                self.from_user = user
                self.data = data
            async def answer(self, text="", show_alert=False):
                pass
                
        mapping = {
            "/marketplace": "mb#main_marketplace",
            "/mystories": "mb#my_buys",
            "/stories": "mb#my_buys",
            "/arya": "mb#about_arya_0",
            "/help": "mb#main_help",
            "/settings": "mb#main_settings",
            "/profile": "mb#main_profile"
        }
        
        if cmd_text in mapping:
            return await _process_callback(client, MockQuery(m, message.from_user, mapping[cmd_text]))

    # Handle DM Part Selection
    pending_s_id = user.get("dm_story_id_pending")
    if pending_s_id and ("files " in txt.lower() or "फ़ाइलें " in txt.lower() or "full delivery" in txt.lower() or "सभी फ़ाइलें" in txt.lower() or "cancel" in txt.lower() or "रद्द" in txt.lower()):
        await db.db.users.update_one({"id": user_id}, {"$unset": {"dm_story_id_pending": 1}})
        
        if "cancel" in txt.lower() or "रद्द" in txt.lower():
            return await message.reply_text("<i>❌ Delivery Selection Cancelled.</i>", reply_markup=ReplyKeyboardRemove())
        
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(pending_s_id)})
        if story:
            start_id = story.get("start_id")
            end_id = story.get("end_id")
            c_start, c_end = start_id, end_id
            
            import re
            match = re.search(r"(\d+)\s*-\s*(\d+)", txt)
            if match:
                fs, fe = int(match.group(1)), int(match.group(2))
                c_start = start_id + fs - 1
                c_end = min(start_id + fe - 1, end_id)
                
            m = await message.reply_text(f"<i>⏳ Initializing DM Delivery (Files {fs if match else 1}-{fe if match else 'All'})... Preparing your files.</i>", reply_markup=ReplyKeyboardRemove())
            return asyncio.create_task(_do_dm_delivery(client, user_id, story, m, c_start, c_end))

    # Back to main menu
    if "𝗕𝗮𝗰𝗸 𝘁𝗼 𝗠𝗲𝗻𝘂" in txt or "BACK TO MAIN MENU" in txt:
        m = await message.reply_text("<i>⏳ Loading...</i>", reply_markup=ReplyKeyboardRemove())
        try:
            await m.delete()
        except:
            pass
        await _send_main_menu(client, user_id, message.from_user, lang)
        return

    # Check if it's a story selection e.g. "1. STORY NAME [ ₹ 49 ]"
    if " [ ₹ " in txt and txt.endswith(" ]"):
        parts = txt.split(". ", 1)
        raw = parts[1] if len(parts) > 1 else txt
        sName = raw.split(" [ ₹ ")[0].strip()
        stories = await db.db.premium_stories.find({"bot_id": client.me.id}).to_list(length=None)
        story = None
        for st in stories:
            candidates = [
                st.get("story_name_en", ""),
                st.get("story_name_hi", ""),
                st.get("story_name_hi", ""),
            ]
            if sName in candidates:
                story = st
                break
        if not story:
            return await message.reply_text("<i>Story not found or removed.</i>")

        has_paid = await db.has_purchase(user_id, str(story['_id']))
        if has_paid:
            t=T[lang]
            await message.reply_text(t["already_owned"], reply_markup=ReplyKeyboardRemove())
            return await dispatch_delivery_choice(client, user_id, story)

        return await _show_story_profile(client, user_id, story, lang)

    # Platform selection
    platforms = await db.db.premium_stories.distinct('platform', {"bot_id": client.me.id})
    platforms.append("Other")

    if txt in platforms:
        query_find = {"bot_id": client.me.id}
        if txt != "Other": query_find["platform"] = txt
        stories = await db.db.premium_stories.find(query_find).to_list(length=None)
        if not stories:
            return await message.reply_text("<i>No stories found for this platform.</i>")

        kb = []
        for idx, s in enumerate(stories, start=1):
            s_name = s.get(f'story_name_{lang}', s.get('story_name_en'))
            kb.append([f"{idx}. {s_name} [ ₹ {s.get('price', 0)} ]"])
        kb.append(["🔍 " + ("SEARCH" if lang=='en' else "खोजें")])
        kb.append([T[lang]["cant_find_btn"]])
        kb.append(["« " + ("𝗕𝗮𝗰𝗸 𝘁𝗼 𝗠𝗲𝗻𝘂" if lang=='en' else "वापस मेनू")])

        t = T[lang]
        title = "AVAILABLE STORIES" if lang == 'en' else "उपलब्ध स्टोरिज"
        desc = (
            f"All available stories and their prices are shown in the menu below. "
            f"Please tap or click on any story name from the keyboard menu below to view details and purchase it:"
        ) if lang == 'en' else (
            f"सभी उपलब्ध कहानियाँ और उनकी कीमतें नीचे मेनू में दिखाई गई हैं। "
            f"विवरण देखने और इसे खरीदने के लिए कृपया नीचे दिए गए कीबोर्ड मेनू से किसी भी कहानी के नाम पर टैप या क्लिक करें:"
        )
        await message.reply_text(
            f"<b>⟦ {title} — {to_mathbold(txt)} ⟧</b>\n\n"
            f"<blockquote expandable>"
            f"<i>{desc}</i>\n"
            f"</blockquote>",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
        )
        return
        
    # ── REQUEST STORY trigger ──
    if txt == T[lang]["cant_find_btn"]:
        try:
            from utils import native_ask, log_arya_event
            from datetime import datetime, timezone
            
            # Step 1
            ans1 = await native_ask(client, user_id, "<b>Step 1/3:</b>\nPlease enter the <b>Story Name</b> you want to request:", reply_markup=ReplyKeyboardMarkup([["« Cancel"]], resize_keyboard=True))
            if ans1.text.startswith("« "):
                 await client.send_message(user_id, "<i>❌ Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
                 return await _send_main_menu(client, user_id, message.from_user, lang)
            str_name = ans1.text.strip()
            
            # Step 2
            ans2 = await native_ask(client, user_id, "<b>Step 2/3:</b>\nPlease enter the <b>Platform Name</b> (e.g. Pocket FM, Kuku FM):", reply_markup=ReplyKeyboardMarkup([["« Cancel"]], resize_keyboard=True))
            if ans2.text.startswith("« "):
                 await client.send_message(user_id, "<i>❌ Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
                 return await _send_main_menu(client, user_id, message.from_user, lang)
            str_plat = ans2.text.strip()
            
            # Step 3
            ans3 = await native_ask(client, user_id, "<b>Step 3/3:</b>\nIs the story <b>Ongoing</b> or <b>Complete</b>?", reply_markup=ReplyKeyboardMarkup([["Ongoing", "Complete"], ["« Cancel"]], resize_keyboard=True))
            if ans3.text.startswith("« "):
                 await client.send_message(user_id, "<i>❌ Process Cancelled!</i>", reply_markup=ReplyKeyboardRemove())
                 return await _send_main_menu(client, user_id, message.from_user, lang)
            str_status = ans3.text.strip()
            
            m_proc = await message.reply_text("<i>Processing Request...</i>", reply_markup=ReplyKeyboardRemove())
            
            # Save to MongoDB
            req_doc = {
                "user_id": user_id,
                "bot_id": client.me.id,
                "story_name": str_name,
                "platform": str_plat,
                "completion_type": str_status,
                "status": "Sent",
                "created_at": datetime.now(timezone.utc)
            }
            await db.db.premium_requests.insert_one(req_doc)
            
            await log_arya_event(
                "NEW STORY REQUEST", user_id, user, 
                f"<b>Story:</b> {str_name}\n<b>Platform:</b> {str_plat}\n<b>Type:</b> {str_status}\n<b>Bot ID:</b> <code>{client.me.id}</code>"
            )
            
            await m_proc.delete()
            await client.send_message(user_id, "✅ <b>Request Submitted Successfully!</b>\n\nYou can check the status of your request in the <b>Profile -> My Requests</b> section.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« " + _sc("BACK TO MENU"), callback_data="mb#main_back")]]))
            
        except asyncio.TimeoutError:
            await client.send_message(user_id, "⏳ Request timed out.", reply_markup=ReplyKeyboardRemove())
            await _send_main_menu(client, user_id, message.from_user, lang)
        return

    # ── SEARCH trigger ──
    if txt == "🔍 " + ("SEARCH" if lang=='en' else "खोजें"):
        await message.reply_text(
            f"<b>🔍 SEARCH</b>\n\n<i>Type a few words of the story name to search:</i>",
            reply_markup=ReplyKeyboardMarkup([["« " + "CANCEL"]], resize_keyboard=True)
        )
        await db.update_user(user_id, {"state": "searching"})
        return

    # ── CANCEL search ──
    if txt == "« " + "CANCEL" or (user.get("state") == "searching" and txt.startswith("«")):
        await db.update_user(user_id, {"state": None})
        m = await message.reply_text("<i>❌ Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        await asyncio.sleep(1.5)
        try: await m.delete()
        except: pass
        await _send_main_menu(client, user_id, message.from_user, lang)
        return

    # ── SEARCH query matching ──
    if user.get("state") == "searching":
        q = txt.lower().strip()
        if not q or len(q) < 2:
            return await message.reply_text("<i>Please type at least 2 characters to search.</i>")
        all_stories = await db.db.premium_stories.find({"bot_id": client.me.id}).to_list(length=None)
        matches = [s for s in all_stories if q in s.get("story_name_en", "").lower() or q in s.get("story_name_hi", "").lower()]
        if not matches:
            return await message.reply_text(f"<i>No stories matched '<b>{txt}</b>'. Try different keywords.</i>")
        kb = []
        for idx, s in enumerate(matches, start=1):
            s_name = s.get(f'story_name_{lang}', s.get('story_name_en'))
            kb.append([f"{idx}. {s_name} [ ₹ {s.get('price', 0)} ]"])
        kb.append(["« " + "CANCEL"])
        await message.reply_text(
            f"<b>🔍 {_sc('Search Results')} ({len(matches)})</b>\n\n"
            f"<blockquote expandable>{_sc('Tap on a story name from the keyboard menu below to view its details and purchase options.')}</blockquote>",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True),
            parse_mode=enums.ParseMode.HTML
        )
        return


async def _show_about_arya(client, query, page: int):
    if page == 0:
        txt = (
            f"<b>⟦ {_sc('ABOUT ARYA PREMIUM')} ⟧</b>\n\n"
            f"<blockquote expandable>"
            f"<i>{_sc('Welcome to Arya Premium — the ultimate, fully automated storefront for exclusive, high-quality stories.')}</i>"
            f"</blockquote>\n\n"
            f"<blockquote expandable>"
            f"<u>{_sc('WHAT IT IS:')}</u>\n"
            f"{_sc('Arya Premium is a state-of-the-art paid content delivery ecosystem. It enables users to browse, purchase, and instantly receive premium stories without any manual intervention.')}"
            f"</blockquote>\n\n"
            f"<blockquote expandable>"
            f"<u>{_sc('HOW IT WORKS:')}</u>\n"
            f"• {_sc('Browse the Marketplace to find your desired story.')}\n"
            f"• {_sc('Make a secure payment via automatic gateways (Razorpay) or manual UPI.')}\n"
            f"• {_sc('Upon successful validation, choose your preferred delivery method (Direct DM or Secure Channel).')}"
            f"</blockquote>\n\n"
            f"<blockquote expandable>"
            f"<u>{_sc('CORE FEATURES:')}</u>\n"
            f"• <b>{_sc('Instant Access:')}</b> {_sc('The moment your payment is verified, the content is unlocked forever.')}\n"
            f"• <b>{_sc('Permanent Library:')}</b> {_sc('All your purchases are safely stored in ')}<b>{_sc('My Stories')}</b>. {_sc('You never lose access.')}\n"
            f"• <b>{_sc('Seamless Experience:')}</b> {_sc('Clean UI, fast response times, and high-quality file delivery.')}"
            f"</blockquote>"
        )
        kb = [
            [InlineKeyboardButton(f"ɴᴇxᴛ ❭", callback_data="mb#about_arya_1")],
            [InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data="mb#main_back")]
        ]
    else:
        txt = (
            f"<b>⟦ {_sc('ABOUT ARYA BOT (PARENT)')} ⟧</b>\n\n"
            f"<blockquote expandable>"
            f"<i>{_sc('Arya Premium is proudly powered by the Main Arya Bot architecture — a trusted name in Telegram automation.')}</i>"
            f"</blockquote>\n\n"
            f"<blockquote expandable>"
            f"<u>{_sc('WHAT IT IS:')}</u>\n"
            f"{_sc('The parent Arya Bot is a highly advanced file management and delivery juggernaut, built to handle massive loads and complex operations.')}"
            f"</blockquote>\n\n"
            f"<blockquote expandable>"
            f"<u>{_sc('WHY CHOOSE US:')}</u>\n"
            f"• <b>{_sc('Instant Delivery:')}</b> {_sc('High-speed servers ensure files are forwarded to you with zero lag.')}\n"
            f"• <b>{_sc('Fully Automatic:')}</b> {_sc('No waiting for human admins. Everything is handled securely by code.')}\n"
            f"• <b>{_sc('Trusted Service:')}</b> {_sc('Used by thousands to manage and deliver files reliably every single day.')}"
            f"</blockquote>\n\n"
            f"<blockquote expandable>"
            f"<u>{_sc('FEATURES:')}</u>\n"
            f"• <b>{_sc('Batch Links:')}</b> {_sc('Group hundreds of files securely for public or private sharing.')}\n"
            f"• <b>{_sc('Live Sync:')}</b> {_sc('Real-time mirroring across multiple channels.')}\n"
            f"• <b>{_sc('Smart Management:')}</b> {_sc('Auto-approve logic, Force Subscribe walls, and deep user analytics.')}"
            f"</blockquote>"
        )
        kb = [
            [InlineKeyboardButton(f"❬ ᴘʀᴇᴠ", callback_data="mb#about_arya_0")],
            [InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data="mb#main_back")]
        ]

    await _safe_edit(query.message, text=txt, markup=InlineKeyboardMarkup(kb))


async def _show_help_menu(client, query, page: int):
    if page == 0:
        txt = (
            f"<b>⟦ {_sc('HELP & DOCUMENTATION')} ⟧</b>\n\n"
            f"<blockquote expandable>"
            f"<i>{_sc('Welcome to the detailed guide for using Arya Premium.')}</i>\n\n"
            f"<u>{_sc('COMMANDS:')}</u>\n"
            f"• /start — {_sc('Launches the main interface menu.')}\n\n"
            f"<u>{_sc('MENU BUTTONS:')}</u>\n"
            f"• <b>{_sc('Marketplace:')}</b> {_sc('Browse all available stories filtered by platform.')}\n"
            f"• <b>{_sc('My Stories:')}</b> {_sc('Access your previously purchased stories. You can instantly redownload them from here.')}\n"
            f"• <b>{_sc('Profile:')}</b> {_sc('View your account details, Telegram ID, and purchase count.')}\n"
            f"• <b>{_sc('Settings:')}</b> {_sc('Change your preferred bot language.')}\n\n"
            f"<u>{_sc('HOW TO BUY:')}</u>\n"
            f"<i><b>1.</b></i> {_sc('Find a story in the Marketplace.')}\n"
            f"<i><b>2.</b></i> {_sc('Choose to pay securely via Razorpay (Instant) or Manual UPI.')}\n"
            f"<i><b>3.</b></i> {_sc('For UPI, send the exact amount to the provided details and upload your screenshot.')}\n"
            f"<i><b>4.</b></i> {_sc('Once verified, tap ')}<b>{_sc('Get Delivery')}</b> {_sc('and choose your method (Direct DM or Secure Channel Invite).')}\n\n"
            f"<i>{_sc('For technical issues, use the Terms & Refund buttons below.')}</i>"
            f"</blockquote>"
        )
        kb = [
            [InlineKeyboardButton(f"{_sc('TERMS')}", callback_data="mb#help_tc"),
             InlineKeyboardButton(f"{_sc('REFUND')}", callback_data="mb#help_refund")],
            [InlineKeyboardButton(f"हिंदी (NEXT) ❭", callback_data="mb#help_page_1")],
            [InlineKeyboardButton(f"« ❮ {_sc('MAIN MENU')}", callback_data="mb#main_back")]
        ]
    else:
        txt = (
            f"<b>⟦ {_sc('सहायता और जानकारी')} ⟧</b>\n\n"
            f"<blockquote expandable>"
            f"<i>{_sc('आर्या प्रीमियम के विस्तृत गाइड में आपका स्वागत है।')}</i>\n\n"
            f"<u>{_sc('कमांड्स:')}</u>\n"
            f"• /start — {_sc('मुख्य मेनू खोलने के लिए।')}\n\n"
            f"<u>{_sc('मेनू बटन:')}</u>\n"
            f"• <b>{_sc('Marketplace:')}</b> {_sc('पसंदीदा प्लेटफार्म द्वारा सभी कहानियों को ब्राउज़ करें।')}\n"
            f"• <b>{_sc('My Stories:')}</b> {_sc('अपनी खरीदी हुई कहानियों तक पहुँचें और उन्हें फिर से डाउनलोड करें।')}\n"
            f"• <b>{_sc('Profile:')}</b> {_sc('अपनी खाता जानकारी और कुल खरीदारी देखें।')}\n"
            f"• <b>{_sc('Settings:')}</b> {_sc('अपनी पसंदीदा भाषा बदलें।')}\n\n"
            f"<u>{_sc('कहानी कैसे खरीदें:')}</u>\n"
            f"<i><b>1.</b></i> {_sc('Marketplace में कहानी चुनें।')}\n"
            f"<i><b>2.</b></i> {_sc('Razorpay (ऑटोमैटिक) या UPI (मैन्युअल) द्वारा सुरक्षित भुगतान करें।')}\n"
            f"<i><b>3.</b></i> {_sc('UPI के मामले में सही राशि भेजें और अपनी रसीद/स्क्रीनशॉट अपलोड करें।')}\n"
            f"<i><b>4.</b></i> {_sc('वेरिफिकेशन पूरा होने के बाद, ')}<b>{_sc('Get Delivery')}</b> {_sc('चुनें (सीधा DM या चैनल लिंक)।')}\n\n"
            f"<i>{_sc('किसी भी समस्या के लिए नीचे दिए गए Terms या Refund बटन का उपयोग करें।')}</i>"
            f"</blockquote>"
        )
        kb = [
            [InlineKeyboardButton(f"{_sc('TERMS')}", callback_data="mb#help_tc"),
             InlineKeyboardButton(f"{_sc('REFUND')}", callback_data="mb#help_refund")],
            [InlineKeyboardButton(f"❬ PREV (English)", callback_data="mb#help_page_0")],
            [InlineKeyboardButton(f"« ❮ {_sc('MAIN MENU')}", callback_data="mb#main_back")]
        ]
        
    await _safe_edit(query.message, text=txt, markup=InlineKeyboardMarkup(kb))


# ─────────────────────────────────────────────────────────────────
# Callback Handler
# ─────────────────────────────────────────────────────────────────
async def _process_callback(client, query):
    user_id = query.from_user.id
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    data = query.data.split('#')
    cmd = data[1]

    # ── Joined Check ──
    if cmd == "joined_check":
        try:
            from pyrogram import enums
            chat_member = await client.get_chat_member("@AryaPremiumTG", user_id)
            if chat_member.status not in (enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT):
                msg = "✓ Joined Success!" if lang == 'en' else "✓ आपने सफलतापूर्वक ज्वाइन कर लिया है!"
                await query.answer(msg, show_alert=True)
                try: await query.message.delete()
                except: pass
                return await _send_main_menu(client, user_id, query.from_user, lang)
            else:
                msg = "Aapne abhi tak join nahi kiya hai। Kripya join karein aur phir check karein।" if lang == 'hi' else "You haven't joined yet. Please join the channel first."
                return await query.answer(msg, show_alert=True)
        except Exception:
            return await query.answer("Error checking status. Make sure you joined.", show_alert=True)


    # ── Skip Channel Prompt ──
    if cmd == "skip_channel_prompt":
        await query.answer()
        try: await query.message.delete()
        except: pass
        return await _send_main_menu(client, user_id, query.from_user, lang)

    # ── About Arya ──
    if cmd.startswith("about_arya_"):
        page = int(cmd.replace("about_arya_", ""))
        await query.answer()
        return await _show_about_arya(client, query, page)

    # ── Help Menu Pagination ──
    if cmd.startswith("help_page_"):
        page = int(cmd.replace("help_page_", ""))
        await query.answer()
        return await _show_help_menu(client, query, page)

    # ── Main Menu actions (inline buttons) ──
    if cmd.startswith("main_"):
        action = cmd.replace("main_", "")
        await query.answer()

        if action == "marketplace":
            platforms = await db.db.premium_stories.distinct('platform', {"bot_id": client.me.id})
            t = T[lang]
            kb = []
            for i in range(0, len(platforms), 2):
                row = platforms[i:i+2]
                kb.append(row)
            if "Other" not in platforms:
                kb.append(["Other"])
            kb.append(["« " + ("𝗕𝗮𝗰𝗸 𝘁𝗼 𝗠𝗲𝗻𝘂" if lang=='en' else "वापस मेनू")])
            
            p_title = "🎧 Platform Selection" if lang == 'en' else "🎧 प्लेटफॉर्म चयन"
            p_desc = "Choose a platform from the keyboard below:" if lang == 'en' else "नीचे दिए गए कीबोर्ड से एक प्लेटफॉर्म चुनें:"
            
            await query.message.delete()
            await client.send_message(
                user_id,
                f"<b>{p_title}</b>\n\n{p_desc}",
                reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
            )

        elif action == "profile":
            u = query.from_user
            joined = user.get('joined_date', 'N/A')
            if isinstance(joined, datetime):
                joined = joined.strftime('%d %b %Y')
            
            # Deduplicate purchases for count
            raw_p = user.get('purchases', [])
            purchases = list(set(str(p) for p in raw_p))
            
            uname = f"@{u.username}" if u.username else "N/A"
            lang_label = "English" if lang == 'en' else "हिंदी"
            name = f"{u.first_name or ''} {u.last_name or ''}".strip() or "Unknown"
            
            t = T[lang]
            txt_p = (
                f"<b>{t['prof_title']}</b>\n\n"
                f"<b>⧉ {t['prof_name']}        ⟶</b> {name}\n"
                f"<b>⧉ {t['prof_uname']}    ⟶</b> {uname}\n"
                f"<b>⧉ {t['prof_id']}       ⟶</b> <code>{u.id}</code>\n\n"
                "<b>╠══════════════════╣</b>\n\n"
                f"<b>⧉ {t['prof_bought']}   ⟶</b> {len(purchases)}\n"
                f"<b>⧉ {t['prof_lang']}    ⟶</b> {lang_label}\n"
                f"<b>⧉ {t['prof_join']}      ⟶</b> {joined}\n\n"
                "<b>╚══════════════════╝</b>"
            )
            kb = [
                [InlineKeyboardButton(t['my_reqs'], callback_data="mb#my_reqs_0")],
                [InlineKeyboardButton(t['set_lang'], callback_data="mb#main_settings")],
                [InlineKeyboardButton("❮ " + t['back'], callback_data="mb#main_back")]
            ]
            await _safe_edit(query.message, text=txt_p, markup=InlineKeyboardMarkup(kb))
            return

        elif action == "settings":
            t = T[lang]
            kb = [
                [InlineKeyboardButton("English", callback_data="mb#lang#en"),
                 InlineKeyboardButton("हिंदी", callback_data="mb#lang#hi")],
                [InlineKeyboardButton("❮ " + t['back'], callback_data="mb#main_back")]
            ]
            await _safe_edit(query.message, text=t['set_prompt'], markup=InlineKeyboardMarkup(kb))

        elif action == "help":
            return await _show_help_menu(client, query, 0)

        elif action == "close":
            await query.message.delete()

        elif action == "back":
            await _edit_main_menu_in_place(client, query, query.from_user, lang)

    elif cmd.startswith("my_reqs_"):
        page = int(cmd.replace("my_reqs_", ""))
        reqs = await db.db.premium_requests.find({"user_id": user_id, "bot_id": client.me.id}).sort("created_at", -1).to_list(length=100)
        t = T[lang]
        if not reqs:
            await query.answer(t['req_empty'], show_alert=True)
            kb = [[InlineKeyboardButton("❮ " + t['back'], callback_data="mb#main_profile")]]
            return await _safe_edit(query.message, text=f"<b>{t['req_main_title']}</b>\n\n{t['req_empty']}", markup=InlineKeyboardMarkup(kb))
        items_per_page = 10
        total_pages = max(1, (len(reqs) + items_per_page - 1) // items_per_page)
        page = max(0, min(page, total_pages - 1))
        subset = reqs[page*items_per_page : (page+1)*items_per_page]
        txt_req = f"<b>{t['req_main_title']} (Page {page+1}/{total_pages})</b>\n\n{t['req_click']}"
        kb = []
        for r in subset:
            sname = r.get('story_name', 'Unknown')
            if len(sname) > 25: sname = sname[:22] + "..."
            status = r.get('status', 'Sent')
            if lang == 'hi':
                status_hi = {"Sent": "भेजा गया", "Pending": "लंबित", "Searching": "ढूंढ रहे हैं", "Posting": "अपलोड हो रहा है", "Posted": "अपलोड हो गया", "Completed": "पूरा हुआ"}.get(status, status)
                label = f"{sname} ({status_hi})"
            else:
                label = f"{sname} ({status})"
            status_emoji = {"Sent": "📮", "Pending": "⏳", "Searching": "🔍", "Posting": "📤", "Posted": "✅", "Completed": "🎉"}.get(status, "📌")
            kb.append([InlineKeyboardButton(f"{status_emoji} {label}", callback_data=f"mb#my_req_{str(r['_id'])}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("❬ Prev" if lang=='en' else "❬ पीछे", callback_data=f"mb#my_reqs_{page-1}"))
        if page < total_pages - 1: nav.append(InlineKeyboardButton("Next ❭" if lang=='en' else "आगे ❭", callback_data=f"mb#my_reqs_{page+1}"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton(t['back_prof'], callback_data="mb#main_profile")])
        await _safe_edit(query.message, text=txt_req, markup=InlineKeyboardMarkup(kb))
        return

    elif cmd.startswith("my_req_"):
        req_id = cmd.replace("my_req_", "")
        try:
            from bson import ObjectId
            r = await db.db.premium_requests.find_one({"_id": ObjectId(req_id), "user_id": user_id})
        except: r = None
        if not r:
            return await query.answer("Request not found.", show_alert=True)
        t_str = r.get("created_at").strftime('%d %b %Y') if r.get("created_at") else "Unknown"
        status = r.get('status', 'Sent')
        txt_d = (
            f"<b>📝 STORY REQUEST DETAILS</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📖 Name:</b> {r.get('story_name')}\n<b>🎧 Platform:</b> {r.get('platform')}\n"
            f"<b>📑 Type:</b> {r.get('completion_type', 'N/A')}\n<b>📅 Date:</b> {t_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n<b>📌 Status:</b> <code>{status}</code>\n"
        )
        msg_sub = {"Sent": "<i>Our team will review your request soon.</i>", "Pending": "<i>Our team will review your request soon.</i>", "Searching": "<i>We are currently looking for this story.</i>", "Posting": "<i>We are uploading this story for you!</i>", "Posted": "<i>We are uploading this story for you!</i>", "Completed": "<i>This story is now available in the marketplace!</i>"}.get(status, "")
        if msg_sub: txt_d += f"\n{msg_sub}"
        kb = [[InlineKeyboardButton("« " + _sc("BACK TO REQUESTS"), callback_data="mb#my_reqs_0")]]
        await _safe_edit(query.message, text=txt_d, markup=InlineKeyboardMarkup(kb))
        return

    elif cmd == "return_main":
        await _edit_main_menu_in_place(client, query, query.from_user, lang)
        return

    elif cmd == "show_tc":
        s_id = data[2]
        try:
            await query.message.delete()
        except:
            pass
        return await _show_tc(client, user_id, s_id, lang)

    elif cmd == "demo":
        s_id = data[2]
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if not story: return await query.answer("Story not found!", show_alert=True)
        await query.answer()
        asyncio.create_task(_send_demo_files(client, user_id, story, lang))

    # -- My Buys (My Stories) --
    elif cmd == "my_buys" or cmd.startswith("my_buys_page_"):
        await query.answer()
        raw_purchases = user.get('purchases', [])
        from bson.objectid import ObjectId
        p_oids = []
        for p in raw_purchases:
            try: p_oids.append(ObjectId(p))
            except: pass
        
        valid_stories_cursor = db.db.premium_stories.find({"_id": {"$in": p_oids}})
        valid_stories = await valid_stories_cursor.to_list(length=1000)
        valid_ids_set = {str(s['_id']) for s in valid_stories}
        
        # DEDUPLICATION: Ensure one entry per unique story ID
        purchases = []
        seen = set()
        for p in raw_purchases:
            pid_str = str(p)
            if pid_str in valid_ids_set and pid_str not in seen:
                purchases.append(p)
                seen.add(pid_str)
        purchases.reverse()

        PAGE_SIZE = 5
        page = 0
        if cmd.startswith("my_buys_page_"):
            try: page = int(cmd.replace("my_buys_page_", ""))
            except: page = 0

        total = len(purchases)
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        page_purchases = purchases[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        kb = []
        for pid in page_purchases:
            try:
                st = next((s for s in valid_stories if str(s['_id']) == str(pid)), None)
                if st:
                    name_en = st.get('story_name_en', 'Story')
                    name_hi = st.get('story_name_hi', name_en)
                    # CLEAN DISPLAY: Only show the selected language version
                    s_name = f"📖 {name_hi if lang == 'hi' else name_en}"
                    kb.append([InlineKeyboardButton(s_name, callback_data=f"mb#purchased_view_{pid}")])
            except Exception: pass

        if lang == 'hi':
            title, total_txt, desc = "⟦ मेरी स्टोरीज ⟧", "कुल स्टोरी ⟶", "आपके अकाउंट में मौजूद सभी स्टोरीज नीचे दी गई हैं।"
            next_btn, prev_btn, back_btn = "आगे ❭", "❬ पीछे", "« वापस मेनू"
            empty_txt, market_btn_l = "कोई खरीद नहीं मिली।", "स्टोर खोलें"
        else:
            title, total_txt, desc = "⟦ 𝗠𝗬 𝗦𝗧𝗢𝗥𝗜𝗘𝗦 ⟧", "ᴛᴏᴛᴀʟ ⟶", "𝖠𝗅𝗅 𝗌𝗍𝗈𝗋𝗂𝖾𝗌 𝗅𝗂𝗌𝗍𝖾𝖽 𝖻𝖾𝗅𝗈𝗐 𝖺𝗋𝖾 𝖺𝗅𝗋𝖾𝖺𝖽𝗒 𝗈𝗇 𝗒𝗈𝗎𝗋 𝖺𝖼𝖼𝗈𝗎𝗇ᴛ."
            next_btn, prev_btn, back_btn = "𝗡𝗲𝘅𝘁 ❭", "❬ 𝗣𝗿𝗲𝘃", _sc("BACK")
            empty_txt, market_btn_l = "ɴᴏ ᴘᴜʀᴄʜᴀꜱᴇꜱ ꜰᴏᴜɴᴅ.", _sc("OPEN MARKETPLACE")

        if total_pages > 1:
            nav = []
            if page > 0: nav.append(InlineKeyboardButton(prev_btn, callback_data=f"mb#my_buys_page_{page - 1}"))
            nav.append(InlineKeyboardButton(f"ᴘᴀɢᴇ {page + 1}/{total_pages}", callback_data="mb#noop"))
            if page < total_pages - 1: nav.append(InlineKeyboardButton(next_btn, callback_data=f"mb#my_buys_page_{page + 1}"))
            kb.append(nav)
        kb.append([InlineKeyboardButton(back_btn, callback_data="mb#main_back")])

        txt_b = f"<b>{title}</b>\n\n<b>{total_txt}</b> {total}\n\n{desc}" if total > 0 else f"<b>{title}</b>\n\n<b>{total_txt}</b> 0\n\n{empty_txt}"
        if total == 0: kb.insert(0, [InlineKeyboardButton(market_btn_l, callback_data="mb#main_marketplace")])
        await _safe_edit(query.message, text=txt_b, markup=InlineKeyboardMarkup(kb))


        # 1. Get raw purchases
        raw_purchases = user.get('purchases', [])
        from bson.objectid import ObjectId

        # 2. Filter VALID stories (handle deleted content)
        # We fetch valid IDs from the database to ensure the count is accurate
        p_oids = []
        for p in raw_purchases:
            try: p_oids.append(ObjectId(p))
            except: pass
        
        valid_stories_cursor = db.db.premium_stories.find({"_id": {"$in": p_oids}})
        valid_stories = await valid_stories_cursor.to_list(length=1000)
        valid_ids_set = {str(s['_id']) for s in valid_stories}
        
        # 3. Maintain user purchase order and filter
        purchases = [p for p in raw_purchases if str(p) in valid_ids_set]
        purchases.reverse() # NEWEST stories on Page 1

        PAGE_SIZE = 5
        page = 0
        if cmd.startswith("my_buys_page_"):
            try:
                page = int(cmd.replace("my_buys_page_", ""))
            except Exception:
                page = 0

        total = len(purchases)
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        page_purchases = purchases[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        kb = []
        for pid in page_purchases:
            try:
                # We already know it exists from the filter step
                st = next((s for s in valid_stories if str(s['_id']) == str(pid)), None)
                if st:
                    s_name = st.get(f'story_name_{lang}', st.get('story_name_en'))
                    kb.append([InlineKeyboardButton(s_name, callback_data=f"mb#purchased_view_{pid}")])
            except Exception:
                pass

        if total_pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton(
                    "❬ ᴘʀᴇᴠ",
                    callback_data=f"mb#my_buys_page_{page - 1}"
                ))
            nav.append(InlineKeyboardButton(
                f"ᴘᴀɢᴇ {page + 1}/{total_pages}",
                callback_data="mb#noop"
            ))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton(
                    "𝗡𝗲𝘅𝘁 ❭",
                    callback_data=f"mb#my_buys_page_{page + 1}"
                ))
            kb.append(nav)

        kb.append([InlineKeyboardButton(_sc("BACK"), callback_data="mb#main_back")])

        if total > 0:
            txt_b = (
                "<b>⟦ 𝗠𝗬 𝗦𝗧𝗢𝗥𝗜𝗘𝗦 ⟧</b>\n\n"
                f"<b>ᴛᴏᴛᴀʟ ⟶</b> {total}\n\n"
                "𝖠𝗅𝗅 𝗌𝗍𝗈𝗋𝗂𝖾𝗌 𝗅𝗂𝗌𝗍𝖾𝖽 𝖻𝖾𝗅𝗈𝗐 𝖺𝗋𝖾 𝖺𝗅𝗋𝖾𝖺𝖽𝗒\n"
                "𝗈𝗇 𝗒𝗈𝗎𝗋 𝖺𝖼𝖼𝗈𝗎𝗇𝗍. 𝖲𝖾𝗅𝖾𝖼𝗍 𝖺𝗇𝗒 𝗌𝗍𝗈𝗋𝗒 𝗍𝗈 𝗏𝗂𝖾𝗐\n"
                "𝖽𝖾𝗍𝖺𝗂𝗅𝗌 𝗈𝗋 𝖺𝖼𝖼𝖾𝗌𝗌 𝗂𝗍 𝖺𝗀𝖺𝗂𝗇."
            )
        else:
            txt_b = (
                "<b>⟦ 𝗠𝗬 𝗦𝗧𝗢𝗥𝗜𝗘𝗦 ⟧</b>\n\n"
                "<b>ᴛᴏᴛᴀʟ ⟶</b> 0\n\n"
                "ɴᴏ ᴘᴜʀᴄʜᴀꜱᴇꜱ ꜰᴏᴜɴᴅ.\n"
                "ᴠɪꜱɪᴛ ᴛʜᴇ ᴍᴀʀᴋᴇᴛᴘʟᴀᴄᴇ ᴛᴏ ᴇxᴘʟᴏʀᴇ."
            )
            kb.insert(0, [InlineKeyboardButton(_sc("OPEN MARKETPLACE"), callback_data="mb#main_marketplace")])

        await _safe_edit(query.message, text=txt_b, markup=InlineKeyboardMarkup(kb))

    elif cmd == "noop":
        await query.answer()

    # ── Language ──
    elif cmd == "lang":
        new_lang = data[2]
        await query.answer("✓ Updates applied!", show_alert=False)
        m = await client.send_message(user_id, "<b>› › Yup, Bro updating... ⏳</b>")
        await asyncio.sleep(2)
        await db.update_user(user_id, {"lang": new_lang})
        try: await m.delete()
        except: pass
        await _edit_main_menu_in_place(client, query, query.from_user, new_lang)

    # ── Story preview Continue button ──
    elif cmd.startswith("story_preview_continue_"):
        s_id = cmd.replace("story_preview_continue_", "")
        await query.answer()
        await query.message.delete()
        return await _show_tc(client, user_id, s_id, lang)

    # ── T&C Accept ──
    elif cmd.startswith("tc_accept_"):
        s_id = cmd.replace("tc_accept_", "")
        await db.update_user(user_id, {"tc_accepted": True})
        await query.answer("Terms Accepted!")
        
        from utils import log_arya_event
        user_obj = await client.get_users(user_id)
        asyncio.create_task(log_arya_event(
            event_type="T&C ACCEPTED",
            user_id=user_id,
            user_info={"first_name": user_obj.first_name if user_obj else "Unknown", "last_name": user_obj.last_name if user_obj else "", "username": user_obj.username if user_obj else ""},
            details=f"User accepted the Terms & Conditions."
        ))

        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if story:
            _bt = await db.db.premium_bots.find_one({"id": client.me.id})
            _bt_cfg = (_bt or {}).get("config", {})
            return await _show_story_details(client, query, story, lang, bot_cfg=_bt_cfg)

    elif cmd == "help_tc":
        await query.answer()
        # Synchronized with updated T&C from purchase flow
        tc_text = (
            f"<b>⟦ 𝗧𝗘𝗥𝗠𝗦 & 𝗖𝗢𝗡𝗗𝗜𝗧𝗜𝗢𝗡𝗦 ⟧</b>\n\n"
            f"𝖡𝖾𝖿𝗈𝗋𝖾 𝗉𝗎𝗋𝖼𝗁𝖺𝗌𝗂𝗇𝗀, 𝗉𝗅𝖾𝖺𝗌𝖾 𝗋𝖾𝖺𝖽 𝖺𝗇𝖽 𝖺𝗀𝗋𝖾𝖾 𝗍𝗈 𝗍𝗁𝖾 𝖿𝗈𝗅𝗅𝗈𝗐𝗂ɴ𝗀:\n\n"
            f"<blockquote expandable>"
            f"• <b>𝗠𝗶𝘀𝘀𝗶𝗻𝗴 𝗘𝗽𝗶𝘀𝗼𝗱𝗲𝘀</b>\n"
            f"𝟹–𝟻 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝖻𝖾 𝗎𝗇𝖺𝗏𝖺𝗂ʟ𝖺𝖻𝗅𝖾 𝗂𝖿 𝗇𝗈𝗍 𝗉𝗎𝖻𝗅𝗂𝖼𝗅𝗒 𝗋𝖾𝗅𝖾𝖺𝗌𝖾𝖽.\n"
            f"𝖨𝖿 𝖺𝗏𝖺𝗂ʟ𝖺𝖻𝗅𝖾 𝗅𝖺𝗍𝖾𝗋, 𝗍𝗁𝖾𝗒 𝗐𝗂𝗅𝗅 𝖻𝖾 𝖺𝖽𝖽𝖾𝖽 𝖺𝗎𝗍𝗈𝗆𝖺𝗍𝗂𝖼𝖺𝗅𝗅𝗒.\n"
            f"𝖬𝗈𝗋𝖾 𝗍𝗁𝖺𝗇 𝟧 𝗆𝗂𝗌𝗌𝗂𝗇𝗀? 𝖢𝗈𝗇𝗍𝖺𝖼𝗍 𝗌𝗎𝗉𝗉𝗈𝗋𝗍.\n"
            f"</blockquote>\n"
            f"<blockquote expandable>"
            f"• <b>𝗤𝘂𝗮𝗹𝗶𝘁𝘆</b>\n"
            f"𝖲𝗈𝗆𝖾 𝗈𝗅𝖽𝖾𝗋 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝗁𝖺𝗏𝖾 𝗋𝖾𝖽𝗎𝖼𝖾𝖽 𝗊𝗎𝖺𝗅𝗂𝘁𝗒.\n"
            f"𝖶𝖾 𝖼𝖺𝗇𝗇𝗈𝗍 𝗀𝗎𝖺𝗋𝖺𝗇𝗍𝖾𝖾 𝟣𝟢𝟢% 𝗊𝗎𝖺𝗅𝗂𝗍𝗒, 𝖻𝗎𝗍 𝖺𝗅𝗐𝖺𝗒𝗌 𝗉𝗋𝗈𝗏𝗂𝖽𝖾 𝖻𝖾𝗌𝗍 𝗏𝖾𝗋𝗌𝗂𝗈𝗻.\n"
            f"</blockquote>\n"
            f"<blockquote expandable>"
            f"• <b>𝗘𝗽𝗶𝘀𝗼𝗱𝗲 𝗢𝗿𝗱𝗲𝗿</b>\n"
            f"𝖤𝗉𝗂𝗌𝗈ᴅ𝖾𝗌 𝗆𝖺𝗒 𝗋𝖺𝗋𝖾𝗅𝗒 𝖻𝖾 𝗈𝗎𝗍 𝗈𝖿 𝗌𝖾𝗊𝗎𝖾𝗇𝖼𝖾.\n"
            f"𝖠𝗅𝗅 𝖿𝗂𝗅𝖾𝗌 𝖺𝗋𝖾 𝖼𝗅𝖾𝖺𝗇𝖾𝖽 𝖺𝗇𝖽 𝖻𝗋𝖺𝗇𝖽𝖾𝖽 𝖻𝗒 𝖠𝗋𝗒𝖺 𝖡𝗈𝗍.\n"
            f"</blockquote>\n"
            f"<blockquote expandable>"
            f"• <b>𝗡𝗼 𝗥𝗲𝗳𝘂𝗻𝗱𝘀</b>\n"
            f"𝖭𝗈 𝗋𝖾𝖿𝗎𝗇𝖽𝗌 𝗈𝗇𝼄𝖾 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗂𝗌 𝖼𝗈𝗇𝖿𝗂𝗋𝗆𝖾𝖽 𝖺𝗇𝖽 𝖽𝖾𝗅𝗂𝗏𝖾𝗋𝗒 𝗌𝗍𝖺𝗋𝗍𝗌.\n"
            f"</blockquote>\n"
            f"<blockquote expandable>"
            f"• <b>𝗙𝗮𝗸𝗲 𝗦𝗰𝗿𝗲𝗲𝗻𝘀𝗵𝗼𝘁𝘀</b>\n"
            f"𝖥𝖺𝗄𝖾 𝗈𝗋 𝗂𝗇𝗏𝖺𝗅𝗂𝖽 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗉𝗋𝗈𝗈𝖿𝗌 𝗐𝗂𝗅𝗅 𝗅𝖾𝖺𝖽 𝗍𝗈 𝗉𝖾𝗋𝗆𝖺𝗇𝖾𝗇𝗍 𝖻𝖺𝗇.\n"
            f"</blockquote>"
        )
        kb = [[InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data="mb#main_help")]]
        await query.message.edit_text(tc_text, reply_markup=InlineKeyboardMarkup(kb))
    elif cmd == "help_refund":
        await query.answer()
        refund_text = (
            f"<b>🔁 {_sc('REFUND POLICY')}</b>\n\n"
            + _sc("If you paid an incorrect/extra amount or did not receive the story, you may be eligible for a refund after verification.") + "\n\n"
            "<blockquote expandable>"
            + "⚠️ " + _sc("IMPORTANT") + "\n"
            + _sc("WE STORE ALL DATA: YOUR PROFILE, PAYMENT HISTORY, WHICH STORY YOU PAID FOR, HOW MUCH YOU PAID, TO WHOM IT WAS PAID, WHETHER YOU RECEIVED THE CHANNEL LINK OR DELIVERY, AND HOW MANY EPISODES WERE DELIVERED.") + "\n\n"
            + _sc("REFUND REQUESTS ARE REVIEWED INDIVIDUALLY AND PROCESSED AFTER FULL VERIFICATION. MISUSE OF REFUND REQUESTS WILL RESULT IN A BAN.")
            + "</blockquote>"
        )
        kb = [[InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data="mb#main_help")]]
        await query.message.edit_text(refund_text, reply_markup=InlineKeyboardMarkup(kb))

    # ── T&C Reject ──
    elif cmd == "tc_reject":
        await query.answer("Cancelled.", show_alert=False)
        
        from utils import log_arya_event
        user_obj = await client.get_users(user_id)
        asyncio.create_task(log_arya_event(
            event_type="T&C REJECTED",
            user_id=user_id,
            user_info={"first_name": user_obj.first_name if user_obj else "Unknown", "last_name": user_obj.last_name if user_obj else "", "username": user_obj.username if user_obj else ""},
            details=f"User rejected the Terms & Conditions and aborted the purchase."
        ))

        await query.message.edit_text(
            "<b>❌ Purchase Cancelled</b>\n\n<i>You have rejected the Terms & Conditions. You can start over anytime from the Marketplace.</i>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❮ {_sc('MAIN MENU')}", callback_data="mb#main_back")]])
        )

    # ── Back (inline) ──
    elif cmd.startswith("show_tc#"):
        s_id = cmd.split("#")[1]
        try:
            await query.message.delete()
        except:
            pass
        return await _show_tc(client, user_id, s_id, lang)

    elif cmd == "back":
        await query.message.delete()

    # ── View Purchased Story Details ──
    elif cmd.startswith("purchased_view_"):
        s_id = data[2] if len(data) > 2 else cmd.replace("purchased_view_", "")
        await query.answer()
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if story:
            purchase = await db.db.premium_purchases.find_one({"user_id": int(user_id), "story_id": ObjectId(s_id)})
            
            s_name = story.get(f'story_name_{lang}', story.get('story_name_en'))
            ep_count = abs(story.get('end_id', 0) - story.get('start_id', 0)) + 1 if story.get('end_id') else "?"

            # Clean payment label
            payment_label = "Verified"
            if purchase:
                src = str(purchase.get("source", "manual")).lower()
                amount_paid = purchase.get("amount", story.get('price', 0))
                payment_label = {
                    "razorpay":   f"Razorpay (₹{amount_paid})",
                    "easebuzz":   f"Easebuzz (₹{amount_paid})",
                    "upi":        f"Manual UPI (₹{amount_paid})",
                    "manual_upi": f"Manual UPI (₹{amount_paid})",
                }.get(src, f"{src.capitalize()} (₹{amount_paid})")

            if lang == 'hi':
                txt_req = (
                    "<b>⟦ स्टोरी विवरण ⟧</b>\n\n"
                    f"<b>{s_name}</b>\n\n"
                    "──────────────\n"
                    f"<b>प्लेटफॉर्म  ⟶</b> {story.get('platform', 'अन्य')}\n"
                    f"<b>एपिसोड्स   ⟶</b> {story.get('episodes', 'N/A')}\n"
                    f"<b>फाइलें     ⟶</b> {ep_count}\n"
                    f"<b>स्थिति     ⟶</b> आपकी अपनी (Owned)\n"
                    f"<b>पेमेंट      ⟶</b> {payment_label}\n"
                    "──────────────\n"
                    "अपनी फाइलें प्राप्त करने के लिए नीचे टैप करें।"
                )
                kb = [
                    [InlineKeyboardButton("डिलीवरी प्राप्त करें", callback_data=f"mb#access_{s_id}")],
                    [InlineKeyboardButton("« मेरी स्टोरीज पर वापस", callback_data="mb#my_buys")]
                ]
            else:
                txt_req = (
                    "<b>⟦ 𝗦𝗧𝗢𝗥𝗬 𝗠𝗘𝗧𝗔 ⟧</b>\n\n"
                    f"<b>{s_name}</b>\n\n"
                    "──────────────\n"
                    f"<b>ᴘʟᴀᴛꜰᴏʀᴍ ⟶</b> {story.get('platform', 'Other')}\n"
                    f"<b>ᴇᴘɪꜱᴏᴅᴇꜱ ⟶</b> {story.get('episodes', 'N/A')}\n"
                    f"<b>ꜰɪʟᴇꜱ    ⟶</b> {ep_count}\n"
                    f"<b>ꜱᴛᴀᴛᴜꜱ   ⟶</b> ᴏᴡɴᴇᴅ\n"
                    f"<b>ᴘᴀʏᴍᴇɴᴛ  ⟶</b> {payment_label}\n"
                    "──────────────\n"
                    "𝖳𝖺𝗉 𝖻𝖾𝗅𝗈𝗐 𝗍𝗈 𝗋𝖾𝖼𝖾𝗂𝗏𝖾 𝗒𝗈𝗎𝗋 𝖿𝗂𝗅𝖾𝗌."
                )
                kb = [
                    [InlineKeyboardButton(_bs("GET DELIVERY"), callback_data=f"mb#access_{s_id}")],
                    [InlineKeyboardButton(_bs("Back to My Stories"), callback_data="mb#my_buys")]
                ]
            await _safe_edit(query.message, text=txt_req, markup=InlineKeyboardMarkup(kb))
            
    # ── Access purchased story directly ──
    elif cmd.startswith("access_"):
        s_id = data[2] if len(data) > 2 else cmd.replace("access_", "")
        await query.answer()
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if story:
            # Delete message to pop a new dialogue
            try: await query.message.delete()
            except: pass
            return await dispatch_delivery_choice(client, user_id, story)

    # ── View story (from inline button if any) ──
    elif cmd.startswith("view_") or cmd == "view":
        from bson.objectid import ObjectId
        s_id = data[2] if cmd == "view" else cmd.replace("view_", "")
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if not story: return await query.answer("Story not found!", show_alert=True)

        has_paid = await db.has_purchase(user_id, s_id)
        if has_paid:
            await query.answer("You already own this!", show_alert=True)
            return await dispatch_delivery_choice(client, user_id, story)

        await query.message.delete()
        # Show profile/description card first (with expandable quote), then user can continue to T&C.
        return await _show_story_profile(client, user_id, story, lang)

    # ── Pay ──
    elif cmd == "pay":
        method = data[2]
        s_id = data[3]

        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if not story: return await query.answer("Story not found!", show_alert=True)

        # Block UPI if time-restricted or admin has disabled it per-bot
        if method == "upi":
            bt_cfg = (await db.db.premium_bots.find_one({"id": client.me.id}) or {}).get("config", {})
            if not bt_cfg.get("upi_enabled", True) or _is_upi_restricted():
                rzp_kb = [
                    [InlineKeyboardButton(f"💳 {_sc('PAY VIA RAZORPAY')}", callback_data=f"mb#pay#razorpay#{s_id}")],
                    [InlineKeyboardButton(f"❮ {_sc('BACK')}", callback_data="mb#return_main")]
                ]
                return await query.message.edit_text(
                    f"<b>🚫 {_sc('UPI UNAVAILABLE')}</b>\n\n"
                    f"<i>{_sc('Manual UPI payments are not available between 9 PM and 6 AM IST.')}</i>\n"
                    f"<i>{_sc('Manual verification is not active during this time.')}</i>\n\n"
                    f"{_sc('Please use')} <b>Razorpay</b> {_sc('for instant automatic payment and immediate delivery.')}\n"
                    f"{_sc('Supports UPI, Card, Net Banking, Wallets & more.')}",
                    reply_markup=InlineKeyboardMarkup(rzp_kb)
                )

        if method in ["razorpay", "easebuzz"]:
            await query.message.edit_text(f"🔐 <b>{_sc('PREPARING YOUR SECURE CHECKOUT')}...</b>\n<i>{_sc('Please wait a moment while we connect to the gateway.')}</i>")
            import time
            ref_id = f"st_{user_id}_{int(time.time())}"
            price = int(story["price"])
            desc = f"Story: {story.get('story_name_en', 'Premium Content')}"
            
            pl_id = None
            url = None
            if method == "razorpay":
                url, pl_id = await _create_rzp_link(price, desc, ref_id, user.get('first_name', "User"))
            else:
                url, pl_id = await _create_easebuzz_link(price, desc, ref_id, user.get('first_name', "User"))
            
            if not url or (method == "razorpay" and not url.startswith("http")):
                empty_key_msg = f"{method.capitalize()} API keys not found in .env!"
                err_msg = pl_id if pl_id else empty_key_msg
                return await query.message.edit_text(f"❌ Could not generate API link for <b>{method}</b>.\n\n<code>{err_msg}</code>\n\nPlease check your .env configuration. For now, try Manual UPI.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❮ {_sc('BACK')}", callback_data="mb#main_back")]]))

            await db.db.premium_checkout.update_one(
                {"user_id": user_id, "bot_id": client.me.id, "story_id": ObjectId(s_id)},
                {"$set": {
                    "status": "pending_gateway",
                    "bot_username": client.me.username,
                    "method": method,
                    "payment_id": pl_id,
                    "amount": price,
                    "pay_link_copy": url,
                    "updated_at": datetime.utcnow(),
                }, "$setOnInsert": {"created_at": datetime.utcnow()}},
                upsert=True
            )
            
            kb = [
                [InlineKeyboardButton(f"💳 {_sc('PAY VIA')} {_sc(method.upper())}", url=url)],
                [InlineKeyboardButton(f"✅ {_sc('VERIFY PAYMENT')}", callback_data=f"mb#{method}_check#{s_id}")],
                [InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data="mb#return_main")]
            ]
            check_txt = (
                f"<b>🛍️ {_sc('CHECKOUT')}</b>\n"
                f"────────────────────\n"
                f"<b>📦 {_sc('Item')} :</b> {story.get('story_name_en', 'Premium Story')}\n"
                f"<b>💰 {_sc('Amount')} :</b> ₹{price}\n"
                f"────────────────────\n"
                f"{_sc('You are paying for this premium story.')}\n"
                f"{_sc('Instant verification & delivery.')}\n"
                f"────────────────────\n"
                f"<i>{_sc('Click below to pay. Once done, tap Verify.')}</i>"
            )
            await query.message.edit_text(check_txt, reply_markup=InlineKeyboardMarkup(kb))

        elif method == "upi":
            upi_id = await db.get_config("upi_id") or "heyjeetx@naviaxis"
            bt = await db.db.premium_bots.find_one({"id": client.me.id})
            bt_cfg = bt.get("config", {}) if bt else {}
            
            s_price = str(story["price"])
            s_name = story.get(f'story_name_{lang}', story.get('story_name_en', 'Story'))
            
            # Generate Premium UPI Card
            qr_card = None
            try:
                # Pass the configured payee name to match the official bank record
                p_name = (bt_cfg.get("upi_name") or "Merchant").strip()
                qr_card = generate_upi_card(upi_id, s_price, s_name, payee_name=p_name)
            except Exception as e:
                logger.error(f"UPI Card generation failed: {e}")
                qr_card = None

            upi_uri = _build_upi_uri(
                upi_id=upi_id,
                payee_name=(bt_cfg.get("upi_name") or "").strip(),
                amount=int(story["price"]),
                note=f"Payment for {s_name[:20]}"
            )

            slice_api_url = (getattr(Config, "SLICEURL_API_URL", "") or "").strip()
            slice_api_key = (getattr(Config, "SLICEURL_API_KEY", "") or "").strip()
            slice_direct = ""
            if slice_api_url and slice_api_key.startswith("slc_"):
                slice_direct = await _sliceurl_api_shorten(upi_uri)
            button_url = slice_direct

            await db.db.premium_checkout.update_one(
                {"user_id": user_id, "bot_id": client.me.id, "story_id": ObjectId(s_id)},
                {"$set": {
                    "status": "pending_gateway",
                    "bot_username": client.me.username,
                    "method": "upi",
                    "upi_uri": upi_uri,
                    "pay_link_copy": button_url,
                    "updated_at": datetime.utcnow(),
                }, "$setOnInsert": {"created_at": datetime.utcnow()}},
                upsert=True
            )

            p_name = (bt_cfg.get("upi_name") or "Merchant").strip()
            
            if lang == 'hi':
                txt = (
                    f"<b>⟦ {_sc('पेमेंट पूरा करें')} ⟧</b>\n\n"
                    f"<b>स्टेप 𝟷: ₹{s_price} का भुगतान करें</b>\n\n"
                    f"<blockquote>• QR कोड स्कैन करें या नीचे दिए गए विवरण का उपयोग करें:</blockquote>\n"
                    f"<blockquote><b>UPI ID:</b> <code>{upi_id}</code>\n"
                    f"<b>नाम:</b> <code>{p_name}</code>\n"
                    f"<b>राशि:</b> <code>₹{s_price}</code></blockquote>\n\n"
                    f"• सुनिश्चित करें कि राशि सही भरी गई है।\n\n"
                    f"<b>स्टेप य: भुगतान का सत्यापन</b>\n\n"
                    f"• भुगतान के बाद, अपना स्क्रीनशॉट अपलोड करने के लिए <b>पेमेंट हो गया</b> पर क्लिक करें।\n"
                    f"────────────────────"
                )
                kb = [
                    [InlineKeyboardButton("☑️ पेमेंट हो गया", callback_data=f"mb#upi_done#{s_id}")],
                    [InlineKeyboardButton("« ❮ वापस", callback_data=f"mb#pay_back#{s_id}")]
                ]
            else:
                txt = (
                    f"<b>⟦ {_sc('COMPLETE PAYMENT')} ⟧</b>\n\n"
                    f"<b>𝚂𝚝𝚎𝚙 𝟷: Pay ₹{s_price}</b>\n\n"
                    f"<blockquote>• Scan the QR code or pay using the details below:</blockquote>\n"
                    f"<blockquote><b>UPI ID:</b> <code>{upi_id}</code>\n"
                    f"<b>Name:</b> <code>{p_name}</code>\n"
                    f"<b>Amount:</b> <code>₹{s_price}</code></blockquote>\n\n"
                    f"• Make sure the amount is entered correctly.\n\n"
                    f"<b>𝚂𝚝𝚎𝚙 𝟸: Verify Payment</b>\n\n"
                    f"• After payment, click <b>PAYMENT DONE</b> to upload your screenshot.\n"
                    f"────────────────────"
                )
                kb = [
                    [InlineKeyboardButton(f"☑️ {_sc('PAYMENT DONE')}", callback_data=f"mb#upi_done#{s_id}")],
                    [InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data=f"mb#pay_back#{s_id}")]
                ]
            
            await query.message.delete()
            try:
                if qr_card:
                    await client.send_photo(user_id, photo=qr_card, caption=txt, reply_markup=InlineKeyboardMarkup(kb))
                else:
                    import urllib.parse
                    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=900x900&margin=1&data={urllib.parse.quote(upi_uri)}"
                    await client.send_photo(user_id, photo=qr_url, caption=txt, reply_markup=InlineKeyboardMarkup(kb))
            except Exception as e:
                logger.warning(f"UPI payment screen send failed: {e}")
                kb2 = [[InlineKeyboardButton(f"☑️ {'पेमेंट हो गया' if lang=='hi' else _sc('PAYMENT DONE')}", callback_data=f"mb#upi_done#{s_id}")]]
                await client.send_message(user_id, txt, reply_markup=InlineKeyboardMarkup(kb2))

    elif cmd == "pay_back":
        s_id = data[2]
        await query.answer()
        # 1. Cleanup current QR message
        try: await query.message.delete()
        except: pass

        # 2. Show temporary loading message
        wait = await client.send_message(user_id, f"« ❮ ⏳ {_sc('Returning to payment section')}...")

        # 3. Reload Payment Selection Screen
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if story:
            _bt = await db.db.premium_bots.find_one({"id": client.me.id})
            _bt_cfg = (_bt or {}).get("config", {})
            # We call this with the message object to trigger a NEW message send (reply_text)
            # since the QR was a photo and cannot be edited into text.
            await _show_story_details(client, query.message, story, lang, bot_cfg=_bt_cfg)
            
        # 4. Auto-delete loading message
        await asyncio.sleep(0.5)
        try: await wait.delete()
        except: pass
        return

    elif cmd == "upi_done":
        s_id = data[2]
        from bson.objectid import ObjectId
        await db.db.premium_checkout.update_one(
            {"user_id": user_id, "bot_id": client.me.id, "story_id": ObjectId(s_id)},
            {"$set": {"status": "waiting_screenshot", "updated_at": datetime.utcnow()}}
        )
        if lang == 'hi':
            await query.answer("कृपया अपना स्क्रीनशॉट भेजें।", show_alert=True)
            await query.message.reply_text(
                "<b>📸 पेमेंट स्क्रीनशॉट भेजें</b>\n\n"
                "सत्यापन शुरू करने के लिए कृपया अपने सफल भुगतान का स्क्रीनशॉट यहाँ भेजें।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« वापस", callback_data=f"mb#pay#upi#{s_id}")]])
            )
        else:
            await query.answer(_sc("Please send your screenshot."), show_alert=True)
            await query.message.reply_text(
                f"<b>📸 {_sc('SEND PAYMENT SCREENSHOT')}</b>\n\n"
                f"{_sc('Please send your successful payment screenshot here to begin verification.')}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"« {_sc('BACK')}", callback_data=f"mb#pay#upi#{s_id}")]])
            )

    elif cmd.endswith("_check") and cmd.split("_")[0] in ("razorpay", "easebuzz"):
        s_id = data[2] if len(data) > 2 else None
        if not s_id: return await query.answer("Invalid.", show_alert=True)
        method = cmd.split("_")[0]
        
        from bson.objectid import ObjectId
        checkout = await db.db.premium_checkout.find_one(
            {"user_id": user_id, "bot_id": client.me.id, "story_id": ObjectId(s_id), "status": "pending_gateway"}
        )
        if not checkout or not checkout.get("payment_id"):
            return await query.answer("No pending payment found. Generate link again.", show_alert=True)
        
        await query.answer("Checking payment status... please wait.", show_alert=False)
        m = await query.message.edit_text(f"🛡️ <b>{_sc('VERIFYING PAYMENT')}...</b>\n<i>{_sc('Checking with')} {method.capitalize()} {_sc('servers.')}</i>")
        
        status = "failed"
        if method == "razorpay":
            status = await _check_rzp_status(checkout["payment_id"])
        else:
            status = await _check_easebuzz_status(checkout["payment_id"], checkout.get("amount", 0))
            
        if status == "paid":
            # Confirmed!
            await db.db.premium_checkout.update_one(
                {"_id": checkout["_id"]},
                {"$set": {"status": "approved", "updated_at": datetime.utcnow()}}
            )
            # Send notification
            await m.edit_text("✅ <b>Payment Confirmed successfully!</b>\nAdding to your unlocked stories...")
            
            # Use utility db function to add purchase 
            story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
            
            if not await db.has_purchase(user_id, str(s_id)):
                import random, string
                order_id = f"OD-{user_id}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
                
                # Record in global audit collection
                await db.db.premium_purchases.insert_one({
                    "user_id": user_id,
                    "story_id": ObjectId(s_id),
                    "bot_id": client.me.id,
                    "purchased_at": datetime.utcnow(),
                    "source": method,
                    "amount": checkout.get("amount", 0),
                    "reference": checkout.get("payment_id"),
                    "order_id": order_id
                })
                # Add to user's personal unlocked list (crucial for Unlocked Stories menu)
                await db.add_purchase(user_id, str(s_id))
                
                # Log success
                from utils import log_payment, log_arya_event
                user_info = await db.get_user(user_id)
                s_name = story.get("story_name_en") if story else "Unknown"
                
                asyncio.create_task(log_arya_event(
                    event_type="PAYMENT PROCESSED",
                    user_id=user_id,
                    user_info=user_info,
                    details=f"Story: {s_name}\nGateway: {method.upper()}\nOrder ID: <code>{order_id}</code>\nAmount: ₹{checkout.get('amount', 0)}"
                ))
                
                asyncio.create_task(log_payment(
                    user_id=user_id,
                    user_first_name=user_info.get("first_name", "User"),
                    username=user.get('username', ''),
                    s_name=s_name,
                    amount=checkout.get("amount", 0),
                    method=method,
                    receipt_id=checkout.get("payment_id", ""),
                    pay_link=checkout.get("pay_link_copy", ""),
                    order_id=order_id,
                    user_last_name=user_info.get("last_name", "")
                ))
            
            # Re-fetch story to ensure we have channel/pool data for delivery options
            story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
            return await dispatch_delivery_choice(client, user_id, story)
            
        else:
            await query.answer(f"Payment not completed yet (Status: {status}). Please pay and try again.", show_alert=True)
            # Revert to payment button state
            kb = [
                [InlineKeyboardButton(f"💳 {_sc('PAY VIA')} {_sc(method.upper())}", url=checkout.get("pay_link_copy", "https://t.me"))] if "pay_link_copy" in checkout else [],
                [InlineKeyboardButton(f"✅ {_sc('VERIFY PAYMENT')}", callback_data=f"mb#{method}_check#{s_id}")],
                [InlineKeyboardButton(f"« ❮ {_sc('BACK')}", callback_data="mb#return_main")]
            ]
            # Since url is not strictly saved, we might have lost it.
            # actually we didn't save the short url in db in the first replacement chunk. Let's fix that below if needed, but for now just show a simple back button.
            await m.edit_text(
                f"<b>❌ Payment Not Found</b>\nStatus: <code>{status}</code>\nIf you have paid, please wait a minute and verify again.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Verify Again", callback_data=f"mb#{method}_check#{s_id}")], [InlineKeyboardButton("« Back", callback_data=f"mb#show_tc#{s_id}")]])
            )

    elif cmd in ("pay_link", "upi_uri"):
        # Copy SliceURL short link only (strict mode).
        s_id = data[2] if len(data) > 2 else None
        if not s_id:
            return await query.answer("Invalid request.", show_alert=True)
        from bson.objectid import ObjectId
        checkout = await db.db.premium_checkout.find_one(
            {"user_id": user_id, "bot_id": client.me.id, "story_id": ObjectId(s_id)}
        )
        link = (checkout or {}).get("pay_link_copy") or ""
        if not link and (checkout or {}).get("upi_uri"):
            link = await _sliceurl_api_shorten(checkout["upi_uri"])
        if not link:
            return await query.answer("SliceURL link unavailable. Check SLICEURL_API_URL/KEY and API deploy.", show_alert=True)
        await query.answer()
        await client.send_message(
            user_id,
            "🔗 <b>Payment link</b> — open in browser; it will launch UPI with amount filled.\n"
            f"<code>{link}</code>",
        )

    # ── Delivery choice (DM vs Channel) - handled via callbacks now ──
    elif cmd == "deliver_dm":
        s_id = data[2]
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if not story: return await query.answer("Story not found!", show_alert=True)
        
        start_id = story.get('start_id')
        end_id = story.get('end_id')
        total_files = (end_id - start_id) + 1 if (start_id and end_id and end_id >= start_id) else 1

        parts_data = len(data) > 3
        if not parts_data and total_files > 40:
            if total_files > 300: chunk = 100
            elif total_files > 100: chunk = 50
            else: chunk = 30
            
            kb = []
            row = []
            for i in range(0, total_files, chunk):
                f_start = i + 1
                f_end = min(i + chunk, total_files)
                lbl = f"Files {f_start} - {f_end}" if lang != "hi" else f"फ़ाइलें {f_start} - {f_end}"
                row.append(lbl)
                if len(row) == 2:
                    kb.append(row)
                    row = []
            if row:
                kb.append(row)
                
            full_btn = "Full Delivery (All Files)" if lang != "hi" else "Full Delivery (सभी फ़ाइलें)"
            cancel_btn = "Cancel" if lang != "hi" else "रद्द करें"
            kb.append([full_btn])
            kb.append([cancel_btn])
            
            await db.db.users.update_one({"id": user_id}, {"$set": {"dm_story_id_pending": s_id}})
            
            await query.answer()
            try: await query.message.delete()
            except: pass
            
            if lang == "hi":
                p_text = "<b>फ़ाइलें चुनें:</b>\n\nआप कौन से भाग प्राप्त करना चाहते हैं? नीचे दिए गए मेन्यू बटन का उपयोग करें।"
            else:
                p_text = "<b>Select Files:</b>\n\nWhich part would you like to receive? Please use the keyboard options below."
            return await client.send_message(user_id, p_text, reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))
            
        c_start = int(data[3]) if len(data) > 3 else start_id
        c_end = int(data[4]) if len(data) > 4 else end_id

        await query.answer()
        await query.message.edit_text(
            f"<i>⏳ Initializing DM Delivery... Preparing your files.</i>",
            reply_markup=None
        )
        asyncio.create_task(_do_dm_delivery(client, user_id, story, query.message, c_start, c_end))

    elif cmd == "cancel_dm":
        dm_aborts.add(user_id)
        await query.answer("⏹️ Stopping delivery...", show_alert=True)
        try:
            await query.message.edit_caption(
                "<b>⏹️ Delivery Stopped!</b>\n\n<i>Processing remaining requests and cleaning up...</i>",
                reply_markup=None
            )
        except:
            try: await query.message.edit_text("<b>⏹️ Delivery Stopped!</b>\n\n<i>Processing remaining requests...</i>", reply_markup=None)
            except: pass

    elif cmd == "deliver_channel":
        s_id = data[2]
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(s_id)})
        if not story: return await query.answer("Story not found!", show_alert=True)
        await query.answer()
        await query.message.edit_text("<i>⏳ Generating secure 1-time channel link...</i>", reply_markup=None)
        asyncio.create_task(_do_channel_delivery(client, user_id, story, query.message))

    # ── Notify Admin ──
    elif cmd == "notify_admin":
        await query.answer("Admin has been notified. Please wait patiently.", show_alert=True)
        admins = Config.SUDO_USERS or Config.OWNER_IDS
        if not admins:
            return
        for admin in admins:
            try:
                await client.send_message(
                    admin,
                    f"🔔 <b>URGENT PING:</b>\nUser <code>{user_id}</code> is waiting for Payment Validation!\nCheck the Pending queue in Management Bot."
                )
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────
# Screenshot Handler
# ─────────────────────────────────────────────────────────────────
async def _process_screenshot(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')

    checkout = await db.db.premium_checkout.find_one(
        {"user_id": user_id, "bot_id": client.me.id, "status": "waiting_screenshot"}
    )
    if not checkout: return

    # Fake Detection
    p = message.photo
    if p.file_size < 50000:
        return await message.reply_text("❌ <b>Invalid Screenshot!</b>\n\nThe image is too small. Please send a clear, full payment screenshot.", quote=True)
    if p.height < p.width:
        return await message.reply_text("❌ <b>Invalid Screenshot!</b>\n\nPlease send a portrait-mode screenshot (not landscape).", quote=True)

    kb_user = [[InlineKeyboardButton(f"✆ {_sc('NOTIFY ADMIN')}", callback_data="mb#notify_admin")]]
    txt_user = (
        f"⏳ <b>{_sc('Your payment is being verified') if lang != 'hi' else 'आपके भुगतान की पुष्टि की जा रही है'}</b>\n"
        "<blockquote expandable>\n"
        f"<i>{_sc('Please wait (approx 5 minutes)...') if lang != 'hi' else 'कृपया प्रतीक्षा करें (लगभग 5 मिनट)...'}</i>\n\n"
        f"<b>{_sc('Time Remaining') if lang != 'hi' else 'शेष समय'} :</b> 05:00\n"
        "</blockquote>"
    )
    msg = await message.reply_text(txt_user, reply_markup=InlineKeyboardMarkup(kb_user))

    import os, hashlib
    if not os.path.exists("downloads"): os.makedirs("downloads")
    file_path = await client.download_media(message, file_name=f"downloads/proof_{checkout['_id']}.jpg")
    
    # Hash for duplicate detection
    with open(file_path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
        
    dup = await db.db.premium_checkout.find_one({"proof_hash": file_hash, "status": {"$in": ["pending_admin_approval", "approved"]}})
    if dup:
        os.remove(file_path)
        return await message.reply_text("❌ <b>Fraud Detected!</b>\n\nThis exact screenshot has already been used. Please provide a genuine, new payment screenshot or contact support.", quote=True)

    await db.db.premium_checkout.update_one(
        {"_id": checkout["_id"]},
        {"$set": {
            "status": "pending_admin_approval",
            "proof_path": file_path,
            "proof_hash": file_hash,
            "proof_file_id": message.photo.file_id,
            "status_msg_id": msg.id,
            "paid_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }}
    )
    if db.mgmt_client:
        try:
            from config import Config
            admins = Config.SUDO_USERS or Config.OWNER_IDS
            from bson.objectid import ObjectId
            story = await db.db.premium_stories.find_one({"_id": ObjectId(str(checkout['story_id']))})
            s_name = story.get("story_name_en") if story else "Unknown"
            price = story.get("price", "0") if story else "0"
            
            kb_admin = [
                [InlineKeyboardButton("✅ Approve", callback_data=f"mk#approve#{checkout['_id']}"),
                 InlineKeyboardButton("❌ Reject", callback_data=f"mk#reject#{checkout['_id']}")]
            ]
            caption = (
                f"<b>🚨 NEW PAYMENT REQUEST</b>\n"
                f"────────────────────\n"
                f"<b>User:</b> {message.from_user.first_name} (<code>{user_id}</code>)\n"
                f"<b>Story:</b> {s_name}\n"
                f"<b>Amount:</b> ₹{price}\n"
                f"<b>Method:</b> Manual UPI\n"
                f"<b>Date:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                f"────────────────────\n"
                f"<i>Please verify the exact amount in the screenshot.</i>"
            )
            for admin_id in admins:
                try: await db.mgmt_client.send_photo(admin_id, photo=file_path, caption=caption, reply_markup=InlineKeyboardMarkup(kb_admin))
                except Exception: pass
        except Exception as e:
            logger.error(f"Mgmt DM send error: {e}")

    async def _live_timer(target_msg, remaining, checkout_id_str):
        try:
            for i in range(remaining, 0, -10):
                await asyncio.sleep(10)
                from bson.objectid import ObjectId
                chk = await db.db.premium_checkout.find_one({"_id": ObjectId(checkout_id_str)})
                if not chk or chk.get("status") != "pending_admin_approval":
                    break
                
                m = i // 60
                s = i % 60
                await target_msg.edit_text(
                    (f"⏳ <b>{_sc('Your payment is being verified') if lang != 'hi' else 'आपके भुगतान की पुष्टि की जा रही है'}</b>\n"
                     "<blockquote expandable>\n"
                     f"<i>{_sc('Please wait (approx 5 minutes)...') if lang != 'hi' else 'कृपया प्रतीक्षा करें (लगभग 5 मिनट)...'}</i>\n\n"
                     f"<b>{_sc('Time Remaining') if lang != 'hi' else 'शेष समय'} :</b> {m:02d}:{s:02d}\n"
                     "</blockquote>"),
                    reply_markup=InlineKeyboardMarkup(kb_user)
                )
            await target_msg.edit_text(
                f"⏳ <b>{_sc('Verification Timeout')}</b>\n\n<i>{_sc('If your payment is valid, it will be approved soon. For urgent queries, please contact Support.')}</i>",
                reply_markup=InlineKeyboardMarkup(kb_user)
                )
        except Exception: pass
    asyncio.create_task(_live_timer(msg, 300, str(checkout['_id'])))


# ─────────────────────────────────────────────────────────────────
# Delivery Choice (now fully Inline — no native_ask needed!)
# ─────────────────────────────────────────────────────────────────
async def dispatch_delivery_choice(client, user_id, story):
    """
    Called when Admin approves OR user already owns. Shows inline delivery options.
    """
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    story_id_str = str(story['_id'])

    used_channels = user.get("used_channels", [])
    mode = story.get("delivery_mode") or ("single" if story.get("channel_id") else "pool")
    fallback = await db.db.premium_channels.find_one({"type": "delivery"})
    pool = story.get("channel_pool") or []
    has_any_delivery = bool(story.get('channel_id') or pool or (fallback and fallback.get("channel_id")))
    can_use_channel = (story_id_str not in used_channels) and (mode != "dm_only") and has_any_delivery

    # Find purchase source to display
    from bson.objectid import ObjectId
    purchase = await db.db.premium_purchases.find_one({"user_id": int(user_id), "story_id": ObjectId(story_id_str)})
    method_info = "Verified Purchase"
    if purchase:
        src = str(purchase.get("source", "manual")).lower()
        if src == "razorpay": method_info = "💳 Razorpay"
        elif src == "easebuzz": method_info = "💸 Easebuzz"
        elif src == "upi": method_info = "🏦 Manual UPI"
        else: method_info = f"🛒 {src.capitalize()}"

    s_name = story.get(f'story_name_{lang}', story.get('story_name_en'))

    if lang == 'hi':
        del_txt = (
            "<b>✅ एक्सेस मिल गया है!</b>\n\n"
            f"<b>स्टोरी:</b> {s_name}\n"
            + (f"<b>भुगतान तरीका:</b> {method_info}\n" if method_info else "")
            + "\n"
            + "<b>ℹ️ डिलीवरी की जानकारी</b>\n\n"
            + "<blockquote>• <b>DM डिलीवरी:</b> फाइलें सीधे यहां भेजी जाती हैं। उन्हें तुरंत सेव या फॉरवर्ड करें—वे कुछ समय बाद अपने आप डिलीट हो जाती हैं।</blockquote>\n"
            + "<blockquote>• <b>चैनल लिंक:</b> एक वन-टाइम प्राइवेट इनवाइट लिंक जेनरेट किया जाता है। प्रत्येक स्टोरी के लिए केवल एक चैनल लिंक की अनुमति है।</blockquote>\n"
            + "<blockquote>• <b>लाइफटाइम एक्सेस:</b> आप किसी भी खरीदी हुई स्टोरी को कभी भी <b>मुख्य मेनू ⟶ मेरी स्टोरीज</b> से एक्सेस कर सकते हैं।</blockquote>\n"
            + "──────────────\n\n"
            + "आप अपनी फाइलें कैसे प्राप्त करना चाहेंगे?"
        )
        dm_btn_txt = "⤓ DM में प्राप्त करें"
        chan_btn_txt = "➦ चैनल लिंक प्राप्त करें"
        back_btn_txt = "« ❮ मुख्य मेनू"
    else:
        del_txt = (
            "<b>✅ Access Granted!</b>\n\n"
            f"<b>Product:</b> {s_name}\n"
            + (f"<b>Method:</b> {method_info}\n" if method_info else "")
            + "\n"
            + "<b>ℹ️ Delivery Info</b>\n\n"
            + "<blockquote>• <b>DM Delivery:</b> Files are sent directly here. Save or forward them immediately—they auto-delete after some time.</blockquote>\n"
            + "<blockquote>• <b>Channel Link:</b> A one-time private invite link is generated. Each story allows only one channel link per account.</blockquote>\n"
            + "<blockquote>• <b>Lifetime Access:</b> You can re-access any purchased story anytime from <b>Main Menu ⟶ My Stories</b>.</blockquote>\n"
            + "──────────────\n\n"
            + "How would you like to receive your files?"
        )
        dm_btn_txt = f"⤓ {_sc('RECEIVE IN DM')}"
        chan_btn_txt = f"➦ {_sc('ACCESS CHANNEL LINK')}"
        back_btn_txt = f"« ❮ {_sc('MAIN MENU')}"

    kb = [[InlineKeyboardButton(dm_btn_txt, callback_data=f"mb#deliver_dm#{story_id_str}")]]
    if can_use_channel:
        kb.append([InlineKeyboardButton(chan_btn_txt, callback_data=f"mb#deliver_channel#{story_id_str}")])
    kb.append([InlineKeyboardButton(back_btn_txt, callback_data="mb#main_back")])

    await client.send_message(user_id, del_txt, reply_markup=InlineKeyboardMarkup(kb))

async def _auto_delete_demo(client, user_id, msg_ids):
    import asyncio
    await asyncio.sleep(300)
    for mid in msg_ids:
        try:
            await client.delete_messages(user_id, mid)
        except Exception:
            pass

async def _send_demo_files(client, user_id, story, lang):
    import asyncio
    start = story.get("start_id")
    end = story.get("end_id")
    src = story.get("source")
    if not start or not end or not src:
        await client.send_message(user_id, "❌ Demo not available for this story.")
        return
        
    start, end, src = int(start), int(end), int(src)
    total = (end - start) + 1
    
    msg_ids = []
    
    try:
        if lang == "hi":
            txt = "<b>👀 डेमो फ़ाइलें भेजी जा रही हैं...</b>\n\n<i>नोट: एपिसोड हमेशा अलग-अलग नहीं दिए जाते हैं; वे ग्रुप फॉर्मेट/बड़ी फाइल में भी हो सकते हैं, इसलिए कृपया इसे ध्यान में रखें।\nये डेमो फाइल्स 5 मिनट बाद सख्ती से अपने आप डिलीट हो जाएंगी।</i>"
        else:
            txt = "<b>👀 Sending Demo Files...</b>\n\n<i>Note: Episodes are not necessarily provided separately; they may also be delivered in a group format, so please keep that in mind.\nThese demo files will be auto-deleted strictly after 5 minutes.</i>"
            
        m = await client.send_message(user_id, txt)
        msg_ids.append(m.id)
        
        lbl_start = "<b>1️⃣ स्टार्टिंग फ़ाइलें (शुरुआत) 👇</b>" if lang == "hi" else "<b>1️⃣ STARTING FILES 👇</b>"
        lbl_end = f"<b>2️⃣ अंतिम फ़ाइल (कुल: {total} फ़ाइलें) 👇</b>" if lang == "hi" else f"<b>2️⃣ ENDING FILE (Total: {total} files) 👇</b>"
        
        m_s = await client.send_message(user_id, lbl_start)
        msg_ids.append(m_s.id)
        
        s_count = 2 if total >= 2 else 1
        for mid in range(start, start + s_count):
            sent = await client.copy_message(chat_id=user_id, from_chat_id=src, message_id=mid, protect_content=True)
            msg_ids.append(sent.id)
            await asyncio.sleep(0.5)
            
        if total > 2:
            m_e = await client.send_message(user_id, lbl_end)
            msg_ids.append(m_e.id)
            sent_end = await client.copy_message(chat_id=user_id, from_chat_id=src, message_id=end, protect_content=True)
            msg_ids.append(sent_end.id)
            
        asyncio.create_task(_auto_delete_demo(client, user_id, msg_ids))
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Demo failed: {e}")

async def _do_dm_delivery(client, user_id, story, status_msg=None, part_start=None, part_end=None):
    try:
        dm_aborts.discard(user_id)
        bt = await db.db.premium_bots.find_one({"id": client.me.id})
        bt_cfg = bt.get("config", {}) if bt else {}
        user_obj = await client.get_users(user_id)
        src = story.get('source')
        start = part_start if part_start else story.get('start_id')
        end = part_end if part_end else story.get('end_id')
        story_id_str = str(story['_id'])
        
        if not src or not start or not end:
            await client.send_message(user_id, "❌ Story file range is not configured correctly. Please contact admin.")
            return

        # Fetching Message with Media & Cancel Button
        fetch_config = bt_cfg.get("fetching_media")
        fetch_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⛔ CANCEL DELIVERY", callback_data=f"mb#cancel_dm#{story_id_str}")]])
        fetch_text = f"<b>⏳ Starting DM Delivery...</b>\n\n<i>Please wait while we fetch and deliver your files. This may take a few moments.</i>"
        
        fetch_msg = None
        if fetch_config:
            try:
                # Optimized multi-media handling (Photo/GIF/Video)
                f_id = fetch_config.get("file_id") if isinstance(fetch_config, dict) else fetch_config
                f_type = fetch_config.get("type", "photo") if isinstance(fetch_config, dict) else "photo"

                if f_type == "photo":
                    fetch_msg = await client.send_photo(user_id, photo=f_id, caption=fetch_text, reply_markup=fetch_kb)
                elif f_type == "animation":
                    fetch_msg = await client.send_animation(user_id, animation=f_id, caption=fetch_text, reply_markup=fetch_kb)
                elif f_type == "video":
                    fetch_msg = await client.send_video(user_id, video=f_id, caption=fetch_text, reply_markup=fetch_kb)
                
                if fetch_msg and status_msg: await status_msg.delete()
            except Exception: pass
        
        # Fallback to Story image or random menu image if custom fetching media fails/not set
        if not fetch_msg:
            alt_media = story.get("image") or bt_cfg.get("menuimg")
            if alt_media:
                try:
                    fetch_msg = await client.send_photo(user_id, photo=alt_media, caption=fetch_text, reply_markup=fetch_kb)
                    if status_msg: await status_msg.delete()
                except Exception: pass

        if not fetch_msg:
            if status_msg:
                try: fetch_msg = await status_msg.edit_text(fetch_text, reply_markup=fetch_kb)
                except: fetch_msg = await client.send_message(user_id, fetch_text, reply_markup=fetch_kb)
            else:
                fetch_msg = await client.send_message(user_id, fetch_text, reply_markup=fetch_kb)

        try:
            autodel = int(str(bt_cfg.get("autodel", "0")).strip() or "0")
        except Exception:
            autodel = 0

        sent_count = 0
        failed_count = 0
        sent_ids = []
        cap_tpl = bt_cfg.get("caption", "")
        
        aborted = False
        msg_range = range(int(start), int(end) + 1)
        total_eps = len(msg_range)
        
        for idx, msg_id in enumerate(msg_range, start=1):
            if user_id in dm_aborts:
                aborted = True
                break
            
            # Progress update
            if idx % 10 == 0 or idx == 1:
                try:
                    p_text = f"<b>⏳ Delivering Files... ({idx}/{total_eps})</b>\n\n<i>Processing your request, please stay tuned.</i>"
                    if fetch_msg.caption:
                        await fetch_msg.edit_caption(p_text, reply_markup=fetch_kb)
                    else:
                        await fetch_msg.edit_text(p_text, reply_markup=fetch_kb)
                except Exception: pass

            try:
                kwargs = dict(
                    chat_id=user_id,
                    from_chat_id=int(src),
                    message_id=msg_id,
                    protect_content=bt_cfg.get("protect", False),
                )
                if cap_tpl:
                    my_kwargs = dict(kwargs)
                    if "{original_caption}" in cap_tpl or "{file_name}" in cap_tpl:
                        try:
                            orig_msg = await client.get_messages(int(src), msg_id)
                            orig_cap = (orig_msg.caption or orig_msg.text or "") if orig_msg else ""
                            doc = getattr(orig_msg, "document", None) or getattr(orig_msg, "video", None) or getattr(orig_msg, "audio", None)
                            fname = getattr(doc, "file_name", "") or ""
                            my_kwargs["caption"] = _fmt_delivery_text(cap_tpl, user_obj, story).replace("{original_caption}", orig_cap).replace("{file_name}", fname)
                        except Exception:
                            my_kwargs["caption"] = _fmt_delivery_text(cap_tpl, user_obj, story).replace("{original_caption}", "").replace("{file_name}", "")
                    else:
                        my_kwargs["caption"] = _fmt_delivery_text(cap_tpl, user_obj, story)
                    sent = await client.copy_message(**my_kwargs)
                else:
                    sent = await client.copy_message(**kwargs)

                sent_ids.append(sent.id)
                sent_count += 1
            except Exception as e:
                logger.warning(f"DM Delivery failed msg {msg_id}: {e}")
                failed_count += 1
            await asyncio.sleep(0.08)

        # Finalize and Cleanup
        try: await fetch_msg.delete()
        except: pass
        dm_aborts.discard(user_id)

        s_name = story.get('story_name_en', 'Story')
        rep_tpl = (bt_cfg.get("delivery_report") or "").strip()
        
        if autodel <= 0:
            time_str = "Disabled"
        elif autodel < 60:
            time_str = f"{autodel} seconds"
        elif autodel % 3600 == 0:
            time_str = f"{autodel // 3600} hours"
        elif autodel % 60 == 0:
            time_str = f"{autodel // 60} minutes"
        else:
            time_str = f"{autodel} seconds"

        status_text = "Important" if not aborted else "Stopped"
        autodel_text = f"{_sc('Due to copyright, all messages will auto-delete after')} <b>{time_str}</b>." if autodel > 0 else _sc("All sent files are now available below.")
        reaccess_text = f"{_sc('To re-access, go to')} <b>{_sc('Main Menu')} ⟶ {_sc('My Stories')}</b>."
        
        if rep_tpl:
            summary = _fmt_delivery_text(
                rep_tpl,
                user_obj,
                story,
                sent_count=sent_count,
                fail_count=failed_count,
            ).replace("{time}", time_str).replace("DELIVERY COMPLETE", _sc(status_text))
        else:
            summary = (
                f"‣ <b>{_sc('IMPORTANT')}:</b> {sent_count} ꜰɪʟᴇ(ꜱ) ᴅᴇʟɪᴠᴇʀᴇᴅ! ᴀʟʟ ꜱᴇɴᴛ ꜰɪʟᴇꜱ ᴀʀᴇ ɴᴏᴡ ᴀᴠᴀɪʟᴀʙʟᴇ ʙᴇʟᴏᴡ.\n"
                f"ᴛᴏ ʀᴇ-ᴀᴄᴄᴇꜱꜱ, ɢᴏ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ ⟶ ᴍʏ ꜱᴛᴏʀɪᴇꜱ."
            )

        kb_regen = [[InlineKeyboardButton(f"⟳ {_sc('Regenerate Files')}", callback_data=f"mb#deliver_dm#{story_id_str}")]]
        notice = await client.send_message(user_id, summary, reply_markup=InlineKeyboardMarkup(kb_regen))

        from utils import log_delivery, log_arya_event
        
        from bson.objectid import ObjectId
        purchase = await db.db.premium_purchases.find_one({"user_id": int(user_id), "story_id": ObjectId(story_id_str)})
        order_id = purchase.get("order_id", "") if purchase else ""
        logged_deliveries = purchase.get("logged_deliveries", []) if purchase else []
        username = user_obj.username if user_obj else ""
        last_name = user_obj.last_name if user_obj else ""
        
        asyncio.create_task(log_arya_event(
            event_type="DELIVERY INITIATED",
            user_id=user_id,
            user_info={"first_name": user_obj.first_name if user_obj else "User", "last_name": last_name, "username": username},
            details=f"Story: {s_name}\nMethod: DM\nOrder ID: <code>{order_id}</code>\nStatus: Sent {sent_count}, Failed {failed_count}\nAuto-delete: {time_str}"
        ))

        if purchase and "dm" not in logged_deliveries:
            await db.db.premium_purchases.update_one({"_id": purchase["_id"]}, {"$addToSet": {"logged_deliveries": "dm"}})
            asyncio.create_task(log_delivery(
                bot_username=client.me.username,
                user_id=user_id,
                user_first_name=user_obj.first_name if user_obj else "Unknown",
                s_name=s_name,
                d_type="dm",
                status=f"Sent {sent_count}, Failed {failed_count}",
                username=username,
                order_id=order_id,
                user_last_name=last_name
            ))

        if autodel > 0 and sent_ids:
            asyncio.create_task(_delete_later(client, user_id, sent_ids, autodel))

    except Exception as e:
        logger.error(f"DM Delivery error: {e}")
        await client.send_message(user_id, f"❌ Delivery failed: {e}\n\nPlease contact admin.")


# ─────────────────────────────────────────────────────────────────
# Channel Link Delivery
# ─────────────────────────────────────────────────────────────────
async def _do_channel_delivery(client, user_id, story, status_msg=None):
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    story_id_str = str(story['_id'])
    try:
        bt = await db.db.premium_bots.find_one({"id": client.me.id})
        bt_cfg = bt.get("config", {}) if bt else {}
        mode = story.get("delivery_mode") or ("single" if story.get("channel_id") else "pool")
        if mode == "dm_only":
            await client.send_message(user_id, "ℹ️ This story is configured for DM delivery only.")
            return await _do_dm_delivery(client, user_id, story)

        # Build candidate pool for rotation/failover
        candidates = []
        if story.get("channel_id"):
            candidates.append(int(story["channel_id"]))
        if isinstance(story.get("channel_pool"), list):
            for cid in story["channel_pool"]:
                try:
                    candidates.append(int(cid))
                except Exception:
                    pass
        if not candidates:
            # fallback to global delivery list
            globals_ = await db.db.premium_channels.find({"type": "delivery"}).to_list(length=300)
            candidates = [int(c["channel_id"]) for c in globals_]

        if not candidates:
            return await client.send_message(user_id, "❌ No delivery channels are configured for this story. Please use DM Delivery instead.")

        # Round-robin start index (stored in premium_settings)
        try:
            rr = await db.db.premium_settings.find_one_and_update(
                {"_id": "delivery_rr"},
                {"$inc": {"idx": 1}},
                upsert=True,
                return_document=True,
            )
            start_idx = int((rr or {}).get("idx", 0)) % len(candidates)
        except Exception:
            start_idx = 0

        def _rot(lst, s):
            return lst[s:] + lst[:s]

        channel_id = None
        last_err = None
        for cid in _rot(candidates, start_idx)[: min(25, len(candidates))]:
            try:
                # Force Pyrogram to resolve and cache the peer before attempting invite link
                try: await client.get_chat(int(cid))
                except Exception: pass
                
                # Validate bot can access/create link
                invite_link = await client.create_chat_invite_link(
                    int(cid),
                    member_limit=1,
                    name=f"user_{user_id}"
                )
                channel_id = int(cid)
                break
            except Exception as e:
                last_err = e
                continue

        if not channel_id:
            return await client.send_message(user_id, f"❌ Failed to generate invite link from delivery pool. Please use DM Delivery instead.\n<i>{last_err}</i>")

        # We already created invite_link in loop above when selecting channel_id
        # Create again to get the link object (safe if already created)
        invite_link = await client.create_chat_invite_link(int(channel_id), member_limit=1, name=f"user_{user_id}")
        
        from utils import log_delivery, log_arya_event
        user_obj = await client.get_users(user_id)
        
        from bson.objectid import ObjectId
        purchase = await db.db.premium_purchases.find_one({"user_id": int(user_id), "story_id": ObjectId(story_id_str)})
        order_id = purchase.get("order_id", "") if purchase else ""
        logged_deliveries = purchase.get("logged_deliveries", []) if purchase else []
        username = user_obj.username if user_obj else ""
        last_name = user_obj.last_name if user_obj else ""
        s_name_h = story.get('story_name_en', 'Unknown Story')
        
        asyncio.create_task(log_arya_event(
            event_type="DELIVERY INITIATED",
            user_id=user_id,
            user_info={"first_name": user_obj.first_name if user_obj else "User", "last_name": last_name, "username": username},
            details=f"Story: {s_name_h}\nMethod: CHANNEL\nOrder ID: <code>{order_id}</code>\nGenerated Link Channel: {channel_id}"
        ))

        if purchase and "channel" not in logged_deliveries:
            await db.db.premium_purchases.update_one({"_id": purchase["_id"]}, {"$addToSet": {"logged_deliveries": "channel"}})
            asyncio.create_task(log_delivery(
                bot_username=client.me.username,
                user_id=user_id,
                user_first_name=user_obj.first_name if user_obj else "Unknown",
                s_name=s_name_h,
                d_type="channel",
                status=f"Link created (Channel ID: {channel_id})",
                username=username,
                order_id=order_id,
                user_last_name=last_name
            ))

        # Mark this user as having used their channel link
        await db.db.users.update_one(
            {"id": int(user_id)},
            {"$addToSet": {"used_channels": story_id_str}}
        )

        s_name = story.get('story_name_en', 'Story')
        suc_tpl = (bt_cfg.get("success_msg") or "").strip()
        if suc_tpl:
            user_obj = await client.get_users(user_id)
            txt = _fmt_delivery_text(suc_tpl, user_obj, story).replace("{channel_link}", invite_link.invite_link)
            msg = await client.send_message(user_id, txt, disable_web_page_preview=True)
            _schedule_auto_delete(msg, 86400)
        else:
            # Result message
            if lang == 'hi':
                txt = (
                    f"<b>आपका 1-टाइम एक्सेस लिंक तैयार है!</b>\n\n"
                    f"<b>{s_name_h}</b>\n\n"
                    f"{invite_link.invite_link}\n\n"
                    "<blockquote>"
                    f"<i>यह लिंक केवल 1 व्यक्ति के लिए है। एक बार उपयोग करने पर, यह एक्सपायर हो जाएगा।\n"
                    f"भविष्य में इस स्टोरी को /mystories का उपयोग करके सीधे DM में प्राप्त किया जा सकता है।</i>\n"
                    "</blockquote>"
                    "<blockquote>"
                    f"<i>⚠️ यह मैसेज 24 घंटे में अपने आप डिलीट हो जाएगा। "
                    f"जैसे ही आप ज्वाइन करेंगे, प्राइवेसी के लिए लिंक को तुरंत रद्द कर दिया जाएगा।</i>\n"
                    "</blockquote>"
                )
                back_btn_txt = "« ❮ मुख्य मेनू"
            else:
                txt = (
                    f"<b>Your 1-Time Access Link is Ready!</b>\n\n"
                    f"<b>{s_name_h}</b>\n\n"
                    f"{invite_link.invite_link}\n\n"
                    "<blockquote>"
                    f"<i>This link works for exactly 1 person only. Once used, it expires.\n"
                    f"Future access to this story will be via DM delivery only by using /mystories.</i>\n"
                    "</blockquote>"
                    "<blockquote>"
                    f"<i>⚠️ This message will auto-delete in 24 hours. "
                    f"Once you join, the link will be revoked automatically to ensure privacy.</i>\n"
                    "</blockquote>"
                )
                back_btn_txt = f"« ❮ {_sc('MAIN MENU')}"

            kb_link = [[InlineKeyboardButton(back_btn_txt, callback_data="mb#main_back")]]
            msg = await client.send_message(user_id, txt, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(kb_link))
            _schedule_auto_delete(msg, 86400)

    except Exception as e:
        logger.error(f"Channel Link Error: {e}")
        await client.send_message(
            user_id,
            f"❌ Failed to generate channel link. Falling back to DM Delivery...\n<i>Error: {e}</i>"
        )
        await _do_dm_delivery(client, user_id, story)

def _schedule_auto_delete(msg, seconds: int):
    async def task():
        await asyncio.sleep(seconds)
        try:
            await msg.delete()
        except:
            pass
    asyncio.create_task(task())

# ─────────────────────────────────────────────────────────────────
# Join Revoker Helper
# ─────────────────────────────────────────────────────────────────
async def _process_chat_member(client, update):
    # If a user joins the channel with an invite link
    if getattr(update, "new_chat_member", None) and getattr(update, "invite_link", None):
        try:
            # Revoke it instantly to prevent reuse/leakage
            await client.revoke_chat_invite_link(update.chat.id, update.invite_link.invite_link)
        except Exception:
            pass

