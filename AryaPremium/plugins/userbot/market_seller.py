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
        "qr_msg": "<b>💳 Complete Payment</b>\n\n• Scan the QR code above.\n• Amount: ₹{price}\n\n<b>After paying, send the successful payment screenshot here.</b>",
        "wait_ver": "⏳ Your payment is being verified, please wait (approx 5 minutes)...",
        "notify": "🔔 Notify Admin"
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
        "qr_msg": "<b>💳 पेमेंट पूरा करें</b>\n\n• ऊपर QR स्कैन करें।\n• राशि: ₹{price}\n\n<b>पेमेंट के बाद स्क्रीनशॉट यहाँ भेजें।</b>",
        "wait_ver": "⏳ आपके भुगतान का सत्यापन हो रहा है...",
        "notify": "🔔 एडमिन को सूचित करें"
    }
}

def _get_main_menu(lang='en'):
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
            r.append(InlineKeyboardButton(_sc("UPDATES"), url=updates_url))
        if support_url:
            r.append(InlineKeyboardButton(_sc("SUPPORT"), url=support_url))
        if r:
            rows.append(r)
    base = _get_main_menu(lang).inline_keyboard
    # Insert URL row above Close
    if rows:
        base = base[:2] + rows + base[2:]
    return InlineKeyboardMarkup(base)


def _menu_card_text(user, bt_cfg: dict, bot_name: str) -> str:
    # 1. Clickable First Name (Escape for HTML safety)
    u_mention = f'<a href="tg://user?id={user.id}">{html.escape((user.first_name or "User").strip())}</a>'
    
    # --- 1. Welcome Section ---
    welcome = bt_cfg.get("welcome")
    if welcome:
        if welcome.lower() == "disable":
            welcome = ""
        else:
            welcome = welcome.replace("{user}", u_mention).replace("{name}", u_mention).replace("{first_name}", u_mention)
    else:
        welcome = ""
    
    # --- 2. About Section ---
    about = bt_cfg.get("about")
    if not about:
        about = ""
        
    # --- 3. Quote Section ---
    quote = bt_cfg.get("quote")
    if quote and quote.lower() == "disable":
        quote = ""
    elif not quote:
        quote = "❝ IF YOU WERE TO WRITE A STORY WITH ME IN THE LEAD ROLE... IT WOULD CERTAINLY BE A TRAGEDY. ❞"
        
    # --- 4. Author Section ---
    author = bt_cfg.get("quote_author")
    if author and author.lower() == "disable":
        author = ""
    elif not author:
        author = "<b>— KEN KENEKI</b>"
    else:
        author = f"<b>— {author}</b>"
    
    # Assembly into Quatoblocks
    # We use expandable="true" for maximum parser compatibility.
    blocks = []
    if welcome.strip():
        blocks.append(f'<blockquote expandable="true">{welcome}</blockquote>')
    if about.strip():
        blocks.append(f'<blockquote expandable="true">{about}</blockquote>')
        
    # Separator for the quote block
    if about.strip() and quote.strip():
        blocks.append("")
        
    if quote.strip():
        blocks.append(f'<blockquote expandable="true">{quote}</blockquote>')
    if author.strip():
        # Author usually follows quote immediately
        blocks.append(f'<blockquote expandable="true">{author}</blockquote>')
        
    return "\n".join(blocks)



async def _edit_main_menu_in_place(client, query, user, lang: str):
    """
    Edit current message back to main menu when possible.
    Supports random media rotation on navigation.
    """
    bt = await db.db.premium_bots.find_one({"id": client.me.id})
    bt_cfg = bt.get("config", {}) if bt else {}
    bot_name = client.me.first_name
    msg_txt = _menu_card_text(user, bt_cfg, bot_name)
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
    msg_txt = _menu_card_text(user, bt_cfg, bot_name)
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
    price = story.get('price', 0)

    # Description intentionally hidden for now.
    header_txt = (
        f"<blockquote expandable>"
        f"<b>♨️Story :</b> {to_mathbold(name)}\n"
        f"<b>🔰Status :</b> <b>{status}</b>\n"
        f"<b>🖥Platform :</b> <b>{platform}</b>\n"
        f"<b>🧩Genre :</b> <b>{genre}</b>\n"
        f"<b>🎬Episodes :</b> <b>{episodes}</b>"
        f"</blockquote>"
    )
    txt = header_txt
        
    kb = [
        [InlineKeyboardButton(f"✅ {_sc('CONFIRM')}", callback_data=f"mb#show_tc#{str(story['_id'])}")],
        [InlineKeyboardButton(f"❮ {_sc('BACK')}", callback_data="mb#return_main")]
    ]
    markup = InlineKeyboardMarkup(kb)
    
    from pyrogram import enums

    # Need to send a message but remove the reply keyboard first (from marketplace)
    tmp = await client.send_message(user_id, "<b>› › ⏳ " + _sc("LOADING PROFILE...") + "</b>", reply_markup=ReplyKeyboardRemove(), parse_mode=enums.ParseMode.HTML)
    
    try:
        if image:
            try:
                await client.send_photo(
                    user_id,
                    photo=image,
                    caption=txt,
                    reply_markup=markup,
                    parse_mode=enums.ParseMode.HTML,
                )
                await tmp.delete()
                return
            except Exception:
                pass
        await client.send_message(
            user_id,
            txt,
            reply_markup=markup,
            disable_web_page_preview=True,
            parse_mode=enums.ParseMode.HTML
        )
        await tmp.delete()
    except Exception as e:
        logger.error(f"Error in show_story_profile: {e}")
        try: await tmp.delete()
        except: pass

async def _show_tc(client, user_id, story_id, lang='en'):
    # Enforced premium UI exactly as specified
    tc_text = (
        "<b>⟦ 𝗧𝗘𝗥𝗠𝗦 & 𝗖𝗢𝗡𝗗𝗜𝗧𝗜𝗢𝗡𝗦 ⟧</b>\n\n"
        "<blockquote expandable>"
        "𝖡𝖾𝖿𝗈𝗋𝖾 𝗉𝗎𝗋𝖼𝗁𝖺𝗌𝗂𝗇𝗀, 𝗉𝗅𝖾𝖺𝗌𝖾 𝗋𝖾𝖺𝖽 𝖺𝗇𝖽 𝖺𝗀𝗋𝖾𝖾 𝗍𝗈 𝗍𝗁𝖾 𝖿𝗈𝗅𝗅𝗈𝗐𝗂𝗇𝗀:\n\n"
        "• <b>𝗠𝗶𝘀𝘀𝗶𝗻𝗴 𝗘𝗽𝗶𝘀𝗼𝗱𝗲𝘀</b>\n"
        "𝟹–𝟻 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝖻𝖾 𝗎𝗇𝖺𝗏𝖺𝗂𝗅𝖺𝖻𝗅𝖾 𝗂𝖿 𝗇𝗈𝗍 𝗉𝗎𝖻𝗅𝗂𝖼𝗅𝗒 𝗋𝖾𝗅𝖾𝖺𝗌𝖾𝖽.\n"
        "𝖨𝖿 𝖺𝗏𝖺𝗂𝗅𝖺𝖻𝗅𝖾 𝗅𝖺𝗍𝖾𝗋, 𝗍𝗁𝖾𝗒 𝗐𝗂𝗅𝗅 𝖻𝖾 𝖺𝖽𝖽𝖾𝖽 𝖺𝗎𝗍𝗈𝗆𝖺𝗍𝗂𝖼𝖺𝗅𝗅𝗒.\n"
        "𝖬𝗈𝗋𝖾 𝗍𝗁𝖺𝗇 𝟻 𝗆𝗂𝗌𝗌𝗂𝗇𝗀? 𝖢𝗈𝗇𝗍𝖺𝖼𝗍 𝗌𝗎𝗉𝗉𝗈𝗋𝗍.\n\n"
        "• <b>𝗤𝘂𝗮𝗹𝗶𝘁𝘆</b>\n"
        "𝖲𝗈𝗆𝖾 𝗈𝗅𝖽𝖾𝗋 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝗁𝖺𝗏𝖾 𝗋𝖾𝖽𝗎𝖼𝖾𝖽 𝗊𝗎𝖺𝗅𝗂𝗍𝗒.\n"
        "𝖶𝖾 𝖼𝖺𝗇𝗇𝗈𝗍 𝗀𝗎𝖺𝗋𝖺𝗇𝗍𝖾𝖾 𝟣𝟢𝟢% 𝗊𝗎𝖺𝗅𝗂𝗍𝗒, 𝖻𝗎𝗍 𝖺𝗅𝗐𝖺𝗒𝗌 𝗉𝗋𝗈𝗏𝗂𝖽𝖾 𝖻𝖾𝗌𝗍 𝗏𝖾𝗋𝗌𝗂𝗈𝗇.\n\n"
        "• <b>𝗘𝗽𝗶𝘀𝗼𝗱𝗲 𝗢𝗿𝗱𝗲𝗿</b>\n"
        "𝖤𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝗋𝖺𝗋𝖾𝗅𝗒 𝖻𝖾 𝗈𝗎𝗍 𝗈𝖿 𝗌𝖾𝗊𝗎𝖾𝗇𝖼𝖾.\n"
        "𝖠𝗅𝗅 𝖿𝗂𝗅𝖾𝗌 𝖺𝗋𝖾 𝖼𝗅𝖾𝖺𝗇𝖾𝖽 𝖺𝗇𝖽 𝖻𝗋𝖺𝗇𝖽𝖾𝖽 𝖻𝗒 𝖠𝗋𝗒𝖺 𝖡𝗈𝗍.\n\n"
        "• <b>𝗡𝗼 𝗥𝗲𝗳𝘂𝗻𝗱𝘀</b>\n"
        "𝖭𝗈 𝗋𝖾𝖿𝗎𝗇𝖽𝗌 𝗈𝗇𝖼𝖾 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗂𝗌 𝖼𝗈𝗇𝖿𝗂𝗋𝗆𝖾𝖽 𝖺𝗇𝖽 𝖽𝖾𝗅𝗂𝗏𝖾𝗋𝗒 𝗌𝗍𝖺𝗋𝗍𝗌.\n\n"
        "• <b>𝗙𝗮𝗸𝗲 𝗦𝗰𝗿𝗲𝗲𝗻𝘀𝗵𝗼𝘁𝘀</b>\n"
        "𝖥𝖺𝗄𝖾 𝗈𝗋 𝗂𝗇𝗏𝖺𝗅𝗂𝖽 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗉𝗋𝗈𝗈𝖿𝗌 𝗐𝗂𝗅𝗅 𝗅𝖾𝖺𝖽 𝗍𝗈 𝗉𝖾𝗋𝗆𝖺𝗇𝖾𝗇𝗍 𝖻𝖺𝗇."
        "</blockquote>"
    )
    
    kb = [
        [InlineKeyboardButton("𝗜 𝗔𝗰𝗰𝗲𝗽𝘁", callback_data=f"mb#tc_accept_{story_id}")],
        [InlineKeyboardButton("𝗥𝗲𝗷𝗲𝗰𝘁", callback_data="mb#tc_reject"),
         InlineKeyboardButton("‹ Back", callback_data=f"mb#view_{story_id}")]
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

    name = story.get(f'story_name_{lang}', story.get('story_name_en'))
    platform = story.get('platform', 'Other')
    ep_count = abs(story.get('end_id', 0) - story.get('start_id', 0)) + 1 if story.get('end_id') else "?"
    price = story.get('price', 0)

    upi_enabled = bot_cfg.get('upi_enabled', True)  # Default ON unless admin disables
    upi_restricted = _is_upi_restricted()
    show_upi = upi_enabled and not upi_restricted

    # The user wants this specific block in a quoteblock and collapsible (expandable)
    # Removing ONLY the ⚡ emoji and fixing font (standard readable)
    rzp_benefit = (
        f"<blockquote expandable>"
        f"Instant methods ensure immediate delivery.\n"
        f"Pay via Razorpay for instant access — no waiting, no manual verification.\n"
        f"Supports UPI, Debit/Credit Card, Net Banking, Wallets & more."
        f"</blockquote>"
    )

    txt = (
        f"<b>🛒 SHOPPING CART</b>\n"
        f"────────────────────\n"
        f"<b>📦 Product :</b> {name}\n"
        f"<b>🏷️ Platform :</b> {platform}\n"
        f"<b>🎬 Episodes :</b> ~{ep_count}\n"
        f"────────────────────\n"
        f"<b>💰 Total Amount : ₹{price}</b>\n"
        f"────────────────────\n"
        f"<i>Select your preferred payment method:</i>\n\n"
        f"💡 {rzp_benefit}"
    )

    kb = [
        [InlineKeyboardButton("RAZORPAY", callback_data=f"mb#pay#razorpay#{str(story['_id'])}")],
    ]
    if show_upi:
        kb.append([InlineKeyboardButton(f"UPI (Manual)", callback_data=f"mb#pay#upi#{str(story['_id'])}")])
    kb.append([InlineKeyboardButton("❮ BACK", callback_data="mb#return_main")])

    if is_msg:
        await msg_or_query.reply_text(txt, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await msg_or_query.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))


# ─────────────────────────────────────────────────────────────────
# /start Handler
# ─────────────────────────────────────────────────────────────────
async def _process_start(client, message):
    user_id = message.from_user.id
    
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
    args = message.command

    if 'lang' not in user:
        kb = [[InlineKeyboardButton("English", callback_data="mb#lang#en"),
               InlineKeyboardButton("हिंदी", callback_data="mb#lang#hi")]]
        return await message.reply_text("Please select your language / कृपया अपनी भाषा चुनें:", reply_markup=InlineKeyboardMarkup(kb))

    lang = user.get('lang', 'en')

    # ── Deep Link Handler: show story preview first ──
    if len(args) > 1 and args[1].startswith("buy_"):
        story_id = args[1].replace("buy_", "")
        from bson.objectid import ObjectId
        story = await db.db.premium_stories.find_one({"_id": ObjectId(story_id)})
        if story:
            has_paid = await db.has_purchase(user_id, story_id)
            if has_paid:
                await message.reply_text("✅ You already own this story. Sending delivery options...")
                return await dispatch_delivery_choice(client, user_id, story)
            return await _show_story_profile(client, user_id, story, lang)

    # Standard Main Menu
    msg_txt = None
        
    wait_msg = await message.reply_text("<b>› › ⏳ " + _sc("WAIT A SECOND...") + "</b>", parse_mode=enums.ParseMode.HTML)
    await asyncio.sleep(0.4)
    await wait_msg.edit_text("<b>› › ⌛ " + _sc("WAIT A SECOND...") + "</b>", parse_mode=enums.ParseMode.HTML)
    await asyncio.sleep(0.4)
    await wait_msg.delete()

    # Always use the centralized menu sender (handles MEDIA_EMPTY and optional URL buttons)
    await _send_main_menu(client, user_id, message.from_user, lang, reply_to_message_id=message.id)

    
# ─────────────────────────────────────────────────────────────────
# /mystories Handler
# ─────────────────────────────────────────────────────────────────
async def _process_my_stories(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    
    # Send a fresh "My Stories" menu
    purchases = user.get('purchases', [])
    from bson.objectid import ObjectId

    PAGE_SIZE = 5
    page = 0
    total = len(purchases)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page_purchases = purchases[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    kb = []
    for pid in page_purchases:
        try:
            st = await db.db.premium_stories.find_one({"_id": ObjectId(pid)})
            if st:
                s_name = st.get(f'story_name_{lang}', st.get('story_name_en'))
                kb.append([InlineKeyboardButton(s_name, callback_data=f"mb#purchased_view_{pid}")])
        except Exception:
            pass

    if total_pages > 1:
        nav = []
        nav.append(InlineKeyboardButton(f"ᴘᴀɢᴇ 1/{total_pages}", callback_data="mb#noop"))
        nav.append(InlineKeyboardButton("𝗡𝗲𝘅𝘁 ❭", callback_data="mb#my_buys_page_1"))
        kb.append(nav)

    kb.append([InlineKeyboardButton(_bs("Back to Menu"), callback_data="mb#main_back")])

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
        kb.insert(0, [InlineKeyboardButton(_bs("OPEN MARKETPLACE"), callback_data="mb#main_marketplace")])

    await client.send_message(user_id, txt_b, reply_markup=InlineKeyboardMarkup(kb))


# ─────────────────────────────────────────────────────────────────
# Text Handler (only for Reply Keyboard marketplace flow)
# ─────────────────────────────────────────────────────────────────
async def _process_text(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    lang = user.get('lang', 'en')
    txt = message.text.strip()

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
            await message.reply_text("✅ You already own this story. Sending delivery options...", reply_markup=ReplyKeyboardRemove())
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
        kb.append(["🔍 " + "SEARCH"])
        kb.append(["CAN'T FIND? REQUEST NOW!"])
        kb.append(["« " + "𝗕𝗮𝗰𝗸 𝘁𝗼 𝗠𝗲𝗻𝘂"])

        await message.reply_text(
            f"<b>Available Stories — {txt}</b>\n\n"
            f"All available stories and their prices are shown in the menu below. "
            f"Please tap or click on any story name from the keyboard menu below to view details and purchase it:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
        )
        return
        
    # ── REQUEST STORY trigger ──
    if txt == "CAN'T FIND? REQUEST NOW!":
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
    if txt == "🔍 " + "SEARCH":
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
            kb = []
            for i in range(0, len(platforms), 2):
                row = platforms[i:i+2]
                kb.append(row)
            if "Other" not in platforms:
                kb.append(["Other"])
            kb.append(["« " + "𝗕𝗮𝗰𝗸 𝘁𝗼 𝗠𝗲𝗻𝘂"])
            await query.message.delete()
            await client.send_message(
                user_id,
                f"<b>🎧 Platform Selection</b>\n\nChoose a platform from the keyboard below:",
                reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
            )

        elif action == "profile":
            u = query.from_user
            joined = user.get('joined_date', 'N/A')
            if isinstance(joined, datetime):
                joined = joined.strftime('%d %b %Y')
            purchases = user.get('purchases', [])
            uname = f"@{u.username}" if u.username else "N/A"
            lang_label = "English" if lang == 'en' else "हिंदी"
            name = f"{u.first_name or ''} {u.last_name or ''}".strip() or "Unknown"
            txt_p = (
                "<b>╔═⟦ 𝗣𝗥𝗢𝗙𝗜𝗟𝗘 ⟧═╗</b>\n\n"
                f"<b>⧉ ɴᴀᴍᴇ        ⟶</b> {name}\n"
                f"<b>⧉ ᴜꜱᴇʀɴᴀᴍᴇ    ⟶</b> {uname}\n"
                f"<b>⧉ ᴛɢ ɪᴅ       ⟶</b> <code>{u.id}</code>\n\n"
                "<b>╠══════════════════╣</b>\n\n"
                f"<b>⧉ ᴘᴜʀᴄʜᴀꜱᴇꜱ   ⟶</b> {len(purchases)}\n"
                f"<b>⧉ ʟᴀɴɢᴜᴀɢᴇ    ⟶</b> {lang_label}\n"
                f"<b>⧉ ᴊᴏɪɴᴇᴅ      ⟶</b> {joined}\n\n"
                "<b>╚══════════════════╝</b>"
            )
            kb = [
                [InlineKeyboardButton("📝 " + _sc("MY REQUESTS"), callback_data="mb#my_reqs_0")],
                [InlineKeyboardButton(f"{_sc('LANGUAGE')}", callback_data="mb#main_settings")],
                [InlineKeyboardButton(f"{_sc('BACK')}", callback_data="mb#main_back")]
            ]
            await _safe_edit(query.message, text=txt_p, markup=InlineKeyboardMarkup(kb))
            return

        elif action == "settings":
            kb = [
                [InlineKeyboardButton("English", callback_data="mb#lang#en"),
                 InlineKeyboardButton("हिंदी", callback_data="mb#lang#hi")],
                [InlineKeyboardButton(f"❮ {_sc('BACK')}", callback_data="mb#main_back")]
            ]
            await _safe_edit(query.message, text=f"<b>⚙️ Settings</b>\n\nSelect your language:", markup=InlineKeyboardMarkup(kb))

        elif action == "help":
            return await _show_help_menu(client, query, 0)

        elif action == "close":
            await query.message.delete()

        elif action == "back":
            await _edit_main_menu_in_place(client, query, query.from_user, lang)

    elif cmd.startswith("my_reqs_"):
        page = int(cmd.replace("my_reqs_", ""))
        reqs = await db.db.premium_requests.find({"user_id": user_id, "bot_id": client.me.id}).sort("created_at", -1).to_list(length=None)
        if not reqs:
            return await _safe_answer(query, "You haven't made any story requests yet.", show_alert=True)
        items_per_page = 10
        total_pages = max(1, (len(reqs) + items_per_page - 1) // items_per_page)
        if page < 0: page = 0
        if page >= total_pages: page = total_pages - 1
        subset = reqs[page*items_per_page : (page+1)*items_per_page]
        txt_req = f"<b>📝 My Story Requests (Page {page+1}/{total_pages})</b>\n\nClick on any request to view its status:"
        kb = []
        for r in subset:
            sname = r.get('story_name', 'Unknown')
            if len(sname) > 25: sname = sname[:22] + "..."
            status_emoji = {"Sent": "📮", "Pending": "⏳", "Searching": "🔍", "Posting": "📤", "Posted": "✅", "Completed": "🎉"}.get(r.get('status', 'Sent'), "📌")
            kb.append([InlineKeyboardButton(f"{status_emoji} {sname}", callback_data=f"mb#my_req_{str(r['_id'])}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("❬ Prev", callback_data=f"mb#my_reqs_{page-1}"))
        if page < total_pages - 1: nav.append(InlineKeyboardButton("Next ❭", callback_data=f"mb#my_reqs_{page+1}"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton("« " + _sc("BACK TO PROFILE"), callback_data="mb#main_profile")])
        await _safe_edit(query.message, text=txt_req, markup=InlineKeyboardMarkup(kb))
        return

    elif cmd.startswith("my_req_"):
        req_id = cmd.replace("my_req_", "")
        try:
            from bson import ObjectId
            r = await db.db.premium_requests.find_one({"_id": ObjectId(req_id), "user_id": user_id})
        except: r = None
        if not r:
            return await _safe_answer(query, "Request not found.", show_alert=True)
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

    # -- My Buys (My Stories) --
    elif cmd == "my_buys" or cmd.startswith("my_buys_page_"):
        await query.answer()
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
        await db.update_user(user_id, {"lang": new_lang})
        await query.answer("Language Updated!")
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
            f"<blockquote expandable>"
            f"𝖡𝖾𝖿𝗈𝗋𝖾 𝗉𝗎𝗋𝖼𝗁𝖺𝗌𝗂𝗇𝗀, 𝗉𝗅𝖾𝖺𝗌𝖾 𝗋𝖾𝖺𝖽 𝖺𝗇𝖽 𝖺𝗀𝗋𝖾𝖾 𝗍𝗈 𝗍𝗁𝖾 𝖿𝗈𝗅𝗅𝗈𝗐𝗂ɴ𝗀:\n\n"
            f"• <b>𝗠𝗶𝘀𝘀𝗶𝗻𝗴 𝗘𝗽𝗶𝘀𝗼𝗱𝗲𝘀</b>\n"
            f"𝟹–𝟻 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝖻𝖾 𝗎𝗇𝖺𝗏𝖺𝗂ʟ𝖺𝖻𝗅𝖾 𝗂𝖿 𝗇𝗈𝗍 𝗉𝗎𝖻𝗅𝗂𝖼𝗅𝗒 𝗋𝖾𝗅𝖾𝖺𝗌𝖾𝖽.\n"
            f"𝖨𝖿 𝖺𝗏𝖺𝗂ʟ𝖺𝖻𝗅𝖾 𝗅𝖺𝗍𝖾𝗋, 𝗍𝗁𝖾𝗒 𝗐𝗂𝗅𝗅 𝖻𝖾 𝖺𝖽𝖽𝖾𝖽 𝖺𝗎𝗍𝗈𝗆𝖺𝗍𝗂𝖼𝖺𝗅𝗅𝗒.\n"
            f"𝖬𝗈𝗋𝖾 𝗍𝗁𝖺𝗇 𝟧 𝗆𝗂𝗌𝗌𝗂𝗇𝗀? 𝖢𝗈𝗇𝗍𝖺𝖼𝗍 𝗌𝗎𝗉𝗉𝗈𝗋𝗍.\n\n"
            f"• <b>𝗤𝘂𝗮𝗹𝗶𝘁𝘆</b>\n"
            f"𝖲𝗈𝗆𝖾 𝗈𝗅𝖽𝖾𝗋 𝖾𝗉𝗂𝗌𝗈𝖽𝖾𝗌 𝗆𝖺𝗒 𝗁𝖺𝗏𝖾 𝗋𝖾𝖽𝗎𝖼𝖾𝖽 𝗊𝗎𝖺𝗅𝗂𝘁𝗒.\n"
            f"𝖶𝖾 𝖼𝖺𝗇𝗇𝗈𝗍 𝗀𝗎𝖺𝗋𝖺𝗇𝗍𝖾𝖾 𝟣𝟢𝟢% 𝗊𝗎𝖺𝗅𝗂𝗍𝗒, 𝖻𝗎𝗍 𝖺𝗅𝗐𝖺𝗒𝗌 𝗉𝗋𝗈𝗏𝗂𝖽𝖾 𝖻𝖾𝗌𝗍 𝗏𝖾𝗋𝗌𝗂𝗈𝗻.\n\n"
            f"• <b>𝗘𝗽𝗶𝘀𝗼𝗱𝗲 𝗢𝗿𝗱𝗲𝗿</b>\n"
            f"𝖤𝗉𝗂𝗌𝗈ᴅ𝖾𝗌 𝗆𝖺𝗒 𝗋𝖺𝗋𝖾𝗅𝗒 𝖻𝖾 𝗈𝗎𝗍 𝗈𝖿 𝗌𝖾𝗊𝗎𝖾𝗇𝖼𝖾.\n"
            f"𝖠𝗅𝗅 𝖿𝗂𝗅𝖾𝗌 𝖺𝗋𝖾 𝖼𝗅𝖾𝖺𝗇𝖾𝖽 𝖺𝗇𝖽 𝖻𝗋𝖺𝗇𝖽𝖾𝖽 𝖻𝗒 𝖠𝗋𝗒𝖺 𝖡𝗈𝗍.\n\n"
            f"• <b>𝗡𝗼 𝗥𝗲𝗳𝘂𝗻𝗱𝘀</b>\n"
            f"𝖭𝗈 𝗋𝖾𝖿𝗎𝗇𝖽𝗌 𝗈𝗇𝼄𝖾 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗂𝗌 𝖼𝗈𝗇𝖿𝗂𝗋𝗆𝖾𝖽 𝖺𝗇𝖽 𝖽𝖾𝗅𝗂𝗏𝖾𝗋𝗒 𝗌𝗍𝖺𝗋𝗍𝗌.\n\n"
            f"• <b>𝗙𝗮𝗸𝗲 𝗦𝗰𝗿𝗲𝗲𝗻𝘀𝗵𝗼𝘁𝘀</b>\n"
            f"𝖥𝖺𝗄𝖾 𝗈𝗋 𝗂𝗇𝗏𝖺𝗅𝗂𝖽 𝗉𝖺𝗒𝗆𝖾𝗇𝗍 𝗉𝗋𝗈𝗈𝖿𝗌 𝗐𝗂𝗅𝗅 𝗅𝖾𝖺𝖽 𝗍𝗈 𝗉𝖾𝗋𝗆𝖺𝗇𝖾𝗇𝗍 𝖻𝖺𝗇."
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
            method_info = "Verified Purchase"
            if purchase:
                src = str(purchase.get("source", "manual")).lower()
                amount_paid = purchase.get("amount", story.get('price', 0))
                if src == "razorpay": method_info = f"💳 Razorpay (₹{amount_paid})"
                elif src == "easebuzz": method_info = f"💸 Easebuzz (₹{amount_paid})"
                elif src == "upi": method_info = f"🏦 Manual UPI (₹{amount_paid})"
                else: method_info = f"🛒 {src.capitalize()} (₹{amount_paid})"
            
            s_name = story.get(f'story_name_{lang}', story.get('story_name_en'))
            ep_count = abs(story.get('end_id', 0) - story.get('start_id', 0)) + 1 if story.get('end_id') else "?"

            # Clean payment label — no emojis
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

            txt_req = (
                "<b>⟦ 𝗦𝗧𝗢𝗥𝗬 ⟧</b>\n\n"
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
            txt = (
                f"<b>⟦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘 𝗣𝗔𝗬𝗠𝗘𝗡𝗧 ⟧</b>\n\n"
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
                kb2 = [[InlineKeyboardButton(f"☑️ {_sc('PAYMENT DONE')}", callback_data=f"mb#upi_done#{s_id}")]]
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
        await query.answer()
        await client.send_message(
            user_id,
            f"<b>☑️ {_sc('PAYMENT SUBMITTED')}</b>\n\n"
            "<blockquote expandable>"
            f"<i>{_sc('Our system needs to verify your payment.')}</i>\n"
            f"<i>{_sc('Please upload the successful payment screenshot here now.')}</i>\n"
            f"<i>{_sc('Ensure the Transaction ID / UTR is clearly visible.')}</i>\n"
            "</blockquote>"
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
        await query.answer()
        
        # Immediate visual feedback
        await query.message.edit_text(
            "<i>⏳ Initializing DM Delivery... Preparing your files.</i>",
            reply_markup=None
        )
        asyncio.create_task(_do_dm_delivery(client, user_id, story, query.message))

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
        f"⏳ <b>{_sc('Your payment is being verified')}</b>\n"
        "<blockquote expandable>"
        f"<i>{_sc('Please wait (approx 5 minutes)...')}</i>\n\n"
        f"<b>{_sc('Time Remaining')} :</b> 05:00\n"
        "</blockquote>"
    )
    msg = await message.reply_text(txt_user, reply_markup=InlineKeyboardMarkup(kb_user))

    import os
    if not os.path.exists("downloads"): os.makedirs("downloads")
    file_path = await client.download_media(message, file_name=f"downloads/proof_{checkout['_id']}.jpg")
    await db.db.premium_checkout.update_one(
        {"_id": checkout["_id"]},
        {"$set": {
            "status": "pending_admin_approval",
            "proof_path": file_path,
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
                    (f"⏳ <b>{_sc('Your payment is being verified')}</b>\n"
                     "<blockquote expandable>"
                     f"<i>{_sc('Please wait (approx 5 minutes)...')}</i>\n\n"
                     f"<b>{_sc('Time Remaining')} :</b> {m:02d}:{s:02d}\n"
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

    # Find purchase source to display as requested
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

    kb = [[InlineKeyboardButton(f"⤓ {_sc('RECEIVE IN DM')}", callback_data=f"mb#deliver_dm#{story_id_str}")]]
    if can_use_channel:
        kb.append([InlineKeyboardButton(f"➦ {_sc('ACCESS CHANNEL LINK')}", callback_data=f"mb#deliver_channel#{story_id_str}")])
    kb.append([InlineKeyboardButton(f"« ❮ {_sc('MAIN MENU')}", callback_data="mb#main_back")])

    await client.send_message(user_id, del_txt, reply_markup=InlineKeyboardMarkup(kb))


# ─────────────────────────────────────────────────────────────────
# DM Delivery (Arya-style: copy messages from source channel)
# ─────────────────────────────────────────────────────────────────
async def _do_dm_delivery(client, user_id, story, status_msg=None):
    try:
        dm_aborts.discard(user_id)
        bt = await db.db.premium_bots.find_one({"id": client.me.id})
        bt_cfg = bt.get("config", {}) if bt else {}
        user_obj = await client.get_users(user_id)
        src = story.get('source')
        start = story.get('start_id')
        end = story.get('end_id')
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
            txt = (
                f"<b>Your 1-Time Access Link is Ready!</b>\n\n"
                f"<b>{s_name}</b>\n\n"
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
            kb_link = [[InlineKeyboardButton(f"« ❮ {_sc('MAIN MENU')}", callback_data="mb#main_back")]]
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
