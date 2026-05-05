"""
Arya Premium Mgmt Bot — Mini App Banner Management
Commands:
  /addbanner  — Upload a banner image (1184×556 px) for the mini app hero slider
  /listbanner — List all manual banners
  /delbanner  — Delete a banner by number
"""
import logging
from datetime import datetime, timezone
from pyrogram import Client, filters
from pyrogram.types import Message
from database import db
from config import Config

logger = logging.getLogger(__name__)

REQUIRED_W = 1184
REQUIRED_H = 556
MAX_MANUAL  = 8

OWNER_IDS = Config.OWNER_IDS


def _is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS


# ── /addbanner ────────────────────────────────────────────────────
@Client.on_message(filters.command("addbanner") & filters.private)
async def cmd_add_banner(client: Client, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply("⛔ Unauthorized")

    # Check current count
    count = await db.db.mini_app_banners.count_documents({})
    if count >= MAX_MANUAL:
        return await message.reply(
            f"❌ Maximum {MAX_MANUAL} manual banners already added.\n"
            f"Use /delbanner to remove one first."
        )

    await message.reply(
        f"📸 **Send the banner image** for the mini app hero slider.\n\n"
        f"📐 **Required size: {REQUIRED_W} × {REQUIRED_H} px**\n"
        f"📏 Aspect ratio: ~2.13:1 (horizontal/wide)\n"
        f"📦 Format: JPG or PNG\n\n"
        f"_The image will be added as banner #{count + 1} of {MAX_MANUAL} manual slots._\n\n"
        f"⚠️ Send the image as a **photo** (not as a file/document).\n"
        f"Optionally add a caption: `Title | Subtitle`",
        parse_mode="markdown"
    )


@Client.on_message(filters.photo & filters.private)
async def recv_banner_photo(client: Client, message: Message):
    """Handle incoming photo for banner upload."""
    if not _is_owner(message.from_user.id):
        return

    # Only process if user initiated banner flow (check via state or caption)
    # We process ANY photo from owner — simpler UX
    caption = (message.caption or "").strip()
    title = ""
    subtitle = ""

    if "|" in caption:
        parts = caption.split("|", 1)
        title = parts[0].strip()
        subtitle = parts[1].strip()
    elif caption:
        title = caption

    # Get the largest photo
    photo = message.photo

    # Save banner
    count = await db.db.mini_app_banners.count_documents({})
    if count >= MAX_MANUAL:
        return await message.reply(f"❌ Max {MAX_MANUAL} banners reached. Delete one first with /delbanner")

    doc = {
        "file_id":   photo.file_id,
        "title":     title,
        "subtitle":  subtitle,
        "badge":     "",
        "order":     count + 1,
        "added_by":  message.from_user.id,
        "added_at":  datetime.now(timezone.utc),
    }
    result = await db.db.mini_app_banners.insert_one(doc)
    banner_id = str(result.inserted_id)

    size_note = ""
    if photo.width and photo.height:
        size_note = f"\n📐 Uploaded size: {photo.width}×{photo.height} px"
        if photo.width != REQUIRED_W or photo.height != REQUIRED_H:
            size_note += f"\n⚠️ Recommended: {REQUIRED_W}×{REQUIRED_H} px for best quality"

    await message.reply(
        f"✅ **Banner #{count + 1} added!**\n"
        f"🆔 ID: `{banner_id}`"
        f"{size_note}\n"
        f"{'📝 Title: ' + title if title else ''}\n"
        f"{'💬 Subtitle: ' + subtitle if subtitle else ''}\n\n"
        f"🌐 Will appear in the Mini App hero slider.\n"
        f"Use /listbanner to see all banners.",
        parse_mode="markdown"
    )


# ── /listbanner ───────────────────────────────────────────────────
@Client.on_message(filters.command("listbanner") & filters.private)
async def cmd_list_banners(client: Client, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply("⛔ Unauthorized")

    banners = await db.db.mini_app_banners.find({}).sort("order", 1).to_list(MAX_MANUAL)
    if not banners:
        return await message.reply(
            "📭 **No manual banners set.**\n\n"
            "Use /addbanner to add a banner image.\n"
            f"📐 Size: {REQUIRED_W}×{REQUIRED_H} px"
        )

    lines = [f"🖼 **Mini App Banners** ({len(banners)}/{MAX_MANUAL}):\n"]
    for i, b in enumerate(banners, 1):
        bid = str(b["_id"])
        t = b.get("title", "") or "—"
        lines.append(f"**{i}.** `{bid}`\n   📝 {t}")

    lines.append(f"\n_Auto banners (trending + newest) are added automatically._")
    lines.append(f"Use /delbanner `<number>` to remove.")

    await message.reply("\n".join(lines), parse_mode="markdown")


# ── /delbanner ────────────────────────────────────────────────────
@Client.on_message(filters.command("delbanner") & filters.private)
async def cmd_del_banner(client: Client, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply("⛔ Unauthorized")

    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "Usage: `/delbanner <number>` or `/delbanner <banner_id>`\n"
            "Use /listbanner to see numbers.",
            parse_mode="markdown"
        )

    arg = args[1].strip()
    banners = await db.db.mini_app_banners.find({}).sort("order", 1).to_list(MAX_MANUAL)

    target = None
    if arg.isdigit():
        idx = int(arg) - 1
        if 0 <= idx < len(banners):
            target = banners[idx]
    else:
        # Try as banner_id
        from bson.objectid import ObjectId
        try:
            target = await db.db.mini_app_banners.find_one({"_id": ObjectId(arg)})
        except Exception:
            pass

    if not target:
        return await message.reply(f"❌ Banner not found. Use /listbanner to see the list.")

    await db.db.mini_app_banners.delete_one({"_id": target["_id"]})

    # Reorder remaining
    remaining = await db.db.mini_app_banners.find({}).sort("order", 1).to_list(MAX_MANUAL)
    for i, b in enumerate(remaining, 1):
        await db.db.mini_app_banners.update_one({"_id": b["_id"]}, {"$set": {"order": i}})

    await message.reply(
        f"🗑 **Banner deleted.**\n"
        f"Remaining: {len(remaining)}/{MAX_MANUAL}",
        parse_mode="markdown"
    )
