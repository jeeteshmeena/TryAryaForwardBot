from pyrogram import Client, filters
from pyrogram.types import Message
from database import db
from config import Config

# Helper command to add, remove, and list protected sources
@Client.on_message(filters.command(["protect"]) & filters.private)
async def protect_command(client: Client, message: Message):
    if message.from_user.id != Config.OWNER_ID:
        return await message.reply("❌ **You do not have permission to use this command.**")

    args = message.command[1:]
    if not args:
        return await message.reply(
            "🛡 **Protection System**\n\n"
            "Use this to block specific channels, groups, or bots from being used as a source.\n\n"
            "**Usage:**\n"
            "• `/protect [ID or Link]` - Adds to protection\n"
            "• `/unprotect [ID or Link]` - Removes from protection\n"
            "• `/protect list` - View all protected sources"
        )

    action_or_source = args[0]
    if action_or_source.lower() == "list":
        sources = await db.get_protected_sources()
        if not sources:
            return await message.reply("✅ **No protected sources currently.**")
        
        text = "🛡 **Protected Sources:**\n\n"
        for i, src in enumerate(sources, 1):
            text += f"{i}. `{src}`\n"
        return await message.reply(text)

    source = action_or_source
    # if it's numeric/negative, make sure it's stored as an integer, otherwise string
    if source.lstrip('-').isdigit():
        source = int(source)

    await db.add_protected_source(source)
    await message.reply(f"✅ **Successfully added** `{source}` **to Protected Sources.**\n"
                        f"Users will no longer be able to forward from this source.")


@Client.on_message(filters.command(["unprotect"]) & filters.private)
async def unprotect_command(client: Client, message: Message):
    if message.from_user.id != Config.OWNER_ID:
        return await message.reply("❌ **You do not have permission to use this command.**")

    args = message.command[1:]
    if not args:
        return await message.reply("❗ **Usage:** `/unprotect [ID or Link]`")

    source = args[0]
    if source.lstrip('-').isdigit():
        source = int(source)

    await db.remove_protected_source(source)
    await message.reply(f"✅ **Successfully removed** `{source}` **from Protected Sources.**")

