import asyncio
import logging
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant
from database import db
from config import Config

logger = logging.getLogger(__name__)

# Global Share Bot Client Instance
share_client = None

async def is_subscribed(client, user_id):
    if not Config.FSUB_ID:
        return True
    try:
        user = await client.get_chat_member(Config.FSUB_ID, user_id)
        if user.status in [enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.KICKED]:
            return False
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.error(f"FSub check error in Share Bot: {e}")
        return True

async def process_start(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        return await message.reply_text("<b>Welcome to the Share Bot!</b>\nI securely deliver files.")

    uuid_str = message.command[1]
    
    # 1. Check FSub
    if Config.FSUB_ID:
        is_sub = await is_subscribed(client, user_id)
        if not is_sub:
            try:
                invite_link = await client.export_chat_invite_link(Config.FSUB_ID)
                btn = [[InlineKeyboardButton("Join Channel 📢", url=invite_link)]]
                return await message.reply_text(
                    "<b>🔒 Access Denied!</b>\n\nYou must join our backup channel to receive these files.",
                    reply_markup=InlineKeyboardMarkup(btn)
                )
            except Exception as e:
                logger.error(f"Failed to generate FSub link: {e}")

    # 2. Fetch Link from DB
    link_data = await db.get_share_link(uuid_str)
    if not link_data:
        return await message.reply_text("<b>❌ Link Expired or Invalid.</b>")

    msg_ids = link_data.get('message_ids', [])
    source_chat = link_data.get('source_chat')

    if not msg_ids or not source_chat:
        return await message.reply_text("<b>❌ Database error: Missing files.</b>")

    sts = await message.reply_text("<i>⏳ Fetching files securely...</i>")
    
    # 3. Deliver Messages
    try:
        # Since Share Bot is a dedicated Pyrogram Client, we use it directly to copy.
        # Ensure Share Bot itself is an Admin in the Private Source Channel!
        await client.copy_messages(
            chat_id=user_id,
            from_chat_id=source_chat,
            message_ids=msg_ids,
            protect_content=True
        )
        await sts.delete()
    except Exception as e:
        await sts.edit_text(f"<b>❌ Error delivering files:</b>\n<code>{e}</code>\n\n(Make sure this File-Sharing bot is an admin in the hidden Database Channel!)")


async def start_share_bot(token=None):
    global share_client
    if share_client:
        try:
            await share_client.stop()
        except: pass
        share_client = None

    if not token:
        token = await db.get_share_bot_token()
    
    if not token:
        logger.info("Share Bot token not set. Skipping File Sharing Bot startup.")
        return

    logger.info(f"Starting Secondary Share Bot...")
    
    try:
        share_client = Client(
            name="share_bot_session",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=token,
            in_memory=True
        )

        # Register Handlers locally
        @share_client.on_message(filters.command("start") & filters.private)
        async def on_start(c, m):
            await process_start(c, m)

        await share_client.start()
        logger.info("✅ Secondary Share Bot successfully started and listening.")
    except Exception as e:
        logger.error(f"❌ Failed to start Share Bot: {e}")
        share_client = None
