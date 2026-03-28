"""
Share Bot - Secure File Delivery Agent
======================================
Handles deep-link batch deliveries for the Share Batch Links system.
Users click a batch link in the public channel, bot delivers the files.
"""
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
    """Returns True if user is subscribed to FSub channel, or if FSub is disabled."""
    fsub_id = getattr(Config, 'FSUB_ID', None)
    if not fsub_id:
        return True
    try:
        member = await client.get_chat_member(fsub_id, user_id)
        if member.status in [enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.KICKED]:
            return False
        return True
    except UserNotParticipant:
        return False
    except Exception:
        return True  # Don't block on FSub errors


async def delete_later(client, chat_id: int, file_msg_ids: list, notice_msg_id: int, delay_secs: int):
    """Background task: deletes delivered files + the notice after a delay."""
    await asyncio.sleep(delay_secs)
    try:
        await client.delete_messages(chat_id, file_msg_ids)
    except Exception as e:
        logger.warning(f"Auto-delete (files) failed for {chat_id}: {e}")
    try:
        await client.delete_messages(chat_id, [notice_msg_id])
    except Exception as e:
        logger.warning(f"Auto-delete (notice) failed for {chat_id}: {e}")


async def process_start(client, message):
    """Handles /start [uuid] deep links from the batch buttons."""
    user_id = message.from_user.id

    # No payload = generic welcome
    if len(message.command) < 2:
        me = await client.get_me()
        await message.reply_text(
            f"<b>Welcome to {me.first_name}! 👋</b>\n\n"
            f"I am a secure file delivery bot.\n"
            f"To receive files, click the episode/batch buttons in the channel.\n\n"
            f"<i>Do not send messages here — just use the channel links.</i>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📢 Go to Channel", url="https://t.me/joinchat")
            ]])
        )
        return

    uuid_str = message.command[1]

    # 1. Fetch the batch record from MongoDB FIRST
    link_data = await db.get_share_link(uuid_str)
    if not link_data:
        await message.reply_text(
            "<b>❌ Link Expired or Invalid</b>\n\n"
            "This batch link no longer exists. The operator may have regenerated the links.\n"
            "Please go back to the channel and click the latest link."
        )
        return

    msg_ids      = link_data.get('message_ids', [])
    source_chat  = link_data.get('source_chat')
    protect_flag = link_data.get('protect', True)
    auto_delete_mins = link_data.get('auto_delete', 0)

    if not msg_ids or not source_chat:
        await message.reply_text("<b>❌ Database Error:</b> Missing file references.")
        return

    # 2. Force-Subscribe check
    fsub_id = getattr(Config, 'FSUB_ID', None)
    if fsub_id:
        is_sub = await is_subscribed(client, user_id)
        if not is_sub:
            try:
                invite_link = await client.export_chat_invite_link(fsub_id)
            except Exception:
                invite_link = f"https://t.me/c/{fsub_id}"
            await message.reply_text(
                "<b>🔒 Join Required to Access Files</b>\n\n"
                "You must join our channel first to receive these files.\n"
                "After joining, click <b>Try Again</b>.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
                    [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{client.me.username}?start={uuid_str}")]
                ])
            )
            return

    # 3. Deliver files
    sts = await message.reply_text("<i>⏳ Fetching your files securely, please wait...</i>")

    try:
        sent_msgs = await client.copy_messages(
            chat_id=user_id,
            from_chat_id=source_chat,
            message_ids=msg_ids,
            protect_content=protect_flag
        )

        total = len(sent_msgs) if isinstance(sent_msgs, list) else 1

        if auto_delete_mins > 0:
            hrs = auto_delete_mins // 60
            mins = auto_delete_mins % 60
            if hrs:
                del_str = f"{hrs}h {mins}m" if mins else f"{hrs}h"
            else:
                del_str = f"{mins} minutes"
            notice = await sts.edit_text(
                f"<b>✅ {total} file(s) delivered!</b>\n\n"
                f"<i>⚠️ These files will auto-delete in <b>{del_str}</b>. "
                f"Save them before they disappear!</i>"
            )
            sent_ids = [m.id for m in sent_msgs] if isinstance(sent_msgs, list) else [sent_msgs.id]
            asyncio.create_task(delete_later(client, user_id, sent_ids, notice.id, auto_delete_mins * 60))
        else:
            await sts.edit_text(f"<b>✅ {total} file(s) delivered successfully!</b>")

    except Exception as e:
        await sts.edit_text(
            f"<b>❌ Delivery Failed</b>\n\n"
            f"<code>{e}</code>\n\n"
            f"<i>The Share Bot must be an admin in the Database Channel to deliver files.</i>"
        )


async def start_share_bot(token=None):
    global share_client

    # Stop existing session
    if share_client:
        try:
            await share_client.stop()
        except Exception:
            pass
        share_client = None

    if not token:
        token = await db.get_share_bot_token()

    if not token:
        logger.info("Share Bot token not set. Skipping.")
        return

    token = token.strip().replace(" ", "").replace("\n", "").replace("\r", "")
    masked = f"{token[:10]}...{token[-4:]}" if len(token) > 15 else "INVALID"
    logger.info(f"Starting Secondary Share Bot [Token: {masked}]...")

    import uuid as _uuid
    name = f"share_bot_{_uuid.uuid4().hex[:8]}"

    try:
        share_client = Client(
            name=name,
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=token,
            in_memory=True
        )

        @share_client.on_message(filters.command("start") & filters.private)
        async def on_start(c, m):
            await process_start(c, m)

        await share_client.start()
        logger.info("✅ Secondary Share Bot started successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to start Share Bot: {e}")
        share_client = None
