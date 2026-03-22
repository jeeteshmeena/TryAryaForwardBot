import re, asyncio
from database import db
from config import temp
from .test import CLIENT , start_clone_bot
from translation import Translation
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

CLIENT = CLIENT()
COMPLETED_BTN = InlineKeyboardMarkup(
   [
      [InlineKeyboardButton('⚡ Support', url='https://t.me/MeJeetX')],
      [InlineKeyboardButton('📢 Updates', url='https://t.me/MeJeetX')]
   ]
)

CANCEL_BTN = InlineKeyboardMarkup([[InlineKeyboardButton('• ᴄᴀɴᴄᴇʟ', 'terminate_frwd')]])

@Client.on_message(filters.command("unequify") & filters.private)
async def unequify(client, message):
   user_id = message.from_user.id
   temp.CANCEL[user_id] = False
   if temp.lock.get(user_id) and str(temp.lock.get(user_id)) == "True":
      return await message.reply("**please wait until previous task complete**")
   _bot = await db.get_bot(user_id)
   if not _bot or _bot['is_bot']:
      return await message.reply("<b>Need userbot to do this process. Please add a userbot using /settings</b>")

   target = await client.ask(user_id, text=(
      "<b>╭──────❰ 🃏 ᴜɴᴏᴅᴜᴩʟɪᴄᴀᴛᴇ ❱──────╮\n"
      "┃\n"
      "┣⊸ sᴇɴᴅ ᴛʜᴇ ʟᴀsᴛ ᴍᴇssᴀɢᴇ ʟɪɴᴋ ᴏʀ\n"
      "┣⊸ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ ᴜsᴇʀɴᴀᴍᴇ / ɪᴅ\n"
      "┃\n"
      "┣⊸ /cancel — ᴄᴀɴᴄᴇʟ ᴛʜɪs ᴘʀᴏᴄᴇss\n"
      "┃\n"
      "╰────────────────────────────────╯</b>"
   ))

   if target.text.startswith("/cancel"):
      return await target.reply("<b>Process cancelled.</b>")

   chat_id = None
   if target.text:
      regex = re.compile(r"(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
      match = regex.match(target.text.replace("?single", ""))
      if match:
         chat_id = match.group(4)
         if str(chat_id).isnumeric():
            chat_id = int("-100" + chat_id)
      elif target.text.lstrip('-').isdigit():
         chat_id = int(target.text.strip())
      elif target.text.startswith("@"):
         chat_id = target.text.strip()
      else:
         # Try as username without @
         chat_id = target.text.strip()

   if target.forward_from_chat:
      chat_id = target.forward_from_chat.username or target.forward_from_chat.id

   if not chat_id:
      return await target.reply("<b>❌ Invalid link or chat. Please send a valid message link or channel ID.</b>")

   confirm = await client.ask(user_id, text=(
      "<b>╭──────❰ ⚠️ ᴄᴏɴғɪʀᴍ ❱──────╮\n"
      f"┃\n┣⊸ ᴄʜᴀᴛ: <code>{chat_id}</code>\n"
      "┣⊸ ᴛʜɪs ᴡɪʟʟ ᴅᴇʟᴇᴛᴇ ᴅᴜᴘʟɪᴄᴀᴛᴇ ᴅᴏᴄᴜᴍᴇɴᴛs\n"
      "┃\n┣⊸ sᴇɴᴅ /yes ᴛᴏ ᴄᴏɴᴛɪɴᴜᴇ ᴏʀ /no ᴛᴏ ᴄᴀɴᴄᴇʟ\n"
      "┃\n╰────────────────────────────────╯</b>"
   ))
   if "/no" in confirm.text.lower() or "/cancel" in confirm.text.lower():
      return await confirm.reply("<b>Process cancelled!</b>")

   sts = await confirm.reply("<code>Starting duplicate scan…</code>")

   try:
      bot = await start_clone_bot(CLIENT.client(_bot))
   except Exception as e:
      return await sts.edit(f"<b>ERROR:</b>\n<code>{e}</code>")

   try:
      k = await bot.send_message(chat_id, text="testing")
      await k.delete()
   except:
      await sts.edit(f"<b>Please make your <a href='t.me/{_bot.get('username','userbot')}'>userbot</a> admin in the target chat with full permissions</b>")
      return await bot.stop()

   # Use a SET for O(1) duplicate lookup — critical for large channels
   MESSAGES = set()
   DUPLICATE = []
   total = deleted = 0
   temp.lock[user_id] = True

   try:
      await sts.edit(
         Translation.DUPLICATE_TEXT.format(total, deleted, "ᴘʀᴏɢʀᴇssɪɴɢ"),
         reply_markup=CANCEL_BTN
      )
      async for msg in bot.search_messages(chat_id=chat_id, filter="document"):
         if temp.CANCEL.get(user_id) == True:
            await sts.edit(Translation.DUPLICATE_TEXT.format(total, deleted, "ᴄᴀɴᴄᴇʟʟᴇᴅ"), reply_markup=COMPLETED_BTN)
            temp.lock[user_id] = False
            return await bot.stop()

         doc = msg.document
         if not doc:
            continue

         # Use file_unique_id — stable across all bots for the same file content
         unique_id = doc.file_unique_id
         if unique_id in MESSAGES:
            DUPLICATE.append(msg.id)
         else:
            MESSAGES.add(unique_id)

         total += 1

         # Update status every 500 messages to reduce edit spam
         if total % 500 == 0:
            await sts.edit(
               Translation.DUPLICATE_TEXT.format(total, deleted, "ᴘʀᴏɢʀᴇssɪɴɢ"),
               reply_markup=CANCEL_BTN
            )

         # Flush duplicates in batches of 100
         if len(DUPLICATE) >= 100:
            try:
               await bot.delete_messages(chat_id, DUPLICATE)
               deleted += len(DUPLICATE)
            except FloodWait as fw:
               await asyncio.sleep(fw.value + 2)
               try:
                  await bot.delete_messages(chat_id, DUPLICATE)
                  deleted += len(DUPLICATE)
               except Exception:
                  pass
            except Exception:
               pass
            await sts.edit(
               Translation.DUPLICATE_TEXT.format(total, deleted, "ᴘʀᴏɢʀᴇssɪɴɢ"),
               reply_markup=CANCEL_BTN
            )
            DUPLICATE = []

      # Flush remaining
      if DUPLICATE:
         try:
            await bot.delete_messages(chat_id, DUPLICATE)
            deleted += len(DUPLICATE)
         except Exception:
            pass

   except Exception as e:
      temp.lock[user_id] = False
      await sts.edit(f"<b>ERROR</b>\n<code>{e}</code>")
      return await bot.stop()

   temp.lock[user_id] = False
   await sts.edit(
      Translation.DUPLICATE_TEXT.format(total, deleted, "ᴄᴏᴍᴘʟᴇᴛᴇᴅ"),
      reply_markup=COMPLETED_BTN
   )
   await bot.stop()
