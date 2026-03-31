import asyncio
import logging
from database import db
from translation import Translation
from plugins.lang import t, _tx
from pyrogram import Client, filters
from .test import get_configs, update_configs, CLIENT, parse_buttons
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)
CLIENT = CLIENT()

#  Future-based ask() — immune to pyrofork stale-listener bug 
_settings_waiting: dict[int, asyncio.Future] = {}

@Client.on_message(filters.private, group=-16)
async def _settings_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _settings_waiting:
        fut = _settings_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation

async def _ask(bot, user_id: int, timeout: int = 300):
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    old = _settings_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _settings_waiting[user_id] = fut
    try:
        from asyncio import wait_for, TimeoutError
        res = await wait_for(fut, timeout=timeout)
        return res
    except TimeoutError:
        _settings_waiting.pop(user_id, None)
        raise

from pyrogram import ContinuePropagation

async def _sb_set_text_flow(bot, user_id, query, b_id: str, key: str,
                             label: str, instructions: str, back_cb: str):
    """Reusable helper: prompt user for a per-bot text, then save it."""
    await query.message.delete()
    ask = await bot.send_message(
        user_id,
        f"<b>»  Set {label}</b>\n\n{instructions}\n\n"
        "Send /reset to remove current value.\n"
        "/cancel to abort."
    )
    try:
        resp = await bot.listen(chat_id=user_id, timeout=300)
        txt = resp.text or resp.caption or ""
        await resp.delete()
        if txt.strip().lower() in ("/cancel", "cancel"):
            return await ask.edit_text(
                "»  Cancelled.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=back_cb)]])
            )
        if txt.strip() == "/reset":
            await db.set_share_bot_text(b_id, key, "")
            return await ask.edit_text(
                f"»  {label} reset to default.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=back_cb)]])
            )
        await db.set_share_bot_text(b_id, key, txt)
        await ask.edit_text(
            f"»  {label} saved!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=back_cb)]])
        )
    except asyncio.TimeoutError:
        try:
            await ask.edit_text(
                "Timeout.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=back_cb)]])
            )
        except Exception:
            pass


@Client.on_message(filters.command('settings'))
async def settings(client, message):
    await message.delete()
    user_id = message.from_user.id
    await message.reply_text(
        await t(user_id, 'settings_title'),
        reply_markup=await main_buttons(user_id)
    )
    
@Client.on_callback_query(filters.regex(r'^settings#(?!lang$|cleanmsg$)'))
async def settings_query(bot, query):
  user_id = query.from_user.id
  i, type = query.data.split("#")
  buttons = [[InlineKeyboardButton('🔙 ʙᴀᴄᴋ', callback_data="settings#main")]]
  
  if type=="main":
     user_id = query.from_user.id
     await query.message.edit_text(
       await t(user_id, 'settings_title'),
       reply_markup=await main_buttons(user_id))
          
  elif type=="accounts":
     bots = await db.get_bots(user_id)
     normal_bots = [b for b in bots if b.get('is_bot', True)]
     userbots    = [b for b in bots if not b.get('is_bot', True)]
     
     buttons = []
     
     # ---- BOTS SECTION ----
     buttons.append([InlineKeyboardButton("    🤖 ʙᴏᴛꜱ    ", callback_data="settings#noop")])
     for b in normal_bots:
         active_mark = "✔️ " if b.get('active') else " "
         buttons.append([InlineKeyboardButton(f"{active_mark} {b['name']}", callback_data=f"settings#editbot_{b['id']}")])
     if len(normal_bots) < 2:
         buttons.append([InlineKeyboardButton('➕ ᴀᴅᴅ ʙᴏᴛ', callback_data="settings#addbot")])

     # ---- USERBOTS SECTION ----
     buttons.append([InlineKeyboardButton("    👤 ᴜꜱᴇʀʙᴏᴛꜱ    ", callback_data="settings#noop")])
     for b in userbots:
         active_mark = "✔️ " if b.get('active') else " "
         buttons.append([InlineKeyboardButton(f"{active_mark} {b['name']}", callback_data=f"settings#editbot_{b['id']}")])
     if len(userbots) < 2:
         buttons.append([InlineKeyboardButton('➕ ᴀᴅᴅ ᴜꜱᴇʀʙᴏᴛ', callback_data="settings#adduserbot")])
         
     buttons.append([InlineKeyboardButton('🔙 ʙᴀᴄᴋ', callback_data="settings#main")])
     
     text = (
         "<b><u>👥 My Accounts</u></b>\n\n"
         f"<b>🤖 Bots:</b> {len(normal_bots)}/2\n"
         f"<b>👤 Userbots:</b> {len(userbots)}/2\n\n"
         "<b>Tap an account to view details or set it active.\n"
         "✔️ = Currently active for that type.</b>"
     )
     await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
     
  elif type=="noop":
     await query.answer()
  
  elif type=="addbot":
     await query.message.delete()
     res = await CLIENT.add_bot(bot, query)
     if res == "LIMIT_REACHED": return await bot.send_message(user_id, "<b>Limit reached: You can only add up to 2 Bots.</b>")
     if res == "EXISTS": return await bot.send_message(user_id, "<b>This bot has already been added.</b>")
     if res != True: return
     await bot.send_message(user_id, "<b>Bot token successfully added to db</b>\nGo back to /settings to configure.")
  
  elif type=="adduserbot":
     await query.message.delete()
     res = await CLIENT.add_session(bot, query)
     if res == "LIMIT_REACHED": return await bot.send_message(user_id, "<b>Limit reached: You can only add up to 2 Userbots.</b>")
     if res == "EXISTS": return await bot.send_message(user_id, "<b>This session has already been added.</b>")
     if res != True: return
     await bot.send_message(user_id, "<b>Session successfully added to db</b>\nGo back to /settings to configure.")
      
  elif type=="channels":
     buttons = []
     channels = await db.get_user_channels(user_id)
     for channel in channels:
        buttons.append([InlineKeyboardButton(f"{channel['title']}",
                         callback_data=f"settings#editchannels_{channel['chat_id']}")])
     buttons.append([InlineKeyboardButton('»  ᴀᴅᴅ ᴄʜᴀɴɴᴇʟ » ', 
                      callback_data="settings#addchannel")])
     buttons.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', 
                      callback_data="settings#main")])
     await query.message.edit_text( 
       "<b><u>My Channels</b></u>\n\n<b>you can manage your target chats in here</b>",
       reply_markup=InlineKeyboardMarkup(buttons))
   
  elif type=="addchannel":  
     await query.message.delete()
     try:
         text = await bot.send_message(user_id, "<b>❪ ADD CHAT ❫\n\nForward a message from the chat, OR send its Chat ID (e.g. -100...), OR send a link to any message in the chat.\n/cancel - cancel this process</b>")
         chat_ids = await _ask(bot, user_id, timeout=300)
         if chat_ids.text == "/cancel":
             await chat_ids.delete()
             return await text.edit_text("<b>process canceled</b>", reply_markup=InlineKeyboardMarkup(buttons))
             
         chat_id, title, username = None, "Unknown Chat", "private"
         
         if getattr(chat_ids, 'forward_from_chat', None):
             chat_id = chat_ids.forward_from_chat.id
             title = chat_ids.forward_from_chat.title
             username = "@" + chat_ids.forward_from_chat.username if chat_ids.forward_from_chat.username else "private"
         elif chat_ids.text:
             txt = chat_ids.text.strip()
             if txt.lstrip('-').isdigit():
                 chat_id = int(txt)
             elif "t.me/c/" in txt:
                 import re
                 m = re.search(r't\.me/c/(\d+)', txt)
                 if m: chat_id = int("-100" + m.group(1))
             elif "t.me/" in txt:
                 import re
                 m = re.search(r't\.me/([^/]+)', txt.replace('https://','').replace('http://',''))
                 if m and m.group(1) not in ['joinchat', '+', 'c']:
                     chat_id = m.group(1)
                     username = "@" + m.group(1)
                     
         if not chat_id:
             await chat_ids.delete()
             return await text.edit_text("**Could not extract Chat ID. Invalid forward or link.**", reply_markup=InlineKeyboardMarkup(buttons))
         
         try:
             # Try to resolve chat title
             chat_info = await bot.get_chat(chat_id)
             chat_id = chat_info.id
             title = chat_info.title or title
             username = "@" + chat_info.username if getattr(chat_info, 'username', None) else username
         except Exception:
             pass
             
         chat = await db.add_channel(user_id, chat_id, title, username)
         await chat_ids.delete()
         await text.edit_text(
            "<b>Successfully updated</b>" if chat else "<b>This channel is already added</b>",
            reply_markup=InlineKeyboardMarkup(buttons))
     except asyncio.exceptions.TimeoutError:
         await text.edit_text('Process has been automatically cancelled', reply_markup=InlineKeyboardMarkup(buttons))
  
  elif type.startswith("editbot"): 
     bot_id = type.split('_')[1] if "_" in type else None
     bott = await db.get_bot(user_id, bot_id)
     if not bott:
         return await query.answer("Account not found!", show_alert=True)
         
     TEXT = Translation.BOT_DETAILS if bott.get('is_bot', True) else Translation.USER_DETAILS
     buttons = []
     if not bott.get('active'):
         buttons.append([InlineKeyboardButton('»  ꜱᴇᴛ ᴀᴄᴛɪᴠᴇ', callback_data=f"settings#setactive_{bott['id']}")])
         
     buttons.append([InlineKeyboardButton('‣  ʀᴇᴍᴏᴠᴇ ‣ ', callback_data=f"settings#removebot_{bott['id']}")])
     buttons.append([InlineKeyboardButton('🔙 ʙᴀᴄᴋ', callback_data="settings#accounts")])
     await query.message.edit_text(
        TEXT.format(bott['name'], bott['id'], bott['username']),
        reply_markup=InlineKeyboardMarkup(buttons))
                                             
  elif type.startswith("setactive"):
     bot_id = type.split('_')[1]
     await db.set_active_bot(user_id, bot_id)
     await query.answer("Account set as ACTIVE!", show_alert=True)
     buttons = [[InlineKeyboardButton('«  ʙᴀᴄᴋ ᴛᴏ ᴀᴄᴄᴏᴜɴᴛꜱ', callback_data="settings#accounts")]]
     await query.message.edit_text("<b>Successfully changed active account.</b>", reply_markup=InlineKeyboardMarkup(buttons))

  elif type == "sharebot":
     bots = await db.get_share_bots()
     protect = await db.get_share_protect_global()
     ptxt = "»  ON" if protect else "‣  OFF"
     
     buttons = []
     buttons.append([InlineKeyboardButton(f"🛡 Pʀᴏᴛᴇᴄᴛɪᴏɴ: {ptxt}", callback_data="settings#sharebotprotect")])
     buttons.append([InlineKeyboardButton("    »  ᴅᴇʟɪᴠᴇʀʏ ʙᴏᴛꜱ    ", callback_data="settings#noop")])
     for b in bots:
         buttons.append([InlineKeyboardButton(f"»  {b['name']}", callback_data=f"settings#sb_view_{b['id']}")])
     if len(bots) < 10:
         buttons.append([InlineKeyboardButton('»  ᴀᴅᴅ ꜱʜᴀʀᴇ ʙᴏᴛ » ', callback_data="settings#sb_add")])
     buttons.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data="settings#main")])
     
     text = (
         "<b>❪ SHARE BOT CONFIGURATION ❫</b>\n\n"
         f"<b>Allocated Bots:</b> {len(bots)}/10\n\n"
         "<b>These bots handle exclusively the delivery payload of your Share Links.</b>\n\n"
         "<i>Protection globally restricts saving and forwarding delivered files.</i>"
     )
     await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

  elif type == "sharebotprotect":
     protect = await db.get_share_protect_global()
     await db.set_share_protect_global(not protect)
     await query.answer(f"Protection turned {'OFF' if protect else 'ON'}")
     query.data = "settings#sharebot"
     return await settings_query(bot, query)



  elif type == "sbt_manage":
      bots = await db.get_share_bots()
      buttons = []
      buttons.append([InlineKeyboardButton("    »  ᴅᴇʟɪᴠᴇʀʏ ʙᴏᴛꜱ    ", callback_data="settings#noop")])
      for b in bots:
          buttons.append([InlineKeyboardButton(f"»  {b['name']}", callback_data=f"settings#sb_view_{b['id']}")])
      if len(bots) < 10:
          buttons.append([InlineKeyboardButton('»  ᴀᴅᴅ ꜱʜᴀʀᴇ ʙᴏᴛ » ', callback_data="settings#sb_add")])
      buttons.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data="settings#sharebot")])
      
      text = (
          "<b><u>»  Share Agent Accounts</u></b>\n\n"
          f"<b>Allocated Bots:</b> {len(bots)}/10\n\n"
          "<b>These bots handle exclusively the delivery payload of your Share Links. They distribute traffic securely to avoid bans across high volumes.</b>"
      )
      await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
      
  elif type == "sb_add":
      await query.message.delete()
      ask = None
      try:
          from config import Config as _Cfg
          ask = await bot.send_message(
              user_id,
              "<b>❪ ADD SHARE BOT ❫</b>\n\n"
              "Send the bot token from @BotFather directly, or forward a message containing it.\n\n"
              "/cancel to abort"
          )
          resp = await bot.listen(chat_id=user_id, timeout=120)
          if resp.text and resp.text.strip() == "/cancel":
              await resp.delete()
              return await ask.edit_text(
                  "<b>Cancelled.</b>",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharebot")]])
              )

          raw = (resp.text or "").strip()
          await resp.delete()

          import re as _re_add
          m = _re_add.search(r"(\d{8,11}:[A-Za-z0-9_-]{35,})", raw)
          tk = m.group(1) if m else raw

          if ":" not in tk or len(tk) < 40:
              return await ask.edit_text(
                  "<b>‣  Invalid token format!</b>\nMake sure you send the full token from @BotFather.",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʀᴇᴛʀʏ", callback_data="settings#sb_add"), InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharebot")]])
              )

          # Validate token by starting a temp client
          test_app = Client(
              f"test_sb_{tk[:8]}", bot_token=tk,
              api_id=_Cfg.API_ID, api_hash=_Cfg.API_HASH, in_memory=True
          )
          await test_app.start()
          me = await test_app.get_me()
          await test_app.stop()

          # Save to DB
          await db.add_share_bot(me.id, tk, me.username or "unknown", me.first_name or "ShareBot")

          # Reload all share bots
          from plugins.share_bot import start_share_bot
          import asyncio as _aio
          _aio.create_task(start_share_bot())

          await ask.edit_text(
              f"»  <b>Successfully added @{me.username}!</b>\n\nThe delivery bot is now active.",
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharebot")]])
          )
      except Exception as e:
          errmsg = f"‣  <b>Error:</b> <code>{e}</code>"
          try:
              if ask:
                  await ask.edit_text(errmsg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharebot")]]))
              else:
                  await bot.send_message(user_id, errmsg)
          except Exception:
              pass

  elif type.startswith("sb_view_"):
      b_id = type.split("sb_view_")[1]
      bots = await db.get_share_bots()
      bt = next((x for x in bots if str(x['id']) == str(b_id)), None)
      if not bt: return await query.answer("Bot not found!")

      buttons = [
          # ─── Welcome & About (grouped) ───────────────────────────
          [
              InlineKeyboardButton('»  ᴡᴇʟᴄᴏᴍᴇ & ᴀʙᴏᴜᴛ', callback_data=f"settings#sb_wa_{b_id}"),
          ],
          # ─── Other messages ───────────────────────────────────────
          [
              InlineKeyboardButton('»  ᴅᴇʟᴇᴛᴇ ᴍꜱɢ',      callback_data=f"settings#sb_set_delete_{b_id}"),
              InlineKeyboardButton('»  ꜱᴜᴄᴄᴇꜱꜱ ᴍꜱɢ',    callback_data=f"settings#sb_set_success_{b_id}"),
          ],
          [InlineKeyboardButton('»  ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ',    callback_data=f"settings#sb_set_caption_{b_id}")],
          # ─── Bot controls ─────────────────────────────────────────
          [InlineKeyboardButton('»  Aᴜᴛᴏ-Dᴇʟᴇᴛᴇ', callback_data=f"settings#sb_set_autodel_{b_id}"),
           InlineKeyboardButton('»  ꜰᴏʀᴄᴇ ꜱᴜʙꜱᴄʀɪʙᴇ',  callback_data=f"settings#sb_fsub_{b_id}")],
          [
              InlineKeyboardButton('»  ꜱᴛᴀᴛꜱ',           callback_data=f"settings#sb_stats_{b_id}"),
              InlineKeyboardButton('»  ʙʀᴏᴀᴅᴄᴀꜱᴛ',       callback_data=f"settings#sb_broadcast_{b_id}")
          ],
          [InlineKeyboardButton('‣  ʀᴇᴍᴏᴠᴇ ʙᴏᴛ ‣ ',      callback_data=f"settings#sb_remove_{b_id}")],
          [InlineKeyboardButton('«  ʙᴀᴄᴋ',               callback_data="settings#sharebot")],
      ]
      await query.message.edit_text(
          f"<b>❪ SHARE BOT PROFILE ❫</b>\n\n"
          f"<b>»  Name:</b> {bt['name']}\n"
          f"<b>»  Username:</b> @{bt['username']}\n"
          f"<b>🆔 ID:</b> <code>{bt['id']}</code>\n\n"
          "<i>All settings below are specific to this bot.</i>",
          reply_markup=InlineKeyboardMarkup(buttons)
      )

  elif type.startswith("sb_wa_"):
      b_id = type.split("sb_wa_")[1]
      buttons = [
          [
              InlineKeyboardButton('»  ᴡᴇʟᴄᴏᴍᴇ ᴍꜱɢ',    callback_data=f"settings#sb_set_welcome_{b_id}"),
              InlineKeyboardButton('🖼  ᴡᴇʟᴄᴏᴍᴇ ɪᴍɢ',  callback_data=f"settings#sb_set_welcome_img_{b_id}"),
          ],
          [InlineKeyboardButton('‣  ᴀʙᴏᴜᴛ',        callback_data=f"settings#sb_about_{b_id}")],
          [InlineKeyboardButton('«  ʙᴀᴄᴋ',         callback_data=f"settings#sb_view_{b_id}")],
      ]
      await query.message.edit_text(
          f"<b>❪ WELCOME & ABOUT ❫</b>\n\n"
          "Select what you want to configure for this bot:",
          reply_markup=InlineKeyboardMarkup(buttons)
      )

  elif type.startswith("sb_set_welcome_img_"):
      b_id = type.split("sb_set_welcome_img_")[1]
      await query.message.delete()
      ask = await bot.send_message(
          user_id,
          "<b>🖼 Set Welcome Image</b>\n\n"
          "Send a photo to use as the welcome banner.\n"
          "The welcome text will appear as the photo's caption.\n\n"
          "Send <code>/clear</code> to remove the current image.\n"
          "Send <code>/cancel</code> to abort."
      )
      try:
          resp = await bot.listen(chat_id=user_id, timeout=120)

          if resp.text and resp.text.strip().lower() in ("/cancel", "cancel"):
              await resp.delete()
              return await ask.edit_text(
                  "»  Cancelled.",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )

          if resp.text and resp.text.strip() == "/clear":
              about = await db.get_share_bot_about(b_id)
              about.pop('welcome_image_id', None)
              await db.set_share_bot_about(b_id, about)
              await resp.delete()
              return await ask.edit_text(
                  "»  Welcome image removed.",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )

          photo = resp.photo
          if not photo:
              await resp.delete()
              return await ask.edit_text(
                  "‣  No photo received. Please send an image.",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )

          # ——————————————————————————————————————————————————————————————————————
          # KEY FIX: file_id is bot-specific in Telegram.
          # The photo was sent to the ARYA (admin) bot, so its file_id belongs to Arya.
          # If the delivery bot tries to send_photo with Arya's file_id, it fails silently.
          # Solution: ask the delivery bot's client to forward the photo to itself
          # (its own DM/saved messages), then capture ITS OWN file_id from that message.
          # ——————————————————————————————————————————————————————————————————————
          from plugins.share_bot import share_clients
          sb_client = share_clients.get(str(b_id))

          final_file_id = photo.file_id  # default: Arya bot's id (may fail in delivery bot)
          if sb_client:
              try:
                  # Forward from Arya's chat to the admin's DM using the delivery bot
                  relay = await sb_client.send_photo(
                      chat_id=user_id,
                      photo=photo.file_id,
                  )
                  if relay and relay.photo:
                      final_file_id = relay.photo.file_id
                  # Clean up relay message from admin's DM
                  try:
                      await relay.delete()
                  except Exception:
                      pass
              except Exception as relay_err:
                  logger.warning(f"Welcome image relay failed, using Arya's file_id: {relay_err}")

          about = await db.get_share_bot_about(b_id)
          about['welcome_image_id'] = final_file_id
          await db.set_share_bot_about(b_id, about)
          await resp.delete()
          await ask.edit_text(
              "»  Welcome image saved! Users will see it with the welcome message.",
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
          )
      except asyncio.TimeoutError:
          await ask.edit_text(
              "Timeout.",
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
          )

  elif type.startswith("sb_set_welcome_"):
      b_id = type.split("sb_set_welcome_")[1]
      await _sb_set_text_flow(bot, user_id, query, b_id, "welcome_msg",
          "Wᴇʟᴄᴏᴍᴇ Mᴇssᴀɢᴇ",
          "Send the new welcome message.\n"
          "Use <code>{first_name}</code>, <code>{full_name}</code>, <code>{mention}</code> as placeholders.\n"
          "Any font/formatting is accepted.",
          f"settings#sb_view_{b_id}")

  elif type.startswith("sb_set_delete_"):
      b_id = type.split("sb_set_delete_")[1]
      await _sb_set_text_flow(bot, user_id, query, b_id, "delete_msg",
          "Dᴇʟᴇᴛᴇ Nᴏᴛɪᴄᴇ Mᴇssᴀɢᴇ",
          "Send the delete notice text.\n"
          "Use <code>{time}</code> for the auto-delete duration.\n"
          "Any font/formatting is accepted.",
          f"settings#sb_view_{b_id}")

  elif type.startswith("sb_set_success_"):
      b_id = type.split("sb_set_success_")[1]
      await _sb_set_text_flow(bot, user_id, query, b_id, "success_msg",
          "Sᴜᴄᴄᴇss Mᴇssᴀɢᴇ",
          "Send the success/delivery confirmation message.\nAny font is accepted.",
          f"settings#sb_view_{b_id}")

  elif type.startswith("sb_set_caption_"):
      b_id = type.split("sb_set_caption_")[1]
      await _sb_set_text_flow(bot, user_id, query, b_id, "custom_caption",
          "Cᴜsᴛᴏᴍ Cᴀᴘᴛɪᴏɴ",
          "Send the caption to add to delivered media. Any font is accepted.",
          f"settings#sb_view_{b_id}")

  elif type.startswith("sb_set_autodel_"):
      b_id = type.split("sb_set_autodel_")[1]
      opts   = [0, 5, 10, 30, 60, 1440]           # minutes; 0 = OFF
      labels = ["OFF","5m","10m","30m","1h","24h"]
      about = await db.get_share_bot_about(b_id)
      cur = about.get('auto_delete', 0)
      try:    cur_idx = opts.index(cur)
      except: cur_idx = 0
      nxt_idx = (cur_idx + 1) % len(opts)
      about['auto_delete'] = opts[nxt_idx]
      await db.set_share_bot_about(b_id, about)
      await query.answer(f"Auto-Delete: {labels[nxt_idx]}")
      query.data = f"settings#sb_view_{b_id}"
      return await settings_query(bot, query)

  #  Stats & Broadcast 
  elif type.startswith("sb_stats_"):
      b_id = type.split("sb_stats_")[1]
      users = await db.get_share_bot_users(b_id)
      cnt = len(users)
      await query.message.edit_text(
          f"<b>»  SHARE BOT STATS</b>\n\n"
          f"<b>Total Users:</b> <code>{cnt}</code>\n\n"
          "<i>These are users who have opened or interacted with this specific bot.</i>",
          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
      )

  elif type.startswith("sb_broadcast_"):
      b_id = type.split("sb_broadcast_")[1]
      await query.message.delete()
      
      ask = await bot.send_message(
          user_id,
          "<b>»  Broadcast Message</b>\n\n"
          "Send the message you want to broadcast to all users of this bot.\n"
          "You can use text, photos, videos, etc.\n\n"
          "/cancel to abort."
      )
      
      try:
          resp = await _ask(bot, user_id, timeout=300)
          msg_to_send = resp.text or resp.caption or "media"
          
          if getattr(resp, "text", "") and str(resp.text).strip() == "/cancel":
              await ask.delete()
              await resp.delete()
              return await bot.send_message(
                  user_id, "<b>Cancelled.</b>",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )
              
          users = await db.get_share_bot_users(b_id)
          if not users:
              await ask.delete()
              return await bot.send_message(
                  user_id, "<b>‣  No users found for this bot.</b>",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )
              
          from plugins.share_bot import share_clients
          sb_client = share_clients.get(str(b_id))
          if not sb_client:
              await ask.delete()
              return await bot.send_message(
                  user_id, "<b>‣  Delivery Bot is not running online.</b>",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )
              
          await ask.edit_text(
              f"<b>»  Broadcast Started...</b>\n\n"
              f"<b>Target:</b> <code>{len(users)} users</code>"
          )
          
          import asyncio
          async def _do_broadcast(sb_app, uids, msg_obj, status_msg, back_btn_data):
              sent = 0
              failed = 0
              blocked = 0
              for u in uids:
                  try:
                      await sb_app.copy_message(
                          chat_id=int(u),
                          from_chat_id=msg_obj.chat.id,
                          message_id=msg_obj.id
                      )
                      sent += 1
                  except Exception as e:
                      failed += 1
                      if "USER_IS_BLOCKED" in str(e) or "bot was blocked" in str(e).lower() or "PEER_ID_INVALID" in str(e):
                          blocked += 1
                  await asyncio.sleep(0.05) # Rate limit protection
                  
                  # Update stats every 5 users
                  if min(sent + failed, len(uids)) % 5 == 0 or (sent + failed) == len(uids):
                      try:
                          await status_msg.edit_text(
                              f"<b>»  Broadcast In Progress...</b>\n\n"
                              f"<b>Total Target:</b> <code>{len(uids)}</code>\n"
                              f"<b>»  Sent:</b> <code>{sent}</code>\n"
                              f"<b>‣  Failed:</b> <code>{failed}</code>\n"
                              f"<b>🚫 Blocked:</b> <code>{blocked}</code>"
                          )
                      except Exception:
                          pass
                          
              await status_msg.edit_text(
                  f"<b>»  Broadcast Complete!</b>\n\n"
                  f"<b>Total Target:</b> <code>{len(uids)}</code>\n"
                  f"<b>»  Sent:</b> <code>{sent}</code>\n"
                  f"<b>‣  Failed:</b> <code>{failed}</code>\n"
                  f"<b>🚫 Blocked:</b> <code>{blocked}</code>\n\n"
                  "<i>Use Arya font and styling.</i>",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=back_btn_data)]])
              )
              try:
                  await msg_obj.delete()
              except: pass
              
          import asyncio as _aio
          _aio.create_task(_do_broadcast(sb_client, users, resp, ask, f"settings#sb_view_{b_id}"))

      except asyncio.TimeoutError:
          try:
              await bot.send_message(
                  user_id, "Timeout.",
                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")]])
              )
          except Exception:
              pass

  #  About section editor 
  elif type.startswith("sb_about_") and not any(type.startswith(f"sb_about_{p}_") for p in ['img', 'txt', 'owner', 'ver', 'reset']):
      b_id = type.split("sb_about_")[1]
      bots = await db.get_share_bots()
      bt = next((x for x in bots if str(x['id']) == str(b_id)), None)
      if not bt: return await query.answer("Bot not found!")
      about = await db.get_share_bot_about(b_id)
      img_set = "»  Set" if about.get('image_id') else "‣  None"
      txt_set = "»  Custom" if about.get('custom_text') else "»  Default"
      btns = [
          [InlineKeyboardButton('🖼 ꜱᴇᴛ ᴀʙᴏᴜᴛ ɪᴍᴀɢᴇ',   callback_data=f"settings#sb_about_img_{b_id}")],
          [InlineKeyboardButton('»  ᴇᴅɪᴛ ᴀʙᴏᴜᴛ ᴛᴇxᴛ',   callback_data=f"settings#sb_about_txt_{b_id}")],
          [InlineKeyboardButton('»  ᴇᴅɪᴛ ᴏᴡɴᴇʀ',         callback_data=f"settings#sb_about_owner_{b_id}")],
          [InlineKeyboardButton('»  ᴇᴅɪᴛ ᴠᴇʀꜱɪᴏɴ',       callback_data=f"settings#sb_about_ver_{b_id}")],
          [InlineKeyboardButton('»  ʀᴇꜱᴇᴛ ᴛᴏ ᴅᴇꜰᴀᴜʟᴛ',    callback_data=f"settings#sb_about_reset_{b_id}")],
          [InlineKeyboardButton('«  ʙᴀᴄᴋ',                callback_data=f"settings#sb_view_{b_id}")],
      ]
      await query.message.edit_text(
          f"<b>‣  Aʙᴏᴜᴛ Sᴇᴄᴛɪᴏɴ — {bt['name']}</b>\n\n"
          f"<b>Image:</b> {img_set}\n"
          f"<b>Text:</b> {txt_set}\n"
          f"<b>Owner:</b> {about.get('owner_name', 'JeetX')}\n"
          f"<b>Version:</b> {about.get('version', 'V1.0')}\n\n"
          "<i>The About section is shown when users tap the About button in the delivery bot.</i>",
          reply_markup=InlineKeyboardMarkup(btns)
      )

  elif type.startswith("sb_about_img_"):
      b_id = type.split("sb_about_img_")[1]
      await query.message.delete()
      ask = await bot.send_message(user_id,
          "<b>🖼 Send the About image</b> (photo).\n"
          "This image will appear in the About section.\n"
          "/cancel to abort."
      )
      try:
          resp = await bot.listen(chat_id=user_id, timeout=120)
          if resp.text and resp.text.strip() == "/cancel":
              return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
          photo = resp.photo
          if not photo:
              return await ask.edit_text("‣  No photo received.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
          about = await db.get_share_bot_about(b_id)
          about['image_id'] = photo.file_id
          await db.set_share_bot_about(b_id, about)
          await ask.edit_text("»  About image saved!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
      except asyncio.TimeoutError:
          await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))

  elif type.startswith("sb_about_txt_"):
      b_id = type.split("sb_about_txt_")[1]
      await query.message.delete()
      ask = await bot.send_message(user_id,
          "<b>»  Send the custom About text</b>.\n"
          "Use any font you like. HTML formatting is supported.\n"
          "/cancel to abort."
      )
      try:
          resp = await bot.listen(chat_id=user_id, timeout=180)
          if resp.text and resp.text.strip() == "/cancel":
              return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
          txt = resp.text or ""
          about = await db.get_share_bot_about(b_id)
          about['custom_text'] = txt
          await db.set_share_bot_about(b_id, about)
          await ask.edit_text("»  About text saved!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
      except asyncio.TimeoutError:
          await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))

  elif type.startswith("sb_about_owner_"):
      b_id = type.split("sb_about_owner_")[1]
      await query.message.delete()
      ask = await bot.send_message(user_id,
          "<b>»  Send owner name and link</b>\n"
          "Format: <code>Owner Name | https://t.me/username</code>\n"
          "/cancel to abort."
      )
      try:
          resp = await bot.listen(chat_id=user_id, timeout=120)
          if resp.text and resp.text.strip() == "/cancel":
              return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
          parts = (resp.text or "").split("|", 1)
          about = await db.get_share_bot_about(b_id)
          about['owner_name'] = parts[0].strip()
          if len(parts) > 1:
              about['owner_link'] = parts[1].strip()
          await db.set_share_bot_about(b_id, about)
          await ask.edit_text("»  Owner updated!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
      except asyncio.TimeoutError:
          await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))

  elif type.startswith("sb_about_ver_"):
      b_id = type.split("sb_about_ver_")[1]
      await query.message.delete()
      ask = await bot.send_message(user_id,
          "<b>»  Send new version string</b> (e.g. <code>V1.2</code>)\n/cancel to abort."
      )
      try:
          resp = await bot.listen(chat_id=user_id, timeout=60)
          if resp.text and resp.text.strip() == "/cancel":
              return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
          about = await db.get_share_bot_about(b_id)
          about['version'] = (resp.text or "V1.0").strip()
          await db.set_share_bot_about(b_id, about)
          await ask.edit_text("»  Version updated!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))
      except asyncio.TimeoutError:
          await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_about_{b_id}")]]))

  elif type.startswith("sb_about_reset_"):
      b_id = type.split("sb_about_reset_")[1]
      await db.set_share_bot_about(b_id, {})
      await query.answer("About reset to defaults.")
      return await edit_settings(client, query, f"sb_about_{b_id}")

  #  Per-bot Force-Subscribe 
  elif type.startswith("sb_fsub_") and not any(type.startswith(f"sb_fsub_{p}_") for p in ['add', 'jr', 'del']):
      b_id = type.split("sb_fsub_")[1]
      fsub_chs = await db.get_bot_fsub_channels(b_id)
      lines = []
      btns  = []
      for i, ch in enumerate(fsub_chs):
          jr_lbl = " [JR]" if ch.get('join_request') else ""
          lines.append(f"{i+1}. {ch.get('title','?')}{jr_lbl}")
          btns.append([
              InlineKeyboardButton(f"»  JR #{i+1}",  callback_data=f"settings#sb_fsub_jr_{b_id}_{i}"),
              InlineKeyboardButton(f"‣  Del #{i+1}", callback_data=f"settings#sb_fsub_del_{b_id}_{i}"),
          ])
      ch_list = "\n".join(lines) if lines else "None configured."
      if len(fsub_chs) < 6:
          btns.append([InlineKeyboardButton("»  ᴀᴅᴅ ᴄʜᴀɴɴᴇʟ", callback_data=f"settings#sb_fsub_add_{b_id}")])
      btns.append([InlineKeyboardButton("»  ꜱᴇᴛ ꜰꜱᴜʙ ᴍꜱɢ", callback_data=f"settings#sb_fsub_msg_{b_id}")])
      btns.append([InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_view_{b_id}")])
      await query.message.edit_text(
          f"<b>»  Force-Subscribe — Bot Specific</b>\n\n"
          f"Users must join ALL listed channels to receive files from this bot.\n"
          f"[JR] = join-request mode.\n\n{ch_list}",
          reply_markup=InlineKeyboardMarkup(btns)
      )

  elif type.startswith("sb_fsub_jr_"):
      rest = type[len("sb_fsub_jr_"):]
      # rest = "{b_id}_{idx}"
      last_under = rest.rfind("_")
      b_id = rest[:last_under]; idx = int(rest[last_under+1:])
      fsub_chs = await db.get_bot_fsub_channels(b_id)
      if 0 <= idx < len(fsub_chs):
          new_jr = not fsub_chs[idx].get('join_request', False)
          fsub_chs[idx]['join_request'] = new_jr
          ch_id = fsub_chs[idx].get('chat_id')
          if ch_id:
              try:
                  if new_jr:
                      lnk_obj = await bot.create_chat_invite_link(int(ch_id), creates_join_request=True)
                      fsub_chs[idx]['invite_link'] = lnk_obj.invite_link
                  else:
                      fsub_chs[idx]['invite_link'] = await bot.export_chat_invite_link(int(ch_id))
              except Exception as e:
                  logger.warning(f"Could not regenerate invite link: {e}")
          await db.set_bot_fsub_channels(b_id, fsub_chs)
          status = "ON » " if new_jr else "OFF ‣ "
          await query.answer(f"JR: {status}")
      return await edit_settings(client, query, f"sb_fsub_{b_id}")

  elif type.startswith("sb_fsub_del_"):
      rest = type[len("sb_fsub_del_"):]
      last_under = rest.rfind("_")
      b_id = rest[:last_under]; idx = int(rest[last_under+1:])
      fsub_chs = await db.get_bot_fsub_channels(b_id)
      if 0 <= idx < len(fsub_chs):
          fsub_chs.pop(idx)
          await db.set_bot_fsub_channels(b_id, fsub_chs)
          await query.answer("Removed.")
      return await edit_settings(client, query, f"sb_fsub_{b_id}")

  elif type.startswith("sb_fsub_msg_"):
      b_id = type.split("sb_fsub_msg_")[1]
      await _sb_set_text_flow(bot, user_id, query, b_id, "fsub_msg",
          "Fᴏʀᴄᴇ-Sᴜʙ Mᴇssᴀɢᴇ",
          "Send the new message to prompt users to subscribe.\nAny font is accepted.",
          f"settings#sb_fsub_{b_id}")

  elif type.startswith("sb_fsub_add_"):
      b_id = type[len("sb_fsub_add_"):]
      await query.message.delete()
      ask = await bot.send_message(user_id,
          "<b>Send the Channel/Group ID or @username</b>\n"
          "Example: <code>-1001234567890</code> or <code>@mychannel</code>\n\n"
          "/cancel to abort"
      )
      try:
          resp = await bot.listen(chat_id=user_id, timeout=120)
          if resp.text.strip() == "/cancel":
              await resp.delete()
              return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_fsub_{b_id}")]]))
          raw_id = resp.text.strip()
          await resp.delete()
          try:
              ch_obj = await bot.get_chat(raw_id)
          except Exception as e:
              err_str = str(e).lower()
              if "private" in err_str or "peer_id_invalid" in err_str or "channel_invalid" in err_str:
                  msg = "<b>‣  Cannot access this channel.</b>\nMake sure the Main Bot is admin."
              else:
                  msg = f"<b>‣  Error:</b> <code>{e}</code>"
              return await ask.edit_text(msg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_fsub_{b_id}")]]))
          try:
              invite = await bot.export_chat_invite_link(ch_obj.id)
          except Exception:
              invite = getattr(ch_obj, 'invite_link', '') or ''
          fsub_chs = await db.get_bot_fsub_channels(b_id)
          fsub_chs.append({
              'chat_id':     str(ch_obj.id),
              'title':       ch_obj.title or ch_obj.username or str(ch_obj.id),
              'invite_link': invite,
              'join_request': False,
          })
          await db.set_bot_fsub_channels(b_id, fsub_chs)
          await ask.edit_text(
              f"<b>»  Added: {ch_obj.title}</b>\n<i>Toggle JR to enable join-request mode.</i>",
              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_fsub_{b_id}")]]))
      except asyncio.TimeoutError:
          await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data=f"settings#sb_fsub_{b_id}")]]))

  elif type.startswith("sb_remove_"):
      b_id = type.split("sb_remove_")[1]
      await db.remove_share_bot(b_id)
      await db.remove_share_bot_config(b_id)  # clean up per-bot config too
      await query.answer("Bot Removed!")
      return await edit_settings(client, query, "sbt_manage")



  elif type == "sharefsub":
     fsub_chs = await db.get_share_fsub_channels()
     lines = []
     btns  = []
     for i, ch in enumerate(fsub_chs):
         jr_lbl = " [JR]" if ch.get('join_request') else ""
         lines.append(f"{i+1}. {ch.get('title','?')}{jr_lbl}")
         btns.append([
             InlineKeyboardButton(f"»  Toggle JR #{i+1}",  callback_data=f"settings#sharefsub_jr_{i}"),
             InlineKeyboardButton(f"‣  Remove #{i+1}", callback_data=f"settings#sharefsub_del_{i}")
         ])
     ch_list = "\n".join(lines) if lines else "None configured."
     if len(fsub_chs) < 6:
         btns.append([InlineKeyboardButton("»  ᴀᴅᴅ ᴄʜᴀɴɴᴇʟ/ɢʀᴏᴜᴘ", callback_data="settings#sharefsub_add")])
     btns.append([InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharebot")])
     await query.message.edit_text(
         f"<b>»  Force-Subscribe Channels</b>\n\n"
         f"Users must join ALL listed channels to receive files.\n"
         f"[JR] = join-request mode (user sends request; admin approves).\n\n"
         f"{ch_list}",
         reply_markup=InlineKeyboardMarkup(btns)
     )

  elif type == "sharefsub_add":
     fsub_chs = await db.get_share_fsub_channels()
     if len(fsub_chs) >= 6:
         return await query.answer("Maximum 6 channels supported.", show_alert=True)
     await query.message.delete()
     try:
         ask = await bot.send_message(
             user_id,
             "<b>Send the Channel/Group ID or @username</b>\n"
             "Example: <code>-1001234567890</code> or <code>@mychannel</code>\n\n"
             "/cancel to abort"
         )
         resp = await bot.listen(chat_id=user_id, timeout=120)
         if resp.text.strip() == "/cancel":
             await resp.delete()
             return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharefsub")]]))
         raw_id = resp.text.strip()
         await resp.delete()
         try:
             ch_obj = await bot.get_chat(raw_id)
         except Exception as e:
             err_str = str(e).lower()
             if "private" in err_str or "peer_id_invalid" in err_str or "channel_invalid" in err_str:
                 msg = (
                     "<b>‣  Cannot access this channel.</b>\n\n"
                     "This is a <b>private channel/group</b>. Make sure:\n"
                     "• The <b>Main Bot</b> is an <b>admin</b> in this channel\n"
                     "• You send the ID (not @username) for private channels\n\n"
                     "<i>Format: <code>-1001234567890</code></i>"
                 )
             else:
                 msg = f"<b>‣  Error:</b> <code>{e}</code>\nMake sure the Main Bot is an admin in that channel."
             return await ask.edit_text(
                 msg,
                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharefsub")]])
             )
         try:
             invite = await bot.export_chat_invite_link(ch_obj.id)
         except Exception:
             invite = getattr(ch_obj, 'invite_link', '') or ''

         ah = 0
         try:
             peer = await bot.resolve_peer(ch_obj.id)
             ah = getattr(peer, 'access_hash', 0)
         except Exception: pass

         fsub_chs.append({
             'chat_id':     str(ch_obj.id),
             'title':       ch_obj.title or ch_obj.username or str(ch_obj.id),
             'invite_link': invite,
             'join_request': False,
             'access_hash': ah,
         })
         await db.set_share_fsub_channels(fsub_chs)
         await ask.edit_text(
             f"<b>»  Added: {ch_obj.title}</b>\n"
             f"<i>Use 'Toggle JR' to enable join-request mode for this channel.</i>",
             reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharefsub")]])
         )
     except asyncio.TimeoutError:
         try: await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#sharefsub")]]))
         except Exception: pass

  elif type.startswith("sharefsub_jr_"):
     idx      = int(type.split("_")[-1])
     fsub_chs = await db.get_share_fsub_channels()
     if 0 <= idx < len(fsub_chs):
         new_jr = not fsub_chs[idx].get('join_request', False)
         fsub_chs[idx]['join_request'] = new_jr
         ch_id = fsub_chs[idx].get('chat_id')
         if ch_id:
             try:
                 if new_jr:
                     lnk_obj = await bot.create_chat_invite_link(int(ch_id), creates_join_request=True)
                     fsub_chs[idx]['invite_link'] = lnk_obj.invite_link
                 else:
                     fsub_chs[idx]['invite_link'] = await bot.export_chat_invite_link(int(ch_id))
             except Exception as lnk_err:
                 logger.warning(f"Could not regenerate fsub invite link: {lnk_err}")
         await db.set_share_fsub_channels(fsub_chs)
         status = "ON » " if new_jr else "OFF ‣ "
         await query.answer(f"Join-Request mode: {status}")
     return await edit_settings(client, query, "sharefsub")

  elif type.startswith("sharefsub_del_"):
     idx      = int(type.split("_")[-1])
     fsub_chs = await db.get_share_fsub_channels()
     if 0 <= idx < len(fsub_chs):
         removed = fsub_chs.pop(idx)
         await db.set_share_fsub_channels(fsub_chs)
         await query.answer(f"Removed: {removed.get('title','?')}")
     return await edit_settings(client, query, "sharefsub")

  elif type == "share_autodelete":
     opts   = [0, 5, 10, 30, 60, 1440]           # minutes; 0 = OFF
     labels = ["OFF","5m","10m","30m","1h","24h"]
     cur    = await db.get_share_autodelete_global()
     try:    cur_idx = opts.index(cur)
     except: cur_idx = 0
     nxt_idx = (cur_idx + 1) % len(opts)
     await db.set_share_autodelete_global(opts[nxt_idx])
     await query.answer(f"Auto-Delete: {labels[nxt_idx]}")
     return await edit_settings(client, query, "sharebot")

  elif type == "editsharebot":
     import re
     await query.message.delete()
     try:
         txtmsg = await bot.send_message(user_id, "<b>Send the Bot Token for the File-Sharing Bot:</b>\n<i>(Get it from @BotFather)</i>\n\n/remove - to delete current token.\n/cancel - to abort.")
         resp = await bot.listen(chat_id=user_id, timeout=120)
         if resp.text == "/cancel":
             await resp.delete()
             return await txtmsg.edit_text("<b>Cancelled.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='settings#sharebot')]]))
         if resp.text == "/remove":
             await resp.delete()
             await db.set_share_bot_token("")
             return await txtmsg.edit_text("<b>Token Removed.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='settings#sharebot')]]))
            
         bot_token = re.findall(r'\d{8,10}:[A-Za-z0-9_-]{35}', resp.text)
         if not bot_token:
             return await txtmsg.edit_text("<b>Invalid Token Format.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='settings#sharebot')]]))
         
         new_token = bot_token[0]
         await db.set_share_bot_token(new_token)
         # Start immediately
         try:
             from plugins.share_bot import start_share_bot
             await start_share_bot(new_token)
             status = "»  Successfully Saved & Started!"
         except Exception as e:
             status = f"»  Saved securely, but failed to start stream:\n<code>{e}</code>"
             
         await resp.delete()
         await txtmsg.edit_text(status, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='settings#sharebot')]]))
     except asyncio.exceptions.TimeoutError:
         try: await txtmsg.edit_text('Timeout.', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('«  ʙᴀᴄᴋ', callback_data='settings#sharebot')]]))
         except: pass

  elif type.startswith("removebot"):
     if "_" in type:
         bot_id = type.split('_')[1]
         await db.remove_bot(user_id, bot_id)
     else:
         await db.remove_bot(user_id)
     buttons = [[InlineKeyboardButton('«  ʙᴀᴄᴋ ᴛᴏ ᴀᴄᴄᴏᴜɴᴛꜱ', callback_data="settings#accounts")]]
     await query.message.edit_text(
        "<b>successfully removed!</b>",
        reply_markup=InlineKeyboardMarkup(buttons))
                                             
  elif type.startswith("editchannels"): 
     chat_id = type.split('_')[1]
     chat = await db.get_channel_details(user_id, chat_id)
     buttons = [[InlineKeyboardButton('‣  ʀᴇᴍᴏᴠᴇ ‣ ', callback_data=f"settings#removechannel_{chat_id}")
               ],
               [InlineKeyboardButton('🔙 ʙᴀᴄᴋ', callback_data="settings#channels")]]
     await query.message.edit_text(
        f"<b><u>»  CHANNEL DETAILS</b></u>\n\n<b>- TITLE:</b> <code>{chat['title']}</code>\n<b>- CHANNEL ID: </b> <code>{chat['chat_id']}</code>\n<b>- USERNAME:</b> {chat['username']}",
        reply_markup=InlineKeyboardMarkup(buttons))
                                             
  elif type.startswith("removechannel"):
     chat_id = type.split('_')[1]
     await db.remove_channel(user_id, chat_id)
     await query.message.edit_text(
        "<b>successfully updated</b>",
        reply_markup=InlineKeyboardMarkup(buttons))
                               
  elif type=="caption":
     data    = await get_configs(user_id)
     caption = data['caption']
     rm_cap  = data.get('filters', {}).get('rm_caption', False)

     # Determine mode label
     if rm_cap is True:
         mode_lbl = "»  Smart Clean  (active)"
     elif rm_cap == 2:
         mode_lbl = "»  Wipe All Captions  (active)"
     else:
         mode_lbl = "»  Keep Original  (active)"

     cap_lbl = "»  Add Custom Caption" if caption is None else "✏️ Edit Custom Caption"

     buttons = [[
         InlineKeyboardButton("      ᴄᴀᴘᴛɪᴏɴ ᴍᴏᴅᴇ      ",
             callback_data="settings_#noop")
     ],[
         InlineKeyboardButton("»  ᴋᴇᴇᴘ ᴏʀɪɢɪɴᴀʟ" + (" ◀" if not rm_cap else ""),
             callback_data="settings#caption_mode-off"),
     ],[
         InlineKeyboardButton("»  ꜱᴍᴀʀᴛ ᴄʟᴇᴀɴ" + (" ◀" if rm_cap is True else ""),
             callback_data="settings#caption_mode-smart"),
     ],[
         InlineKeyboardButton("»  ᴡɪᴘᴇ ᴀʟʟ ᴄᴀᴘᴛɪᴏɴꜱ" + (" ◀" if rm_cap == 2 else ""),
             callback_data="settings#caption_mode-wipe"),
     ],[
         InlineKeyboardButton("      ᴄᴜꜱᴛᴏᴍ ᴛᴇᴍᴘʟᴀᴛᴇ      ",
             callback_data="settings_#noop")
     ],[
         InlineKeyboardButton(cap_lbl, callback_data="settings#addcaption"),
     ]]
     if caption is not None:
         buttons.append([
             InlineKeyboardButton("👁 ᴠɪᴇᴡ ᴛᴇᴍᴘʟᴀᴛᴇ",  callback_data="settings#seecaption"),
             InlineKeyboardButton("»  ᴄʟᴇᴀʀ ᴛᴇᴍᴘʟᴀᴛᴇ", callback_data="settings#deletecaption"),
         ])
     buttons.append([InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#main")])

     await query.message.edit_text(
         "<b><u>»  Caption Settings</u></b>\n\n"
         f"<b>Current mode:</b> {mode_lbl}\n\n"
         "<b>Modes:</b>\n"
         "• <b>Keep Original</b> — forward caption as-is\n"
         "• <b>Smart Clean</b> — strip links/usernames but keep text\n"
         "• <b>Wipe All Captions</b> — remove caption completely from every file\n\n"
         "<b>Custom Template</b> — override caption with your own text.\n"
         "  Supports: <code>{filename}</code>, <code>{size}</code>, <code>{caption}</code>",
         reply_markup=InlineKeyboardMarkup(buttons))

                               
  elif type=="seecaption":   
     data = await get_configs(user_id)
     buttons = [[InlineKeyboardButton('🖋️ ᴇᴅɪᴛ ᴄᴀᴘᴛɪᴏɴ', 
                  callback_data="settings#addcaption")
               ],[
               InlineKeyboardButton('«  ʙᴀᴄᴋ', 
                 callback_data="settings#caption")]]
     await query.message.edit_text(
        f"<b><u>YOUR CUSTOM CAPTION</b></u>\n\n<code>{data['caption']}</code>",
        reply_markup=InlineKeyboardMarkup(buttons))
    
  elif type=="deletecaption":
     await update_configs(user_id, 'caption', None)
     await query.answer("\u2705 Caption template cleared.", show_alert=False)
     # Redirect back to caption sub-menu
     data    = await get_configs(user_id)
     rm_cap  = data.get('filters', {}).get('rm_caption', False)
     buttons = [[
         InlineKeyboardButton(("✅ " if not rm_cap else "» ") + "ᴋᴇᴇᴘ ᴏʀɪɢɪɴᴀʟ", callback_data="settings#caption_mode-off"),
     ],[
         InlineKeyboardButton(("✅ " if rm_cap is True else "» ") + "ꜱᴍᴀʀᴛ ᴄʟᴇᴀɴ", callback_data="settings#caption_mode-smart"),
     ],[
         InlineKeyboardButton(("✅ " if rm_cap == 2 else "» ") + "ᴡɪᴘᴇ ᴀʟʟ ᴄᴀᴘᴛɪᴏɴꜱ", callback_data="settings#caption_mode-wipe"),
     ],[
         InlineKeyboardButton("»  ᴀᴅᴅ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ", callback_data="settings#addcaption"),
     ],[
         InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#main")
     ]]
     await query.message.edit_text(
         "<b><u>»  Caption Settings</u></b>\n\n<b>Template cleared successfully.</b>",
         reply_markup=InlineKeyboardMarkup(buttons))
                              
  elif type.startswith("caption_mode"):
     mode = type.split("-")[1]  # off | smart | wipe
     if mode == "off":
         val = False
     elif mode == "smart":
         val = True
     else:
         val = 2  # wipe
         
     await update_configs(user_id, 'rm_caption', val)
     
     await query.answer("»  Caption mode updated!", show_alert=False)
     # Refresh the caption sub-menu
     data    = await get_configs(user_id)
     caption = data['caption']
     rm_cap  = data.get('filters', {}).get('rm_caption', False)
     if rm_cap is True:
         mode_lbl = "»  Smart Clean  (active)"
     elif rm_cap == 2:
         mode_lbl = "»  Wipe All Captions  (active)"
     else:
         mode_lbl = "»  Keep Original  (active)"
     cap_lbl = "»  Add Custom Caption" if caption is None else "✏️ Edit Custom Caption"
     buttons = [[
         InlineKeyboardButton("      ᴄᴀᴘᴛɪᴏɴ ᴍᴏᴅᴇ      ", callback_data="settings_#noop")
     ],[
         InlineKeyboardButton(("✅ " if not rm_cap else "» ") + "ᴋᴇᴇᴘ ᴏʀɪɢɪɴᴀʟ", callback_data="settings#caption_mode-off"),
     ],[
         InlineKeyboardButton(("✅ " if rm_cap is True else "» ") + "ꜱᴍᴀʀᴛ ᴄʟᴇᴀɴ", callback_data="settings#caption_mode-smart"),
     ],[
         InlineKeyboardButton(("✅ " if rm_cap == 2 else "» ") + "ᴡɪᴘᴇ ᴀʟʟ ᴄᴀᴘᴛɪᴏɴꜱ", callback_data="settings#caption_mode-wipe"),
     ],[
         InlineKeyboardButton("      ᴄᴜꜱᴛᴏᴍ ᴛᴇᴍᴘʟᴀᴛᴇ      ", callback_data="settings_#noop")
     ],[
         InlineKeyboardButton(cap_lbl, callback_data="settings#addcaption"),
     ]]
     if caption is not None:
         buttons.append([
             InlineKeyboardButton("👁 ᴠɪᴇᴡ ᴛᴇᴍᴘʟᴀᴛᴇ",  callback_data="settings#seecaption"),
             InlineKeyboardButton("»  ᴄʟᴇᴀʀ ᴛᴇᴍᴘʟᴀᴛᴇ", callback_data="settings#deletecaption"),
         ])
     buttons.append([InlineKeyboardButton("«  ʙᴀᴄᴋ", callback_data="settings#main")])
     await query.message.edit_text(
         "<b><u>»  Caption Settings</u></b>\n\n"
         f"<b>Current mode:</b> {mode_lbl}\n\n"
         "<b>Modes:</b>\n"
         "• <b>Keep Original</b> — forward caption as-is\n"
         "• <b>Smart Clean</b> — strip links/usernames but keep text\n"
         "• <b>Wipe All Captions</b> — remove caption completely from every file\n\n"
         "<b>Custom Template</b> — override caption with your own text.\n"
         "  Supports: <code>{filename}</code>, <code>{size}</code>, <code>{caption}</code>",
         reply_markup=InlineKeyboardMarkup(buttons))

  elif type=="addcaption":
     await query.message.delete()
     try:
         text = await bot.send_message(query.message.chat.id, "Send your custom caption\n/cancel - <code>cancel this process</code>")
         caption = await bot.listen(chat_id=user_id, timeout=300)
         if caption.text=="/cancel":
            await caption.delete()
            return await text.edit_text(
                  "<b>process canceled !</b>",
                  reply_markup=InlineKeyboardMarkup(buttons))
         try:
            caption.text.format(filename='', size='', caption='')
         except KeyError as e:
            await caption.delete()
            return await text.edit_text(
               f"<b>wrong filling {e} used in your caption. change it</b>",
               reply_markup=InlineKeyboardMarkup(buttons))
         await update_configs(user_id, 'caption', caption.text)
         await caption.delete()
         await text.edit_text(
            "<b>successfully updated</b>",
            reply_markup=InlineKeyboardMarkup(buttons))
     except asyncio.exceptions.TimeoutError:
         await text.edit_text('Process has been automatically cancelled', reply_markup=InlineKeyboardMarkup(buttons))
  
  elif type=="button":
     buttons = []
     button = (await get_configs(user_id))['button']
     if button is None:
        buttons.append([InlineKeyboardButton('»  ᴀᴅᴅ ʙᴜᴛᴛᴏɴ » ', 
                      callback_data="settings#addbutton")])
     else:
        buttons.append([InlineKeyboardButton('👀 ꜱᴇᴇ ʙᴜᴛᴛᴏɴ', 
                      callback_data="settings#seebutton")])
        buttons[-1].append(InlineKeyboardButton('»  ʀᴇᴍᴏᴠᴇ ʙᴜᴛᴛᴏɴ ', 
                      callback_data="settings#deletebutton"))
     buttons.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', 
                      callback_data="settings#main")])
     await query.message.edit_text(
        "<b><u>CUSTOM BUTTON</b></u>\n\n<b>You can set a inline button to messages.</b>\n\n<b><u>FORMAT:</b></u>\n`[Forward bot][buttonurl:https://t.me/devgaganbot]`\n",
        reply_markup=InlineKeyboardMarkup(buttons))
  
  elif type=="addbutton":
     await query.message.delete()
     try:
         txt = await bot.send_message(user_id, text="**Send your custom button.\n\nFORMAT:**\n`[forward bot][buttonurl:https://t.me/devgaganbot]`\n")
         ask = await bot.listen(chat_id=user_id, timeout=300)
         button = parse_buttons(ask.text.html)
         if not button:
            await ask.delete()
            return await txt.edit_text("**INVALID BUTTON**")
         await update_configs(user_id, 'button', ask.text.html)
         await ask.delete()
         await txt.edit_text("**Successfully button added**",
            reply_markup=InlineKeyboardMarkup(buttons))
     except asyncio.exceptions.TimeoutError:
         await txt.edit_text('Process has been automatically cancelled', reply_markup=InlineKeyboardMarkup(buttons))
  
  elif type=="seebutton":
      button = (await get_configs(user_id))['button']
      button = parse_buttons(button, markup=False)
      button.append([InlineKeyboardButton("«  ʙᴀᴄᴋ", "settings#button")])
      await query.message.edit_text(
         "**YOUR CUSTOM BUTTON**",
         reply_markup=InlineKeyboardMarkup(button))
      
  elif type=="deletebutton":
     await update_configs(user_id, 'button', None)
     await query.message.edit_text(
        "**Successfully button deleted**",
        reply_markup=InlineKeyboardMarkup(buttons))
   
  elif type=="database":
     buttons = []
     db_uri = (await get_configs(user_id))['db_uri']
     if db_uri is None:
        buttons.append([InlineKeyboardButton('»  ᴀᴅᴅ ᴜʀʟ » ', 
                      callback_data="settings#addurl")])
     else:
        buttons.append([InlineKeyboardButton('👀 ꜱᴇᴇ ᴜʀʟ', 
                      callback_data="settings#seeurl")])
        buttons[-1].append(InlineKeyboardButton('»  ʀᴇᴍᴏᴠᴇ ᴜʀʟ ', 
                      callback_data="settings#deleteurl"))
     buttons.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', 
                      callback_data="settings#main")])
     await query.message.edit_text(
        "<b><u>DATABASE</u>\n\nDatabase is required for store your duplicate messages permenant. other wise stored duplicate media may be disappeared when after bot restart.</b>",
        reply_markup=InlineKeyboardMarkup(buttons))

  elif type=="addurl":
     await query.message.delete()
     uri = await bot.ask(user_id, "<b>please send your mongodb url.</b>\n\n<i>get your Mongodb url from [here](https://mongodb.com)</i>", disable_web_page_preview=True)
     if uri.text=="/cancel":
        return await uri.reply_text(
                  "<b>process canceled !</b>",
                  reply_markup=InlineKeyboardMarkup(buttons))
     if not uri.text.startswith("mongodb+srv://") and not uri.text.endswith("majority"):
        return await uri.reply("<b>Invalid Mongodb Url</b>",
                   reply_markup=InlineKeyboardMarkup(buttons))
     await update_configs(user_id, 'db_uri', uri.text)
     await uri.reply("**Successfully database url added**",
             reply_markup=InlineKeyboardMarkup(buttons))
  
  elif type=="seeurl":
     db_uri = (await get_configs(user_id))['db_uri']
     await query.answer(f"DATABASE URL: {db_uri}", show_alert=True)
  
  elif type=="deleteurl":
     await update_configs(user_id, 'db_uri', None)
     await query.message.edit_text(
        "**Successfully your database url deleted**",
        reply_markup=InlineKeyboardMarkup(buttons))
      
  elif type=="filters":
     await query.message.edit_text(
        "<b><u>💠 CUSTOM FILTERS 💠</b></u>\n\n**configure the type of messages which you want forward**",
        reply_markup=await filters_buttons(user_id))
  
  elif type=="nextfilters":
     await query.edit_message_reply_markup( 
        reply_markup=await next_filters_buttons(user_id))
   
  elif type.startswith("updatefilter"):
     i, key, value = type.split('-')
     
     if key == 'rm_caption':
         # Three states: False (Remove), True (Smart Clean), 2 (Keep Original)
         if value == "False":
             await update_configs(user_id, key, True)
         elif value == "True":
             await update_configs(user_id, key, 2)
         else:
             await update_configs(user_id, key, False)
     else:
         if value == "True":
            await update_configs(user_id, key, False)
         else:
            await update_configs(user_id, key, True)
            
     if key in ['poll', 'protect', 'download', 'rm_caption', 'links']:
        return await query.edit_message_reply_markup(
           reply_markup=await next_filters_buttons(user_id)) 
     await query.edit_message_reply_markup(
        reply_markup=await filters_buttons(user_id))
        
  elif type == "set_duration":
    await query.message.delete()
    dur_msg = await bot.ask(user_id, text="**Please send your duration in seconds (between forwards):**")
    if dur_msg.text == '/cancel':
       return await dur_msg.reply_text("<b>process canceled</b>", reply_markup=await next_filters_buttons(user_id))
    try:
        duration = int(dur_msg.text)
        await update_configs(user_id, 'duration', duration)
        await dur_msg.reply_text(f"**successfully updated duration to {duration} seconds**", reply_markup=await next_filters_buttons(user_id))
    except ValueError:
        await dur_msg.reply_text("<b>invalid duration, process canceled</b>", reply_markup=await next_filters_buttons(user_id))
   
  elif type.startswith("file_size"):
    settings = await get_configs(user_id)
    size = settings.get('file_size', 0)
    i, limit = size_limit(settings['size_limit'])
    await query.message.edit_text(
       f'<b><u>SIZE LIMIT</b></u><b>\n\nyou can set file size limit to forward\n\nStatus: files with {limit} `{size} MB` will forward</b>',
       reply_markup=size_button(size))
  
  elif type.startswith("update_size"):
    size = int(query.data.split('-')[1])
    if 0 < size > 2000:
      return await query.answer("size limit exceeded", show_alert=True)
    await update_configs(user_id, 'file_size', size)
    i, limit = size_limit((await get_configs(user_id))['size_limit'])
    await query.message.edit_text(
       f'<b><u>SIZE LIMIT</b></u><b>\n\nyou can set file size limit to forward\n\nStatus: files with {limit} `{size} MB` will forward</b>',
       reply_markup=size_button(size))
  
  elif type.startswith('update_limit'):
    i, limit, size = type.split('-')
    limit, sts = size_limit(limit)
    await update_configs(user_id, 'size_limit', limit) 
    await query.message.edit_text(
       f'<b><u>SIZE LIMIT</b></u><b>\n\nyou can set file size limit to forward\n\nStatus: files with {sts} `{size} MB` will forward</b>',
       reply_markup=size_button(int(size)))
      
  elif type == "add_extension":
    await query.message.delete() 
    ext = await bot.ask(user_id, text="**please send your extensions (seperete by space)**")
    if ext.text == '/cancel':
       return await ext.reply_text(
                  "<b>process canceled</b>",
                  reply_markup=InlineKeyboardMarkup(buttons))
    extensions = ext.text.split(" ")
    extension = (await get_configs(user_id))['extension']
    if extension:
        for extn in extensions:
            extension.append(extn)
    else:
        extension = extensions
    await update_configs(user_id, 'extension', extension)
    await ext.reply_text(
        f"**successfully updated**",
        reply_markup=InlineKeyboardMarkup(buttons))
      
  elif type == "get_extension":
    extensions = (await get_configs(user_id))['extension']
    btn = extract_btn(extensions)
    btn.append([InlineKeyboardButton('»  ᴀᴅᴅ » ', 'settings#add_extension')])
    btn.append([InlineKeyboardButton('ʀᴇᴍᴏᴠᴇ ᴀʟʟ', 'settings#rmve_all_extension')])
    btn.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', 'settings#main')])
    await query.message.edit_text(
        text='<b><u>EXTENSIONS</u></b>\n\n**Files with these extiontions will not forward**',
        reply_markup=InlineKeyboardMarkup(btn))
  
  elif type == "rmve_all_extension":
    await update_configs(user_id, 'extension', None)
    await query.message.edit_text(text="**successfully deleted**",
                                   reply_markup=InlineKeyboardMarkup(buttons))
  elif type == "add_keyword":
    await query.message.delete()
    ask = await bot.ask(user_id, text="**please send the keywords (seperete by space)**")
    if ask.text == '/cancel':
       return await ask.reply_text(
                  "<b>process canceled</b>",
                  reply_markup=InlineKeyboardMarkup(buttons))
    keywords = ask.text.split(" ")
    keyword = (await get_configs(user_id))['keywords']
    if keyword:
        for word in keywords:
            keyword.append(word)
    else:
        keyword = keywords
    await update_configs(user_id, 'keywords', keyword)
    await ask.reply_text(
        f"**successfully updated**",
        reply_markup=InlineKeyboardMarkup(buttons))
  
  elif type == "get_keyword":
    keywords = (await get_configs(user_id))['keywords']
    btn = extract_btn(keywords)
    btn.append([InlineKeyboardButton('»  ᴀᴅᴅ » ', 'settings#add_keyword')])
    btn.append([InlineKeyboardButton('ʀᴇᴍᴏᴠᴇ ᴀʟʟ', 'settings#rmve_all_keyword')])
    btn.append([InlineKeyboardButton('«  ʙᴀᴄᴋ', 'settings#main')])
    await query.message.edit_text(
        text='<b><u>KEYWORDS</u></b>\n\n**File with these keywords in file name will forwad**',
        reply_markup=InlineKeyboardMarkup(btn))
      
  elif type == "rmve_all_keyword":
    await update_configs(user_id, 'keywords', None)
    await query.message.edit_text(text="**successfully deleted**",
                                   reply_markup=InlineKeyboardMarkup(buttons))
  elif type.startswith("alert"):
    alert = type.split('_')[1]
    await query.answer(alert, show_alert=True)

  elif type == "toggle_mode":
    data = await get_configs(user_id)
    current = data.get('bot_mode', 'forward')
    new_mode = 'merger' if current == 'forward' else 'forward'
    await update_configs(user_id, 'bot_mode', new_mode)
    mode_lbl = "🔀 Merger" if new_mode == 'merger' else "📤 Forward"
    await query.answer(f"Mode switched to {mode_lbl}!", show_alert=True)
    await query.message.edit_text(
        await t(user_id, 'settings_title'),
        reply_markup=await main_buttons(user_id)
    )

async def main_buttons(user_id=None):
  # Get current mode
  mode = 'forward'
  if user_id:
      try:
          data = await get_configs(user_id)
          mode = data.get('bot_mode', 'forward')
      except Exception:
          pass

  if mode == 'merger':
      #  MERGER MODE: Clean separate menu 
      buttons = [[
           InlineKeyboardButton('🔀 ᴍᴇʀɢᴇʀ ᴍᴏᴅᴇ »   ⟶  ᴛᴀᴘ ᴛᴏ ꜱᴡɪᴛᴄʜ',
                        callback_data='settings#toggle_mode')
           ],[
           InlineKeyboardButton('👥 ᴀᴄᴄᴏᴜɴᴛꜱ',
                        callback_data='settings#accounts'),
           InlineKeyboardButton('📢 ᴄʜᴀɴɴᴇʟꜱ',
                        callback_data='settings#channels')
           ],[
           InlineKeyboardButton('🎵 ᴀᴜᴅɪᴏ ᴍᴇʀɢᴇ',
                        callback_data='mg#audio_list'),
           InlineKeyboardButton('🎬 ᴠɪᴅᴇᴏ ᴍᴇʀɢᴇ',
                        callback_data='mg#video_list')
           ],[
           InlineKeyboardButton('🤖 ꜱʜᴀʀᴇ ʟɪɴᴋ ʙᴏᴛ ꜱᴇᴛᴜᴘ',
                        callback_data='settings#sharebot')
           ],[
           InlineKeyboardButton('🔙 ʙᴀᴄᴋ', callback_data='back')
           ]]
  else:
      #  FORWARD MODE: Full original menu 
      buttons = [[
           InlineKeyboardButton('↔️ ꜰᴏʀᴡᴀʀᴅ ᴍᴏᴅᴇ »   ⟶  ᴛᴀᴘ ᴛᴏ ꜱᴡɪᴛᴄʜ',
                        callback_data='settings#toggle_mode')
           ],[
           InlineKeyboardButton('👥 ᴀᴄᴄᴏᴜɴᴛꜱ',
                        callback_data='settings#accounts'),
           InlineKeyboardButton('📢 ᴄʜᴀɴɴᴇʟꜱ',
                        callback_data='settings#channels')
           ],[
           InlineKeyboardButton('📝 ᴄᴀᴘᴛɪᴏɴ',
                        callback_data='settings#caption'),
           InlineKeyboardButton('🗄️ ᴍᴏɴɢᴏᴅʙ',
                        callback_data='settings#database')
           ],[
           InlineKeyboardButton('🎯 ꜰɪʟᴛᴇʀꜱ',
                        callback_data='settings#filters'),
           InlineKeyboardButton('🖱️ ʙᴜᴛᴛᴏɴꜱ',
                        callback_data='settings#button')
           ],[
           InlineKeyboardButton('🧬 ᴇxᴛʀᴀ ꜱᴇᴛᴛɪɴɢꜱ',
                        callback_data='settings#nextfilters'),
           InlineKeyboardButton('🧹 ᴄʟᴇᴀɴ ᴍꜱɢ',
                        callback_data='settings#cleanmsg')
           ],[
           InlineKeyboardButton('🌍 ʟᴀɴɢᴜᴀɢᴇ / भाषा',
                        callback_data='settings#lang')
           ],[
           InlineKeyboardButton('🤖 ꜱʜᴀʀᴇ ʟɪɴᴋ ʙᴏᴛ ꜱᴇᴛᴜᴘ',
                        callback_data='settings#sharebot')
           ],[
           InlineKeyboardButton('🔙 ʙᴀᴄᴋ', callback_data='back')
           ]]
  return InlineKeyboardMarkup(buttons)



def size_limit(limit):
   if str(limit) == "None":
      return None, ""
   elif str(limit) == "True":
      return True, "more than"
   else:
      return False, "less than"

def extract_btn(datas):
    i = 0
    btn = []
    if datas:
       for data in datas:
         if i >= 5:
            i = 0
         if i == 0:
            btn.append([InlineKeyboardButton(data, f'settings#alert_{data}')])
            i += 1
            continue
         elif i > 0:
            btn[-1].append(InlineKeyboardButton(data, f'settings#alert_{data}'))
            i += 1
    return btn 

def size_button(size):
  buttons = [[
       InlineKeyboardButton('+',
                    callback_data=f'settings#update_limit-True-{size}'),
       InlineKeyboardButton('=',
                    callback_data=f'settings#update_limit-None-{size}'),
       InlineKeyboardButton('-',
                    callback_data=f'settings#update_limit-False-{size}')
       ],[
       InlineKeyboardButton('+1',
                    callback_data=f'settings#update_size-{size + 1}'),
       InlineKeyboardButton('-1',
                    callback_data=f'settings#update_size_-{size - 1}')
       ],[
       InlineKeyboardButton('+5',
                    callback_data=f'settings#update_size-{size + 5}'),
       InlineKeyboardButton('-5',
                    callback_data=f'settings#update_size_-{size - 5}')
       ],[
       InlineKeyboardButton('+10',
                    callback_data=f'settings#update_size-{size + 10}'),
       InlineKeyboardButton('-10',
                    callback_data=f'settings#update_size_-{size - 10}')
       ],[
       InlineKeyboardButton('+50',
                    callback_data=f'settings#update_size-{size + 50}'),
       InlineKeyboardButton('-50',
                    callback_data=f'settings#update_size_-{size - 50}')
       ],[
       InlineKeyboardButton('+100',
                    callback_data=f'settings#update_size-{size + 100}'),
       InlineKeyboardButton('-100',
                    callback_data=f'settings#update_size_-{size - 100}')
       ],[
       InlineKeyboardButton('«  ʙᴀᴄᴋ',
                    callback_data="settings#main")
     ]]
  return InlineKeyboardMarkup(buttons)
       
async def filters_buttons(user_id):
  filter = await get_configs(user_id)
  filters = filter['filters']
  buttons = [[
       InlineKeyboardButton('🏷️ ꜰᴏʀᴡᴀʀᴅ ᴛᴀɢ',
                    callback_data=f'settings_#updatefilter-forward_tag-{filter["forward_tag"]}'),
       InlineKeyboardButton('✅' if filter['forward_tag'] else '❌',
                    callback_data=f'settings#updatefilter-forward_tag-{filter["forward_tag"]}')
       ],[
       InlineKeyboardButton('🖍️ ᴛᴇxᴛꜱ',
                    callback_data=f'settings_#updatefilter-text-{filters["text"]}'),
       InlineKeyboardButton('✅' if filters['text'] else '❌',
                    callback_data=f'settings#updatefilter-text-{filters["text"]}')
       ],[
       InlineKeyboardButton('»  ᴅᴏᴄᴜᴍᴇɴᴛꜱ',
                    callback_data=f'settings_#updatefilter-document-{filters["document"]}'),
       InlineKeyboardButton('✅' if filters['document'] else '❌',
                    callback_data=f'settings#updatefilter-document-{filters["document"]}')
       ],[
       InlineKeyboardButton('🎞️ ᴠɪᴅᴇᴏꜱ',
                    callback_data=f'settings_#updatefilter-video-{filters["video"]}'),
       InlineKeyboardButton('✅' if filters['video'] else '❌',
                    callback_data=f'settings#updatefilter-video-{filters["video"]}')
       ],[
       InlineKeyboardButton('📷 ᴘʜᴏᴛᴏꜱ',
                    callback_data=f'settings_#updatefilter-photo-{filters["photo"]}'),
       InlineKeyboardButton('✅' if filters['photo'] else '❌',
                    callback_data=f'settings#updatefilter-photo-{filters["photo"]}')
       ],[
       InlineKeyboardButton('🎧 ᴀᴜᴅɪᴏꜱ',
                    callback_data=f'settings_#updatefilter-audio-{filters["audio"]}'),
       InlineKeyboardButton('✅' if filters['audio'] else '❌',
                    callback_data=f'settings#updatefilter-audio-{filters["audio"]}')
       ],[
       InlineKeyboardButton('🎤 ᴠᴏɪᴄᴇꜱ',
                    callback_data=f'settings_#updatefilter-voice-{filters["voice"]}'),
       InlineKeyboardButton('✅' if filters['voice'] else '❌',
                    callback_data=f'settings#updatefilter-voice-{filters["voice"]}')
       ],[
       InlineKeyboardButton('🎭 ᴀɴɪᴍᴀᴛɪᴏɴꜱ',
                    callback_data=f'settings_#updatefilter-animation-{filters["animation"]}'),
       InlineKeyboardButton('» ' if filters['animation'] else '‣ ',
                    callback_data=f'settings#updatefilter-animation-{filters["animation"]}')
       ],[
       InlineKeyboardButton('🃏 ꜱᴛɪᴄᴋᴇʀꜱ',
                    callback_data=f'settings_#updatefilter-sticker-{filters["sticker"]}'),
       InlineKeyboardButton('» ' if filters['sticker'] else '‣ ',
                    callback_data=f'settings#updatefilter-sticker-{filters["sticker"]}')
       ],[
       InlineKeyboardButton('»  ꜱᴋɪᴘ ᴅᴜᴘʟɪᴄᴀᴛᴇ',
                    callback_data=f'settings_#updatefilter-duplicate-{filter["duplicate"]}'),
       InlineKeyboardButton('» ' if filter['duplicate'] else '‣ ',
                    callback_data=f'settings#updatefilter-duplicate-{filter["duplicate"]}')
       ],[
               InlineKeyboardButton('»  ᴄᴀᴘᴛɪᴏɴ ꜱᴇᴛᴛɪɴɢꜱ →',

                     callback_data='settings#caption'),

        InlineKeyboardButton(

            '» ' if filters.get('rm_caption', False) is True else (

            '» ' if filters.get('rm_caption', False) == 2 else '» '),

                     callback_data='settings#caption')

        ],[
       InlineKeyboardButton('⫷ ʙᴀᴄᴋ',
                    callback_data="settings#main")
       ]]
  return InlineKeyboardMarkup(buttons) 

async def next_filters_buttons(user_id):
  filter = await get_configs(user_id)
  filters = filter['filters']
  links_on = filters.get('links', False)
  buttons = [[
       InlineKeyboardButton('»  ᴘᴏʟʟ',
                    callback_data=f'settings_#updatefilter-poll-{filters["poll"]}'),
       InlineKeyboardButton('» ' if filters['poll'] else '‣ ',
                    callback_data=f'settings#updatefilter-poll-{filters["poll"]}')
       ],[
       InlineKeyboardButton('‣  ꜱᴇᴄᴜʀᴇ ᴍᴇꜱꜱᴀɢᴇ',
                    callback_data=f'settings_#updatefilter-protect-{filter["protect"]}'),
       InlineKeyboardButton('» ' if filter['protect'] else '‣ ',
                    callback_data=f'settings#updatefilter-protect-{filter["protect"]}')
       ],[
       InlineKeyboardButton('»  ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴏᴅᴇ',
                    callback_data=f'settings_#updatefilter-download-{filter["download"]}'),
       InlineKeyboardButton('» ' if filter.get('download') else '‣ ',
                    callback_data=f'settings#updatefilter-download-{filter["download"]}')
       ],[
       InlineKeyboardButton('»  ʟɪɴᴋꜱ',
                    callback_data=f'settings_#updatefilter-links-{links_on}'),
       InlineKeyboardButton('» ' if links_on else '‣ ',
                    callback_data=f'settings#updatefilter-links-{links_on}')
       ],[
       InlineKeyboardButton('🛑 ꜱɪᴢᴇ ʟɪᴍɪᴛ',
                    callback_data='settings#file_size')
       ],[
       InlineKeyboardButton('» ️ ꜱᴇᴛ ᴅᴜʀᴀᴛɪᴏɴ',
                    callback_data='settings#set_duration')
       ],[
       InlineKeyboardButton('»  ᴇxᴛᴇɴꜱɪᴏɴ',
                    callback_data='settings#get_extension')
       ],[
       InlineKeyboardButton('♦️ ᴋᴇʏᴡᴏʀᴅꜱ ♦️',
                    callback_data='settings#get_keyword')
       ],[
       InlineKeyboardButton('⫷ ʙᴀᴄᴋ', 
                    callback_data="settings#main")
       ]]
  return InlineKeyboardMarkup(buttons) 
   


