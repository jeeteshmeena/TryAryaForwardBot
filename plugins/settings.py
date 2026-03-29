import asyncio
from database import db
from translation import Translation
from plugins.lang import t, _tx
from pyrogram import Client, filters
from .test import get_configs, update_configs, CLIENT, parse_buttons
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

CLIENT = CLIENT()

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
  buttons = [[InlineKeyboardButton('↩ Back', callback_data="settings#main")]]
  
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
     buttons.append([InlineKeyboardButton("━━━ 🤖 Bots ━━━", callback_data="settings#noop")])
     for b in normal_bots:
         active_mark = "✅ " if b.get('active') else ""
         buttons.append([InlineKeyboardButton(f"{active_mark}🤖 {b['name']}", callback_data=f"settings#editbot_{b['id']}")])
     if len(normal_bots) < 2:
         buttons.append([InlineKeyboardButton('✚ Add Bot ✚', callback_data="settings#addbot")])

     # ---- USERBOTS SECTION ----
     buttons.append([InlineKeyboardButton("━━━ 👤 Userbots ━━━", callback_data="settings#noop")])
     for b in userbots:
         active_mark = "✅ " if b.get('active') else ""
         buttons.append([InlineKeyboardButton(f"{active_mark}👤 {b['name']}", callback_data=f"settings#editbot_{b['id']}")])
     if len(userbots) < 2:
         buttons.append([InlineKeyboardButton('✚ Add Userbot ✚', callback_data="settings#adduserbot")])
         
     buttons.append([InlineKeyboardButton('↩ Back', callback_data="settings#main")])
     
     text = (
         "<b><u>📋 My Accounts</u></b>\n\n"
         f"<b>🤖 Bots:</b> {len(normal_bots)}/2\n"
         f"<b>👤 Userbots:</b> {len(userbots)}/2\n\n"
         "<b>Tap an account to view details or set it active.\n"
         "✅ = Currently active for that type.</b>"
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
     buttons.append([InlineKeyboardButton('✚ Add Channel ✚', 
                      callback_data="settings#addchannel")])
     buttons.append([InlineKeyboardButton('↩ Back', 
                      callback_data="settings#main")])
     await query.message.edit_text( 
       "<b><u>My Channels</b></u>\n\n<b>you can manage your target chats in here</b>",
       reply_markup=InlineKeyboardMarkup(buttons))
   
  elif type=="addchannel":  
     await query.message.delete()
     try:
         text = await bot.send_message(user_id, "<b>❪ SET TARGET CHAT ❫\n\nForward a message from Your target chat\n/cancel - cancel this process</b>")
         chat_ids = await bot.listen(chat_id=user_id, timeout=300)
         if chat_ids.text=="/cancel":
            await chat_ids.delete()
            return await text.edit_text(
                  "<b>process canceled</b>",
                  reply_markup=InlineKeyboardMarkup(buttons))
         elif not chat_ids.forward_date:
            await chat_ids.delete()
            return await text.edit_text("**This is not a forward message**")
         else:
            chat_id = chat_ids.forward_from_chat.id
            title = chat_ids.forward_from_chat.title
            username = chat_ids.forward_from_chat.username
            username = "@" + username if username else "private"
         chat = await db.add_channel(user_id, chat_id, title, username)
         await chat_ids.delete()
         await text.edit_text(
            "<b>Successfully updated</b>" if chat else "<b>This channel already added</b>",
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
         buttons.append([InlineKeyboardButton('✅ Set Active', callback_data=f"settings#setactive_{bott['id']}")])
         
     buttons.append([InlineKeyboardButton('❌ Remove ❌', callback_data=f"settings#removebot_{bott['id']}")])
     buttons.append([InlineKeyboardButton('↩ Back', callback_data="settings#accounts")])
     await query.message.edit_text(
        TEXT.format(bott['name'], bott['id'], bott['username']),
        reply_markup=InlineKeyboardMarkup(buttons))
                                             
  elif type.startswith("setactive"):
     bot_id = type.split('_')[1]
     await db.set_active_bot(user_id, bot_id)
     await query.answer("Account set as ACTIVE!", show_alert=True)
     buttons = [[InlineKeyboardButton('↩ Back to Accounts', callback_data="settings#accounts")]]
     await query.message.edit_text("<b>Successfully changed active account.</b>", reply_markup=InlineKeyboardMarkup(buttons))

  elif type == "sharebot":
     token       = await db.get_share_bot_token()
     protect     = await db.get_share_protect_global()
     auto_delete = await db.get_share_autodelete_global()
     bpp         = await db.get_share_buttons_per_post()
      fsub_chs    = await db.get_share_fsub_channels()
      bots_list   = await db.get_share_bots()

      ptxt = "✅ ON" if protect else "❌ OFF"
      if auto_delete == 0:    adtxt = "❌ OFF"
      elif auto_delete < 60:  adtxt = f"⏱ {auto_delete}m"
      else:                   adtxt = f"⏱ {auto_delete // 60}h"

      buttons = [
          [InlineKeyboardButton(f'🛡 Protection: {ptxt}', callback_data='settings#sharebotprotect'),
           InlineKeyboardButton(f'⏱ Auto-Delete: {adtxt}', callback_data='settings#sharebotautodel')],
          [InlineKeyboardButton(f'🗂 Buttons/Post: {bpp}', callback_data='settings#sharebot_bpp'),
           InlineKeyboardButton(f'📢 Force-Subscribe ({len(fsub_chs)}/6)', callback_data='settings#sharefsub')],
          [InlineKeyboardButton('📖 Welcome Msg', callback_data='settings#sbt_welcome'),
           InlineKeyboardButton('🗑 Delete Msg', callback_data='settings#sbt_delete')],
          [InlineKeyboardButton('📝 Custom Caption', callback_data='settings#sbt_caption'),
           InlineKeyboardButton('✅ Success Msg', callback_data='settings#sbt_success')],
          [InlineKeyboardButton('🔐 FSub Message', callback_data='settings#sbt_fsub'),
           InlineKeyboardButton(f'🤖 Share Bots ({len(bots_list)})', callback_data='settings#sbt_manage')],
          [InlineKeyboardButton('↩ Back', callback_data='settings#main')]
      ]
      txt = (
          f"<b>❪ SHARE BOT CONFIGURATION ❫</b>\n\n"
          f"<b>❯ Share Bots:</b> {len(bots_list)} active agent(s)\n\n"
          f"<b>⚙️ Settings Overview:</b>\n"
          f"<b>┠ Protection</b> — restricts saving & forwarding delivered files.\n"
          f"<b>┠ Auto-Delete</b> — globally deletes files after the timer.\n"
          f"<b>┠ Buttons/Post</b> — how many episode buttons appear in channel posts.\n"
          f"<b>┠ Force-Subscribe</b> — users must join channels before receiving files.\n"
          f"<b>┖ Messaging</b> — click the lower buttons to customize text & captions."
      )
      await query.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(buttons))

  elif type == "sharebotprotect":
     protect = await db.get_share_protect_global()
     await db.set_share_protect_global(not protect)
     await query.answer(f"Protection turned {'OFF' if protect else 'ON'}")
     return await edit_settings(client, query, "sharebot")

  elif type == "sharebotautodel":
     # Global cycle: Off → 5m → 15m → 3h → 6h → 12h → 24h → 48h → Off
     current = await db.get_share_autodelete_global()
     if current == 0:      nxt = 5
     elif current == 5:    nxt = 15
     elif current == 15:   nxt = 180
     elif current == 180:  nxt = 360
     elif current == 360:  nxt = 720
     elif current == 720:  nxt = 1440
     elif current == 1440: nxt = 2880
     else:                 nxt = 0
     await db.set_share_autodelete_global(nxt)
     await query.answer("Auto-Delete Timer Updated!")
     return await edit_settings(client, query, "sharebot")

  elif type == "sharebot_bpp":
     # Cycle buttons-per-post: 4 → 6 → 8 → 10 → 12 → 16 → 20 → 4
     current = await db.get_share_buttons_per_post()
     cycle   = [4, 6, 8, 10, 12, 16, 20]
     try:
         idx = cycle.index(current)
         nxt = cycle[(idx + 1) % len(cycle)]
     except ValueError:
         nxt = 10
     await db.set_share_buttons_per_post(nxt)
     await query.answer(f"Buttons per post set to {nxt}")
     return await edit_settings(client, query, "sharebot")

  elif type.startswith("sbt_"):
     key_map = {
         "sbt_welcome": ("welcome_msg", "Welcome Message"),
         "sbt_delete": ("delete_msg", "Delete Message"),
         "sbt_caption": ("custom_caption", "Custom Caption"),
         "sbt_success": ("success_msg", "Success Message"),
         "sbt_fsub": ("fsub_msg", "FSub Message"),
     }
     db_key, title = key_map[type]
     await query.message.delete()
     try:
         ask = await bot.send_message(
             user_id,
             f"<b>❪ CUSTOMIZE TEXT: {title.upper()} ❫</b>\n\n"
             f"<b>Send the new text to be used.</b>\n"
             f"<i>Supported variables:</i>\n"
             f"<code>{{first_name}}</code> — User's first name\n"
             f"<code>{{last_name}}</code> — User's last name\n"
             f"<code>{{mention}}</code> — @username or strict ID mention\n\n"
             "Send <code>/clear</code> to reset to default.\n"
             "Send <code>/cancel</code> to abort."
         )
         resp = await bot.listen(chat_id=user_id, timeout=300)
         if resp.text == "/cancel":
             await resp.delete()
             return await ask.edit_text("<b>Cancelled.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharebot")]]))
         elif resp.text == "/clear":
             await db.set_share_text(db_key, "")
             await ask.edit_text(f"✅ <b>{title} has been reset to default!</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharebot")]]))
         elif resp.text:
             await db.set_share_text(db_key, resp.text.html if getattr(resp.text, 'html', None) else resp.text)
             await ask.edit_text(f"✅ <b>{title} successfully updated!</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharebot")]]))
         await resp.delete()
     except Exception as e:
         pass

  elif type == "sbt_manage":
      bots = await db.get_share_bots()
      buttons = []
      buttons.append([InlineKeyboardButton("━━━ 🤖 Delivery Bots ━━━", callback_data="settings#noop")])
      for b in bots:
          buttons.append([InlineKeyboardButton(f"🤖 {b['name']}", callback_data=f"settings#sb_view_{b['id']}")])
      if len(bots) < 10:
          buttons.append([InlineKeyboardButton('✚ Add Share Bot ✚', callback_data="settings#sb_add")])
      buttons.append([InlineKeyboardButton('↩ Back', callback_data="settings#sharebot")])
      
      text = (
          "<b><u>📋 Share Agent Accounts</u></b>\n\n"
          f"<b>Allocated Bots:</b> {len(bots)}/10\n\n"
          "<b>These bots handle exclusively the delivery payload of your Share Links. They distribute traffic securely to avoid bans across high volumes.</b>"
      )
      await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
      
  elif type == "sb_add":
      await query.message.delete()
      from plugins.test import CLIENT
      tmp = CLIENT()
      res = await tmp.add_bot(client, query) # Reuse bot token fetch flow!
      # Actually wait, add_bot adds to general account! Let's build a dedicated simple token fetcher:
      try:
          ask = await bot.send_message(user_id, "<b>❪ ADD SHARE BOT ❫</b>\n\nForward a message containing the token from @BotFather, or send the token directly.\n\n/cancel to abort")
          resp = await bot.listen(chat_id=user_id, timeout=120)
          if resp.text == "/cancel":
              await resp.delete()
              return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sbt_manage")]]))
          
          tk = resp.text.strip()
          import re
          # If it's a forwarded msg:
          match = re.search(r"([0-9]{8,11}:[a-zA-Z0-9_-]{35,})", tk)
          if match: tk = match.group(1)
          
          if ":" not in tk:
              return await ask.edit_text("<b>❌ Invalid Token Format!</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sbt_manage")]]))
              
          # Validate Token
          test_app = Client("test_sb_"+tk[:10], api_id=CLIENT.api_id, api_hash=CLIENT.api_hash, bot_token=tk, in_memory=True)
          await test_app.start()
          me = await test_app.get_me()
          await test_app.stop()
          
          # Add to DB
          await db.add_share_bot(me.id, tk, me.username, me.first_name)
          
          # Start it immediately globally!
          from plugins.share_bot import start_share_bot
          import asyncio
          asyncio.create_task(start_share_bot())
          
          await ask.edit_text(f"✅ <b>Successfully Linked 🤖 {me.first_name}</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sbt_manage")]]))
          await resp.delete()
      except Exception as e:
          try: await ask.edit_text(f"❌ <b>Error:</b> {e}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sbt_manage")]]))
          except: pass

  elif type.startswith("sb_view_"):
      b_id = type.split("_")[-1]
      bots = await db.get_share_bots()
      bt = next((x for x in bots if x['id'] == str(b_id)), None)
      if not bt: return await query.answer("Bot not found!")
      
      buttons = [
          [InlineKeyboardButton('❌ Remove Bot ❌', callback_data=f"settings#sb_remove_{b_id}")],
          [InlineKeyboardButton('↩ Back', callback_data="settings#sbt_manage")]
      ]
      await query.message.edit_text(
          f"<b>❪ SHARE BOT PROFILE ❫</b>\n\n"
          f"<b>📝 Name:</b> {bt['name']}\n"
          f"<b>🔗 Username:</b> @{bt['username']}\n"
          f"<b>🆔 ID:</b> <code>{bt['id']}</code>",
          reply_markup=InlineKeyboardMarkup(buttons)
      )
      
  elif type.startswith("sb_remove_"):
      b_id = type.split("_")[-1]
      await db.remove_share_bot(b_id)
      
      # We could stop the client from share_bot.py but it's fine if it hangs active until restart.
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
             InlineKeyboardButton(f"🔄 Toggle JR #{i+1}",  callback_data=f"settings#sharefsub_jr_{i}"),
             InlineKeyboardButton(f"❌ Remove #{i+1}", callback_data=f"settings#sharefsub_del_{i}")
         ])
     ch_list = "\n".join(lines) if lines else "None configured."
     if len(fsub_chs) < 6:
         btns.append([InlineKeyboardButton("➕ Add Channel/Group", callback_data="settings#sharefsub_add")])
     btns.append([InlineKeyboardButton("↩ Back", callback_data="settings#sharebot")])
     await query.message.edit_text(
         f"<b>📢 Force-Subscribe Channels</b>\n\n"
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
             return await ask.edit_text("Cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharefsub")]]))
         raw_id = resp.text.strip()
         await resp.delete()
         try:
             ch_obj = await bot.get_chat(raw_id)
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
                 f"✅ Added: <b>{ch_obj.title}</b>",
                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharefsub")]])
             )
         except Exception as e:
             await ask.edit_text(
                 f"❌ Failed to add channel: <code>{e}</code>\n"
                 "Make sure the Main Bot is an admin in that channel.",
                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharefsub")]])
             )
     except asyncio.exceptions.TimeoutError:
         try: await ask.edit_text("Timeout.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩ Back", callback_data="settings#sharefsub")]]))
         except Exception: pass

  elif type.startswith("sharefsub_jr_"):
     idx      = int(type.split("_")[-1])
     fsub_chs = await db.get_share_fsub_channels()
     if 0 <= idx < len(fsub_chs):
         fsub_chs[idx]['join_request'] = not fsub_chs[idx].get('join_request', False)
         await db.set_share_fsub_channels(fsub_chs)
         status = "ON" if fsub_chs[idx]['join_request'] else "OFF"
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
             return await txtmsg.edit_text("<b>Cancelled.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='settings#sharebot')]]))
         if resp.text == "/remove":
             await resp.delete()
             await db.set_share_bot_token("")
             return await txtmsg.edit_text("<b>Token Removed.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='settings#sharebot')]]))
            
         bot_token = re.findall(r'\d{8,10}:[A-Za-z0-9_-]{35}', resp.text)
         if not bot_token:
             return await txtmsg.edit_text("<b>Invalid Token Format.</b>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='settings#sharebot')]]))
         
         new_token = bot_token[0]
         await db.set_share_bot_token(new_token)
         # Start immediately
         try:
             from plugins.share_bot import start_share_bot
             await start_share_bot(new_token)
             status = "✅ Successfully Saved & Started!"
         except Exception as e:
             status = f"✅ Saved securely, but failed to start stream:\n<code>{e}</code>"
             
         await resp.delete()
         await txtmsg.edit_text(status, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='settings#sharebot')]]))
     except asyncio.exceptions.TimeoutError:
         try: await txtmsg.edit_text('Timeout.', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('↩ Back', callback_data='settings#sharebot')]]))
         except: pass

  elif type.startswith("removebot"):
     if "_" in type:
         bot_id = type.split('_')[1]
         await db.remove_bot(user_id, bot_id)
     else:
         await db.remove_bot(user_id)
     buttons = [[InlineKeyboardButton('↩ Back to Accounts', callback_data="settings#accounts")]]
     await query.message.edit_text(
        "<b>successfully removed!</b>",
        reply_markup=InlineKeyboardMarkup(buttons))
                                             
  elif type.startswith("editchannels"): 
     chat_id = type.split('_')[1]
     chat = await db.get_channel_details(user_id, chat_id)
     buttons = [[InlineKeyboardButton('❌ Remove ❌', callback_data=f"settings#removechannel_{chat_id}")
               ],
               [InlineKeyboardButton('↩ Back', callback_data="settings#channels")]]
     await query.message.edit_text(
        f"<b><u>📄 CHANNEL DETAILS</b></u>\n\n<b>- TITLE:</b> <code>{chat['title']}</code>\n<b>- CHANNEL ID: </b> <code>{chat['chat_id']}</code>\n<b>- USERNAME:</b> {chat['username']}",
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
         mode_lbl = "🤖 Smart Clean  (active)"
     elif rm_cap == 2:
         mode_lbl = "🗑️ Wipe All Captions  (active)"
     else:
         mode_lbl = "✅ Keep Original  (active)"

     cap_lbl = "✚ Add Custom Caption" if caption is None else "✏️ Edit Custom Caption"

     buttons = [[
         InlineKeyboardButton("━━━━  Caption Mode  ━━━━",
             callback_data="settings_#noop")
     ],[
         InlineKeyboardButton("✅ Keep Original" + (" ◀" if not rm_cap else ""),
             callback_data="settings#caption_mode-off"),
     ],[
         InlineKeyboardButton("🤖 Smart Clean" + (" ◀" if rm_cap is True else ""),
             callback_data="settings#caption_mode-smart"),
     ],[
         InlineKeyboardButton("🗑️ Wipe All Captions" + (" ◀" if rm_cap == 2 else ""),
             callback_data="settings#caption_mode-wipe"),
     ],[
         InlineKeyboardButton("━━━━  Custom Template  ━━━━",
             callback_data="settings_#noop")
     ],[
         InlineKeyboardButton(cap_lbl, callback_data="settings#addcaption"),
     ]]
     if caption is not None:
         buttons.append([
             InlineKeyboardButton("👁 View Template",  callback_data="settings#seecaption"),
             InlineKeyboardButton("🗑️ Clear Template", callback_data="settings#deletecaption"),
         ])
     buttons.append([InlineKeyboardButton("↩ Back", callback_data="settings#main")])

     await query.message.edit_text(
         "<b><u>📝 Caption Settings</u></b>\n\n"
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
     buttons = [[InlineKeyboardButton('🖋️ Edit Caption', 
                  callback_data="settings#addcaption")
               ],[
               InlineKeyboardButton('↩ Back', 
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
         InlineKeyboardButton("✅ Keep Original" + (" ◀" if not rm_cap else ""), callback_data="settings#caption_mode-off"),
     ],[
         InlineKeyboardButton("🤖 Smart Clean" + (" ◀" if rm_cap is True else ""), callback_data="settings#caption_mode-smart"),
     ],[
         InlineKeyboardButton("🗑️ Wipe All Captions" + (" ◀" if rm_cap == 2 else ""), callback_data="settings#caption_mode-wipe"),
     ],[
         InlineKeyboardButton("✚ Add Custom Caption", callback_data="settings#addcaption"),
     ],[
         InlineKeyboardButton("↩ Back", callback_data="settings#main")
     ]]
     await query.message.edit_text(
         "<b><u>📝 Caption Settings</u></b>\n\n<b>Template cleared successfully.</b>",
         reply_markup=InlineKeyboardMarkup(buttons))
                              
  elif type.startswith("caption_mode"):
     mode = type.split("-")[1]  # off | smart | wipe
     if mode == "off":
         val = False
     elif mode == "smart":
         val = True
     else:
         val = 2  # wipe
         
     # Update inside filters dict appropriately
     data = await get_configs(user_id)
     filters_dict = data.get('filters', {})
     filters_dict['rm_caption'] = val
     await update_configs(user_id, 'filters', filters_dict)
     
     await query.answer("✅ Caption mode updated!", show_alert=False)
     # Refresh the caption sub-menu
     data    = await get_configs(user_id)
     caption = data['caption']
     rm_cap  = data.get('filters', {}).get('rm_caption', False)
     if rm_cap is True:
         mode_lbl = "🤖 Smart Clean  (active)"
     elif rm_cap == 2:
         mode_lbl = "🗑️ Wipe All Captions  (active)"
     else:
         mode_lbl = "✅ Keep Original  (active)"
     cap_lbl = "✚ Add Custom Caption" if caption is None else "✏️ Edit Custom Caption"
     buttons = [[
         InlineKeyboardButton("━━━━  Caption Mode  ━━━━", callback_data="settings_#noop")
     ],[
         InlineKeyboardButton("✅ Keep Original" + (" ◀" if not rm_cap else ""), callback_data="settings#caption_mode-off"),
     ],[
         InlineKeyboardButton("🤖 Smart Clean" + (" ◀" if rm_cap is True else ""), callback_data="settings#caption_mode-smart"),
     ],[
         InlineKeyboardButton("🗑️ Wipe All Captions" + (" ◀" if rm_cap == 2 else ""), callback_data="settings#caption_mode-wipe"),
     ],[
         InlineKeyboardButton("━━━━  Custom Template  ━━━━", callback_data="settings_#noop")
     ],[
         InlineKeyboardButton(cap_lbl, callback_data="settings#addcaption"),
     ]]
     if caption is not None:
         buttons.append([
             InlineKeyboardButton("👁 View Template",  callback_data="settings#seecaption"),
             InlineKeyboardButton("🗑️ Clear Template", callback_data="settings#deletecaption"),
         ])
     buttons.append([InlineKeyboardButton("↩ Back", callback_data="settings#main")])
     await query.message.edit_text(
         "<b><u>📝 Caption Settings</u></b>\n\n"
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
        buttons.append([InlineKeyboardButton('✚ Add Button ✚', 
                      callback_data="settings#addbutton")])
     else:
        buttons.append([InlineKeyboardButton('👀 See Button', 
                      callback_data="settings#seebutton")])
        buttons[-1].append(InlineKeyboardButton('🗑️ Remove Button ', 
                      callback_data="settings#deletebutton"))
     buttons.append([InlineKeyboardButton('↩ Back', 
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
      button.append([InlineKeyboardButton("↩ Back", "settings#button")])
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
        buttons.append([InlineKeyboardButton('✚ Add Url ✚', 
                      callback_data="settings#addurl")])
     else:
        buttons.append([InlineKeyboardButton('👀 See Url', 
                      callback_data="settings#seeurl")])
        buttons[-1].append(InlineKeyboardButton('🗑️ Remove Url ', 
                      callback_data="settings#deleteurl"))
     buttons.append([InlineKeyboardButton('↩ Back', 
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
    btn.append([InlineKeyboardButton('✚ ADD ✚', 'settings#add_extension')])
    btn.append([InlineKeyboardButton('Remove all', 'settings#rmve_all_extension')])
    btn.append([InlineKeyboardButton('↩ Back', 'settings#main')])
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
    btn.append([InlineKeyboardButton('✚ ADD ✚', 'settings#add_keyword')])
    btn.append([InlineKeyboardButton('Remove all', 'settings#rmve_all_keyword')])
    btn.append([InlineKeyboardButton('↩ Back', 'settings#main')])
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
      # ── MERGER MODE: Clean separate menu ────────────────────────────────
      buttons = [[
           InlineKeyboardButton('🔀 Merger Mode ✅  ⟶  Tap to switch',
                        callback_data='settings#toggle_mode')
           ],[
           InlineKeyboardButton('🤖 Aᴄᴄᴏᴜɴᴛs',
                        callback_data='settings#accounts'),
           InlineKeyboardButton('🏷 Cʜᴀɴɴᴇʟs',
                        callback_data='settings#channels')
           ],[
           InlineKeyboardButton('🎵 Aᴜᴅɪᴏ Mᴇʀɢᴇ',
                        callback_data='mg#audio_list'),
           InlineKeyboardButton('🎬 Vɪᴅᴇᴏ Mᴇʀɢᴇ',
                        callback_data='mg#video_list')
           ],[
           InlineKeyboardButton('🔗 Sʜᴀʀᴇ Bᴏᴛ sᴇᴛᴜᴘ',
                        callback_data='settings#sharebot')
           ],[
           InlineKeyboardButton('⫷ Bᴀᴄᴋ', callback_data='back')
           ]]
  else:
      # ── FORWARD MODE: Full original menu ────────────────────────────────
      buttons = [[
           InlineKeyboardButton('📤 Forward Mode ✅  ⟶  Tap to switch',
                        callback_data='settings#toggle_mode')
           ],[
           InlineKeyboardButton('🤖 Aᴄᴄᴏᴜɴᴛs',
                        callback_data='settings#accounts'),
           InlineKeyboardButton('🏷 Cʜᴀɴɴᴇʟs',
                        callback_data='settings#channels')
           ],[
           InlineKeyboardButton('🖋️ Cᴀᴘᴛɪᴏɴ',
                        callback_data='settings#caption'),
           InlineKeyboardButton('🗃 MᴏɴɢᴏDB',
                        callback_data='settings#database')
           ],[
           InlineKeyboardButton('🕵‍♀ Fɪʟᴛᴇʀs 🕵‍♀',
                        callback_data='settings#filters'),
           InlineKeyboardButton('⏹ Bᴜᴛᴛᴏɴ',
                        callback_data='settings#button')
           ],[
           InlineKeyboardButton('Exᴛʀᴀ Sᴇᴛᴛɪɴɢs 🧪',
                        callback_data='settings#nextfilters'),
           InlineKeyboardButton('🗑 Cʟᴇᴀɴ MSG',
                        callback_data='settings#cleanmsg')
           ],[
           InlineKeyboardButton('🌐 Language / भाषा',
                        callback_data='settings#lang')
           ],[
           InlineKeyboardButton('🔗 Sʜᴀʀᴇ ʟɪɴᴋ Bᴏᴛ sᴇᴛᴜᴘ',
                        callback_data='settings#sharebot')
           ],[
           InlineKeyboardButton('⫷ Bᴀᴄᴋ', callback_data='back')
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
       InlineKeyboardButton('↩ Back',
                    callback_data="settings#main")
     ]]
  return InlineKeyboardMarkup(buttons)
       
async def filters_buttons(user_id):
  filter = await get_configs(user_id)
  filters = filter['filters']
  buttons = [[
       InlineKeyboardButton('🏷️ Forward tag',
                    callback_data=f'settings_#updatefilter-forward_tag-{filter["forward_tag"]}'),
       InlineKeyboardButton('✅' if filter['forward_tag'] else '❌',
                    callback_data=f'settings#updatefilter-forward_tag-{filter["forward_tag"]}')
       ],[
       InlineKeyboardButton('🖍️ Texts',
                    callback_data=f'settings_#updatefilter-text-{filters["text"]}'),
       InlineKeyboardButton('✅' if filters['text'] else '❌',
                    callback_data=f'settings#updatefilter-text-{filters["text"]}')
       ],[
       InlineKeyboardButton('📁 Documents',
                    callback_data=f'settings_#updatefilter-document-{filters["document"]}'),
       InlineKeyboardButton('✅' if filters['document'] else '❌',
                    callback_data=f'settings#updatefilter-document-{filters["document"]}')
       ],[
       InlineKeyboardButton('🎞️ Videos',
                    callback_data=f'settings_#updatefilter-video-{filters["video"]}'),
       InlineKeyboardButton('✅' if filters['video'] else '❌',
                    callback_data=f'settings#updatefilter-video-{filters["video"]}')
       ],[
       InlineKeyboardButton('📷 Photos',
                    callback_data=f'settings_#updatefilter-photo-{filters["photo"]}'),
       InlineKeyboardButton('✅' if filters['photo'] else '❌',
                    callback_data=f'settings#updatefilter-photo-{filters["photo"]}')
       ],[
       InlineKeyboardButton('🎧 Audios',
                    callback_data=f'settings_#updatefilter-audio-{filters["audio"]}'),
       InlineKeyboardButton('✅' if filters['audio'] else '❌',
                    callback_data=f'settings#updatefilter-audio-{filters["audio"]}')
       ],[
       InlineKeyboardButton('🎤 Voices',
                    callback_data=f'settings_#updatefilter-voice-{filters["voice"]}'),
       InlineKeyboardButton('✅' if filters['voice'] else '❌',
                    callback_data=f'settings#updatefilter-voice-{filters["voice"]}')
       ],[
       InlineKeyboardButton('🎭 Animations',
                    callback_data=f'settings_#updatefilter-animation-{filters["animation"]}'),
       InlineKeyboardButton('✅' if filters['animation'] else '❌',
                    callback_data=f'settings#updatefilter-animation-{filters["animation"]}')
       ],[
       InlineKeyboardButton('🃏 Stickers',
                    callback_data=f'settings_#updatefilter-sticker-{filters["sticker"]}'),
       InlineKeyboardButton('✅' if filters['sticker'] else '❌',
                    callback_data=f'settings#updatefilter-sticker-{filters["sticker"]}')
       ],[
       InlineKeyboardButton('▶️ Skip duplicate',
                    callback_data=f'settings_#updatefilter-duplicate-{filter["duplicate"]}'),
       InlineKeyboardButton('✅' if filter['duplicate'] else '❌',
                    callback_data=f'settings#updatefilter-duplicate-{filter["duplicate"]}')
       ],[
               InlineKeyboardButton('📝 Caption Settings →',

                     callback_data='settings#caption'),

        InlineKeyboardButton(

            '🤖' if filters.get('rm_caption', False) is True else (

            '🗑️' if filters.get('rm_caption', False) == 2 else '✅'),

                     callback_data='settings#caption')

        ],[
       InlineKeyboardButton('⫷ Bᴀᴄᴋ',
                    callback_data="settings#main")
       ]]
  return InlineKeyboardMarkup(buttons) 

async def next_filters_buttons(user_id):
  filter = await get_configs(user_id)
  filters = filter['filters']
  links_on = filters.get('links', False)
  buttons = [[
       InlineKeyboardButton('📊 Poll',
                    callback_data=f'settings_#updatefilter-poll-{filters["poll"]}'),
       InlineKeyboardButton('✅' if filters['poll'] else '❌',
                    callback_data=f'settings#updatefilter-poll-{filters["poll"]}')
       ],[
       InlineKeyboardButton('🔒 Secure message',
                    callback_data=f'settings_#updatefilter-protect-{filter["protect"]}'),
       InlineKeyboardButton('✅' if filter['protect'] else '❌',
                    callback_data=f'settings#updatefilter-protect-{filter["protect"]}')
       ],[
       InlineKeyboardButton('⬇️ Download Mode',
                    callback_data=f'settings_#updatefilter-download-{filter["download"]}'),
       InlineKeyboardButton('✅' if filter.get('download') else '❌',
                    callback_data=f'settings#updatefilter-download-{filter["download"]}')
       ],[
       InlineKeyboardButton('🔗 Links',
                    callback_data=f'settings_#updatefilter-links-{links_on}'),
       InlineKeyboardButton('✅' if links_on else '❌',
                    callback_data=f'settings#updatefilter-links-{links_on}')
       ],[
       InlineKeyboardButton('🛑 size limit',
                    callback_data='settings#file_size')
       ],[
       InlineKeyboardButton('⏱️ Set Duration',
                    callback_data='settings#set_duration')
       ],[
       InlineKeyboardButton('💾 Extension',
                    callback_data='settings#get_extension')
       ],[
       InlineKeyboardButton('♦️ keywords ♦️',
                    callback_data='settings#get_keyword')
       ],[
       InlineKeyboardButton('⫷ Bᴀᴄᴋ', 
                    callback_data="settings#main")
       ]]
  return InlineKeyboardMarkup(buttons) 
   

