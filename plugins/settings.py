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
        reply_markup=main_buttons()
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
       reply_markup=main_buttons())
          
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
     buttons = []
     data = await get_configs(user_id)
     caption = data['caption']
     if caption is None:
        buttons.append([InlineKeyboardButton('✚ Add Caption ✚', 
                      callback_data="settings#addcaption")])
     else:
        buttons.append([InlineKeyboardButton('See Caption', 
                      callback_data="settings#seecaption")])
        buttons[-1].append(InlineKeyboardButton('🗑️ Delete Caption', 
                      callback_data="settings#deletecaption"))
     buttons.append([InlineKeyboardButton('↩ Back', 
                      callback_data="settings#main")])
     await query.message.edit_text(
        "<b><u>CUSTOM CAPTION</b></u>\n\n<b>You can set a custom caption to videos and documents. Normaly use its default caption</b>\n\n<b><u>AVAILABLE FILLINGS:</b></u>\n- <code>{filename}</code> : Filename\n- <code>{size}</code> : File size\n- <code>{caption}</code> : default caption",
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
     await query.message.edit_text(
        "<b>successfully updated</b>",
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
     if value=="True":
        await update_configs(user_id, key, False)
     else:
        await update_configs(user_id, key, True)
     if key in ['poll', 'protect', 'download', 'rm_caption']:
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
      
def main_buttons():
  buttons = [[
       InlineKeyboardButton('🤖 Aᴄᴄᴏᴜɴᴛs',
                    callback_data=f'settings#accounts'),
       InlineKeyboardButton('🏷 Cʜᴀɴɴᴇʟs',
                    callback_data=f'settings#channels')
       ],[
       InlineKeyboardButton('🖋️ Cᴀᴘᴛɪᴏɴ',
                    callback_data=f'settings#caption'),
       InlineKeyboardButton('🗃 MᴏɴɢᴏDB',
                    callback_data=f'settings#database')
       ],[
       InlineKeyboardButton('🕵‍♀ Fɪʟᴛᴇʀs 🕵‍♀',
                    callback_data=f'settings#filters'),
       InlineKeyboardButton('⏹ Bᴜᴛᴛᴏɴ',
                    callback_data=f'settings#button')
       ],[
       InlineKeyboardButton('Exᴛʀᴀ Sᴇᴛᴛɪɴɢs 🧪',
                    callback_data='settings#nextfilters'),
       InlineKeyboardButton('🗑 Cʟᴇᴀɴ MSG',
                    callback_data='settings#cleanmsg')
       ],[
       InlineKeyboardButton('🌐 Language / भाषा',
                    callback_data='settings#lang')
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
       InlineKeyboardButton('✂️ Caption Remove',
                    callback_data=f'settings_#updatefilter-rm_caption-{filters.get("rm_caption", False)}'),
       InlineKeyboardButton('✅' if filters.get('rm_caption', False) else '❌',
                    callback_data=f'settings#updatefilter-rm_caption-{filters.get("rm_caption", False)}')
       ],[
       InlineKeyboardButton('⫷ back',
                    callback_data="settings#main")
       ]]
  return InlineKeyboardMarkup(buttons) 

async def next_filters_buttons(user_id):
  filter = await get_configs(user_id)
  filters = filter['filters']
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
       InlineKeyboardButton('🛑 size limit',
                    callback_data='settings#file_size')
       ],[
       InlineKeyboardButton('⏱️ Set Duration',
                    callback_data='settings#set_duration')
       ],[
       InlineKeyboardButton('💾 Extension',
                    callback_data='settings#get_extension')
              ],[
       InlineKeyboardButton('🔗 Links',
                     callback_data=f'settings_#updatefilter-links-{filters.get("links", True)}'),
       InlineKeyboardButton('✅' if filters.get('links', True) else '❌',
                     callback_data=f'settings#updatefilter-links-{filters.get("links", True)}')
],[
       InlineKeyboardButton('♦️ keywords ♦️',
                    callback_data='settings#get_keyword')
       ],[
       InlineKeyboardButton('⫷ back', 
                    callback_data="settings#main")
       ]]
  return InlineKeyboardMarkup(buttons) 
   
