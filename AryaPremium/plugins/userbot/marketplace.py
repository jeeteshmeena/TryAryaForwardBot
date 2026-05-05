from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from database import db
from config import Config
from utils import to_smallcap, native_ask

@Client.on_message(filters.command("start") & filters.private)
async def user_start(bot: Client, message):
    user_id = message.from_user.id
    
    # --- ORDER CHECKOUT LOGIC ---
    if len(message.command) > 1:
        arg = message.command[1]
        if arg.startswith("order_"):
            order_id = arg.split("order_")[1]
            order = await db.db.orders.find_one({"order_id": order_id})
            
            if not order:
                return await message.reply_text("❌ Invalid Order! Please try again from the Mini App.")
            if order.get("status") == "completed":
                return await message.reply_text("✅ This order is already paid and delivered.")
                
            total_amount = order.get("total_amount", 0)
            stories_count = len(order.get("story_ids", []))
            
            await message.reply_text(
                f"🛒 **Your Order: {order_id}**\n"
                f"📚 Total Stories: {stories_count}\n"
                f"💰 Total Amount: ₹{total_amount}\n\n"
                f"Please contact admin to complete your payment and instantly get your files!"
            )
            return
    # ----------------------------

    text = (
        f"**Welcome to the Arya Premium Store!** 🛍️\n\n"
        f"Here you can purchase exclusive premium stories directly.\n"
        f"Select an option below to continue."
    )
    
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Marketplace", callback_data="open_marketplace")],
        [InlineKeyboardButton("🛍️ My Buys", callback_data="my_buys"), InlineKeyboardButton("👤 Profile", callback_data="profile")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"), InlineKeyboardButton("ℹ️ Help", callback_data="help")]
    ])
    
    await message.reply_text(text, reply_markup=btns)

@Client.on_callback_query(filters.regex(r'^open_marketplace$'))
async def marketplace_cb(bot: Client, query):
    stories = await db.get_all_stories()
    if not stories:
        return await query.answer("Marketplace is empty right now. Check back later!", show_alert=True)
        
    btns = []
    for s in stories:
        title = s.get("name_en", "Unknown Story")
        price = s.get("price", 99)
        btns.append([InlineKeyboardButton(f"{title} - ₹{price}", callback_data=f"view_st_{s['story_id']}")])
        
    btns.append([InlineKeyboardButton("❮ Back", callback_data="nav_start")])
    
    await query.message.edit_text("**🛒 Premium Marketplace**\n\nSelect a story to view details:", reply_markup=InlineKeyboardMarkup(btns))

@Client.on_callback_query(filters.regex(r'^view_st_'))
async def view_story_cb(bot: Client, query):
    story_id = query.data.split("_")[2]
    story = await db.get_story(story_id)
    if not story:
        return await query.answer("Story not found.", show_alert=True)
        
    text = (
        f"**{story['name_en']}** | {story.get('name_hi', '')}\n\n"
        f"**Platform:** {story.get('platform', 'N/A')}\n"
        f"**Episodes:** {story.get('ep_count', '?')}\n\n"
        f"{story.get('desc', 'No description.')}\n\n"
        f"**Price:** ₹{story.get('price', 99)}"
    )
    
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Buy via UPI (Manual)", callback_data=f"buy_upi_{story_id}")],
        [InlineKeyboardButton("❮ Back to Marketplace", callback_data="open_marketplace")]
    ])
    
    if story.get("image_id"):
        await query.message.delete()
        await bot.send_photo(query.message.chat.id, story["image_id"], caption=text, reply_markup=btns)
    else:
        await query.message.edit_text(text, reply_markup=btns)

@Client.on_callback_query(filters.regex(r'^buy_upi_'))
async def buy_upi_cb(bot: Client, query):
    story_id = query.data.split("_")[2]
    story = await db.get_story(story_id)
    user_id = query.from_user.id
    
    # Send strict T&C
    tnc = (
        f"⚠️ **TERMS & CONDITIONS**\n\n"
        f"1. Some episodes may be missing or low quality.\n"
        f"2. Episodes may not be in exact order.\n"
        f"3. Strictly NO Refunds after payment.\n"
        f"4. Submitting fake payment screenshots will result in a permanent ecosystem ban.\n\n"
        f"Do you accept these terms to proceed to payment?"
    )
    
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I ACCEPT", callback_data=f"confirm_tnc_{story_id}")],
        [InlineKeyboardButton("❌ CANCEL", callback_data="open_marketplace")]
    ])
    if query.message.photo:
        await query.message.delete()
        await bot.send_message(query.message.chat.id, tnc, reply_markup=btns)
    else:
        await query.message.edit_text(tnc, reply_markup=btns)

@Client.on_callback_query(filters.regex(r'^confirm_tnc_'))
async def confirm_tnc_cb(bot: Client, query):
    story_id = query.data.split("_")[2]
    story = await db.get_story(story_id)
    user_id = query.from_user.id
    
    await query.message.delete()
    
    amt = story.get("price", 99)
    upi_id = await db.get_config("upi_id", "Not configured. Ask admin.")
    
    # Prompt for screenshot using ask
    resp_msg = await native_ask(
        bot,
        query.message.chat.id, 
        f"**Manual UPI Payment — ₹{amt}**\n\n"
        f"Please send the exact amount to: `{upi_id}`\n\n"
        f"Once paid, **Send your payment screenshot here.**\n"
        f"*(Send 'cancel' to abort)*"
    )
    
    if getattr(resp_msg, 'text', '').lower() == 'cancel':
        return await bot.send_message(query.message.chat.id, "Payment cancelled.")
        
    if not resp_msg.photo:
        return await bot.send_message(query.message.chat.id, "❌ Valid screenshot not provided. Order cancelled.")
        
    await bot.send_message(query.message.chat.id, "⏳ **Payment received. Verification in progress (Est. 5 minutes)...**")
    
    db_channel = await db.get_config("db_channel")
    target = db_channel if db_channel else (Config.OWNER_IDS[0] if Config.OWNER_IDS else user_id)
    
    approval_btns = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"upi_approve_{user_id}_{story_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"upi_reject_{user_id}_{story_id}")
        ]
    ])
    
    try:
        await bot.send_photo(
            chat_id=target, 
            photo=resp_msg.photo.file_id, 
            caption=f"💸 **NEW UPI PAYMENT**\n\n**Buyer ID:** `{user_id}`\n**Story:** {story.get('name_en')}\n**Amount:** ₹{amt}",
            reply_markup=approval_btns
        )
    except Exception as e:
        await bot.send_message(query.message.chat.id, "Error notifying admins. Please contact support.")
