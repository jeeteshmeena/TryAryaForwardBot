import re

def rewrite_public_ui():
    with open('plugins/public.py', 'r', encoding='utf-8') as f:
        code = f.read()

    # We need to extract the chat_type while fetching title
    find_chat_type = """    if chat_id != "me":
        try:
            title = (await bot.get_chat(chat_id)).title
      #  except ChannelInvalid:
            #return await fromid.reply("**Given source chat is copyrighted channel/group. you can't forward messages from there**")
        except (PrivateChat, ChannelPrivate, ChannelInvalid):
            title = "private" if fromid.text else fromid.forward_from_chat.title
        except (UsernameInvalid, UsernameNotModified):
            return await message.reply('Invalid Link specified.')
        except Exception as e:
            # Main bot might not have access to the user/bot chat, but the userbot might.
            # We bypass the error so the userbot can try during the actual forwarding.
            title = str(chat_id)"""

    repl_chat_type = """    source_type_display = "Unknown"
    if chat_id == "me":
        source_type_display = "Saved Messages"
    else:
        try:
            c = await bot.get_chat(chat_id)
            title = c.title or c.first_name or "Unknown"
            from pyrogram.enums import ChatType
            if c.type == ChatType.CHANNEL: source_type_display = "Channel"
            elif c.type in (ChatType.SUPERGROUP, ChatType.GROUP): source_type_display = "Group"
            elif c.type == ChatType.BOT: source_type_display = "Bot"
            elif c.type == ChatType.PRIVATE: source_type_display = "Private"
        except (PrivateChat, ChannelPrivate, ChannelInvalid):
            title = "private" if fromid.text else getattr(fromid.forward_from_chat, 'title', 'private')
            source_type_display = "Private Channel/Group"
        except (UsernameInvalid, UsernameNotModified):
            return await message.reply('Invalid Link specified.')
        except Exception as e:
            title = str(chat_id)
            source_type_display = "Private/Uncached" """

    code = code.replace(find_chat_type, repl_chat_type)

    # Now rewrite the check_text UI
    find_check_text = """    if acc_is_bot:
        hints = (
            f"<b>│</b> ⚠️ <b>{acc_name}</b> (@{acc_username}) must be <b>admin</b> in TARGET\\n"
            f"<b>│</b> ⚠️ If SOURCE is private, bot must be <b>admin</b> there too\\n"
            f"<b>└──────────────────────────────────</b>\\n"
        )
    else:
        hints = (
            f"<b>│</b> ⚠️ Userbot <b>{acc_name}</b> must be a <b>member</b> of SOURCE\\n"
            f"<b>│</b> ⚠️ Userbot must be <b>admin</b> in TARGET channel\\n"
            f"<b>└──────────────────────────────────</b>\\n"
        )

    # Calculate if this needs the SLOW MODE warning
    is_private_source = title == "private" or title == "Saved Messages" or str(title).lstrip('-').isdigit()
    needs_download = configs.get('download') or (is_private_source and not configs.get('forward_tag'))
    
    warning_box = ""
    if not acc_is_bot or needs_download or reverse_order:
        warning_box = (
            f"<b>┌─────❮ ⚠️ 𝐒𝐋𝐎𝐖 𝐌𝐎𝐃𝐄 𝐖𝐀𝐑𝐍𝐈𝐍𝐆 ❯─────</b>\\n"
            f"<b>│</b> ⊸ Forwarding will be slow (Telegram restrictions)\\n"
            f"<b>│</b> ⊸ Bot relies on parsing or downloading/re-uploading\\n"
            f"<b>│</b> ⊸ High data usage & slower speeds expected. Be patient.\\n"
            f"<b>└──────────────────────────────────</b>\\n\\n"
        )

    check_text = (
        f"<b>╭──────❰ ⚠️ 𝐃𝐎𝐔𝐁𝐋𝐄 𝐂𝐇𝐄𝐂𝐊 ❱──────╮</b>\\n"
        f"<b>┃</b>\\n"
        f"<b>┣⊸ ◈ 𝐀𝐂𝐂𝐎𝐔𝐍𝐓 ({acc_type_label}):</b> {acc_name}\\n"
        f"<b>┣⊸ ◈ 𝐒𝐎𝐔𝐑𝐂𝐄  :</b> <code>{title}</code>\\n"
        f"<b>┣⊸ ◈ 𝐓𝐀𝐑𝐆𝐄𝐓  :</b> <code>{to_title}</code>\\n"
        f"<b>┣⊸ ◈ 𝐒𝐊𝐈𝐏    :</b> <code>{skip_lbl}</code>\\n"
        f"<b>┃</b>\\n"
        f"<b>┌──────❮ ⚙️ 𝐒𝐞𝐭𝐭𝐢𝐧𝐠𝐬 ❯────────────</b>\\n"
        f"<b>│</b> ⊸ <b>Mode:</b> {mode_lbl}\\n"
        f"<b>│</b> ⊸ <b>Order:</b> {order_lbl}\\n"
        f"<b>│</b> ⊸ <b>Smart Order:</b> {smart_lbl}\\n"
        f"<b>│</b> ⊸ <b>Status:</b> {fwd_mode}\\n"
        f"<b>│</b> ⊸ <b>Caption:</b> {caption_m}\\n"
        f"<b>│</b> ⊸ <b>Transfer:</b> {dl_mode}\\n"
        f"<b>│</b> ⊸ <b>Filters:</b> {filter_str}\\n"
        f"<b>└──────────────────────────────────</b>\\n\\n"
        f"{warning_box}"
        f"<b>┌──────❮ 💡 𝐑𝐞𝐦𝐢𝐧𝐝𝐞𝐫𝐬 ❯───────────</b>\\n"
        f"{hints}\\n"
        f"<b>╰─── 𝐈𝐟 𝐯𝐞𝐫𝐢𝐟𝐢𝐞𝐝, 𝐜𝐥𝐢𝐜𝐤 𝐘𝐞𝐬 𝐁𝐞𝐥𝐨𝐰 ───╯</b>"
    )"""

    repl_check_text = """    if acc_is_bot:
        hints = (
            f"> <b>Guide:</b>\\n"
            f"> • Bot <b>{acc_name}</b> must be admin in Target.\\n"
            f"> • Bot must be admin in Source if it is a private channel."
        )
    else:
        hints = (
            f"> <b>Guide:</b>\\n"
            f"> • Userbot <b>{acc_name}</b> must be a member of Source.\\n"
            f"> • Userbot must be admin in Target channel."
        )

    # Calculate if this needs the SLOW MODE warning
    is_private_source = title == "private" or title == "Saved Messages" or str(title).lstrip('-').isdigit()
    needs_download = configs.get('download') or (is_private_source and not configs.get('forward_tag'))
    
    warning_box = ""
    if not acc_is_bot or needs_download or reverse_order:
        warning_box = (
            f"\\n> <b>Warning (Slow Mode):</b>\\n"
            f"> Telegram restrictions may slow down forwarding speeds.\\n"
            f"> High data usage & slower speeds expected. Be patient."
        )

    check_text = (
        f"<b>Confirmation (Double Check)</b>\\n\\n"
        f"<b>Task Information:</b>\\n"
        f"• <b>Account:</b> {acc_name} ({acc_type_label})\\n"
        f"• <b>Source Type:</b> {source_type_display}\\n"
        f"• <b>Source:</b> <code>{title}</code>\\n"
        f"• <b>Target:</b> <code>{to_title}</code>\\n"
        f"• <b>Skip:</b> <code>{skip_lbl}</code>\\n\\n"
        f"<b>Running Settings:</b>\\n"
        f"• <b>Mode:</b> {mode_lbl}\\n"
        f"• <b>Order:</b> {order_lbl}\\n"
        f"• <b>Smart Order:</b> {smart_lbl}\\n"
        f"• <b>Status:</b> {fwd_mode}\\n"
        f"• <b>Caption:</b> {caption_m}\\n"
        f"• <b>Transfer:</b> {dl_mode}\\n"
        f"• <b>Filters:</b> {filter_str}\\n\\n"
        f"{hints}{warning_box}\\n\\n"
        f"<b>If everything is correct, click Yes below to start.</b>"
    )"""

    code = code.replace(find_check_text, repl_check_text)

    # Also clean regix.py status box:
    with open('plugins/regix.py', 'r', encoding='utf-8') as f:
        regix_code = f.read()

    # The user asked:
    # "The forward status should: have proper spacing, clean font, structured layout"

    # We can rewrite the end summary in regix.py
    find_summary = r'''        # 🔔 Detailed Completion Notification
        summary = \(
            f"<b>✅ Batch Forwarding Completed!</b>\\n\\n"
            f"<b>📊 Summary:</b>\\n"
            f" ┣ <b>Fetched:</b> <code>{sts.get\('fetched'\)}</code>\\n"
            f" ┣ <b>Forwarded:</b> <code>{sts.get\('total_files'\)}</code>\\n"
            f" ┣ <b>Duplicates skipped:</b> <code>{sts.get\('duplicate'\)}</code>\\n"
            f" ┣ <b>Filtered out:</b> <code>{sts.get\('filtered'\)}</code>\\n"
            f" ┗ <b>Deleted sources:</b> <code>{sts.get\('deleted'\)}</code>\\n"
        \)'''

    repl_summary = '''        # 🔔 Detailed Completion Notification
        summary = (
            f"<b>✅ Batch Forwarding Completed!</b>\\n\\n"
            f"<b>Final Summary:</b>\\n"
            f"  • <b>Fetched:</b> <code>{sts.get('fetched')}</code>\\n"
            f"  • <b>Forwarded:</b> <code>{sts.get('total_files')}</code>\\n"
            f"  • <b>Duplicates skipped:</b> <code>{sts.get('duplicate')}</code>\\n"
            f"  • <b>Filtered out:</b> <code>{sts.get('filtered')}</code>\\n"
            f"  • <b>Deleted sources:</b> <code>{sts.get('deleted')}</code>\\n"
        )'''

    regix_code = re.sub(find_summary, repl_summary, regix_code, flags=re.DOTALL)

    # Re-write the Progress box in `edit` logic:
    # wait, regix.py uses _S["TEXT"] from lang.py!
    # I should clean lang.py TEXT mapping!
    with open('plugins/lang.py', 'r', encoding='utf-8') as f:
        lang_code = f.read()

    find_lang_text = r'_S\["TEXT"\] = \{((?:.|\n)*?)\}'
    repl_lang_text = r'''_S["TEXT"] = {
    "en": (
        "<b>Forwarding Progress</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hi": (
        "<b>फॉरवर्डिंग प्रोग्रेस</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hinglish": (
        "<b>Forwarding Progress</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
}'''
    lang_code = re.sub(find_lang_text, repl_lang_text, lang_code, flags=re.DOTALL)

    find_unq_text = r'_S\["DUPLICATE_TEXT"\] = \{((?:.|\n)*?)\}'
    repl_unq_text = r'''_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "Status: {}"
    ),
    "hi": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "Status: {}"
    ),
    "hinglish": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "Status: {}"
    ),
}'''
    lang_code = re.sub(find_unq_text, repl_unq_text, lang_code, flags=re.DOTALL)
    
    # Save files
    with open('plugins/public.py', 'w', encoding='utf-8') as f: f.write(code)
    with open('plugins/regix.py', 'w', encoding='utf-8') as f: f.write(regix_code)
    with open('plugins/lang.py', 'w', encoding='utf-8') as f: f.write(lang_code)

if __name__ == '__main__':
    rewrite_public_ui()
