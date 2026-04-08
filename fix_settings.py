with open('plugins/settings.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_chunk = (
    "           InlineKeyboardButton('Dʟᴠʀ Bᴏᴛ Sᴇᴛᴜᴘ',\n"
    "                        callback_data='settings#sharebot'),\n"
    "           InlineKeyboardButton('Lᴇᴛ\\'s Eɴʜᴀɴᴄᴇ',\n"
    "                        callback_data='settings#enhancer')\n"
    "           ],[\n"
    "           InlineKeyboardButton('👑 Oᴡɴᴇʀ Pᴀɴᴇʟ',\n"
    "                        callback_data='settings#owners')\n"
    "           ],[\n"
    "           InlineKeyboardButton('❮ Bᴀᴄᴋ', callback_data='back')\n"
    "           ]]"
)

new_chunk = (
    "           InlineKeyboardButton('Dʟᴠʀ Bᴏᴛ Sᴇᴛᴜᴘ',\n"
    "                        callback_data='settings#sharebot'),\n"
    "           InlineKeyboardButton('Lᴇᴛ\\'s Eɴʜᴀɴᴄᴇ',\n"
    "                        callback_data='settings#enhancer')\n"
    "           ],[\n"
    "           InlineKeyboardButton('🤖 Sᴀʀᴠᴀᴍ Aɪ',\n"
    "                        callback_data='sarvam#main'),\n"
    "           InlineKeyboardButton('👑 Oᴡɴᴇʀ Pᴀɴᴇʟ',\n"
    "                        callback_data='settings#owners')\n"
    "           ],[\n"
    "           InlineKeyboardButton('❮ Bᴀᴄᴋ', callback_data='back')\n"
    "           ]]"
)

if old_chunk in content:
    content = content.replace(old_chunk, new_chunk, 1)
    with open('plugins/settings.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print('SUCCESS - Sarvam AI button added to main settings')
else:
    print('NOT FOUND - dumping actual chars around the location')
    idx = content.find("settings#sharebot")
    occurrences = []
    start = 0
    while True:
        idx2 = content.find("settings#sharebot", start)
        if idx2 == -1:
            break
        occurrences.append(idx2)
        start = idx2 + 1
    print(f"Found {len(occurrences)} occurrences of 'settings#sharebot' at: {occurrences}")
    for oc in occurrences:
        print(f"\n--- occurrence at {oc} ---")
        print(repr(content[oc-100:oc+300]))
