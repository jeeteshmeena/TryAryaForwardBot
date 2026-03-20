import re

def fix_test_py():
    with open('plugins/test.py', 'r', encoding='utf-8') as f:
        code = f.read()

    find_str = """        # ── USERBOT & PRIVATE PATH ────────────────────────────────────────────────
        # Userbots can freely use get_chat_history for any chat type.
        # Bots can ALSO use get_chat_history for private chats (DMs / Saved Messages).
        is_private_src = (chat_id == "me") or (isinstance(chat_id, int) and chat_id > 0)
        
        if not is_bot or is_private_src:
            messages = []"""

    repl_str = """        # ── USERBOT & PRIVATE PATH ────────────────────────────────────────────────
        # If it is NOT a channel/supergroup (e.g. DM, Bot, Basic Group), we MUST use 
        # get_chat_history. get_messages by ID list fetches from the global inbox in Pyrogram!
        if not is_channel_or_supergroup:
            messages = []"""

    if find_str in code:
        code = code.replace(find_str, repl_str)
        with open('plugins/test.py', 'w', encoding='utf-8') as f:
            f.write(code)
        print("Patched plugins/test.py successfully.")
    else:
        print("ERROR: Target string not found in plugins/test.py")

if __name__ == '__main__':
    fix_test_py()
