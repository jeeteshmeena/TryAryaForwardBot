import re

def rewrite_test_py_channel():
    with open('plugins/test.py', 'r', encoding='utf-8') as f:
        code = f.read()

    find_str = """        chat = await self.get_chat(chat_id)
        is_channel_or_supergroup = chat.type in [
            pyrogram.enums.ChatType.CHANNEL,
            pyrogram.enums.ChatType.SUPERGROUP,
        ]"""

    repl_str = """        is_channel_or_supergroup = False
        if str(chat_id).startswith("-100"):
            is_channel_or_supergroup = True
        else:
            try:
                from pyrogram.raw.types import InputPeerChannel
                peer = await self.resolve_peer(chat_id)
                if isinstance(peer, InputPeerChannel):
                    is_channel_or_supergroup = True
            except Exception:
                try:
                    import pyrogram
                    chat = await self.get_chat(chat_id)
                    is_channel_or_supergroup = chat.type in [
                        pyrogram.enums.ChatType.CHANNEL,
                        pyrogram.enums.ChatType.SUPERGROUP,
                    ]
                except Exception:
                    # If all checks fail and it's a bot account with a string username, assume channel
                    if is_bot and isinstance(chat_id, str):
                        is_channel_or_supergroup = True"""

    code = code.replace(find_str, repl_str)
    with open('plugins/test.py', 'w', encoding='utf-8') as f:
        f.write(code)
    print("Patched plugins/test.py")

if __name__ == '__main__':
    rewrite_test_py_channel()
