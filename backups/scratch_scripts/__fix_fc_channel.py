import re

def rewrite_fc_is_channel():
    for filename in ['plugins/jobs.py', 'plugins/taskjob.py']:
        with open(filename, 'r', encoding='utf-8') as f:
            code = f.read()
            
        # We find the block we injected earlier:
        #         # CRITICAL BUG FIX: determine if source is channel
        #         fc_is_channel = False
        #         try:
        #             if str(fc).startswith("-100"):
        #                 fc_is_channel = True
        #             else:
        #                 from pyrogram.enums import ChatType
        #                 c_obj = await client.get_chat(fc)
        #                 if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
        #                     fc_is_channel = True
        #         except Exception as e:
        #             logger.warning(f"Could not verify chat type for {fc}: {e}")
        
        # We'll use regex to replace it entirely
        pattern = re.compile(r"        # CRITICAL BUG FIX: determine if source is channel(.*?)logger\.warning\(.*?e\}\"\)", re.DOTALL)
        
        replacement = """        # CRITICAL BUG FIX: determine if source is channel safely
        fc_is_channel = False
        try:
            if str(fc).startswith("-100"):
                fc_is_channel = True
            else:
                try:
                    # Best: resolve_peer cleanly identifies users vs channels without joining
                    from pyrogram.raw.types import InputPeerChannel
                    peer = await client.resolve_peer(fc)
                    if isinstance(peer, InputPeerChannel):
                        fc_is_channel = True
                except Exception:
                    # Fallback to get_chat if resolve_peer fails (e.g. invite links)
                    from pyrogram.enums import ChatType
                    c_obj = await client.get_chat(fc)
                    if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                        fc_is_channel = True
        except Exception as e:
            # If all checks failed, and it's a string username, assume channel for bots
            # because bots can't use get_chat_history on public channels anyway.
            if getattr(client, 'me', None) and client.me.is_bot and isinstance(fc, str):
                fc_is_channel = True"""
                
        if pattern.search(code):
            code = pattern.sub(replacement, code)
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(code)
            print(f"Patched {filename}")
        else:
            print(f"Could not find exact block in {filename}")

if __name__ == "__main__":
    rewrite_fc_is_channel()
