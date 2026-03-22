import re

def fix_jobs_latest_id():
    with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
        code = f.read()

    find_str = """    # Fallback: binary search (works for bots on public channels by numeric ID)
    try:
        lo, hi = 1, 9_999_999"""
        
    repl_str = """    # Fallback: binary search (works ONLY for channels by numeric ID)
    # NEVER do this for private entities because get_messages queries the user's global inbox!
    is_ch = False
    try:
        c_obj = await client.get_chat(chat_id)
        from pyrogram.enums import ChatType
        if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP): is_ch = True
    except:
        if str(chat_id).startswith("-100"): is_ch = True
        
    if not is_ch:
        return 0

    try:
        lo, hi = 1, 9_999_999"""

    if find_str in code:
        code = code.replace(find_str, repl_str)
        with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
            f.write(code)
        print("Patched _get_latest_id in jobs.py successfully.")
    else:
        print("ERROR: Could not find _get_latest_id fallback block in jobs.py")

if __name__ == '__main__':
    fix_jobs_latest_id()
