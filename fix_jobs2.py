"""
Patch jobs.py fake batch forwarded stats
"""

with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
    text = f.read()

OLD = """                    try:
                        await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:"""

NEW = """                    try:
                        success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                               to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                        if success:
                            await _inc_forwarded(job_id, 1, forward_type='batch')
                    except FloodWait as fw:"""

text = text.replace(OLD, NEW)
print("Replaced chunk 1")

OLD2 = """                try:
                    await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                           to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                    await _inc_forwarded(job_id, 1, forward_type='batch')
                except FloodWait as fw:"""

NEW2 = """                try:
                    success = await _forward_message(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                           to_thread, to_chat_2, to_thread_2, replacements, remove_links)
                    if success:
                        await _inc_forwarded(job_id, 1, forward_type='batch')
                except FloodWait as fw:"""

text = text.replace(OLD2, NEW2)
print("Replaced chunk 2")

with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
    f.write(text)
