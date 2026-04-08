import re

with open('plugins/share_jobs.py', 'r', encoding='utf-8') as f:
    text = f.read()

# I will find lines up to `def _start_share_job(client, message):`
# ... wait...

print("ready")
