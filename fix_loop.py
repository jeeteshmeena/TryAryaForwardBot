"""
Fix the raw_buttons loop to use _buckets_to_use (rebuilt with real batch_size)
instead of the original 'buckets' variable.
Also fix buttons_per_post to use the updated sj value.
"""

with open('plugins/share_jobs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: Change the for loop from 'buckets' to '_buckets_to_use'
OLD_LOOP = '''        raw_buttons = []
        # Use the rebuilt buckets (with real batch_size) if available, else fall back
        _buckets_to_use = buckets_final if 'buckets_final' in dir() else buckets
        for b_s, b_e, mids in buckets:'''

NEW_LOOP = '''        raw_buttons = []
        # Use the rebuilt buckets (with real batch_size) if available, else fall back
        _buckets_to_use = buckets_final if 'buckets_final' in locals() else buckets
        for b_s, b_e, mids in _buckets_to_use:'''

if OLD_LOOP in content:
    content = content.replace(OLD_LOOP, NEW_LOOP, 1)
    print("Fix 1 (loop var): SUCCESS")
else:
    # try simpler
    old2 = "        for b_s, b_e, mids in buckets:\n            if not mids:\n                continue\n            uuid_str = str(uuid.uuid4())"
    new2 = "        for b_s, b_e, mids in _buckets_to_use:\n            if not mids:\n                continue\n            uuid_str = str(uuid.uuid4())"
    if old2 in content:
        content = content.replace(old2, new2, 1)
        print("Fix 1 alt (loop var): SUCCESS")
    else:
        print("Fix 1: NOT FOUND - searching...")
        idx = content.find("for b_s, b_e, mids in bucket")
        print(f"Loop at index: {idx}")
        if idx >= 0:
            print(repr(content[idx-50:idx+120]))

with open('plugins/share_jobs.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Saved.")
