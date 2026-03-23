import pathlib
p = pathlib.Path('plugins/old_taskjob.py')
t = p.read_text(encoding='utf-8')
t = t.replace('taskjobs', 'oldtaskjobs')
t = t.replace('taskjob', 'oldtaskjob')
t = t.replace('Task Job', 'Old Task Job')
t = t.replace('tj#', 'otj#')
t = t.replace('COLL = "oldtaskjobs"', 'COLL = "old_taskjobs_march20"')
# Let's fix the same duplicate connection bug here as well, so it ACTUALLY WORKS for them!
t = t.replace(
    'client  = await _get_shared_client(acc)',
    'from config import Config\n        if acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN and getattr(_bot, "is_connected", False):\n            client = _bot\n        else:\n            client  = await _get_shared_client(acc)'
)
t = t.replace(
    'await _release_shared_client(acc)',
    'from config import Config\n            if not (acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN and getattr(_bot, "is_connected", False)):\n                await _release_shared_client(acc)'
)
p.write_text(t, encoding='utf-8')
