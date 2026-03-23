import pathlib, re

p = pathlib.Path('plugins/taskjob.py')
t = p.read_text(encoding='utf-8')

# ─────────────────────────────────────────────────────────────────────────────
# 1. Fix resume_task_jobs to accept _bot so main.py can pass the native client
# ─────────────────────────────────────────────────────────────────────────────
t = t.replace(
    'async def resume_task_jobs(user_id: int = None):',
    'async def resume_task_jobs(user_id: int = None, _bot=None):'
)

# Resume: pass _bot down to the start helper (whatever it is called in this file)
for old_start in ['_start_task(jid, uid)', '_start_job_task(jid, uid)', '_start_task_job(jid, uid)']:
    if old_start in t:
        t = t.replace(old_start, old_start.rstrip(')') + ', _bot=_bot)')
        break

# ─────────────────────────────────────────────────────────────────────────────
# 2. Long-Polling fix: don't create a duplicate client when main bot token used
#    Target: the line that calls _get_shared_client(acc) inside _run_task_job
# ─────────────────────────────────────────────────────────────────────────────
old_get = 'client  = await _get_shared_client(acc)'
new_get = (
    'from config import Config as _Cfg\n'
    '        _is_main = acc.get("is_bot") and acc.get("token") == _Cfg.BOT_TOKEN\n'
    '        if _is_main and getattr(_bot, "is_connected", False):\n'
    '            client = _bot\n'
    '        else:\n'
    '            client  = await _get_shared_client(acc)'
)
if old_get in t:
    t = t.replace(old_get, new_get)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Long-Polling fix: don't release the main-bot client in finally
# ─────────────────────────────────────────────────────────────────────────────
old_rel = 'await _release_shared_client(acc)'
new_rel = (
    'from config import Config as _Cfg\n'
    '            _is_main = acc.get("is_bot") and acc.get("token") == _Cfg.BOT_TOKEN\n'
    '            if not (_is_main and getattr(_bot, "is_connected", False)):\n'
    '                await _release_shared_client(acc)'
)
if old_rel in t:
    t = t.replace(old_rel, new_rel)

p.write_text(t, encoding='utf-8')
print('Patched successfully')
