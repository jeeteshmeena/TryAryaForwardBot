import pathlib

# Read the early March 20 version (65c1918 - "Fix forwarding gaps, formats...")
p = pathlib.Path('plugins/taskjob.py')
src = pathlib.Path('march20_first.py').read_text(encoding='utf-8')

# ─── Fix 1: resume_task_jobs signature — accept _bot ─────────────────────────
src = src.replace(
    'async def resume_task_jobs(user_id: int = None):\n'
    '    q = {"status": "running"}\n'
    '    if user_id: q["user_id"] = user_id\n'
    '    async for job in db.db[COLL].find(q):\n'
    '        jid, uid = job["job_id"], job["user_id"]\n'
    '        if jid not in _task_jobs:\n'
    '            _start_task(jid, uid)',
    'async def resume_task_jobs(user_id: int = None, _bot=None):\n'
    '    q = {"status": "running"}\n'
    '    if user_id: q["user_id"] = user_id\n'
    '    async for job in db.db[COLL].find(q):\n'
    '        jid, uid = job["job_id"], job["user_id"]\n'
    '        if jid not in _task_jobs:\n'
    '            _start_task(jid, uid, _bot=_bot)',
)

# ─── Fix 2: Long Polling freeze — don't duplicate main bot client ─────────────
OLD_GET = '        from plugins.jobs import _get_shared_client\n        client  = await _get_shared_client(acc)'
NEW_GET = (
    '        from plugins.jobs import _get_shared_client\n'
    '        from config import Config as _Cfg\n'
    '        _is_main = acc.get("is_bot") and acc.get("token") == _Cfg.BOT_TOKEN\n'
    '        if _is_main and getattr(_bot, "is_connected", False):\n'
    '            client = _bot\n'
    '        else:\n'
    '            client  = await _get_shared_client(acc)'
)
src = src.replace(OLD_GET, NEW_GET)

# ─── Fix 3: Don't release the main-bot client in finally ─────────────────────
OLD_REL = (
    '        if acc:\n'
    '            from plugins.jobs import _release_shared_client\n'
    '            await _release_shared_client(acc)'
)
NEW_REL = (
    '        if acc:\n'
    '            from plugins.jobs import _release_shared_client\n'
    '            from config import Config as _Cfg2\n'
    '            _im2 = acc.get("is_bot") and acc.get("token") == _Cfg2.BOT_TOKEN\n'
    '            if not (_im2 and getattr(_bot, "is_connected", False)):\n'
    '                await _release_shared_client(acc)'
)
src = src.replace(OLD_REL, NEW_REL)

p.write_text(src, encoding='utf-8')
print('Done — written', len(src), 'chars')
