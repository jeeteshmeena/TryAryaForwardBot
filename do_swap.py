"""
This script:
1. Reads old_taskjob.py from root, renames commands (/oldtaskjobs -> /taskjobs, /newoldtaskjob -> /newtaskjob)
2. Writes the correct file to plugins/taskjob.py (replacing the current broken one)
3. Deletes the misplaced root-level old_taskjob.py
4. Patches main.py to import correctly
5. Patches commands.py (_pause_events import stays from .taskjob — which now IS the old system)
6. Patches translation.py to reference /taskjobs correctly
"""
import pathlib
import re
import os

root = pathlib.Path('.')

# ── Step 1: Load old_taskjob.py from root ────────────────────────────────────
src = root / 'old_taskjob.py'
t = src.read_text(encoding='utf-8')

# ── Step 2: Fix COLL — keep same DB as March 20 used (taskjobs collection) ──
# COLL is already "taskjobs" — good, keep it, old jobs are in same DB

# ── Step 3: Rename prefixed commands back to primary names ───────────────────
# The file was patched to use "oldtaskjobs/oldtaskjob/newoldtaskjob"
# But since we're now REPLACING the main taskjob, commands should be:
#   /taskjobs, /taskjob  (list)
#   /newtaskjob          (create)

# Broad replacements: rename plugin-internal names back to canonical ones
t = t.replace('oldtaskjobs', 'taskjobs')
t = t.replace('oldtaskjob', 'taskjob')
t = t.replace('otj#', 'tj#')
t = t.replace('Old Task Job', 'Task Job')
t = t.replace('old_task_jobs_march20', 'taskjobs')  # COLL

# Fix /newoldtaskjob -> /newtaskjob if present
t = t.replace('newoldtaskjob', 'newtaskjob')

# Apply the Long Polling / bot duplication fix
BOT_FIX_OLD = 'client  = await _get_shared_client(acc)'
BOT_FIX_NEW = '''from config import Config
        is_main = acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN
        if is_main and getattr(_bot, "is_connected", False):
            client = _bot
        else:
            client  = await _get_shared_client(acc)'''

RELEASE_FIX_OLD = 'await _release_shared_client(acc)'
RELEASE_FIX_NEW = '''from config import Config
            is_main = acc.get("is_bot") and acc.get("token") == Config.BOT_TOKEN
            if not (is_main and getattr(_bot, "is_connected", False)):
                await _release_shared_client(acc)'''

if BOT_FIX_OLD in t:
    t = t.replace(BOT_FIX_OLD, BOT_FIX_NEW)
    print("Applied Long Polling main-bot fix")
else:
    print("WARN: Long Polling fix target not found — manual check needed")

if RELEASE_FIX_OLD in t:
    t = t.replace(RELEASE_FIX_OLD, RELEASE_FIX_NEW)
else:
    print("WARN: Release client fix target not found")

# Fix resume function name to the canonical one
t = t.replace('async def resume_old_task_jobs(', 'async def resume_task_jobs(')
t = t.replace('async def resume_task_jobs(user_id: int = None):', 'async def resume_task_jobs(user_id: int = None, _bot=None):')

# ── Step 4: Write to plugins/taskjob.py ──────────────────────────────────────
dest = root / 'plugins' / 'taskjob.py'
dest.write_text(t, encoding='utf-8')
print(f"Written plugins/taskjob.py ({len(t)} bytes)")

# ── Step 5: Remove the misplaced root-level old_taskjob.py ───────────────────
src.unlink()
print("Removed old_taskjob.py from root")

# ── Step 6: Patch main.py ─────────────────────────────────────────────────────
main_p = root / 'main.py'
mt = main_p.read_text(encoding='utf-8')

# Remove the old_taskjob import lines added earlier
mt = mt.replace(
    '    from plugins.old_taskjob import resume_old_task_jobs\n',
    ''
)
mt = mt.replace(
    '    asyncio.create_task(resume_old_task_jobs(_bot=bot))\n',
    ''
)
# Also clean up any double resume_task_jobs lines that may have duplicated
# Ensure exactly one of the right call exists
if 'resume_task_jobs(_bot=bot)' not in mt:
    mt = mt.replace(
        '    asyncio.create_task(resume_task_jobs())\n',
        '    asyncio.create_task(resume_task_jobs(_bot=bot))\n'
    )

main_p.write_text(mt, encoding='utf-8')
print("Patched main.py")

# ── Step 7: Patch commands.py to keep using taskjob._pause_events ────────────
cmd_p = root / 'plugins' / 'commands.py'
ct = cmd_p.read_text(encoding='utf-8')
# commands.py already imports from .taskjob — this is now correct since
# plugins/taskjob.py IS the old March-20 system. Nothing to change.
print("commands.py — already references .taskjob, no change needed")

print("\nAll done!")
