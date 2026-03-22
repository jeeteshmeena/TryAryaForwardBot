import re

# ── Patch jobs.py ────────────────────────────────────────────────────────────
with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
    jobs = f.read()

# 1. Add status notifier helper after the _inc_forwarded function
old_inc = """async def _inc_forwarded(job_id: str, n: int = 1):
    await db.db.jobs.update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})"""

new_inc = """async def _inc_forwarded(job_id: str, n: int = 1):
    await db.db.jobs.update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})


# ── Auto-status notifier ────────────────────────────────────────────────────
# Holds (bot_instance, user_id) -> message_id of live status message
_status_msgs: dict = {}

async def _notify_status(bot, job: dict, phase: str = ""):
    \"\"\"Send/edit a live status message to the user so they see real-time progress.\"\"\"
    if not bot:
        return
    uid       = job["user_id"]
    job_id    = job["job_id"]
    st        = _st(job.get("status", "running"))
    fwd       = job.get("forwarded", 0)
    src       = job.get("from_title", "?")
    dst       = job.get("to_title", "?")
    cname     = job.get("custom_name", "")
    name_part = f" <b>{cname}</b>" if cname else ""
    batch_part = ""
    if job.get("batch_mode"):
        if job.get("batch_done"):
            batch_part = "\\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ✅ ᴄᴏᴍᴘʟᴇᴛᴇ"
        else:
            cur = job.get("batch_cursor") or job.get("batch_start_id") or "?"
            end = job.get("batch_end_id") or "…"
            batch_part = f"\\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : 📦 <code>{cur}</code> / <code>{end}</code>"
    phase_part = f"\\n┣⊸ ◈ 𝐏𝐡𝐚𝐬𝐞   : <code>{phase}</code>" if phase else ""
    err_part   = f"\\n┣⊸ ⚠️ <code>{job['error']}</code>" if job.get("error") else ""
    text = (
        f"<b>╭──────❰ 📋 ʟɪᴠᴇ ᴊᴏʙ ᴘʀᴏɢʀᴇss ❱──────╮\\n"
        f"┃\\n"
        f"┣⊸ ◈ 𝐈𝐃      : <code>{job_id[-6:]}</code>{name_part}\\n"
        f"┣⊸ ◈ 𝐒𝐭𝐚𝐭𝐮𝐬  : {st} {job.get('status','running')}\\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {src}\\n"
        f"┣⊸ ◈ 𝐃𝐞𝐬𝐭    : {dst}\\n"
        f"┣⊸ ◈ 𝐅𝐰𝐝     : <code>{fwd}</code>"
        f"{batch_part}{phase_part}{err_part}\\n"
        f"┃\\n"
        f"╰────────────────────────────────╯</b>"
    )
    key = (uid, job_id)
    try:
        existing_msg_id = _status_msgs.get(key)
        if existing_msg_id:
            try:
                await bot.edit_message_text(uid, existing_msg_id, text)
                return
            except Exception:
                pass  # message deleted or too old — send a new one
        sent = await bot.send_message(uid, text)
        _status_msgs[key] = sent.id
    except Exception:
        pass"""

if old_inc in jobs:
    jobs = jobs.replace(old_inc, new_inc)
    print("Added _notify_status helper")
else:
    print("WARN: could not find _inc_forwarded")

# 2. Add bot parameter to _run_job and wire up auto-notify
# The _run_job function needs the main bot instance to send messages
# We pass it in from _start_job_task
old_run_job_sig = "async def _run_job(job_id: str, user_id: int):"
new_run_job_sig = "async def _run_job(job_id: str, user_id: int, _bot=None):"

if old_run_job_sig in jobs:
    jobs = jobs.replace(old_run_job_sig, new_run_job_sig)
    print("Updated _run_job signature")
else:
    print("WARN: _run_job signature not found")

old_last_hb = "        last_hb = 0  # last heartbeat timestamp"
new_last_hb = "        last_hb = 0  # last heartbeat timestamp\n        last_notify = 0  # last status notification timestamp"

if old_last_hb in jobs:
    jobs = jobs.replace(old_last_hb, new_last_hb)
    print("Added last_notify variable")
else:
    print("WARN: last_hb not found")

# 3. Add status notification in the batch heartbeat block (every 60s)
old_batch_hb = """                # Heartbeat every 30s
                ts = int(time.time())
                if ts - last_hb >= 30:
                    await _update_job(job_id, last_heartbeat=ts); last_hb = ts"""

new_batch_hb = """                # Heartbeat every 30s
                ts = int(time.time())
                if ts - last_hb >= 30:
                    await _update_job(job_id, last_heartbeat=ts); last_hb = ts
                # Status notification every 60s
                if _bot and ts - last_notify >= 60:
                    fresh_for_notify = await _get_job(job_id)
                    if fresh_for_notify:
                        await _notify_status(_bot, fresh_for_notify, "ʙᴀᴛᴄʜ")
                    last_notify = ts"""

if old_batch_hb in jobs:
    jobs = jobs.replace(old_batch_hb, new_batch_hb)
    print("Added batch heartbeat notify")
else:
    print("WARN: batch heartbeat not found")

# 4. Add status notification in the live phase heartbeat block
old_live_hb = """            # Heartbeat every 30s
            ts = int(time.time())
            if ts - last_hb >= 30:
                await _update_job(job_id, last_heartbeat=ts); last_hb = ts"""

new_live_hb = """            # Heartbeat every 30s
            ts = int(time.time())
            if ts - last_hb >= 30:
                await _update_job(job_id, last_heartbeat=ts); last_hb = ts
            # Status notification every 60s
            if _bot and ts - last_notify >= 60:
                fresh_for_notify = await _get_job(job_id)
                if fresh_for_notify:
                    await _notify_status(_bot, fresh_for_notify, "ʟɪᴠᴇ")
                last_notify = ts"""

if old_live_hb in jobs:
    jobs = jobs.replace(old_live_hb, new_live_hb)
    print("Added live heartbeat notify")
else:
    print("WARN: live heartbeat not found")

# 5. Update _start_job_task to accept + pass bot
old_start = """def _start_job_task(job_id: str, user_id: int) -> asyncio.Task:
    t = asyncio.create_task(_run_job(job_id, user_id))
    _job_tasks[job_id] = t
    return t"""

new_start = """def _start_job_task(job_id: str, user_id: int, _bot=None) -> asyncio.Task:
    t = asyncio.create_task(_run_job(job_id, user_id, _bot=_bot))
    _job_tasks[job_id] = t
    return t"""

if old_start in jobs:
    jobs = jobs.replace(old_start, new_start)
    print("Updated _start_job_task")
else:
    print("WARN: _start_job_task not found")

# 6. Wire bot into job_start_cb (_start_job_task call from callback)
old_start_cb = """    _start_job_task(job_id, uid)
    await q.answer(\"▶️ ᴊᴏʙ sᴛᴀʀᴛᴇᴅ.\")
    await _render_jobs_list(bot, uid, q)"""

new_start_cb = """    _start_job_task(job_id, uid, _bot=bot)
    await q.answer(\"▶️ ᴊᴏʙ sᴛᴀʀᴛᴇᴅ.\")
    await _render_jobs_list(bot, uid, q)"""

if old_start_cb in jobs:
    jobs = jobs.replace(old_start_cb, new_start_cb)
    print("Updated job_start_cb")
else:
    print("WARN: job_start_cb not found")

# 7. Wire bot into _create_job_flow (the initial start after creation)
old_create_start = """    _start_job_task(job_id, uid)"""
new_create_start = """    _start_job_task(job_id, uid, _bot=bot)"""

count = jobs.count(old_create_start)
if count > 0:
    jobs = jobs.replace(old_create_start, new_create_start)
    print(f"Updated _create_job_flow start ({count} occurrence(s))")
else:
    print("WARN: _create_job_flow start not found")

# 8. Wire bot into resume_live_jobs
old_resume = "            _start_job_task(jid, uid)"
new_resume  = "            _start_job_task(jid, uid)  # no bot available during resume; user can press refresh"

if old_resume in jobs:
    jobs = jobs.replace(old_resume, new_resume)
    print("Updated resume comment")

with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
    f.write(jobs)

print("\nDone patching jobs.py")

# ── Patch taskjob.py ─────────────────────────────────────────────────────────
with open('plugins/taskjob.py', 'r', encoding='utf-8') as f:
    tj = f.read()

# 1. Add notifier helper after _tj_inc
old_tj_inc = """async def _tj_inc(job_id: str, n: int = 1):
    await db.db[COLL].update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})"""

new_tj_inc = """async def _tj_inc(job_id: str, n: int = 1):
    await db.db[COLL].update_one({"job_id": job_id}, {"$inc": {"forwarded": n}})


_tj_status_msgs: dict = {}

async def _tj_notify(bot, job: dict, phase: str = ""):
    \"\"\"Send/edit a live task job status message to the user.\"\"\"
    if not bot:
        return
    uid    = job["user_id"]
    job_id = job["job_id"]
    st     = _st(job.get("status", "running"))
    fwd    = job.get("forwarded", 0)
    cur    = job.get("current_id", "?")
    end    = job.get("end_id", 0)
    cname  = job.get("custom_name", "")
    name_p = f" <b>{cname}</b>" if cname else ""
    rng_p  = f"<code>{job.get('start_id',1)}</code> → <code>{end}</code>" if end else f"<code>{job.get('start_id',1)}</code> → ∞"
    err_p  = f"\\n┣⊸ ⚠️ <code>{job['error']}</code>" if job.get("error") else ""
    phase_p = f"\\n┣⊸ ◈ 𝐏𝐡𝐚𝐬𝐞   : <code>{phase}</code>" if phase else ""
    text = (
        f"<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙ ᴘʀᴏɢʀᴇss ❱──────╮\\n"
        f"┃\\n"
        f"┣⊸ ◈ 𝐈𝐃      : <code>{job_id[-6:]}</code>{name_p}\\n"
        f"┣⊸ ◈ 𝐒𝐭𝐚𝐭𝐮𝐬  : {st} {job.get('status','running')}\\n"
        f"┣⊸ ◈ 𝐒𝐨𝐮𝐫𝐜𝐞  : {job.get('from_title','?')}\\n"
        f"┣⊸ ◈ 𝐓𝐚𝐫𝐠𝐞𝐭  : {job.get('to_title','?')}\\n"
        f"┣⊸ ◈ 𝐑𝐚𝐧𝐠𝐞   : {rng_p}\\n"
        f"┣⊸ ◈ 𝐂𝐮𝐫𝐫𝐞𝐧𝐭 : <code>{cur}</code>\\n"
        f"┣⊸ ◈ 𝐅𝐰𝐝     : <code>{fwd}</code>"
        f"{phase_p}{err_p}\\n"
        f"┃\\n"
        f"╰────────────────────────────────╯</b>"
    )
    key = (uid, job_id)
    try:
        existing_mid = _tj_status_msgs.get(key)
        if existing_mid:
            try:
                await bot.edit_message_text(uid, existing_mid, text)
                return
            except Exception:
                pass
        sent = await bot.send_message(uid, text)
        _tj_status_msgs[key] = sent.id
    except Exception:
        pass"""

if old_tj_inc in tj:
    tj = tj.replace(old_tj_inc, new_tj_inc)
    print("Added _tj_notify helper")
else:
    print("WARN: _tj_inc not found")

# 2. Update _run_task_job signature
old_tj_sig = "async def _run_task_job(job_id: str, user_id: int):"
new_tj_sig = "async def _run_task_job(job_id: str, user_id: int, _bot=None):"

if old_tj_sig in tj:
    tj = tj.replace(old_tj_sig, new_tj_sig)
    print("Updated _run_task_job signature")

# 3. Add last_notify var after the pause_ev setup
old_pause_ev = """    pause_ev = _pause_events[job_id]

    acc = client = None"""

new_pause_ev = """    pause_ev = _pause_events[job_id]
    last_notify = 0  # for auto status notifications

    acc = client = None"""

if old_pause_ev in tj:
    tj = tj.replace(old_pause_ev, new_pause_ev)
    print("Added last_notify to taskjob")

# 4. Add notify call every 60s inside the main while loop
# Inject after "await _tj_update(job_id, consecutive_empty=0)"
old_consec = "            await _tj_update(job_id, consecutive_empty=0)"
new_consec = """            await _tj_update(job_id, consecutive_empty=0)

            # Auto status notification every 60s
            _now = int(time.time())
            if _bot and _now - last_notify >= 60:
                _fresh_j = await _tj_get(job_id)
                if _fresh_j:
                    await _tj_notify(_bot, _fresh_j, "ʀᴜɴɴɪɴɢ")
                last_notify = _now"""

if old_consec in tj:
    tj = tj.replace(old_consec, new_consec)
    print("Added taskjob notify in loop")
else:
    print("WARN: consecutive_empty=0 not found")

# 5. Update _start_task
old_start_task = """def _start_task(job_id: str, user_id: int):
    ev = asyncio.Event(); ev.set()
    _pause_events[job_id] = ev
    task = asyncio.create_task(_run_task_job(job_id, user_id))
    _task_jobs[job_id] = task
    return task"""

new_start_task = """def _start_task(job_id: str, user_id: int, _bot=None):
    ev = asyncio.Event(); ev.set()
    _pause_events[job_id] = ev
    task = asyncio.create_task(_run_task_job(job_id, user_id, _bot=_bot))
    _task_jobs[job_id] = task
    return task"""

if old_start_task in tj:
    tj = tj.replace(old_start_task, new_start_task)
    print("Updated _start_task")

# 6. Wire bot into callbacks that call _start_task
tj = tj.replace("_start_task(jid, uid)", "_start_task(jid, uid)")  # resume unchanged
tj = tj.replace(
    "    _start_task(job_id, uid)\n    await q.answer",
    "    _start_task(job_id, uid, _bot=bot)\n    await q.answer"
)

# Also wire bot into _create_taskjob_flow which calls _start_task at the end
tj = re.sub(
    r'(_start_task\(job_id, uid\))',
    r'_start_task(job_id, uid, _bot=bot)',
    tj
)
print("Wired bot into _start_task calls")

with open('plugins/taskjob.py', 'w', encoding='utf-8') as f:
    f.write(tj)

print("\nDone patching taskjob.py")
