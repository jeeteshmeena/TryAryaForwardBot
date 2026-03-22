import re

def clean_ui():
    # ---- JOBS.PY ----
    with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
        jobs_code = f.read()

    # 1. Box function
    jobs_code = re.sub(
        r'def _box\(title: str, lines: list\[str\]\) -> str:(.*?)\s+return \(\s+f"<b>╭──────.*?</b>"\s+\)',
        r'''def _box(title: str, lines: list[str]) -> str:\n    body = "\\n".join(f"  • {l}" for l in lines)\n    return (f"✦ {title.upper()} ✦\\n\\n{body}")''',
        jobs_code, flags=re.DOTALL
    )

    # 2. task notify
    find_notify = r'f"<b>╭──────❰ 📋 ʟɪᴠᴇ ᴊᴏʙ ᴘʀᴏɢʀᴇss ❱──────╮\\n".*?╰────────────────────────────────╯</b>"'
    repl_notify = r'''f"<b>Live Job Progress</b>\\n\\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_part}\\n"
        f"  • <b>Status:</b> {st} {job.get('status','running')}\\n"
        f"  • <b>Source:</b> {src}\\n"
        f"  • <b>Destination:</b> {dst}\\n\\n"
        f"  • <b>Forwarded:</b> <code>{fwd}</code>"
        f"{batch_part}{phase_part}{err_part}"'''
    jobs_code = re.sub(find_notify, repl_notify, jobs_code, flags=re.DOTALL)
    
    # Also adjust batch_part: "\\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ✅ ᴄᴏᴍᴘʟᴇᴛᴇ"
    jobs_code = jobs_code.replace(r'\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : ✅ ᴄᴏᴍᴘʟᴇᴛᴇ', r'\n  • <b>Batch:</b> ✅ Complete')
    jobs_code = jobs_code.replace(r'\n┣⊸ ◈ 𝐁𝐚𝐭𝐜𝐡   : 📦 <code>{cur}</code> / <code>{end}</code>', r'\n  • <b>Batch:</b> 📦 <code>{cur}</code> / <code>{end}</code>')
    jobs_code = jobs_code.replace(r'\n┣⊸ ◈ 𝐏𝐡𝐚𝐬𝐞   :', r'\n  • <b>Phase:</b>')
    jobs_code = jobs_code.replace(r'\n┣⊸ ⚠️', r'\n  • ⚠️')
    
    # 3. List jobs
    jobs_code = re.sub(
        r'f"┣⊸ {st} <b>.*?FC:.*?LC:.*?\\n',
        r'f"  • {st} <b>{j.get(\'from_title\',\'?\')} → {j.get(\'to_title\',\'?\')}</b>" '
        r'f" <code>[{j[\'job_id\'][-6:]}]</code>{name_disp}" '
        r'f"\\n      Fwd: <code>{fwd}</code> | Last: <code>{lst}</code>{_batch_tag(j)}{err}\\n\\n',
        jobs_code, flags=re.DOTALL
    )
    # List jobs empty state
    jobs_code = re.sub(
        r'"<b>╭──────❰ 🔴 ʟɪᴠᴇ ᴊᴏʙs ❱──────╮(.*?)╰────────────────────────────────╯</b>"',
        r'"<b>Live Jobs</b>\n\n  • No active jobs found.\n\nCreates scheduled tasks to automatically forward new messages from sources to destinations in the background."',
        jobs_code, flags=re.DOTALL
    )
    # List jobs header and footer
    jobs_code = jobs_code.replace('["<b>╭──────❰ 🔴 ʟɪᴠᴇ ᴊᴏʙs ❱──────╮</b>\\n┃"]', '["<b>Live Jobs</b>\\n"]')
    jobs_code = jobs_code.replace('lines.append("┃\\n<b>╰────────────────────────────────╯</b>")', 'pass')
    jobs_code = jobs_code.replace('  📦✅', '  📦 ✅')
    
    # 4. Job Info CB
    find_info = r'f"<b>╭──────❰ 📋 ʟɪᴠᴇ ᴊᴏʙ ɪɴғᴏ ❱──────╮\\n".*?╰────────────────────────────────╯</b>"'
    repl_info = r'''f"<b>Live Job Information</b>\\n\\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_lbl}\\n"
        f"  • <b>Status:</b> {st} {job.get('status','?')}\\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}\\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}\\n"
        f"  • <b>Batch:</b> {grp_lbl}\\n"
        f"  • <b>Forwarded:</b> <code>{job.get('forwarded', 0)}</code>\\n"
        f"  • <b>Last ID:</b> <code>{job.get('last_seen_id', 0)}</code>\\n"
        f"  • <b>Created:</b> {created}"
        f"{err_lbl}"'''
    jobs_code = re.sub(find_info, repl_info, jobs_code, flags=re.DOTALL)
    jobs_code = jobs_code.replace('\\n┣⊸ ◈ 𝐍𝐚𝐦𝐞    : <b>{c_name}</b>', ' <b>{c_name}</b>')
    jobs_code = jobs_code.replace('\\n┣⊸ ⚠️ ᴇʀʀᴏʀ : ', '\\n  • ⚠️ <b>Error:</b> ')
    jobs_code = jobs_code.replace('✅ ᴄᴏᴍᴘʟᴇᴛᴇ', '✅ Complete')
    jobs_code = jobs_code.replace('▶sᴛᴀʀᴛ', '▶️ Start')
    jobs_code = jobs_code.replace('▶️ sᴛᴀʀᴛ', '▶️ Start')
    jobs_code = jobs_code.replace('⏹ sᴛᴏᴘ', '⏹ Stop')
    jobs_code = jobs_code.replace('🗑 ᴅᴇʟᴇᴛᴇ', '🗑 Delete')
    jobs_code = jobs_code.replace('➕ ᴄʀᴇᴀᴛᴇ ʟɪᴠᴇ ᴊᴏʙ', '➕ Create Live Job')
    jobs_code = jobs_code.replace('🔄 ʀᴇғʀᴇsʜ', '🔄 Refresh')
    jobs_code = jobs_code.replace('↩ ʙᴀᴄᴋ', '↩ Back')
    jobs_code = jobs_code.replace('✅ sᴀᴠᴇ', '✅ Save')
    jobs_code = jobs_code.replace('❌ ᴄᴀɴᴄᴇʟ', '❌ Cancel')
    jobs_code = jobs_code.replace('ᴄᴀɴᴄᴇʟ', 'Cancel')
    jobs_code = jobs_code.replace('<b>╭──────❰ ❌ ᴄᴀɴᴄᴇʟʟᴇᴅ ❱──────╮\n┃\n╰────────────────────────────────╯</b>', '<b>❌ Cancelled.</b>')

    with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
        f.write(jobs_code)


    # ---- TASKJOB.PY ----
    with open('plugins/taskjob.py', 'r', encoding='utf-8') as f:
        tj_code = f.read()

    # 1. notify
    find_tj_notify = r'f"<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙ ᴘʀᴏɢʀᴇss ❱──────╮\\n".*?╰────────────────────────────────╯</b>"'
    repl_tj_notify = r'''f"<b>Task Job Progress</b>\\n\\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_p}\\n"
        f"  • <b>Status:</b> {st} {job.get('status','running')}\\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}\\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}\\n\\n"
        f"  • <b>Range:</b> {rng_p}\\n"
        f"  • <b>Current:</b> <code>{cur}</code>\\n"
        f"  • <b>Forwarded:</b> <code>{fwd}</code>"
        f"{phase_p}{err_p}"'''
    tj_code = re.sub(find_tj_notify, repl_tj_notify, tj_code, flags=re.DOTALL)
    tj_code = tj_code.replace(r'\n┣⊸ ◈ 𝐏𝐡𝐚𝐬𝐞   :', r'\n  • <b>Phase:</b>')
    
    # 2. list
    tj_code = re.sub(
        r'"<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙs ❱──────╮(.*?)╰────────────────────────────────╯</b>"',
        r'"<b>Task Jobs</b>\n\n  • No task jobs yet.\n\nCopies all existing messages from a source to a destination in the background."',
        tj_code, flags=re.DOTALL
    )
    tj_code = tj_code.replace('lines = ["<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙs ❱──────╮</b>\\n┃"]', 'lines = ["<b>Task Jobs</b>\\n"]')
    tj_code = re.sub(
        r'f"┣⊸ {st}.*?\\n┃   ◈ 𝐅𝐰𝐝:(.*?){err}"',
        r'f"  • {st} <b>{j.get(\'from_title\',\'?\')} → {j.get(\'to_title\',\'?\')}</b>" '
        r'f" <code>[{j[\'job_id\'][-6:]}]</code>{name_disp}" '
        r'f"\\n      Fwd: <code>{fwd}</code> | Pos: {rng}{err}\\n\\n"',
        tj_code, flags=re.DOTALL
    )
    tj_code = tj_code.replace('lines.append("┃\\n<b>╰────────────────────────────────╯</b>")', 'pass')
    tj_code = tj_code.replace('\\n┃   ⚠️', '\\n      ⚠️')

    # 3. info cb
    find_tj_info = r'f"<b>╭──────❰ 📦 ᴛᴀsᴋ ᴊᴏʙ ɪɴғᴏ ❱──────╮\\n".*?╰────────────────────────────────╯</b>"'
    repl_tj_info = r'''f"<b>Task Job Information</b>\\n\\n"
        f"  • <b>ID:</b> <code>{job_id[-6:]}</code>{name_lbl}\\n"
        f"  • <b>Status:</b> {st} {job.get('status','?')}\\n"
        f"  • <b>Source:</b> {job.get('from_title','?')}\\n"
        f"  • <b>Target:</b> {job.get('to_title','?')}\\n"
        f"  • <b>Range:</b> {rng_lbl}\\n"
        f"  • <b>Current:</b> <code>{cur}</code>\\n"
        f"  • <b>Forwarded:</b> <code>{job.get('forwarded', 0)}</code>\\n"
        f"  • <b>Created:</b> {created}"
        f"{err_lbl}"'''
    tj_code = re.sub(find_tj_info, repl_tj_info, tj_code, flags=re.DOTALL)
    
    tj_code = tj_code.replace('➕ ᴄʀᴇᴀᴛᴇ ᴛᴀsᴋ ᴊᴏʙ', '➕ Create Task Job')
    tj_code = tj_code.replace('▶️ sᴛᴀʀᴛ', '▶️ Start')
    tj_code = tj_code.replace('⏸ ᴘᴀᴜsᴇ', '⏸ Pause')
    tj_code = tj_code.replace('▶️ ʀᴇsᴜᴍᴇ', '▶️ Resume')
    tj_code = tj_code.replace('⏹ sᴛᴏᴘ', '⏹ Stop')

    with open('plugins/taskjob.py', 'w', encoding='utf-8') as f:
        f.write(tj_code)


    print("UI cleansed.")

if __name__ == "__main__":
    clean_ui()
