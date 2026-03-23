import pathlib

def process():
    # 1. Take current taskjob.py and make it tmp_taskjob.py
    curr = pathlib.Path('plugins/taskjob.py')
    tmp = pathlib.Path('plugins/tmp_taskjob.py')
    old = pathlib.Path('plugins/old_taskjob.py')
    
    # Read current
    t_curr = curr.read_text('utf-8')
    t_curr = t_curr.replace('taskjobs', 'tmptaskjobs')
    t_curr = t_curr.replace('taskjob', 'tmptaskjob')
    t_curr = t_curr.replace('tj#', 'tmptj#')
    t_curr = t_curr.replace('COLL = "tmptaskjobs"', 'COLL = "tmp_taskjobs"')
    t_curr = t_curr.replace('resume_task_jobs', 'resume_tmp_task_jobs')
    t_curr = t_curr.replace('Task Job', 'TMP Task Job')
    t_curr = t_curr.replace('@Client.on_message(filters.command("cleanup") & filters.user(Config.BOT_OWNER_ID))',
                           '@Client.on_message(filters.command("tmpcleanup") & filters.user(Config.BOT_OWNER_ID))')
    
    tmp.write_text(t_curr, 'utf-8')

    # 2. Take old_taskjob.py and make it taskjob.py (RESTORING MARCH 20 RUNNER)
    t_old = old.read_text('utf-8')
    t_old = t_old.replace('oldtaskjobs', 'taskjobs')
    t_old = t_old.replace('oldtaskjob', 'taskjob')
    t_old = t_old.replace('otj#', 'tj#')
    t_old = t_old.replace('COLL = "old_taskjobs_march20"', 'COLL = "taskjobs"')
    t_old = t_old.replace('resume_old_task_jobs', 'resume_task_jobs')
    t_old = t_old.replace('Old Task Job', 'Task Job')
    
    curr.write_text(t_old, 'utf-8')
    
    # 3. Update main.py to resume the correct ones
    main_py = pathlib.Path('main.py')
    t_main = main_py.read_text('utf-8')
    t_main = t_main.replace('from plugins.old_taskjob import resume_old_task_jobs', 
                           'from plugins.tmp_taskjob import resume_tmp_task_jobs')
    t_main = t_main.replace('asyncio.create_task(resume_old_task_jobs(_bot=bot))',
                           'asyncio.create_task(resume_tmp_task_jobs(_bot=bot))')
    main_py.write_text(t_main, 'utf-8')

    # 4. Delete the now-obsolete old_taskjob.py
    old.unlink()

if __name__ == "__main__":
    process()
