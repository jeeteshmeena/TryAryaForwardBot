import re

def fix_ui_newlines():
    # Fix jobs.py
    with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
        jobs = f.read()
    
    # We will just replace physical newlines inside the replaced strings
    # But wait, python ast is broken, let's just use string replace.
    broken_job_str = '''"<b>Live Jobs</b>

  • No active jobs found.

Creates scheduled tasks to automatically forward new messages from sources to destinations in the background."'''
    fixed_job_str = '"<b>Live Jobs</b>\\n\\n  • No active jobs found.\\n\\nCreates scheduled tasks to automatically forward new messages from sources to destinations in the background."'
    
    jobs = jobs.replace(broken_job_str, fixed_job_str)

    with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
        f.write(jobs)

    # Fix taskjob.py
    with open('plugins/taskjob.py', 'r', encoding='utf-8') as f:
        tasks = f.read()

    broken_tj_str = '''"<b>Task Jobs</b>

  • No task jobs yet.

Copies all existing messages from a source to a destination in the background."'''
    fixed_tj_str = '"<b>Task Jobs</b>\\n\\n  • No task jobs yet.\\n\\nCopies all existing messages from a source to a destination in the background."'

    tasks = tasks.replace(broken_tj_str, fixed_tj_str)

    with open('plugins/taskjob.py', 'w', encoding='utf-8') as f:
        f.write(tasks)

if __name__ == '__main__':
    fix_ui_newlines()
