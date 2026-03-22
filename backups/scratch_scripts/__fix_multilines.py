import re

def fix_jobs_tasks_full():
    with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
        jobs = f.read()

    broken1 = '''        lines = ["<b>Live Jobs</b>\\n\\n  • No active jobs found.\\n\\nCreates scheduled tasks to automatically forward new messages from sources to destinations in the background."]'''
    fixed1 = r'''        lines = ["<b>Live Jobs</b>\n\n  • No active jobs found.\n\nCreates scheduled tasks to automatically forward new messages from sources to destinations in the background."]'''
    
    # Wait, if I read it via f.read(), a physical newline is literally '\n'.
    # A single string literal across multiple lines is missing its ending bracket maybe?
    # Ah! The error says "SyntaxError: unterminated string literal" or "closing parenthesis '['".
    # This means the code looks like:
    # lines = ["<b>Task Jobs</b>
    # 
    #   • No task jobs yet.
    # 
    # Copies all existing messages from a source to a destination in the background."]
    
    # And python string parser fails. Let me just replace the whole problematic chunk using regex matching any whitespace.

    
    import re

    with open('plugins/taskjob.py', 'r', encoding='utf-8') as f:
        tasks = f.read()

    # We will search for 'lines = ["<b>Task Jobs</b>' until 'background."]'
    tasks = re.sub(r'lines = \["<b>Task Jobs</b>(.*?)background\."\]', 
                   r'lines = ["<b>Task Jobs</b>\\n\\n  • No task jobs yet.\\n\\nCopies all existing messages from a source to a destination in the background."]',
                   tasks, flags=re.DOTALL)

    with open('plugins/taskjob.py', 'w', encoding='utf-8') as f:
        f.write(tasks)


    with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
        jobs = f.read()

    jobs = re.sub(r'lines = \["<b>Live Jobs</b>(.*?)background\."\]',
                  r'lines = ["<b>Live Jobs</b>\\n\\n  • No active jobs found.\\n\\nCreates scheduled tasks to automatically forward new messages from sources to destinations in the background."]',
                  jobs, flags=re.DOTALL)

    with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
        f.write(jobs)

if __name__ == '__main__':
    fix_jobs_tasks_full()
