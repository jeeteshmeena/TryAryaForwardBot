import re

def fix_regix_newlines():
    with open('plugins/regix.py', 'r', encoding='utf-8') as f:
        code = f.read()

    # The actual literal newlines inside f"..." caused syntax error.
    # We will replace literal newlines that are inside f"..." with \n
    
    find_str = '''        # 🔔 Detailed Completion Notification
        summary = (
            f"<b>✅ Batch Forwarding Completed!</b>

"
            f"<b>Final Summary:</b>
"
            f"  • <b>Fetched:</b> <code>{sts.get('fetched')}</code>
"
            f"  • <b>Forwarded:</b> <code>{sts.get('total_files')}</code>
"
            f"  • <b>Duplicates skipped:</b> <code>{sts.get('duplicate')}</code>
"
            f"  • <b>Filtered out:</b> <code>{sts.get('filtered')}</code>
"
            f"  • <b>Deleted sources:</b> <code>{sts.get('deleted')}</code>
"
        )'''

    repl_str = '''        # 🔔 Detailed Completion Notification
        summary = (
            f"<b>✅ Batch Forwarding Completed!</b>\\n\\n"
            f"<b>Final Summary:</b>\\n"
            f"  • <b>Fetched:</b> <code>{sts.get('fetched')}</code>\\n"
            f"  • <b>Forwarded:</b> <code>{sts.get('total_files')}</code>\\n"
            f"  • <b>Duplicates skipped:</b> <code>{sts.get('duplicate')}</code>\\n"
            f"  • <b>Filtered out:</b> <code>{sts.get('filtered')}</code>\\n"
            f"  • <b>Deleted sources:</b> <code>{sts.get('deleted')}</code>\\n"
        )'''

    code = code.replace(find_str, repl_str)
    
    with open('plugins/regix.py', 'w', encoding='utf-8') as f:
        f.write(code)

if __name__ == '__main__':
    fix_regix_newlines()
