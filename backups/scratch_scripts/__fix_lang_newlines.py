def fix_lang_newlines():
    with open('plugins/lang.py', 'r', encoding='utf-8') as f:
        code = f.read()

    # The actual literal newlines inside "..." caused syntax error.
    find_str = '''_S["TEXT"] = {
    "en": (
        "<b>Forwarding Progress</b>

"
        "  • <b>Fetched:</b> <code>{}</code>
"
        "  • <b>Forwarded:</b> <code>{}</code>
"
        "  • <b>Duplicates:</b> <code>{}</code>
"
        "  • <b>Skipped:</b> <code>{}</code>
"
        "  • <b>Deleted:</b> <code>{}</code>

"
        "  • <b>Status:</b> <code>{}</code>
"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hi": (
        "<b>फॉरवर्डिंग प्रोग्रेस</b>

"
        "  • <b>Fetched:</b> <code>{}</code>
"
        "  • <b>Forwarded:</b> <code>{}</code>
"
        "  • <b>Duplicates:</b> <code>{}</code>
"
        "  • <b>Skipped:</b> <code>{}</code>
"
        "  • <b>Deleted:</b> <code>{}</code>

"
        "  • <b>Status:</b> <code>{}</code>
"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hinglish": (
        "<b>Forwarding Progress</b>

"
        "  • <b>Fetched:</b> <code>{}</code>
"
        "  • <b>Forwarded:</b> <code>{}</code>
"
        "  • <b>Duplicates:</b> <code>{}</code>
"
        "  • <b>Skipped:</b> <code>{}</code>
"
        "  • <b>Deleted:</b> <code>{}</code>

"
        "  • <b>Status:</b> <code>{}</code>
"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
}'''

    repl_str = '''_S["TEXT"] = {
    "en": (
        "<b>Forwarding Progress</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Forwarded:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n"
        "  • <b>Skipped:</b> <code>{}</code>\\n"
        "  • <b>Deleted:</b> <code>{}</code>\\n\\n"
        "  • <b>Status:</b> <code>{}</code>\\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hi": (
        "<b>फॉरवर्डिंग प्रोग्रेस</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Forwarded:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n"
        "  • <b>Skipped:</b> <code>{}</code>\\n"
        "  • <b>Deleted:</b> <code>{}</code>\\n\\n"
        "  • <b>Status:</b> <code>{}</code>\\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hinglish": (
        "<b>Forwarding Progress</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Forwarded:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n"
        "  • <b>Skipped:</b> <code>{}</code>\\n"
        "  • <b>Deleted:</b> <code>{}</code>\\n\\n"
        "  • <b>Status:</b> <code>{}</code>\\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
}'''

    code = code.replace(find_str, repl_str)

    find_unq = '''_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>Unequify Status</b>

"
        "  • <b>Fetched:</b> <code>{}</code>
"
        "  • <b>Duplicates:</b> <code>{}</code>

"
        "Status: {}"
    ),
    "hi": (
        "<b>Unequify Status</b>

"
        "  • <b>Fetched:</b> <code>{}</code>
"
        "  • <b>Duplicates:</b> <code>{}</code>

"
        "Status: {}"
    ),
    "hinglish": (
        "<b>Unequify Status</b>

"
        "  • <b>Fetched:</b> <code>{}</code>
"
        "  • <b>Duplicates:</b> <code>{}</code>

"
        "Status: {}"
    ),
}'''

    repl_unq = '''_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>Unequify Status</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n\\n"
        "Status: {}"
    ),
    "hi": (
        "<b>Unequify Status</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n\\n"
        "Status: {}"
    ),
    "hinglish": (
        "<b>Unequify Status</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n\\n"
        "Status: {}"
    ),
}'''

    code = code.replace(find_unq, repl_unq)

    with open('plugins/lang.py', 'w', encoding='utf-8') as f:
        f.write(code)

if __name__ == '__main__':
    fix_lang_newlines()
