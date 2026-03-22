import re

def fix_lang_raw():
    with open('plugins/lang.py', 'r', encoding='utf-8') as f:
        code = f.read()

    pattern = re.compile(r'_S\["TEXT"\] = \{.*?\}', re.DOTALL)
    
    new_text = r'''_S["TEXT"] = {
    "en": (
        "<b>Forwarding Progress</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hi": (
        "<b>फॉरवर्डिंग प्रोग्रेस</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
    "hinglish": (
        "<b>Forwarding Progress</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Forwarded:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n"
        "  • <b>Skipped:</b> <code>{}</code>\n"
        "  • <b>Deleted:</b> <code>{}</code>\n\n"
        "  • <b>Status:</b> <code>{}</code>\n"
        "  • <b>ETA:</b> <code>{}</code>"
    ),
}'''
    code = pattern.sub(new_text, code)

    pattern2 = re.compile(r'_S\["DUPLICATE_TEXT"\] = \{.*?\}', re.DOTALL)
    new_unq = r'''_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "Status: {}"
    ),
    "hi": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "Status: {}"
    ),
    "hinglish": (
        "<b>Unequify Status</b>\n\n"
        "  • <b>Fetched:</b> <code>{}</code>\n"
        "  • <b>Duplicates:</b> <code>{}</code>\n\n"
        "Status: {}"
    ),
}'''
    code = pattern2.sub(new_unq, code)

    with open('plugins/lang.py', 'w', encoding='utf-8') as f:
        f.write(code)

if __name__ == '__main__':
    fix_lang_raw()
