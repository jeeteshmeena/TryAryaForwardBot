def fix_lang_ultimate():
    with open('plugins/lang.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()

    start_idx = -1
    for i, line in enumerate(lines):
        if line.startswith('_S["TEXT"] = {'):
            start_idx = i
            break
            
    end_idx = -1
    for i in range(start_idx, len(lines)):
        if line.startswith('# ── Simple one-liners ──────────────────────────────────────────────────────'):
            pass  # Wait, we know line 554 in the current file is the start of simple one-liners.
            
    # Actually, simpler to just find the line that starts "# ── Simple one-liners"
    for i, line in enumerate(lines):
        if "# ── Simple one-liners" in line:
            end_idx = i
            break

    if start_idx != -1 and end_idx != -1:
        new_content = lines[:start_idx]
        injection = """_S["TEXT"] = {
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
}

# ── DUPLICATE_TEXT ─────────────────────────────────────────────────────────
_S["DUPLICATE_TEXT"] = {
    "en": (
        "<b>Unequify Status</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n\\n"
        "  • <b>Status:</b> {}"
    ),
    "hi": (
        "<b>Unequify Status</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n\\n"
        "  • <b>Status:</b> {}"
    ),
    "hinglish": (
        "<b>Unequify Status</b>\\n\\n"
        "  • <b>Fetched:</b> <code>{}</code>\\n"
        "  • <b>Duplicates:</b> <code>{}</code>\\n\\n"
        "  • <b>Status:</b> {}"
    ),
}

"""
        new_content.append(injection)
        new_content.extend(lines[end_idx:])
        
        with open('plugins/lang.py', 'w', encoding='utf-8') as f:
            f.writelines(new_content)
        print("Success")
    else:
        print("Failed to locate boundaries", start_idx, end_idx)

if __name__ == "__main__":
    fix_lang_ultimate()
