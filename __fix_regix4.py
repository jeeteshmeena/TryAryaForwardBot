with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and complete the link filter section + caption strip
# Find: "# For media messages — strip the link/URL from caption, keep the file"
target = "# For media messages — strip the link/URL from caption, keep the file"
for i, line in enumerate(lines):
    if target in line:
        # Replace this single line with the complete caption-strip logic
        indent = len(line) - len(line.lstrip())
        sp = ' ' * indent
        sp_out = ' ' * (indent - 4)
        replacement = [
            f"{sp}# For media messages: strip links from caption below (file is never dropped)\n",
            f"\n",
            f"{sp_out}# Compute caption & replacements for this message before buffering\n",
            f"{sp_out}_filters = data.get('filters', [])\n",
            f"{sp_out}new_caption = custom_caption(message, caption)\n",
            f"{sp_out}# rm_caption: read from data flags (set by get_filter_flags in get_data)\n",
            f"{sp_out}if (message.audio or message.video or message.photo or message.document) and data.get('rm_caption'):\n",
            f"{sp_out}    new_caption = \"\"\n",
            f"\n",
            f"{sp_out}# Strip links from caption when block_links is ON (never drop the file)\n",
            f"{sp_out}if _link_disabled and message.media and new_caption:\n",
            f"{sp_out}    _LSRE = re.compile(\n",
            f"{sp_out}        r'(https?://\\S+|t\\.me/\\S+|@[A-Za-z0-9_]{{4,}}'\n",
            f"{sp_out}        r'|\\b(?:www\\.|bit\\.ly/|youtu\\.be/)\\S+'\n",
            f"{sp_out}        r'|\\b[\\w.-]+\\.(?:com|net|org|io|co|me|tv|gg|app|xyz|info|news|link|site)(?:/\\S*)?\\b)',\n",
            f"{sp_out}        re.IGNORECASE)\n",
            f"{sp_out}    new_caption = _LSRE.sub('', new_caption).strip()\n",
        ]
        lines = lines[:i] + replacement + lines[i+1:]
        # Now find and remove the duplicate "# Compute caption & replacements" block below
        print(f"Replaced link filter comment at line {i}")
        break

# Find and remove the now-duplicate "# Compute caption & replacements for this message before buffering" section
# (it will appear right after what we just inserted)
to_remove_markers = [
    "# Compute caption & replacements for this message before buffering",
    "_filters = data.get('filters', [])",
    "new_caption = custom_caption(message, caption)",
    "# rm_caption: read from data flags",
    "if (message.audio or message.video or message.photo or message.document) and data.get('rm_caption'):",
    "    new_caption = \"\"",
]

# Find the duplicate section
dup_start = None
for i, line in enumerate(lines):
    if "# Compute caption & replacements for this message before buffering" in line:
        # Count occurrences so far
        count = sum(1 for l in lines[:i] if "# Compute caption & replacements for this message before buffering" in l)
        if count >= 1:  # second occurrence = duplicate
            dup_start = i
            break

if dup_start is not None:
    # Find where it ends (at the replacements block)
    dup_end = dup_start
    for j in range(dup_start, min(dup_start + 10, len(lines))):
        if 'sort_buffer.append' in lines[j]:
            break
        dup_end = j
    print(f"Removing duplicate caption block lines {dup_start}-{dup_end}")
    lines = lines[:dup_start] + lines[dup_end+1:]

with open('plugins/regix.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Done.")
