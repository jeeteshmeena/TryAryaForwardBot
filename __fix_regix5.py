with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the broken fragment at lines 428-433 and fix it
# The replacements for loop was orphaned — add the enclosing `if replacements:` back
for i, line in enumerate(lines):
    if '                        try:\n' == line and i > 425 and i < 435:
        # This is the orphaned inner try block, fix context
        # Print context
        print(f"Found orphaned try at line {i}")
        for j in range(max(0,i-3), min(len(lines), i+8)):
            print(f"  {j}: {repr(lines[j])}")
        break

# Replace lines 428-432 with proper replacements block
# Line 428 is the blank line after the link strip, then 429-432 are orphaned
# They should be:
# replacements = data.get('replacements', {})
# if replacements and new_caption:
#     for old_txt, new_txt in replacements.items():
#         try: ...
#         except: ...

# Find precisely
start = None
for i, line in enumerate(lines):
    if '                        try:\n' == line and i > 425 and i < 435:
        start = i - 1  # the blank line before
        break

if start is not None:
    # Find end of this orphaned block
    end = start
    for j in range(start, len(lines)):
        if 'sort_buffer.append' in lines[j]:
            end = j
            break
    
    print(f"Replacing orphaned block {start}-{end-1}")
    
    replacement = [
        "\n",
        "                replacements = data.get('replacements', {})\n",
        "                if replacements and new_caption:\n",
        "                    for old_txt, new_txt in replacements.items():\n",
        "                        try:\n",
        "                            new_caption = re.sub(old_txt, new_txt, new_caption, flags=re.IGNORECASE)\n",
        "                        except Exception:\n",
        "                            new_caption = new_caption.replace(old_txt, new_txt)\n",
        "\n",
    ]
    lines = lines[:start] + replacement + lines[end:]

with open('plugins/regix.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)
print("Done.")
