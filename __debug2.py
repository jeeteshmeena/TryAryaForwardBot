with open('plugins/regix.py', 'rb') as f:
    raw = f.read()

content = raw.decode('utf-8')

# ── Fix 1: copy() download block — detect exact indentation/newline format ──
# Find the anchor
anchor = "# Preserve original file name from message; fall back to safe unique name"
idx = content.find(anchor)
print(f"anchor at {idx}")

# Find what comes before (the indentation)
line_start = content.rfind('\n', 0, idx) + 1
indent_str = content[line_start:idx]  # should be spaces
print(f"indent: {repr(indent_str)}\nanchor line: {repr(content[line_start:line_start+80])}")

# Detect newline style
nl = '\r\n' if '\r\n' in content[:200] else '\n'
print(f"newline style: {repr(nl)}")

# Build exact old block using detected indent/nl
sp18 = indent_str           # spaces before "# Preserve..."
# The "if message.media:" line is one level up (3 less spaces typically)
# Check by looking backwards for "if message.media:"
im_idx = content.rfind('if message.media:', 0, idx)
im_line_start = content.rfind('\n', 0, im_idx) + 1
sp_im = content[im_line_start:im_idx]
print(f"'if message.media:' indent: {repr(sp_im)}")

with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Print lines around the section
for i, line in enumerate(lines[508:568], start=509):
    print(f"{i}: {repr(line[:80])}")
