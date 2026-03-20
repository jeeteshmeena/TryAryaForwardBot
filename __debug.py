with open('plugins/regix.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the exact indentation of the download section
for i, line in enumerate(lines[510:570], start=511):
    print(f"{i}: {repr(line)}")
