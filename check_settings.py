with open('plugins/settings.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact location by looking for the marker
marker = "'settings#owners'"
idx = content.rfind(marker)  # last occurrence = in the main menu
print("Found at index:", idx)
print("Context around it:")
print(repr(content[idx-300:idx+100]))
