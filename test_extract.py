import re
def _extract_ep_label(fname: str) -> str:
    base = re.sub(r'\.\w{2,4}$', '', fname)
    base = re.sub(r'\s*\(\d+\)\s*$', '', base).strip()
    base_norm = base.replace('\u2013', '-').replace('\u2014', '-')
    m = re.search(r'\b(\d{1,4})\s*(?:-|to|and)\s*(\d{1,4})\b', base_norm, re.IGNORECASE)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if 0 < a < 5000 and a <= b + 1 < 5001:
            start_pos, end_pos = m.start(), m.end()
            orig_slice = base[start_pos:end_pos].strip()
            if orig_slice: return orig_slice
            return f'{a}-{b}' if a != b else str(a)
    nums = [int(x) for x in re.findall(r'\b(\d{1,4})\b', base_norm) if 0 < int(x) < 5000 and not (1900 <= int(x) <= 2100)]
    if nums: return str(nums[-1])
    return ''

print('File 30-35.mp3:', _extract_ep_label('File 30-35.mp3'))
print('File 30_35.mp3:', _extract_ep_label('File 30_35.mp3'))
print('File 30 - 35.mp3:', _extract_ep_label('File 30 - 35.mp3'))
print('File 30 – 35.mp3:', _extract_ep_label('File 30 – 35.mp3'))
print('File 30—35.mp3:', _extract_ep_label('File 30—35.mp3'))
print('30–35.mp3:', _extract_ep_label('30–35.mp3'))
