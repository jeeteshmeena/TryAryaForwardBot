import re as _re
def _extract_ep_label(fname: str) -> str:
    base = _re.sub(r'\.\w{2,4}$', '', fname)
    base = _re.sub(r'\s*\(\d+\)\s*$', '', base).strip()
    base_norm = base.replace('\u2013', '-').replace('\u2014', '-')
    m = _re.search(r'\b(\d{1,4})\s*(?:-|to|and)\s*(\d{1,4})\b', base_norm, _re.IGNORECASE)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if 0 < a < 5000 and a <= b < 5000:
            return base[m.start():m.end()].strip()
    nums = [int(x) for x in _re.findall(r'\b(\d{1,4})\b', base_norm) if 0 < int(x) < 5000 and not (1900 <= int(x) <= 2100)]
    if nums:
        return str(nums[-1])
    return ''

print(repr(_extract_ep_label('246–354.mkv')))
print(repr(_extract_ep_label('364 to 400.mp4')))
print(repr(_extract_ep_label('466 and 476.mp4')))
print(repr(_extract_ep_label('Malang 576–580.mp4')))
