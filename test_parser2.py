import sys, re

def _extract_range_from_text(c):
    kw = re.search(r'(?i)(?:ep|epi|episode|e|ch|chapter|part|एपिसोड|भाग)[\s\-\:\.\#\_]*(\d{1,4})(?!\d)', c)
    if kw:
        n = int(kw.group(1))
        if 0 < n < 5000: return (n, n, False)

    c2 = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', c)
    c2 = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', c2)
    c2 = re.sub(r'(?i)\b19\d{2}\b|\b20\d{2}\b', ' ', c2)
    nums = [int(x) for x in re.findall(r'(?<!\d)(\d{1,4})(?!\d)', c2) if 0 < int(x) < 5000]
    if nums: return (nums[-1], nums[-1], False)
    return None

c = 'MY_VAMPIRE_SYSTEM_EPISODE_76_POCKET_FM_xLg2Wb_vPbg_140'
print(_extract_range_from_text(c))
