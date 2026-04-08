import re

def _extract_range_from_text(c):
    kw = re.search(r'(?i)(?:ep|epi|episode|e|ch|chapter|part|एपिसोड|भाग)[\s\-\:\.\#\_]*(\d{1,4})(?!\d)', c)
    if kw:
        n = int(kw.group(1))
        if 0 < n < 5000: return (n, n, False)

    c2 = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', c)
    c2 = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', c2)
    c2 = re.sub(r'(?i)\b19\d{2}\b|\b20\d{2}\b', ' ', c2) 
    
    nums = [int(x) for x in re.findall(r'(?<!\d)(\d{1,4})(?!\d)', c2) if 0 < int(x) < 5000]
    if nums:
        return (nums[-1], nums[-1], False)
        
    return None

files = [
    'My_Vampire_system_Episode_74_Hindi_My_Vampire_system_73_sQwzME07E6s.mp3',
    'MY_VAMPIRE_SYSTEM_EPISODE_76_POCKET_FM_xLg2Wb_vPbg_140.mp3',
    '_My_Vampire_System_Ep_115_pocket_fm_CsitoliVfi8_251.ogg',
    'MVS 41 .mp3',
    'MVS 42.Evolver.mp3',
    'Vampire 45.mp3',
    'My_Vampire_system_Episode_49_Hindi_5hOjQGJHGag_140.mp3',
    'MVS 51.mp3',
    'My_Vampire_system_Episode_53_Hindi_My_Vampire_system_53_5Y8qpfuqOSw.mp3'
]
for f in files:
    print(f"{f}: {_extract_range_from_text(f)}")
