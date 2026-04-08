import re
c = 'My_Vampire_system_Episode_98_Hindi_My_Vampire_system_98_mN22s2p4kT4.m4a'
match = re.search(r'(?i)(?:ep|epi|episode|e|ch|chapter|part|एपिसोड|भाग)[\s\-\:\.\#\_]*(\d{1,4})(?!\d)', c)
print(match.group(1) if match else "None")
