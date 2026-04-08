import re
c = 'My_Vampire_system_Episode_100'
match = re.search(r'(?i)(?:episode|epi|ep|e|ch|chapter|part|एपिसोड|भाग)[\s\-\:\.\#\_]*(\d{1,4})(?!\d)', c)
print(match.group(1) if match else "None")
