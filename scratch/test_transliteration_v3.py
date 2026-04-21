import requests
import urllib.parse

def transliterate_to_hindi(text: str) -> str:
    if not text: return ""
    try:
        url = f"https://inputtools.google.com/request?text={urllib.parse.quote(text)}&itc=hi-t-i0-und&num=1&cp=0&cs=1&ie=utf-8&oe=utf-8&app=test"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data[0] == "SUCCESS":
                return data[1][0][1][0]
    except Exception:
        pass
    return text

results = [
    f"'The Warrior' -> '{transliterate_to_hindi('The Warrior')}'",
    f"'Secret Fauji' -> '{transliterate_to_hindi('Secret Fauji')}'",
    f"'Tere Aane Se' -> '{transliterate_to_hindi('Tere Aane Se')}'"
]

with open("scratch/test_out_v3.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(results))
print("Results saved to scratch/test_out_v3.txt")
