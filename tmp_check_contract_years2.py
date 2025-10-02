import requests
import time
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
year = 2024
url = f"https://www.basketball-reference.com/contracts/players-{year}.html"
for attempt in range(6):
    res = requests.get(url, headers=headers, timeout=30)
    print(year, attempt, res.status_code)
    if res.status_code != 429:
        print(len(res.text))
        open(f'contracts_{year}.html','w',encoding='utf-8').write(res.text)
        break
    time.sleep(2*(attempt+1))
