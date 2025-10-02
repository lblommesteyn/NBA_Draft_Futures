import requests
from bs4 import BeautifulSoup
url = "https://www.basketball-reference.com/leagues/NBA_2024_advanced.html"
res = requests.get(url, timeout=30)
soup = BeautifulSoup(res.text, "html.parser")
table = soup.find("table", id="advanced_stats")
print(table is not None)
first_row = table.find("tbody").find("tr")
player_cell = first_row.find("td", {"data-stat": "player"})
print(player_cell.find("a")["href"], player_cell.get_text(strip=True))
ws_cell = first_row.find("td", {"data-stat": "ws"})
print(ws_cell.get_text(strip=True))
