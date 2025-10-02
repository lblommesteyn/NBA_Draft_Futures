"""Retrieve NBA draft data from Basketball Reference."""
from __future__ import annotations

import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

DRAFT_URL_TEMPLATE = "https://www.basketball-reference.com/draft/NBA_{season_end}.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.basketball-reference.com/"
}


def _request_with_retry(url: str, max_attempts: int = 6, backoff: float = 1.5) -> requests.Response | None:
    delay = 1.5
    for _ in range(max_attempts):
        time.sleep(delay)
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 429:
            delay = min(delay * backoff, 8.0)
            continue
        if response.ok:
            return response
        delay = min(delay * backoff, 8.0)
    return None


def fetch_draft_class(season_end: int) -> pd.DataFrame:
    """Return draft picks for a given draft year (season_end)."""
    response = _request_with_retry(DRAFT_URL_TEMPLATE.format(season_end=season_end))
    if response is None:
        return pd.DataFrame(columns=["season_end", "pick", "team", "player_name", "player_slug"])
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="stats")
    if table is None:
        return pd.DataFrame(columns=["season_end", "pick", "team", "player_name", "player_slug"])
    rows = []
    for tr in table.tbody.find_all("tr"):
        if "class" in tr.attrs and "thead" in tr["class"]:
            continue
        pick_cell = tr.find(attrs={"data-stat": "pick_overall"})
        player_cell = tr.find("td", {"data-stat": "player"})
        team_cell = tr.find("td", {"data-stat": "team_id"})
        if pick_cell is None or player_cell is None:
            continue
        pick_text = pick_cell.get_text(strip=True)
        if not pick_text.isdigit():
            continue
        link = player_cell.find("a")
        if link is None:
            continue
        slug = link["href"].split("/")[-1].replace(".html", "")
        rows.append(
            {
                "season_end": season_end,
                "pick": int(pick_text),
                "team": team_cell.get_text(strip=True) if team_cell else "",
                "player_name": player_cell.get_text(strip=True),
                "player_slug": slug,
            }
        )
    return pd.DataFrame(rows)


__all__ = ["fetch_draft_class"]
