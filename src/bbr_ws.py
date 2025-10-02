"""Utilities for grabbing season-level Win Shares from Basketball Reference."""
from __future__ import annotations

import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

ADVANCED_URL_TEMPLATE = "https://www.basketball-reference.com/leagues/NBA_{season_end}_advanced.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.basketball-reference.com/"
}


def _request_with_retry(url: str, max_attempts: int = 7, backoff: float = 1.6) -> requests.Response:
    delay = 2.0
    last_response: requests.Response | None = None
    for _ in range(max_attempts):
        time.sleep(delay)
        response = requests.get(url, headers=HEADERS, timeout=60)
        last_response = response
        if response.status_code == 429:
            delay = min(delay * backoff, 10.0)
            continue
        if response.ok:
            return response
        delay = min(delay * backoff, 10.0)
    if last_response is None:
        raise RuntimeError(f"Failed to fetch {url}")
    last_response.raise_for_status()
    return last_response


def fetch_season_win_shares(season_end: int) -> pd.DataFrame:
    """Return a dataframe of player win shares for a given season."""
    response = _request_with_retry(ADVANCED_URL_TEMPLATE.format(season_end=season_end))
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="advanced")
    if table is None:
        raise ValueError(f"Advanced stats table not found for season {season_end}")
    rows = []
    for tr in table.tbody.find_all("tr"):
        if "class" in tr.attrs and "thead" in tr["class"]:
            continue
        player_cell = tr.find("td", {"data-stat": "name_display"})
        ws_cell = tr.find("td", {"data-stat": "ws"})
        team_cell = tr.find("td", {"data-stat": "team_name_abbr"})
        if player_cell is None or ws_cell is None or team_cell is None:
            continue
        slug = player_cell.get("data-append-csv")
        if not slug:
            continue
        ws_value = ws_cell.get_text(strip=True)
        try:
            ws = float(ws_value)
        except ValueError:
            ws = 0.0
        rows.append(
            {
                "player_slug": slug,
                "player_name": player_cell.get_text(strip=True),
                "team": team_cell.get_text(strip=True),
                "ws": ws,
                "season_end": season_end,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"No rows parsed for season {season_end}")
    tot = df[df["team"] == "TOT"]
    if not tot.empty:
        df = tot
    else:
        df = df.sort_values(["player_slug", "ws"], ascending=[True, False])
        df = df.drop_duplicates("player_slug", keep="first")
    return df[["player_slug", "player_name", "ws", "season_end"]]


__all__ = ["fetch_season_win_shares"]
