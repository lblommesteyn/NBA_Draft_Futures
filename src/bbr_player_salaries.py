"""Fetch player salary history from Basketball Reference."""
from __future__ import annotations

import re
import time
from functools import lru_cache

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

PLAYER_URL_TEMPLATE = "https://www.basketball-reference.com/players/{first}/{slug}.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.basketball-reference.com/"
}
MONEY_RE = re.compile(r"[^0-9.]")


def _clean_salary(value: str) -> float:
    if not isinstance(value, str):
        return float(value or 0.0)
    cleaned = MONEY_RE.sub("", value)
    if cleaned == "":
        return 0.0
    return float(cleaned)


def _request_with_retry(url: str, max_attempts: int = 6, backoff: float = 1.4) -> requests.Response | None:
    delay = 1.0
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


@lru_cache(maxsize=None)
def fetch_player_salaries(slug: str) -> pd.DataFrame:
    if not slug or len(slug) < 1:
        return pd.DataFrame(columns=["player_slug", "season_end", "salary"])
    first_letter = slug[0]
    url = PLAYER_URL_TEMPLATE.format(first=first_letter, slug=slug)
    response = _request_with_retry(url)
    if response is None:
        return pd.DataFrame(columns=["player_slug", "season_end", "salary"])
    soup = BeautifulSoup(response.text, "html.parser")
    salary_table_html = None
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if "id=\"all_salaries\"" in comment:
            salary_table_html = BeautifulSoup(comment, "html.parser").find("table")
            break
    if salary_table_html is None:
        return pd.DataFrame(columns=["player_slug", "season_end", "salary"])
    df = pd.read_html(str(salary_table_html))[0]
    if "Season" not in df.columns or "Salary" not in df.columns:
        return pd.DataFrame(columns=["player_slug", "season_end", "salary"])
    df = df[df["Season"].astype(str).str.contains("-")]

    def season_to_end(season: str) -> int:
        start, end = season.split("-")
        start_year = int(start)
        end_year = int(end) if len(end) == 4 else int(start[:2] + end)
        return end_year

    df = df.assign(
        season_end=df["Season"].apply(season_to_end),
        salary=df["Salary"].apply(_clean_salary),
        player_slug=slug,
    )
    return df[["player_slug", "season_end", "salary"]]


__all__ = ["fetch_player_salaries"]
