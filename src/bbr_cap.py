"""Utility functions for scraping Basketball Reference salary cap history."""
from __future__ import annotations

from dataclasses import dataclass
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

CAP_URL = "https://www.basketball-reference.com/contracts/salary-cap-history.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.basketball-reference.com/"
}


@dataclass
class CapRecord:
    season: str
    season_start: int
    cap: float


def _clean_currency(series: pd.Series) -> pd.Series:
    """Convert a salary-like string column to numeric."""
    return (
        series.astype(str)
        .str.replace(r"[$,]", "", regex=True)
        .str.strip()
        .replace({"": None, "nan": None})
        .astype(float)
    )


def _request_with_retry(url: str, max_attempts: int = 5, backoff: float = 1.5) -> requests.Response:
    delay = 0.5
    last_response: requests.Response | None = None
    for _ in range(max_attempts):
        time.sleep(delay)
        response = requests.get(url, headers=HEADERS, timeout=30)
        last_response = response
        if response.status_code == 429:
            delay = min(delay * backoff, 5.0)
            continue
        if response.ok:
            return response
        delay = min(delay * backoff, 5.0)
    if last_response is None:
        raise RuntimeError(f"Failed to fetch {url}")
    last_response.raise_for_status()
    return last_response


def get_cap_history() -> pd.DataFrame:
    """Return season salary cap history as a DataFrame."""
    response = _request_with_retry(CAP_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            comment_soup = BeautifulSoup(comment, "html.parser")
            tables = comment_soup.find_all("table")
            if tables:
                break
    if not tables:
        raise ValueError("Failed to locate cap history table on Basketball Reference")
    df = pd.read_html(str(tables[0]))[0]
    if "Year" not in df.columns or "Salary Cap" not in df.columns:
        raise ValueError("Unexpected cap history table schema")
    df = df.loc[:, ["Year", "Salary Cap"]].copy()
    df["season_start"] = df["Year"].str.slice(0, 4).astype(int)
    df["cap"] = _clean_currency(df["Salary Cap"])
    df = df.rename(columns={"Year": "season"})
    return df[["season", "season_start", "cap"]]


__all__ = ["CapRecord", "get_cap_history"]
