"""Scrapers for extracting free-agent signing events from Basketball Reference."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

TRANSACTIONS_URL = "https://www.basketball-reference.com/leagues/NBA_{season}_transactions.html"
FREE_AGENT_PATTERN = re.compile(r"signed as a free agent", re.IGNORECASE)


@dataclass
class FreeAgentSigning:
    season_end: int
    date: str
    player: str
    team: str


def _extract_signing_rows(soup: BeautifulSoup) -> Iterable[str]:
    """Yield transaction text snippets that look like free-agent signings."""
    for paragraph in soup.select("#content p"):
        text = paragraph.get_text(" ", strip=True)
        if FREE_AGENT_PATTERN.search(text):
            yield text


def _parse_signing_text(text: str, season_end: int) -> FreeAgentSigning | None:
    """Parse a free-agent signing line into structured data."""
    if " - " not in text:
        return None
    date_part, body = text.split(" - ", 1)
    marker = " signed as a free agent with "
    if marker not in body:
        return None
    player, team = body.split(marker, 1)
    team = team.rstrip(".").strip()
    return FreeAgentSigning(
        season_end=season_end,
        date=date_part.strip(),
        player=player.strip(),
        team=team,
    )


def get_free_agent_signings(season_end: int) -> pd.DataFrame:
    """Return free-agent signings for the off-season preceding `season_end`.

    Example: season_end=2024 captures summer 2023 signings.
    """
    url = TRANSACTIONS_URL.format(season=season_end)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    signings: List[FreeAgentSigning] = []
    for text in _extract_signing_rows(soup):
        record = _parse_signing_text(text, season_end)
        if record is not None:
            signings.append(record)
    return pd.DataFrame([s.__dict__ for s in signings])


__all__ = ["FreeAgentSigning", "get_free_agent_signings"]
