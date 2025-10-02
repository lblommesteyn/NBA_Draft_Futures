"""Salary scraping utilities for RealGM team pages."""
from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable

import pandas as pd
import requests

TEAM_IDS: Dict[str, str] = {
    "ATL": "atlanta-hawks",
    "BOS": "boston-celtics",
    "BKN": "brooklyn-nets",
    "CHA": "charlotte-hornets",
    "CHI": "chicago-bulls",
    "CLE": "cleveland-cavaliers",
    "DAL": "dallas-mavericks",
    "DEN": "denver-nuggets",
    "DET": "detroit-pistons",
    "GSW": "golden-state-warriors",
    "HOU": "houston-rockets",
    "IND": "indiana-pacers",
    "LAC": "los-angeles-clippers",
    "LAL": "los-angeles-lakers",
    "MEM": "memphis-grizzlies",
    "MIA": "miami-heat",
    "MIL": "milwaukee-bucks",
    "MIN": "minnesota-timberwolves",
    "NOP": "new-orleans-pelicans",
    "NYK": "new-york-knicks",
    "OKC": "oklahoma-city-thunder",
    "ORL": "orlando-magic",
    "PHI": "philadelphia-76ers",
    "PHX": "phoenix-suns",
    "POR": "portland-trail-blazers",
    "SAC": "sacramento-kings",
    "SAS": "san-antonio-spurs",
    "TOR": "toronto-raptors",
    "UTA": "utah-jazz",
    "WAS": "washington-wizards",
}

TEAM_META: Dict[str, int] = {
    "ATL": 1,
    "BOS": 2,
    "BKN": 38,
    "CHA": 30,
    "CHI": 4,
    "CLE": 5,
    "DAL": 6,
    "DEN": 7,
    "DET": 8,
    "GSW": 9,
    "HOU": 10,
    "IND": 11,
    "LAC": 12,
    "LAL": 13,
    "MEM": 29,
    "MIA": 15,
    "MIL": 16,
    "MIN": 17,
    "NOP": 3,
    "NYK": 20,
    "OKC": 21,
    "ORL": 22,
    "PHI": 23,
    "PHX": 24,
    "POR": 25,
    "SAC": 26,
    "SAS": 27,
    "TOR": 28,
    "UTA": 18,
    "WAS": 31,
}

BASE_URL = "https://basketball.realgm.com/nba/teams/{slug}/{tid}/Rosters/{season}"
MONEY_RE = re.compile(r"[^0-9.]")


def _clean_money(value: str) -> float:
    if not isinstance(value, str):
        return float(value or 0.0)
    cleaned = MONEY_RE.sub("", value)
    if cleaned == "":
        return 0.0
    return float(cleaned)


def fetch_team_salaries(team_abbr: str, season_end: int) -> pd.DataFrame:
    slug = TEAM_IDS[team_abbr]
    team_id = TEAM_META[team_abbr]
    url = BASE_URL.format(slug=slug, tid=team_id, season=season_end)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    tables = pd.read_html(response.text)
    salary_table = None
    for table in tables:
        if "Salary" in table.columns:
            salary_table = table
            break
    if salary_table is None:
        return pd.DataFrame(columns=["player", "team", "season_end", "salary"])
    salary_table = salary_table.rename(columns={"Player": "player"})
    if "player" not in salary_table.columns or "Salary" not in salary_table.columns:
        return pd.DataFrame(columns=["player", "team", "season_end", "salary"])
    salary_table["Salary"] = salary_table["Salary"].astype(str).apply(_clean_money)
    salary_table["team"] = team_abbr
    salary_table["season_end"] = season_end
    return salary_table[["player", "team", "season_end", "Salary"]]


def get_league_salaries(season_end: int) -> pd.DataFrame:
    teams: Iterable[str] = TEAM_IDS.keys()
    with ThreadPoolExecutor(max_workers=8) as executor:
        frames = list(executor.map(lambda abbr: fetch_team_salaries(abbr, season_end), teams))
    if not frames:
        return pd.DataFrame(columns=["player", "season_end", "salary"])
    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=["player", "season_end", "salary"])
    combined = (
        combined.groupby(["player", "season_end"], as_index=False)["Salary"].max()
        .rename(columns={"Salary": "salary"})
    )
    return combined


__all__ = ["get_league_salaries", "fetch_team_salaries"]
