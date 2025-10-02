"""Scrape RealGM roster salaries into data/player_salary.csv."""
from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd
import requests

TEAM_INFO: Dict[str, tuple[int, str]] = {
    "ATL": (1, "atlanta-hawks"),
    "BOS": (2, "boston-celtics"),
    "BKN": (38, "brooklyn-nets"),
    "CHA": (30, "charlotte-hornets"),
    "CHI": (4, "chicago-bulls"),
    "CLE": (5, "cleveland-cavaliers"),
    "DAL": (6, "dallas-mavericks"),
    "DEN": (7, "denver-nuggets"),
    "DET": (8, "detroit-pistons"),
    "GSW": (9, "golden-state-warriors"),
    "HOU": (10, "houston-rockets"),
    "IND": (11, "indiana-pacers"),
    "LAC": (12, "los-angeles-clippers"),
    "LAL": (13, "los-angeles-lakers"),
    "MEM": (29, "memphis-grizzlies"),
    "MIA": (15, "miami-heat"),
    "MIL": (16, "milwaukee-bucks"),
    "MIN": (17, "minnesota-timberwolves"),
    "NOP": (3, "new-orleans-pelicans"),
    "NYK": (20, "new-york-knicks"),
    "OKC": (21, "oklahoma-city-thunder"),
    "ORL": (22, "orlando-magic"),
    "PHI": (23, "philadelphia-76ers"),
    "PHX": (24, "phoenix-suns"),
    "POR": (25, "portland-trail-blazers"),
    "SAC": (26, "sacramento-kings"),
    "SAS": (27, "san-antonio-spurs"),
    "TOR": (28, "toronto-raptors"),
    "UTA": (18, "utah-jazz"),
    "WAS": (31, "washington-wizards"),
}

BASE_URL = "https://basketball.realgm.com/nba/teams/{slug}/{tid}/Rosters/{season_end}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://basketball.realgm.com/",
}
MONEY_RE = re.compile(r"[^0-9.]")


def _clean_money(value: str) -> float:
    if not isinstance(value, str):
        return float(value or 0.0)
    cleaned = MONEY_RE.sub("", value)
    return float(cleaned) if cleaned else 0.0


def _fetch_team_table(season_end: int, abbr: str) -> pd.DataFrame:
    team_id, slug = TEAM_INFO[abbr]
    url = BASE_URL.format(slug=slug, tid=team_id, season_end=season_end)
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    tables = pd.read_html(response.text)
    table = next((df for df in tables if {"Player", "Salary"}.issubset(df.columns)), None)
    if table is None:
        return pd.DataFrame(columns=["player", "season_end", "salary"])
    cleaned = table.rename(columns={"Player": "player"}).copy()
    cleaned["salary"] = cleaned["Salary"].astype(str).map(_clean_money)
    cleaned["player"] = cleaned["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    cleaned["season_end"] = season_end
    return cleaned[["player", "season_end", "salary"]]


def fetch_league_salaries(season_end: int) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_team_table, season_end, abbr): abbr for abbr in TEAM_INFO}
        frames = []
        for future in as_completed(futures):
            try:
                frames.append(future.result())
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: failed fetching salaries for {season_end} {futures[future]}: {exc}")
    if not frames:
        return pd.DataFrame(columns=["player", "season_end", "salary"])
    league = pd.concat(frames, ignore_index=True)
    if league.empty:
        return league
    # Players may appear multiple times (trades/10-days). Take the max salary reported for the season.
    league = league.groupby(["player", "season_end"], as_index=False)["salary"].max()
    return league


def main(start_season: int = 2016, end_season: int = 2024) -> None:
    out_frames = []
    for season_end in range(start_season, end_season + 1):
        print(f"Fetching salaries for {season_end}...")
        df = fetch_league_salaries(season_end)
        out_frames.append(df)
        time.sleep(0.6)  # gentle pacing
    all_salaries = pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame(
        columns=["player", "season_end", "salary"]
    )
    output_path = Path("data") / "player_salary.csv"
    output_path.parent.mkdir(exist_ok=True)
    all_salaries.to_csv(output_path, index=False)
    print(f"Wrote {len(all_salaries):,} rows to {output_path}")


if __name__ == "__main__":
    main()
