"""Basketball Reference contract scraping helpers."""
from __future__ import annotations

import re
from typing import Sequence

import pandas as pd
import requests

CONTRACTS_URL = "https://www.basketball-reference.com/contracts/players.html"
SEASON_RE = re.compile(r"^\d{4}-\d{2}$")
NON_NUMERIC_RE = re.compile(r"[^0-9.]")


def _flatten_columns(columns: Sequence[tuple[str, str]]) -> list[str]:
    flat = []
    for top, bottom in columns:
        pieces = []
        if top and not top.startswith("Unnamed"):
            pieces.append(top)
        if bottom and not bottom.startswith("Unnamed"):
            pieces.append(bottom)
        flat_name = "_".join(pieces)
        flat.append(flat_name if flat_name else bottom)
    return flat


def _clean_salary(value: str) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s in {"nan", "", "Salary", "?"}:
        return 0.0
    s = NON_NUMERIC_RE.sub("", s)
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def fetch_player_contracts() -> pd.DataFrame:
    """Return the full player contracts table in long form."""
    response = requests.get(CONTRACTS_URL, timeout=30)
    response.raise_for_status()
    tables = pd.read_html(response.text, header=[0, 1])
    raw = tables[0]
    raw.columns = _flatten_columns(raw.columns)
    season_columns: Sequence[str] = [c for c in raw.columns if SEASON_RE.match(c.split("_")[-1])]
    meta_columns = [c for c in raw.columns if c not in season_columns]
    df = raw.melt(
        id_vars=meta_columns,
        value_vars=season_columns,
        var_name="season",
        value_name="salary",
    )
    rename_map = {c: c.split("_")[-1] for c in season_columns}
    df["season"] = df["season"].map(rename_map)
    player_col = next((c for c in meta_columns if c.endswith("Player")), None)
    team_col = next((c for c in meta_columns if c.endswith("Tm")), None)
    if player_col is None or team_col is None:
        raise ValueError("Expected player/team columns not found in contracts table")
    df = df.rename(columns={player_col: "player", team_col: "team"})
    df["salary"] = df["salary"].apply(_clean_salary)
    df["season_start"] = df["season"].str.slice(0, 4).astype(int)
    return df[["player", "team", "season", "season_start", "salary"]]


__all__ = ["fetch_player_contracts"]
