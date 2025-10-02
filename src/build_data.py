"""Build datasets for Pick?Cap Arbitrage Map using public data."""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import pandas as pd

from bbr_draft import fetch_draft_class

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RAPTOR_PATH = DATA_DIR / "modern_RAPTOR_by_player.csv"
SALARY_PATH = DATA_DIR / "player_salary.csv"


def _canonicalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = name.replace(".", "").replace(",", "").replace("'", "")
    name = name.replace("-", " ")
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[^a-zA-Z0-9 ]", "", name)
    name = re.sub(r"\s+", " ", name).strip().lower()
    return name


def load_war_data(start_season: int = 2017, end_season: int = 2024) -> pd.DataFrame:
    war = pd.read_csv(RAPTOR_PATH)
    war = war.rename(columns={"player_id": "player_slug", "season": "season_end", "war_total": "war"})
    war["season_end"] = war["season_end"].astype(int)
    war = war[(war["season_end"] >= start_season) & (war["season_end"] <= end_season)]
    war["canonical_name"] = war["player_name"].apply(_canonicalize)
    war = war[["player_slug", "player_name", "canonical_name", "season_end", "war"]]
    war.to_csv(DATA_DIR / "player_war.csv", index=False)
    return war


def load_salary_data(start_season: int = 2016, end_season: int = 2024) -> pd.DataFrame:
    salary = pd.read_csv(SALARY_PATH)
    if "canonical_name" not in salary.columns:
        salary["canonical_name"] = salary["player"].apply(_canonicalize)
    salary["season_end"] = salary["season_end"].astype(int)
    salary = salary[(salary["season_end"] >= start_season) & (salary["season_end"] <= end_season)]
    salary = salary.groupby(["canonical_name", "season_end"], as_index=False)["salary"].max()
    salary.to_csv(DATA_DIR / "player_salary_clean.csv", index=False)
    return salary


def build_draft_data(start_draft_year: int = 2016, end_draft_year: int = 2020) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for season_end in range(start_draft_year, end_draft_year + 1):
        try:
            df = fetch_draft_class(season_end)
            frames.append(df)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: failed to fetch draft {season_end}: {exc}")
    if not frames:
        raise RuntimeError("No draft data fetched")
    draft = pd.concat(frames, ignore_index=True)
    draft["canonical_name"] = draft["player_name"].apply(_canonicalize)
    draft.to_csv(DATA_DIR / "draft_classes.csv", index=False)
    return draft


def build_salary_market(war: pd.DataFrame, salary: pd.DataFrame) -> pd.DataFrame:
    market = war.merge(salary, on=["canonical_name", "season_end"], how="inner")
    market = market[market["war"] > 0]
    market.to_csv(DATA_DIR / "salary_market_raw.csv", index=False)
    return market


def build_pick_outcomes(
    draft: pd.DataFrame,
    war: pd.DataFrame,
    salary: pd.DataFrame,
    rookie_years: int = 4,
) -> pd.DataFrame:
    war_lookup = war.set_index(["player_slug", "season_end"])["war"].to_dict()
    salary_lookup = salary.set_index(["canonical_name", "season_end"])["salary"].to_dict()
    rows = []
    for _, pick in draft.iterrows():
        slug = pick["player_slug"]
        canonical = pick["canonical_name"]
        draft_year = int(pick["season_end"])
        war_sum = 0.0
        cost_sum = 0.0
        for season_end in range(draft_year + 1, draft_year + 1 + rookie_years):
            war_sum += war_lookup.get((slug, season_end), 0.0)
            cost_sum += salary_lookup.get((canonical, season_end), 0.0)
        rows.append(
            {
                "draft_year": draft_year,
                "pick": int(pick["pick"]),
                "player_slug": slug,
                "player_name": pick["player_name"],
                "canonical_name": canonical,
                "war_first4": war_sum,
                "cost_first4": cost_sum,
            }
        )
    df = pd.DataFrame(rows)
    df.to_csv(DATA_DIR / "pick_outcomes_first4.csv", index=False)
    return df


def main() -> None:
    war = load_war_data()
    salary = load_salary_data()
    draft = build_draft_data()
    market = build_salary_market(war, salary)
    picks = build_pick_outcomes(draft, war, salary)
    summary = {
        "war_rows": len(war),
        "salary_rows": len(salary),
        "market_rows": len(market),
        "pick_rows": len(picks),
        "unique_canonical_names": len(set(war["canonical_name"].unique()).union(draft["canonical_name"].unique())),
    }
    Path("data/build_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
