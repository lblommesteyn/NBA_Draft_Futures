"""Convert Kaggle NBA salaries CSV to data/player_salary.csv."""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd

CANDIDATE_PATHS = [
    Path("data") / "kaggle_nba_salaries.csv",
    Path("data") / "NBA Player Salaries_2000-2025.csv",
]
OUTPUT_PATH = Path("data") / "player_salary.csv"
MONEY_RE = re.compile(r"[^0-9.]")


def _clean_salary(value: str) -> float:
    if not isinstance(value, str):
        return float(value or 0.0)
    cleaned = MONEY_RE.sub("", value)
    return float(cleaned) if cleaned else 0.0


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


def _find_input_file() -> Path:
    for path in CANDIDATE_PATHS:
        if path.exists():
            return path
    raise FileNotFoundError("No salary CSV found in data/. Expected one of: " + ", ".join(str(p) for p in CANDIDATE_PATHS))


def main() -> None:
    input_path = _find_input_file()
    df = pd.read_csv(input_path)
    if not {"Player", "Season", "Salary"}.issubset(df.columns):
        raise ValueError("Salary file missing required columns (Player, Season, Salary)")
    df = df.rename(columns={"Player": "player", "Season": "season", "Salary": "salary"})
    df["player"] = df["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["season_end"] = df["season"].astype(str).str.slice(0, 4).astype(int)
    df["salary"] = df["salary"].astype(str).map(_clean_salary)
    df["canonical_name"] = df["player"].apply(_canonicalize)
    df = df[["player", "canonical_name", "season_end", "salary"]]
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Using {input_path.name}; wrote {len(df):,} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
