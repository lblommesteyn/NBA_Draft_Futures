"""Compute pricing bands, pick costs, and arbitrage map outputs."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Literal, Optional, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_DIR = Path("data")
FIG_DIR = Path("figs")
TABLE_DIR = Path("tables")

FIG_DIR.mkdir(exist_ok=True)
TABLE_DIR.mkdir(exist_ok=True)

PickBucket = Literal["01-05", "06-10", "11-20", "21-30", "31-45", "46-60"]

ZONE_COLORS = {"BUY": "#1b9e77", "NEUTRAL": "#9e9e9e", "SELL": "#d95f02"}
ZONE_TEXT_COLORS = {"BUY": "#0c4c33", "NEUTRAL": "#4b4b4b", "SELL": "#7f2704"}
BASELINE_BAND_FILL = "#cdd1ff"
SCENARIO_BAND_FILL = "#90a0ff"
SCENARIO_LINE_COLOR = "#263bd6"
BASELINE_LINE_COLOR = "#6b70c3"
PICK_LINE_COLOR = "#0b5bc4"

ORDER = ["01-05", "06-10", "11-20", "21-30", "31-45", "46-60"]


def assign_bucket(slot: int) -> PickBucket:
    if 1 <= slot <= 5:
        return "01-05"
    if 6 <= slot <= 10:
        return "06-10"
    if 11 <= slot <= 20:
        return "11-20"
    if 21 <= slot <= 30:
        return "21-30"
    if 31 <= slot <= 45:
        return "31-45"
    return "46-60"


def prepare_salary_pricing() -> pd.DataFrame:
    market = pd.read_csv(DATA_DIR / "salary_market_raw.csv")
    market = market[(market["salary"] > 0) & (market["war"] > 0)]
    market["dollars_per_war"] = market["salary"] / market["war"]
    market = market.replace([np.inf, -np.inf], np.nan).dropna(subset=["dollars_per_war"])
    market.to_csv(DATA_DIR / "salary_pricing_prepared.csv", index=False)
    band = compute_band(market)
    pd.DataFrame({"quantile": list(band.keys()), "value": list(band.values())}).to_csv(
        TABLE_DIR / "salary_price_band_overall.csv", index=False
    )
    return market


def prepare_pick_costs() -> pd.DataFrame:
    picks = pd.read_csv(DATA_DIR / "pick_outcomes_first4.csv")
    picks = picks[picks["war_first4"] > 0]
    picks["bucket"] = picks["pick"].apply(assign_bucket)
    picks["cost_per_war_per_season"] = (picks["cost_first4"] / picks["war_first4"]) / 4.0
    picks["war_per_season"] = picks["war_first4"] / 4.0
    picks.to_csv(DATA_DIR / "pick_costs_prepared.csv", index=False)
    return picks


def compute_band(market: pd.DataFrame) -> Dict[str, float]:
    quantiles = market["dollars_per_war"].quantile([0.25, 0.5, 0.75])
    return {"q25": quantiles.loc[0.25], "q50": quantiles.loc[0.5], "q75": quantiles.loc[0.75]}


def build_bucket_table(
    picks: pd.DataFrame,
    band: Dict[str, float],
    save_path: Optional[Path] = None,
) -> pd.DataFrame:
    grouped = picks.groupby("bucket")
    stats = grouped["cost_per_war_per_season"].agg(
        median="median",
        q25=lambda s: s.quantile(0.25),
        q75=lambda s: s.quantile(0.75),
    )
    war_stats = grouped["war_first4"].agg(war_med="median")
    cost_stats = grouped["cost_first4"].agg(cost_med="median")
    table = stats.join(war_stats).join(cost_stats).reset_index()
    table["market_q25"] = band["q25"]
    table["market_q50"] = band["q50"]
    table["market_q75"] = band["q75"]
    delta = 0.07

    def zone(row: pd.Series) -> str:
        if row["median"] < band["q25"] * (1 - delta):
            return "BUY"
        if row["median"] > band["q75"] * (1 + delta):
            return "SELL"
        return "NEUTRAL"

    table["arbitrage_zone"] = table.apply(zone, axis=1)
    table["market_equiv_cost_4yr"] = table["war_med"] * band["q50"]
    table["surplus_4yr"] = table["market_equiv_cost_4yr"] - table["cost_med"]
    if save_path is not None:
        table.to_csv(save_path, index=False)
    return table


def format_table_for_export(table: pd.DataFrame) -> pd.DataFrame:
    table_round = table.copy()
    for col in ["median", "q25", "q75", "market_q25", "market_q50", "market_q75"]:
        table_round[col] = table_round[col] / 1_000_000
    table_round["surplus_4yr"] = table_round["surplus_4yr"] / 1_000_000
    table_round.rename(
        columns={
            "median": "rookie_cost_per_war_mil",
            "q25": "rookie_cost_q25_mil",
            "q75": "rookie_cost_q75_mil",
            "market_q25": "market_cost_q25_mil",
            "market_q50": "market_cost_q50_mil",
            "market_q75": "market_cost_q75_mil",
            "surplus_4yr": "surplus_4yr_mil",
        },
        inplace=True,
    )
    return table_round


def plot_arbitrage_map(
    table: pd.DataFrame,
    band: Dict[str, float],
    *,
    ax: Optional[plt.Axes] = None,
    title: str = "Pick–Cap Arbitrage Map",
    save_path: Optional[Path] = None,
    show_legend: bool = True,
) -> None:
    table = table.set_index("bucket").loc[ORDER].reset_index()
    x = np.arange(len(ORDER))

    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 5))
        created_fig = True

    ax.fill_between(
        [-0.5, len(ORDER) - 0.5],
        band["q25"],
        band["q75"],
        color=BASELINE_BAND_FILL,
        alpha=0.35,
        label="FA price band (25-75th)" if show_legend else None,
    )
    ax.axhline(band["q50"], color=BASELINE_LINE_COLOR, linestyle="--", linewidth=1.4, label="FA median $/WAR" if show_legend else None)

    ax.plot(x, table["median"], color=PICK_LINE_COLOR, linewidth=2, alpha=0.85)
    yerr = np.vstack((table["median"] - table["q25"], table["q75"] - table["median"]))
    ax.errorbar(
        x,
        table["median"],
        yerr=yerr,
        fmt="none",
        ecolor=PICK_LINE_COLOR,
        alpha=0.45,
        capsize=5,
        linewidth=1.1,
    )

    colors = table["arbitrage_zone"].map(ZONE_COLORS).fillna("#9e9e9e")
    ax.scatter(x, table["median"], s=140, color=colors, edgecolor="white", linewidth=1.3, zorder=5)

    for xi, (_, row) in zip(x, table.iterrows()):
        zone = row["arbitrage_zone"]
        if zone:
            text_color = ZONE_TEXT_COLORS.get(zone, "#333")
            ax.text(
                xi,
                row["median"] * 1.05,
                zone,
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
                color=text_color,
                clip_on=True,
            )

    fa_high = max(band["q75"], table["q75"].max())
    ax.set_ylim(0, fa_high * 1.25)
    ax.set_xticks(x)
    ax.set_xticklabels(ORDER)
    ax.set_xlabel("Pick bucket")
    ax.set_ylabel("$ per WAR (per season)")
    ax.set_title(title)
    ax.set_xlim(-0.5, len(ORDER) - 0.5)
    ax.grid(alpha=0.2, linestyle=":", linewidth=0.8)
    if show_legend:
        ax.legend(loc="upper left", frameon=False)

    if created_fig:
        fig.tight_layout()
        if save_path is not None:
            fig.savefig(save_path, dpi=200)
        plt.close(fig)


def plot_scenario_bars(
    baseline: pd.DataFrame,
    scenario_tables: List[pd.DataFrame],
    scenario_titles: List[str],
    saves: List[Path],
) -> None:
    baseline_surplus = baseline.set_index("bucket")["surplus_4yr"] / 1_000_000
    fig, axes = plt.subplots(len(scenario_tables), 1, figsize=(8, 4 * len(scenario_tables)), sharex=True)
    if len(scenario_tables) == 1:
        axes = [axes]

    for ax, table, title, save_path in zip(axes, scenario_tables, scenario_titles, saves):
        table = table.set_index("bucket").loc[ORDER]
        surplus = table["surplus_4yr"] / 1_000_000
        colors = table["arbitrage_zone"].map(ZONE_COLORS).fillna("#9e9e9e")
        ax.barh(ORDER, surplus.loc[ORDER], color=colors.loc[ORDER], alpha=0.8)
        ax.axvline(0, color="#444", linewidth=1)
        ax.plot(baseline_surplus.loc[ORDER], ORDER, marker="o", color="#4f6cd6", linestyle="--", linewidth=1.2, label="Baseline surplus")
        for y, val, zone in zip(ORDER, surplus.loc[ORDER], table["arbitrage_zone"].loc[ORDER]):
            ax.text(
                val + (0.15 if val >= 0 else -0.15),
                y,
                f"{val:+.1f}M",
                va="center",
                ha="left" if val >= 0 else "right",
                color=ZONE_TEXT_COLORS.get(zone, "#444"),
                fontsize=9,
                fontweight="bold",
            )
        ax.set_title(title)
        ax.set_ylabel("Pick bucket")
        ax.grid(axis="x", linestyle=":", alpha=0.3)
        ax.legend(loc="lower right", frameon=False)
    axes[-1].set_xlabel("Scenario surplus vs FA (Millions $ over 4 years)")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure2_arbitrage_scenarios.png", dpi=200)
    plt.close(fig)


def main() -> None:
    market = prepare_salary_pricing()
    picks = prepare_pick_costs()

    band = compute_band(market)
    table_baseline = build_bucket_table(picks, band, save_path=TABLE_DIR / "pick_bucket_summary.csv")
    plot_arbitrage_map(table_baseline, band, save_path=FIG_DIR / "figure1_arbitrage_map.png")
    format_table_for_export(table_baseline).to_csv(TABLE_DIR / "table1_arbitrage_summary.csv", index=False)

    season_medians = market.groupby("season_end")["dollars_per_war"].median()
    high_threshold = season_medians.quantile(0.75)
    low_threshold = season_medians.quantile(0.25)
    high_seasons = season_medians[season_medians >= high_threshold].index.tolist()
    low_seasons = season_medians[season_medians <= low_threshold].index.tolist()

    scenario_specs = []
    scenario_titles = []
    scenario_paths = []

    market_high = market[market["season_end"].isin(high_seasons)]
    if not market_high.empty:
        scenario_specs.append(compute_band(market_high))
        scenario_titles.append("Thin FA class (top quartile)")
        scenario_paths.append(TABLE_DIR / "table_scenario_thin.csv")
    market_low = market[market["season_end"].isin(low_seasons)]
    if not market_low.empty:
        scenario_specs.append(compute_band(market_low))
        scenario_titles.append("Deep FA class (bottom quartile)")
        scenario_paths.append(TABLE_DIR / "table_scenario_deep.csv")

    scenario_specs.append({k: v * 1.10 for k, v in band.items()})
    scenario_titles.append("Second apron pressure (+10% FA $/WAR)")
    scenario_paths.append(TABLE_DIR / "table_scenario_apron.csv")

    scenario_tables = []
    for spec, path in zip(scenario_specs, scenario_paths):
        table = build_bucket_table(picks, spec, save_path=path)
        formatted_path = path.with_name(path.stem + "_formatted" + path.suffix)
        format_table_for_export(table).to_csv(formatted_path, index=False)
        scenario_tables.append(table)

    plot_scenario_bars(table_baseline, scenario_tables, scenario_titles, scenario_paths)


if __name__ == "__main__":
    main()
