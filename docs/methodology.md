# Pick–Cap Arbitrage Methodology

## Data Sources
- **Free agency performance and wins:** FiveThirtyEight "modern_RAPTOR_by_player.csv" (2006-07 onward). Fields include player name, `player_id`, `season`, RAPTOR offensive/defensive metrics, and `war_total` (wins above replacement).
- **Salary history:** Kaggle dataset "NBA Player Salaries 2000-2025". Only three columns (`Player`, `Season`, `Salary`); the pipeline normalizes names and converts the salary string to numeric values.
- **Draft outcomes:** Basketball-Reference draft pages (`https://www.basketball-reference.com/draft/NBA_{season}.html`). Pick slots, teams, and player names are scraped directly.

All player names are canonicalized (remove punctuation, accents, and generational suffixes; lowercase) before joining across sources.

## Processing Pipeline (`src/build_data.py`)
1. **RAPTOR WAR ingestion**  
   - Filter seasons 2017–2024 and retain `war_total` as the market benchmark.  
   - Augment with canonical name key; persist as `data/player_war.csv`.
2. **Salary ingestion**  
   - Convert Kaggle salaries to canonical names.  
   - Aggregate duplicate entries (e.g., 10-day contracts) by taking the maximum salary per player-season.  
   - Persist cleaned file (`data/player_salary_clean.csv`).
3. **Draft history**  
   - Collect draft classes 2016–2020.  
   - For each player, compute canonical name and keep pick metadata.
4. **Market alignment**  
   - Inner-join salary and WAR by canonical name and season to produce `data/salary_market_raw.csv`.  
   - Only seasons with positive salary and WAR are retained.  
5. **Rookie pick outcomes**  
   - For each drafted player, sum WAR and salary over the first four seasons after the draft (`war_first4`, `cost_first4`).  
   - Output stored in `data/pick_outcomes_first4.csv`.
6. **Summary diagnostics**  
   - `data/build_summary.json` logs row counts for WAR, salary, market, and pick tables.

## Pricing and Arbitrage Calculation (`src/process_arbitrage.py`)
1. **Free agency price surface**  
   - Compute $/WAR for every joined record (`salary / war`).  
   - Aggregate to market-wide quartiles (25th/50th/75th) → baseline FA price band.  
2. **Rookie pick implied cost**  
   - Convert rookie cost to $/WAR per season: `(cost_first4 / war_first4) / 4`.  
   - Group picks into buckets (01-05, 06-10, 11-20, 21-30, 31-45, 46-60).  
   - For each bucket, record the median and interquartile range.
3. **Arbitrage decision rule**  
   - "BUY" if rookie $/WAR falls below the FA 25th percentile minus 7% friction premium.  
   - "SELL" if rookie $/WAR exceeds the FA 75th percentile plus 7%.  
   - Otherwise, "NEUTRAL".
4. **Outputs**  
   - `figs/figure1_arbitrage_map.png` – baseline chart.  
   - `tables/table1_arbitrage_summary.csv` – baseline metrics, expressed in $M per WAR.  
   - Raw tables for further analysis (`pick_bucket_summary.csv`, `salary_price_band_overall.csv`).

## Scenario Panel (`figs/figure2_arbitrage_scenarios.png`)
We generate three state-dependent variants using the same pick yield curve:
1. **Thin FA class** – restrict FA data to seasons where the median $/WAR sits in the top quartile.  
2. **Deep FA class** – restrict to bottom-quartile seasons.  
3. **Second apron pressure** – apply a +10% markup to the baseline FA band to mimic teams operating above the second apron.

Each scenario emits both raw and formatted tables under `tables/table_scenario_*.csv` (and corresponding `_formatted` versions in $M per WAR).

## How to reproduce
```bash
python src/ingest_salaries_from_kaggle.py  # only needed if the Kaggle CSV changes
python src/build_data.py                  # rebuild market + pick datasets
python src/process_arbitrage.py           # refresh figures and tables
```

## Extending the framework
- **Alternative metrics:** Swap RAPTOR for EPM/LEBRON by replacing the WAR CSV and editing `load_war_data`.  
- **Archetype splits:** Add a position/archetype column prior to the joins, then filter `market` accordingly when building FA bands.  
- **Risk adjustments:** Replace the variance-based friction rule with CVaR or utility-based certainty equivalents as outlined in the planning notes.

