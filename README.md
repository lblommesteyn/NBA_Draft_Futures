# NBA Draft Futures Analysis

This repository contains analysis and tools for evaluating NBA draft pick value and salary cap management strategies, with a focus on identifying arbitrage opportunities in the NBA draft market.

## Repository Structure

```
├── data/                   # Raw and processed data files
│   ├── NBA Player Salaries_2000-2025.csv
│   ├── draft_classes.csv
│   ├── modern_RAPTOR_by_player.csv
│   ├── pick_costs_prepared.csv
│   ├── pick_outcomes_first4.csv
│   ├── player_salary.csv
│   └── ...
├── docs/                   # Documentation
│   └── methodology.md
├── figs/                   # Generated figures
│   ├── figure1_arbitrage_map.png
│   └── figure2_arbitrage_scenarios.png
├── src/                    # Source code
│   ├── bbr_cap.py
│   ├── bbr_draft.py
│   ├── bbr_player_salaries.py
│   ├── build_data.py
│   ├── process_arbitrage.py
│   └── ...
└── tables/                 # Output tables
    ├── pick_bucket_summary.csv
    ├── table1_arbitrage_summary.csv
    └── ...
```

## Key Features

- **Draft Pick Valuation**: Analysis of historical draft pick values and outcomes
- **Salary Cap Analysis**: Tools for evaluating player contracts and team salary situations
- **Arbitrage Detection**: Identification of potential market inefficiencies in draft pick valuation
- **Scenario Modeling**: Simulation of different team building strategies under various salary cap scenarios

## Getting Started

### Prerequisites

- Python 3.8+
- Required Python packages (install via `pip install -r requirements.txt`)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/lblommesteyn/NBA_Draft_Futures.git
   cd NBA_Draft_Futures
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Data Processing

To process the raw data and generate analysis:

```bash
python src/build_data.py
python src/process_arbitrage.py
```

### Running Analysis

Run the main analysis scripts:

```bash
python src/bbr_cap.py
python src/bbr_draft.py
```


## Contact

For questions or feedback, please open an issue in the repository.
