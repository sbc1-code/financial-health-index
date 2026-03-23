# Community Financial Health Index

Maps where Americans lack access to fair financial services by combining bank branch data, consumer complaints, Census demographics, and unbanked survey results into a county-level financial exclusion score.

## What It Does

- Identifies banking deserts: counties with zero FDIC-insured bank branches
- Scores every US county on financial access (0-100) and economic distress (0-100)
- Computes an exclusion score highlighting where distress outpaces access
- Provides rule-based intervention recommendations for underserved communities

## Why

An estimated 23 million Americans are unbanked or underbanked. Communities without bank branches are forced into payday lenders, check cashers, and other predatory alternatives. This tool makes the geographic pattern visible and actionable.

## Stack

- **Python 3.11** - ETL pipeline and data processing
- **DuckDB** - Embedded analytical database with SQL window functions for scoring
- **Streamlit + Plotly** - Interactive dashboard with choropleth maps and scatter plots
- **Census ACS, FDIC, CFPB** - All real public data, no synthetic or estimated values

## Data Sources

| Source | What | Level | Link |
|--------|------|-------|------|
| FDIC Summary of Deposits | Bank branch locations, deposit totals | County | [banks.data.fdic.gov](https://banks.data.fdic.gov/api/locations) |
| CFPB Consumer Complaints | Financial product complaints (2022-2025) | State | [consumerfinance.gov](https://www.consumerfinance.gov/data-research/consumer-complaints/) |
| Census ACS 5-Year | Population, income, poverty, race | County | [data.census.gov](https://data.census.gov) |
| FDIC Household Survey 2023 | Unbanked/underbanked rates | State/Region | [fdic.gov](https://www.fdic.gov/analysis/household-survey) |

## Known Limitations

- **CFPB ZIP codes truncated to 3 digits:** county-level complaint mapping is impossible, so state-level rates are used
- **FDIC unbanked survey is state-level:** all counties in a state share the same unbanked rate
- **No credit unions:** FDIC data covers banks only, not NCUA-regulated credit unions
- **No HMDA data:** mortgage lending patterns and denial rates are not yet integrated
- **Simple desert definition:** a county with 0 FDIC branches is a banking desert, without considering adjacent county proximity

## Run Locally

```bash
# Clone and setup
git clone https://github.com/sbc1-code/financial-health-index.git
cd financial-health-index
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Get a free Census API key at https://api.census.gov/data/key_signup.html
export CENSUS_API_KEY=your_key_here

# Run the pipeline
python -m src.ingest          # Download raw data (~30 min for FDIC + CFPB)
python -m src.clean           # Merge and standardize
python -m src.seed            # Load into DuckDB, compute scores

# Launch dashboard
streamlit run dashboard.py
```

Or use the Makefile:

```bash
make setup
make all
```

## Future Expansion

- **HMDA** - Home Mortgage Disclosure Act lending data
- **CRA** - Community Reinvestment Act bank ratings
- **NCUA** - Credit union branch locations
- **FCC Broadband** - Internet access for digital banking readiness
- **Fed Banking Deserts** - Federal Reserve research integration

## Author

**Sebastian Becerra** | [LinkedIn](https://www.linkedin.com/in/sebastianbecerra)
