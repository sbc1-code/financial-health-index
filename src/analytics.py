"""
Analytics layer for the Community Financial Health Index.
Runs SQL queries against DuckDB, returns pandas DataFrames.

All dashboard data flows through these functions.
Auto-seeds if the database is missing but processed data exists.
"""

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "financial.duckdb"
PROCESSED_PATH = Path(__file__).parent.parent / "data" / "processed" / "county_financial_health.json"

# Border county FIPS codes (US-Mexico border), from VerdeAzul
BORDER_FIPS = [
    # Texas
    "48141", "48243", "48377", "48043", "48371", "48443", "48465",
    "48323", "48479", "48427", "48215", "48061", "48505", "48247", "48311",
    # New Mexico
    "35013", "35023", "35029", "35035",
    # Arizona
    "04003", "04019", "04023", "04027",
    # California
    "06025", "06073",
]


def _ensure_db():
    """Check DB exists. Auto-seed if processed data available."""
    if DB_PATH.exists():
        try:
            con = duckdb.connect(str(DB_PATH), read_only=True)
            count = con.execute("SELECT COUNT(*) FROM counties").fetchone()[0]
            con.close()
            if count > 0:
                return True
        except Exception:
            pass

    # Try auto-seed
    if PROCESSED_PATH.exists():
        print("Database not found. Auto-seeding from processed data...")
        from src.seed import seed
        seed()
        return DB_PATH.exists()

    print("No database or processed data found.")
    print("Run the full pipeline:")
    print("  python -m src.ingest")
    print("  python -m src.clean")
    print("  python -m src.seed")
    return False


def _query(sql, params=None):
    """Execute SQL query and return a DataFrame."""
    _ensure_db()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if params:
            result = con.execute(sql, params).fetchdf()
        else:
            result = con.execute(sql).fetchdf()
        return result
    finally:
        con.close()


def get_overview_stats():
    """
    KPIs: county count, banking desert count, avg exclusion score,
    most excluded state.
    """
    return _query("""
        SELECT
            COUNT(*) AS total_counties,
            SUM(CASE WHEN is_banking_desert THEN 1 ELSE 0 END) AS banking_desert_count,
            ROUND(AVG(exclusion_score), 2) AS avg_exclusion_score,
            (
                SELECT state FROM counties
                GROUP BY state
                ORDER BY AVG(exclusion_score) DESC
                LIMIT 1
            ) AS most_excluded_state
        FROM counties
    """)


def get_all_counties():
    """All counties with scores for table display."""
    return _query("""
        SELECT
            fips, name, state, population,
            median_income, poverty_rate,
            bank_branches, branches_per_10k, total_deposits,
            is_banking_desert, is_at_risk_desert,
            state_complaints_per_100k, state_top_complaint,
            state_unbanked_pct, state_underbanked_pct,
            access_score, distress_score, exclusion_score, quadrant
        FROM counties
        ORDER BY exclusion_score DESC
    """)


def get_county_detail(fips):
    """Full detail for a single county."""
    return _query("""
        SELECT *
        FROM counties
        WHERE fips = $1
    """, [fips])


def get_top_excluded_counties(limit=10):
    """Counties with the highest exclusion scores."""
    return _query("""
        SELECT
            fips, name, state, population,
            median_income, poverty_rate,
            bank_branches, branches_per_10k,
            state_unbanked_pct,
            access_score, distress_score, exclusion_score, quadrant
        FROM counties
        ORDER BY exclusion_score DESC
        LIMIT $1
    """, [limit])


def get_quadrant_distribution():
    """Count of counties per quadrant."""
    return _query("""
        SELECT
            quadrant,
            COUNT(*) AS count,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(distress_score), 1) AS avg_distress,
            ROUND(AVG(exclusion_score), 1) AS avg_exclusion,
            ROUND(AVG(population)) AS avg_population,
            ROUND(AVG(median_income)) AS avg_income
        FROM counties
        GROUP BY quadrant
        ORDER BY count DESC
    """)


def get_state_summary():
    """Aggregated metrics by state."""
    return _query("""
        SELECT
            state,
            COUNT(*) AS county_count,
            SUM(population) AS total_population,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(distress_score), 1) AS avg_distress,
            ROUND(AVG(exclusion_score), 1) AS avg_exclusion,
            SUM(CASE WHEN is_banking_desert THEN 1 ELSE 0 END) AS banking_deserts,
            ROUND(AVG(median_income)) AS avg_income,
            ROUND(AVG(poverty_rate), 1) AS avg_poverty,
            ROUND(AVG(branches_per_10k), 1) AS avg_branches_per_10k
        FROM counties
        GROUP BY state
        ORDER BY avg_exclusion DESC
    """)


def get_scatter_data():
    """Distress vs access score for scatter plot."""
    return _query("""
        SELECT
            fips, name, state, population,
            access_score, distress_score, exclusion_score, quadrant,
            is_banking_desert,
            bank_branches, branches_per_10k,
            median_income, poverty_rate,
            state_unbanked_pct
        FROM counties
        WHERE access_score IS NOT NULL AND distress_score IS NOT NULL
        ORDER BY population DESC
    """)


def get_county_financial_profile(fips):
    """Financial metrics for a single county detail view."""
    return _query("""
        SELECT
            fips, name, state, population,
            median_income, poverty_rate,
            pct_white, pct_black, pct_hispanic,
            bank_branches, branches_per_10k, total_deposits,
            is_banking_desert, is_at_risk_desert,
            state_complaints_per_100k, state_top_complaint,
            state_unbanked_pct, state_underbanked_pct,
            access_score, distress_score, exclusion_score, quadrant
        FROM counties
        WHERE fips = $1
    """, [fips])


def get_national_averages():
    """National benchmark averages for comparison."""
    return _query("""
        SELECT
            ROUND(AVG(population)) AS avg_population,
            ROUND(AVG(median_income)) AS avg_income,
            ROUND(AVG(poverty_rate), 1) AS avg_poverty,
            ROUND(AVG(branches_per_10k), 1) AS avg_branches_per_10k,
            ROUND(AVG(bank_branches)) AS avg_branches,
            ROUND(AVG(state_unbanked_pct), 1) AS avg_unbanked,
            ROUND(AVG(state_underbanked_pct), 1) AS avg_underbanked,
            ROUND(AVG(state_complaints_per_100k), 1) AS avg_complaints_per_100k,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(distress_score), 1) AS avg_distress,
            ROUND(AVG(exclusion_score), 1) AS avg_exclusion
        FROM counties
    """)


def get_banking_deserts():
    """All counties with 0 bank branches."""
    return _query("""
        SELECT
            fips, name, state, population,
            median_income, poverty_rate,
            bank_branches, branches_per_10k,
            state_unbanked_pct, state_underbanked_pct,
            access_score, distress_score, exclusion_score, quadrant
        FROM counties
        WHERE is_banking_desert = true
        ORDER BY population DESC
    """)


def get_desert_count_by_state():
    """Number of banking deserts per state."""
    return _query("""
        SELECT
            state,
            COUNT(*) AS desert_count,
            SUM(population) AS affected_population
        FROM counties
        WHERE is_banking_desert = true
        GROUP BY state
        ORDER BY desert_count DESC
    """)


def get_border_comparison():
    """
    Border vs non-border county comparison.
    Uses the same border FIPS list as VerdeAzul.
    """
    border_list = ", ".join(f"'{f}'" for f in BORDER_FIPS)
    return _query(f"""
        SELECT
            CASE WHEN fips IN ({border_list}) THEN 'Border' ELSE 'Non-Border' END AS category,
            COUNT(*) AS county_count,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(distress_score), 1) AS avg_distress,
            ROUND(AVG(exclusion_score), 1) AS avg_exclusion,
            ROUND(AVG(median_income)) AS avg_income,
            ROUND(AVG(poverty_rate), 1) AS avg_poverty,
            ROUND(AVG(branches_per_10k), 1) AS avg_branches_per_10k,
            ROUND(AVG(state_unbanked_pct), 1) AS avg_unbanked,
            SUM(CASE WHEN is_banking_desert THEN 1 ELSE 0 END) AS banking_deserts
        FROM counties
        GROUP BY CASE WHEN fips IN ({border_list}) THEN 'Border' ELSE 'Non-Border' END
    """)
