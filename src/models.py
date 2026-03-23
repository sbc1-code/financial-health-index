"""
DuckDB schema for the Community Financial Health Index.

Creates the counties table with all metrics, scores, and quadrant assignments.
"""

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "financial.duckdb"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS counties (
    fips TEXT PRIMARY KEY,
    name TEXT,
    state TEXT,
    state_fips TEXT,
    population INTEGER,
    median_income REAL,
    poverty_rate REAL,
    pct_white REAL,
    pct_black REAL,
    pct_hispanic REAL,
    latitude REAL,
    longitude REAL,
    bank_branches INTEGER,
    branches_per_10k REAL,
    total_deposits REAL,
    is_banking_desert BOOLEAN,
    is_at_risk_desert BOOLEAN,
    state_complaints_per_100k REAL,
    state_top_complaint TEXT,
    state_unbanked_pct REAL,
    state_underbanked_pct REAL,
    access_score REAL,
    distress_score REAL,
    exclusion_score REAL,
    quadrant TEXT
);
"""


def get_connection():
    """Get a DuckDB connection to the database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def create_schema(con=None):
    """Create the counties table. Drops existing table first."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True

    con.execute("DROP TABLE IF EXISTS counties")
    con.execute(SCHEMA_SQL)

    if close_after:
        con.close()


def db_exists():
    """Check if the database file exists and has data."""
    if not DB_PATH.exists():
        return False
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        result = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'counties'"
        ).fetchone()
        has_table = result[0] > 0
        if has_table:
            count = con.execute("SELECT COUNT(*) FROM counties").fetchone()
            has_data = count[0] > 0
        else:
            has_data = False
        con.close()
        return has_data
    except Exception:
        return False
