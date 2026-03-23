"""
Seed the DuckDB database from processed county data.

Loads data/processed/county_financial_health.json, computes access/distress/
exclusion scores via SQL window functions, assigns quadrants, and writes
to data/financial.duckdb.

Usage:
    python -m src.seed
"""

import json
from pathlib import Path

import duckdb

from src.models import DB_PATH, create_schema, get_connection

DATA_DIR = Path(__file__).parent.parent / "data"
PROCESSED_PATH = DATA_DIR / "processed" / "county_financial_health.json"


def seed():
    """Load processed JSON into DuckDB, compute scores and quadrants."""
    if not PROCESSED_PATH.exists():
        print("No processed data found. Run the pipeline first:")
        print("  python -m src.ingest")
        print("  python -m src.clean")
        return

    with open(PROCESSED_PATH) as f:
        counties = json.load(f)

    print(f"Seeding {len(counties)} counties into DuckDB...")

    # Create fresh database
    con = get_connection()
    create_schema(con)

    # Insert raw county data (scores will be computed via SQL after)
    insert_sql = """
        INSERT INTO counties (
            fips, name, state, state_fips, population,
            median_income, poverty_rate, pct_white, pct_black, pct_hispanic,
            latitude, longitude,
            bank_branches, branches_per_10k, total_deposits,
            is_banking_desert, is_at_risk_desert,
            state_complaints_per_100k, state_top_complaint,
            state_unbanked_pct, state_underbanked_pct,
            access_score, distress_score, exclusion_score, quadrant
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12,
            $13, $14, $15,
            $16, $17,
            $18, $19,
            $20, $21,
            NULL, NULL, NULL, NULL
        )
    """

    for c in counties:
        con.execute(insert_sql, [
            c["fips"], c["name"], c["state"], c["state_fips"], c["population"],
            c.get("median_income"), c.get("poverty_rate"),
            c.get("pct_white"), c.get("pct_black"), c.get("pct_hispanic"),
            c.get("latitude"), c.get("longitude"),
            c["bank_branches"], c["branches_per_10k"], c["total_deposits"],
            c["is_banking_desert"], c["is_at_risk_desert"],
            c["state_complaints_per_100k"], c["state_top_complaint"],
            c["state_unbanked_pct"], c["state_underbanked_pct"],
        ])

    print(f"  Inserted {len(counties)} rows")

    # Compute access_score via SQL window functions
    # Access Score (0-100, higher = better access):
    #   50% weight on branches_per_10k percentile rank
    #   20% weight on total_deposits percentile rank
    #   30% weight on inverse unbanked rate percentile rank
    print("  Computing access scores...")
    con.execute("""
        UPDATE counties SET access_score = scored.access_score
        FROM (
            SELECT fips,
                ROUND(
                    (PERCENT_RANK() OVER (ORDER BY branches_per_10k) * 50) +
                    (PERCENT_RANK() OVER (ORDER BY total_deposits) * 20) +
                    (PERCENT_RANK() OVER (ORDER BY (100 - COALESCE(state_unbanked_pct, 5))) * 30)
                , 1) AS access_score
            FROM counties
        ) AS scored
        WHERE counties.fips = scored.fips
    """)

    # Compute distress_score via SQL window functions
    # Distress Score (0-100, higher = more distress):
    #   40% weight on poverty rate percentile rank
    #   25% weight on complaint rate percentile rank
    #   20% weight on unbanked rate percentile rank
    #   15% weight on inverse income percentile rank
    print("  Computing distress scores...")
    con.execute("""
        UPDATE counties SET distress_score = scored.distress_score
        FROM (
            SELECT fips,
                ROUND(
                    (PERCENT_RANK() OVER (ORDER BY poverty_rate) * 40) +
                    (PERCENT_RANK() OVER (ORDER BY state_complaints_per_100k) * 25) +
                    (PERCENT_RANK() OVER (ORDER BY state_unbanked_pct) * 20) +
                    (PERCENT_RANK() OVER (ORDER BY (100000 - COALESCE(median_income, 50000))) * 15)
                , 1) AS distress_score
            FROM counties
        ) AS scored
        WHERE counties.fips = scored.fips
    """)

    # Compute exclusion_score = distress - access
    print("  Computing exclusion scores...")
    con.execute("""
        UPDATE counties
        SET exclusion_score = ROUND(distress_score - access_score, 1)
    """)

    # Assign quadrants
    print("  Assigning quadrants...")
    con.execute("""
        UPDATE counties
        SET quadrant = CASE
            WHEN distress_score <= 50 AND access_score >= 50 THEN 'well_served'
            WHEN distress_score <= 50 AND access_score < 50 THEN 'thin_access'
            WHEN distress_score > 50 AND access_score >= 50 THEN 'strained'
            WHEN distress_score > 50 AND access_score < 50 THEN 'financial_desert'
        END
    """)

    # Print summary
    total = con.execute("SELECT COUNT(*) FROM counties").fetchone()[0]
    deserts = con.execute(
        "SELECT COUNT(*) FROM counties WHERE is_banking_desert = true"
    ).fetchone()[0]
    avg_exclusion = con.execute(
        "SELECT ROUND(AVG(exclusion_score), 2) FROM counties"
    ).fetchone()[0]

    quad_dist = con.execute("""
        SELECT quadrant, COUNT(*) AS n
        FROM counties
        GROUP BY quadrant
        ORDER BY n DESC
    """).fetchall()

    most_excluded_state = con.execute("""
        SELECT state, ROUND(AVG(exclusion_score), 2) AS avg_exc
        FROM counties
        GROUP BY state
        ORDER BY avg_exc DESC
        LIMIT 1
    """).fetchone()

    print(f"\nDatabase seeded successfully at {DB_PATH}")
    print(f"  Total counties: {total}")
    print(f"  Banking deserts: {deserts}")
    print(f"  Avg exclusion score: {avg_exclusion}")
    if most_excluded_state:
        print(f"  Most excluded state: {most_excluded_state[0]} (avg {most_excluded_state[1]})")
    print(f"  Quadrant distribution:")
    for quad, n in quad_dist:
        print(f"    {quad}: {n}")

    con.close()
    print("\nRun `streamlit run dashboard.py` to view the dashboard.")


if __name__ == "__main__":
    seed()
