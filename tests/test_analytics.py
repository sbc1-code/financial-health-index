"""
Tests for the analytics layer.

Auto-seeds the database if it does not exist but processed data is available.
Uses pytest. Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import analytics
from src.models import db_exists

PROCESSED_PATH = Path(__file__).parent.parent / "data" / "processed" / "county_financial_health.json"


@pytest.fixture(scope="session", autouse=True)
def ensure_database():
    """Ensure the database exists before running tests."""
    if not db_exists():
        if PROCESSED_PATH.exists():
            from src.seed import seed
            seed()
        else:
            pytest.skip(
                "No database or processed data. Run: python -m src.ingest && python -m src.clean && python -m src.seed"
            )


def test_get_overview_stats():
    df = analytics.get_overview_stats()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["total_counties"].iloc[0] > 0
    assert df["banking_desert_count"].iloc[0] >= 0
    assert df["avg_exclusion_score"].iloc[0] is not None
    assert df["most_excluded_state"].iloc[0] is not None


def test_get_all_counties():
    df = analytics.get_all_counties()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    required_cols = [
        "fips", "name", "state", "population",
        "access_score", "distress_score", "exclusion_score", "quadrant",
    ]
    for col in required_cols:
        assert col in df.columns, f"Missing column: {col}"


def test_get_county_detail():
    # Get a valid FIPS from the database
    all_counties = analytics.get_all_counties()
    if len(all_counties) == 0:
        pytest.skip("No counties in database")
    test_fips = all_counties["fips"].iloc[0]
    df = analytics.get_county_detail(test_fips)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["fips"].iloc[0] == test_fips


def test_get_top_excluded_counties():
    df = analytics.get_top_excluded_counties(limit=10)
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 10
    assert len(df) > 0
    # Should be sorted by exclusion_score descending
    scores = df["exclusion_score"].tolist()
    assert scores == sorted(scores, reverse=True)


def test_get_quadrant_distribution():
    df = analytics.get_quadrant_distribution()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    valid_quadrants = {"well_served", "thin_access", "strained", "financial_desert"}
    for q in df["quadrant"]:
        assert q in valid_quadrants, f"Invalid quadrant: {q}"


def test_get_state_summary():
    df = analytics.get_state_summary()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "state" in df.columns
    assert "avg_exclusion" in df.columns
    assert "banking_deserts" in df.columns


def test_get_scatter_data():
    df = analytics.get_scatter_data()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "access_score" in df.columns
    assert "distress_score" in df.columns
    assert "quadrant" in df.columns


def test_get_county_financial_profile():
    all_counties = analytics.get_all_counties()
    if len(all_counties) == 0:
        pytest.skip("No counties in database")
    test_fips = all_counties["fips"].iloc[0]
    df = analytics.get_county_financial_profile(test_fips)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    profile_cols = [
        "bank_branches", "branches_per_10k", "total_deposits",
        "state_complaints_per_100k", "state_unbanked_pct",
    ]
    for col in profile_cols:
        assert col in df.columns, f"Missing column: {col}"


def test_get_national_averages():
    df = analytics.get_national_averages()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["avg_income"].iloc[0] > 0
    assert df["avg_poverty"].iloc[0] > 0
    assert df["avg_access"].iloc[0] is not None


def test_get_banking_deserts():
    df = analytics.get_banking_deserts()
    assert isinstance(df, pd.DataFrame)
    # All returned counties should have 0 branches
    if len(df) > 0:
        assert (df["bank_branches"] == 0).all()


def test_get_desert_count_by_state():
    df = analytics.get_desert_count_by_state()
    assert isinstance(df, pd.DataFrame)
    if len(df) > 0:
        assert "state" in df.columns
        assert "desert_count" in df.columns
        assert (df["desert_count"] > 0).all()


def test_get_border_comparison():
    df = analytics.get_border_comparison()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    categories = set(df["category"].tolist())
    assert "Non-Border" in categories
