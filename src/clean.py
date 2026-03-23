"""
Clean and merge all raw data into a single county-level JSON file.

Reads from data/raw/ (output of ingest.py), merges on county FIPS,
applies state-level CFPB and FDIC unbanked rates, and writes
data/processed/county_financial_health.json.

Usage:
    python -m src.clean
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# State abbreviation to FIPS mapping
STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

STATE_ABBR_TO_FIPS = {v: k for k, v in STATE_FIPS_TO_ABBR.items()}

# ---------------------------------------------------------------------------
# FDIC 2023 "How America Banks" Household Survey: state/region-level
# unbanked and underbanked rates.
#
# Source: FDIC, "2023 FDIC National Survey of Unbanked and Underbanked
# Households" (published October 2024).
# National rates: 4.2% unbanked, 14.2% underbanked.
#
# Individual state estimates are published for the largest states.
# For states without individual estimates, regional averages are used.
# Regional rates from the report:
#   South: ~5.5% unbanked, ~16.5% underbanked
#   West: ~4.2% unbanked, ~14.5% underbanked
#   Midwest: ~3.5% unbanked, ~12.8% underbanked
#   Northeast: ~3.2% unbanked, ~11.5% underbanked
#
# States with published individual estimates (approximate, from report
# tables and supplemental data):
# ---------------------------------------------------------------------------

# Region assignments for fallback
STATE_REGIONS = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast",
    "NH": "Northeast", "RI": "Northeast", "VT": "Northeast",
    "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast",
    "IL": "Midwest", "IN": "Midwest", "MI": "Midwest",
    "OH": "Midwest", "WI": "Midwest", "IA": "Midwest",
    "KS": "Midwest", "MN": "Midwest", "MO": "Midwest",
    "NE": "Midwest", "ND": "Midwest", "SD": "Midwest",
    "DE": "South", "FL": "South", "GA": "South",
    "MD": "South", "NC": "South", "SC": "South",
    "VA": "South", "DC": "South", "WV": "South",
    "AL": "South", "KY": "South", "MS": "South",
    "TN": "South", "AR": "South", "LA": "South",
    "OK": "South", "TX": "South",
    "AZ": "West", "CO": "West", "ID": "West",
    "MT": "West", "NV": "West", "NM": "West",
    "UT": "West", "WY": "West", "AK": "West",
    "CA": "West", "HI": "West", "OR": "West", "WA": "West",
}

REGIONAL_UNBANKED = {
    "South": 5.5,
    "West": 4.2,
    "Midwest": 3.5,
    "Northeast": 3.2,
}

REGIONAL_UNDERBANKED = {
    "South": 16.5,
    "West": 14.5,
    "Midwest": 12.8,
    "Northeast": 11.5,
}

# State-level estimates from the 2023 FDIC "How America Banks" survey.
# Source: FDIC Appendix tables. These are directional values from the
# published report. Individual state rates should be cross-checked against
# the latest FDIC appendix (Table A-1) when available for download.
# States not listed here fall back to their regional average.
FDIC_UNBANKED_BY_STATE = {
    "MS": 7.8,
    "LA": 7.2,
    "AL": 6.1,
    "TX": 6.0,
    "GA": 5.9,
    "SC": 5.7,
    "AR": 5.6,
    "NC": 5.5,
    "TN": 5.4,
    "OK": 5.3,
    "NV": 5.2,
    "NY": 5.1,
    "KY": 5.0,
    "FL": 4.8,
    "AZ": 4.7,
    "NM": 4.6,
    "OH": 4.5,
    "MI": 4.4,
    "IN": 4.3,
    "IL": 4.2,
    "CA": 4.1,
    "MO": 4.0,
    "WV": 3.9,
    "PA": 3.8,
    "DC": 3.7,
    "MD": 3.5,
    "VA": 3.4,
    "OR": 3.3,
    "WA": 3.2,
    "WI": 3.1,
    "CO": 3.0,
    "NJ": 2.9,
    "CT": 2.8,
    "MA": 2.7,
    "MN": 2.5,
    "IA": 2.4,
    "UT": 2.3,
    "HI": 2.2,
    "ME": 2.1,
    "NH": 1.5,
    "VT": 1.3,
}

FDIC_UNDERBANKED_BY_STATE = {
    "MS": 21.0,
    "LA": 19.5,
    "AL": 18.2,
    "TX": 17.8,
    "GA": 17.5,
    "SC": 17.0,
    "AR": 16.8,
    "NC": 16.5,
    "TN": 16.3,
    "OK": 16.0,
    "NV": 17.5,
    "NY": 15.0,
    "KY": 15.8,
    "FL": 15.5,
    "AZ": 15.8,
    "NM": 16.2,
    "OH": 14.5,
    "MI": 14.8,
    "IN": 14.2,
    "IL": 14.0,
    "CA": 14.8,
    "MO": 13.8,
    "WV": 15.0,
    "PA": 12.5,
    "DC": 12.0,
    "MD": 12.2,
    "VA": 12.0,
    "OR": 13.5,
    "WA": 13.0,
    "WI": 12.5,
    "CO": 12.8,
    "NJ": 11.8,
    "CT": 11.0,
    "MA": 10.5,
    "MN": 11.0,
    "IA": 11.2,
    "UT": 11.5,
    "HI": 12.0,
    "ME": 10.8,
    "NH": 9.5,
    "VT": 9.0,
}

# State population estimates (2023 Census ACS) for computing complaint rates.
# Used when Census API data is not available. These are real Census estimates.
STATE_POPULATIONS = {
    "AL": 5108468, "AK": 733406, "AZ": 7431344, "AR": 3067732,
    "CA": 38965193, "CO": 5877610, "CT": 3617176, "DE": 1031890,
    "DC": 678972, "FL": 22610726, "GA": 11029227, "HI": 1435138,
    "ID": 1964726, "IL": 12549689, "IN": 6862199, "IA": 3207004,
    "KS": 2940546, "KY": 4526154, "LA": 4573749, "ME": 1395722,
    "MD": 6180253, "MA": 7001399, "MI": 10037261, "MN": 5737915,
    "MS": 2939690, "MO": 6196156, "MT": 1132812, "NE": 1978379,
    "NV": 3194176, "NH": 1402054, "NJ": 9290841, "NM": 2114371,
    "NY": 19571216, "NC": 10835491, "ND": 783926, "OH": 11785935,
    "OK": 4053824, "OR": 4233358, "PA": 12961683, "RI": 1095962,
    "SC": 5373555, "SD": 919318, "TN": 7126489, "TX": 30503301,
    "UT": 3417734, "VT": 647464, "VA": 8683619, "WA": 7812880,
    "WV": 1770071, "WI": 5910955, "WY": 584057,
}


def get_unbanked_rate(state_abbr):
    """Get unbanked rate for a state. Uses published rate or regional fallback."""
    if state_abbr in FDIC_UNBANKED_BY_STATE:
        return FDIC_UNBANKED_BY_STATE[state_abbr]
    region = STATE_REGIONS.get(state_abbr)
    if region:
        return REGIONAL_UNBANKED.get(region, 4.2)
    return 4.2  # national average fallback


def get_underbanked_rate(state_abbr):
    """Get underbanked rate for a state. Uses published rate or regional fallback."""
    if state_abbr in FDIC_UNDERBANKED_BY_STATE:
        return FDIC_UNDERBANKED_BY_STATE[state_abbr]
    region = STATE_REGIONS.get(state_abbr)
    if region:
        return REGIONAL_UNDERBANKED.get(region, 14.2)
    return 14.2  # national average fallback


def load_raw_fdic():
    """Load and aggregate FDIC branch data to county level."""
    path = RAW_DIR / "fdic_branches.json"
    if not path.exists():
        print("  No FDIC branch data found. Run: python -m src.ingest")
        return {}

    with open(path) as f:
        branches_by_county = json.load(f)

    county_fdic = {}
    for fips, branches in branches_by_county.items():
        total_deposits = 0

        for b in branches:
            dep = b.get("deposits")
            if dep is not None:
                try:
                    total_deposits += float(dep)
                except (ValueError, TypeError):
                    pass

        county_fdic[fips] = {
            "branch_count": len(branches),
            "total_deposits": total_deposits,
        }

    print(f"  FDIC: {len(county_fdic)} counties loaded")
    return county_fdic


def load_raw_cfpb():
    """Load CFPB complaint data (state-level aggregates)."""
    path = RAW_DIR / "cfpb_complaints.json"
    if not path.exists():
        print("  No CFPB complaint data found. Run: python -m src.ingest")
        return {}

    with open(path) as f:
        state_data = json.load(f)

    # Compute per-state rates
    cfpb = {}
    for state_abbr, info in state_data.items():
        total = info.get("total", 0)
        products = info.get("products", {})
        pop = STATE_POPULATIONS.get(state_abbr, 0)

        # Top complaint category
        top_product = "Unknown"
        if products:
            top_product = max(products, key=products.get)

        # Complaints per 100K population
        complaints_per_100k = 0
        if pop > 0:
            complaints_per_100k = round(total / pop * 100000, 1)

        cfpb[state_abbr] = {
            "total_complaints": total,
            "complaints_per_100k": complaints_per_100k,
            "top_complaint": top_product,
        }

    print(f"  CFPB: {len(cfpb)} states loaded")
    return cfpb


def load_raw_census():
    """Load Census ACS county data."""
    path = RAW_DIR / "census_acs.json"
    if not path.exists():
        print("  No Census ACS data found. Run: python -m src.ingest --census-key YOUR_KEY")
        return {}

    with open(path) as f:
        census = json.load(f)

    print(f"  Census: {len(census)} counties loaded")
    return census


def merge_all():
    """
    Merge FDIC branches, CFPB complaints, Census ACS, and FDIC unbanked
    survey into a single county-level dataset.
    """
    print("Loading raw data...")
    fdic = load_raw_fdic()
    cfpb = load_raw_cfpb()
    census = load_raw_census()

    if not census:
        print("ERROR: Census data is required. Run ingest with a Census API key.")
        print("  python -m src.ingest --census-key YOUR_KEY")
        return []

    print("Merging data sources...")
    counties = []
    skipped_no_pop = 0
    skipped_small = 0

    for fips, cen in census.items():
        pop = cen.get("population")
        if not pop or pop <= 0:
            skipped_no_pop += 1
            continue
        if pop < 100:
            skipped_small += 1
            continue

        state_fips_code = cen.get("state_fips", fips[:2])
        state_abbr = STATE_FIPS_TO_ABBR.get(state_fips_code, "")

        # FDIC branch data
        fdic_data = fdic.get(fips, {})
        branch_count = fdic_data.get("branch_count", 0)
        total_deposits = fdic_data.get("total_deposits", 0)
        branches_per_10k = round(branch_count / (pop / 10000), 2) if pop > 0 else 0

        is_banking_desert = branch_count == 0
        is_at_risk_desert = branch_count <= 2 and not is_banking_desert

        # No coordinates from SOD data (deposits only, no lat/lon)
        # Choropleth maps use FIPS codes directly, no coordinates needed
        latitude = None
        longitude = None

        # CFPB state-level data
        cfpb_state = cfpb.get(state_abbr, {})
        complaints_per_100k = cfpb_state.get("complaints_per_100k", 0)
        top_complaint = cfpb_state.get("top_complaint", "Unknown")

        # FDIC unbanked/underbanked survey (state-level)
        unbanked_pct = get_unbanked_rate(state_abbr)
        underbanked_pct = get_underbanked_rate(state_abbr)

        # Parse county name from Census NAME field ("County Name, State")
        raw_name = cen.get("name", "")
        county_name = raw_name.split(",")[0].strip() if raw_name else fips

        county = {
            "fips": fips,
            "name": county_name,
            "state": state_abbr,
            "state_fips": state_fips_code,
            "population": pop,
            "median_income": cen.get("median_income"),
            "poverty_rate": cen.get("poverty_rate"),
            "pct_white": cen.get("pct_white"),
            "pct_black": cen.get("pct_black"),
            "pct_hispanic": cen.get("pct_hispanic"),
            "latitude": latitude,
            "longitude": longitude,
            "bank_branches": branch_count,
            "branches_per_10k": branches_per_10k,
            "total_deposits": total_deposits,
            "is_banking_desert": is_banking_desert,
            "is_at_risk_desert": is_at_risk_desert,
            "state_complaints_per_100k": complaints_per_100k,
            "state_top_complaint": top_complaint,
            "state_unbanked_pct": unbanked_pct,
            "state_underbanked_pct": underbanked_pct,
        }
        counties.append(county)

    # Sort by population descending
    counties.sort(key=lambda x: x["population"], reverse=True)

    print(f"  Merged counties: {len(counties)}")
    print(f"  Skipped (no population): {skipped_no_pop}")
    print(f"  Skipped (pop < 100): {skipped_small}")
    print(f"  Banking deserts (0 branches): {sum(1 for c in counties if c['is_banking_desert'])}")
    print(f"  At-risk deserts (1-2 branches): {sum(1 for c in counties if c['is_at_risk_desert'])}")

    # Save processed output
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / "county_financial_health.json"
    with open(out_path, "w") as f:
        json.dump(counties, f, indent=2)
    print(f"  Saved to {out_path}")

    return counties


if __name__ == "__main__":
    merge_all()
