"""
Download all raw data for the Community Financial Health Index.

Sources:
- FDIC bank branch locations (county-level branch counts + deposits)
- CFPB Consumer Complaint Database (state-level complaint aggregates)
- Census ACS 5-year (population, income, poverty, race by county)

Census ACS requires a free API key. Set CENSUS_API_KEY env var.

Usage:
    python -m src.ingest
    python -m src.ingest --census-key XXXXX
"""

import csv
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
]


def fetch_json(url, label=""):
    """Fetch JSON from URL with retries."""
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "FinHealthIndex/1.0"})
            with urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"  Retry {attempt+1}/3 for {label}: {e}")
            time.sleep(2)
    print(f"  FAILED: {label}")
    return None


def fetch_fdic_branches():
    """
    Fetch FDIC bank branch data from the Summary of Deposits (SOD) endpoint.
    Uses /api/sod with YEAR filter for current data.
    Returns dict: {county_fips: [list of branch records]}
    Each record has STCNTYBR (county FIPS), DEPSUM (deposits), and branch info.
    """
    print("Fetching FDIC bank branch data (SOD 2024)...")
    branches_by_county = {}
    total_branches = 0

    for state in STATES:
        # Use SOD endpoint which has STCNTYBR and DEPSUM fields
        # Filter to 2024 (most recent SOD year)
        url = (
            f"https://banks.data.fdic.gov/api/sod"
            f"?filters=STALP:{state}%20AND%20YEAR:2024"
            f"&fields=STCNTYBR,DEPSUM,NAMEFULL,CITYBR,STALPBR,ZIPBR,ADDRESBR"
            f"&limit=10000&fmt=json"
        )
        data = fetch_json(url, f"FDIC {state}")
        if not data or "data" not in data:
            continue

        def process_items(items):
            nonlocal total_branches
            for item in items:
                rec = item.get("data", {})
                fips = str(rec.get("STCNTYBR", "")).zfill(5)
                if not fips or fips == "00000":
                    continue
                total_branches += 1
                if fips not in branches_by_county:
                    branches_by_county[fips] = []
                branches_by_county[fips].append({
                    "fips": fips,
                    "deposits": rec.get("DEPSUM"),
                    "name": rec.get("NAMEFULL", ""),
                    "city": rec.get("CITYBR", ""),
                    "state": rec.get("STALPBR", ""),
                    "zip": rec.get("ZIPBR", ""),
                    "address": rec.get("ADDRESBR", ""),
                })

        process_items(data["data"])

        # Handle pagination for large states
        total_records = data.get("meta", {}).get("total", 0)
        if total_records > 10000:
            offset = 10000
            while offset < total_records:
                page_url = f"{url}&offset={offset}"
                page_data = fetch_json(page_url, f"FDIC {state} page {offset}")
                if page_data and "data" in page_data:
                    process_items(page_data["data"])
                offset += 10000

        time.sleep(0.3)

    print(f"  Total branches: {total_branches}")
    print(f"  Counties with branches: {len(branches_by_county)}")

    # Save raw data
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / "fdic_branches.json"
    with open(out_path, "w") as f:
        json.dump(branches_by_county, f)
    print(f"  Saved to {out_path}")

    return branches_by_county


def fetch_cfpb_complaints():
    """
    Fetch CFPB complaint data. Tries bulk CSV download first.
    Falls back to API aggregation by state if bulk download fails.
    Returns dict: {state_abbr: {total, products: {product: count}}}
    """
    print("Fetching CFPB complaint data...")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Try bulk CSV download first
    bulk_url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
    try:
        print("  Attempting bulk CSV download (this may take several minutes)...")
        req = Request(bulk_url, headers={"User-Agent": "FinHealthIndex/1.0"})
        with urlopen(req, timeout=600) as resp:
            zip_data = resp.read()

        print(f"  Downloaded {len(zip_data) / 1024 / 1024:.1f} MB")
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            csv_name = zf.namelist()[0]
            print(f"  Extracting {csv_name}...")
            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                state_data = {}
                row_count = 0
                skipped = 0

                for row in reader:
                    row_count += 1
                    date_str = row.get("Date received", "")
                    # Filter to last 3 years (2022-2025)
                    if date_str:
                        year = date_str[:4]
                        if year < "2022":
                            skipped += 1
                            continue

                    state = row.get("State", "")
                    product = row.get("Product", "Unknown")
                    if not state or len(state) != 2:
                        skipped += 1
                        continue

                    if state not in state_data:
                        state_data[state] = {"total": 0, "products": {}}
                    state_data[state]["total"] += 1
                    state_data[state]["products"][product] = (
                        state_data[state]["products"].get(product, 0) + 1
                    )

                print(f"  Processed {row_count} rows, skipped {skipped}")
                print(f"  States with data: {len(state_data)}")

        out_path = RAW_DIR / "cfpb_complaints.json"
        with open(out_path, "w") as f:
            json.dump(state_data, f, indent=2)
        print(f"  Saved to {out_path}")
        return state_data

    except Exception as e:
        print(f"  Bulk download failed: {e}")
        print("  Falling back to CFPB API aggregation...")
        return _fetch_cfpb_api_fallback()


def _fetch_cfpb_api_fallback():
    """
    Fallback: query CFPB API state by state for aggregate complaint counts.
    Uses the API's aggregation endpoint to avoid downloading full records.
    """
    state_data = {}
    base_url = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

    for state in STATES:
        url = (
            f"{base_url}"
            f"?state={state}"
            f"&date_received_min=2022-01-01"
            f"&date_received_max=2025-12-31"
            f"&size=0"
            f"&agg=product"
            f"&aggs_size=10"
        )
        data = fetch_json(url, f"CFPB {state}")
        if not data:
            continue

        total = data.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = int(total) if total else 0

        products = {}
        agg_buckets = (
            data.get("aggregations", {})
            .get("product", {})
            .get("product", {})
            .get("buckets", [])
        )
        for bucket in agg_buckets:
            products[bucket.get("key", "Unknown")] = bucket.get("doc_count", 0)

        if total_count > 0:
            state_data[state] = {"total": total_count, "products": products}

        time.sleep(0.5)

    print(f"  API fallback: {len(state_data)} states with complaint data")
    out_path = RAW_DIR / "cfpb_complaints.json"
    with open(out_path, "w") as f:
        json.dump(state_data, f, indent=2)
    print(f"  Saved to {out_path}")
    return state_data


def fetch_census_acs(api_key=None):
    """
    Fetch Census ACS 5-year data: population, income, poverty, race by county.
    Tables: B01003 (pop), B19013 (income), B17001 (poverty), B02001 (race).
    """
    if not api_key:
        print("Skipping Census ACS (no API key). Set CENSUS_API_KEY env var.")
        return {}

    print("Fetching Census ACS data...")
    variables = (
        "NAME,"
        "B01003_001E,"     # total population
        "B19013_001E,"     # median household income
        "B17001_001E,"     # poverty universe
        "B17001_002E,"     # below poverty
        "B02001_001E,"     # race total
        "B02001_002E,"     # white alone
        "B02001_003E,"     # black alone
        "B03003_003E"      # hispanic/latino
    )
    url = (
        f"https://api.census.gov/data/2023/acs/acs5"
        f"?get={variables}"
        f"&for=county:*&key={api_key}"
    )

    try:
        req = Request(url, headers={"User-Agent": "FinHealthIndex/1.0"})
        with urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())

        headers = data[0]
        census_data = {}

        for row in data[1:]:
            state_fips = row[headers.index("state")]
            county_fips = row[headers.index("county")]
            fips = state_fips + county_fips

            def safe_int(val):
                if val in (None, "", "-666666666", "-999999999", "null"):
                    return None
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return None

            pop = safe_int(row[headers.index("B01003_001E")])
            income = safe_int(row[headers.index("B19013_001E")])
            pov_total = safe_int(row[headers.index("B17001_001E")])
            pov_below = safe_int(row[headers.index("B17001_002E")])
            race_total = safe_int(row[headers.index("B02001_001E")])
            white = safe_int(row[headers.index("B02001_002E")])
            black = safe_int(row[headers.index("B02001_003E")])
            hispanic = safe_int(row[headers.index("B03003_003E")])

            entry = {
                "fips": fips,
                "name": row[headers.index("NAME")],
                "state_fips": state_fips,
                "population": pop,
                "median_income": income,
            }

            # Poverty rate
            if pov_total and pov_below and pov_total > 0:
                entry["poverty_rate"] = round(pov_below / pov_total * 100, 1)
            else:
                entry["poverty_rate"] = None

            # Race percentages
            if race_total and race_total > 0:
                entry["pct_white"] = round(white / race_total * 100, 1) if white else None
                entry["pct_black"] = round(black / race_total * 100, 1) if black else None
            else:
                entry["pct_white"] = None
                entry["pct_black"] = None

            if pop and pop > 0 and hispanic is not None:
                entry["pct_hispanic"] = round(hispanic / pop * 100, 1)
            else:
                entry["pct_hispanic"] = None

            census_data[fips] = entry

        print(f"  Census data for {len(census_data)} counties")

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RAW_DIR / "census_acs.json"
        with open(out_path, "w") as f:
            json.dump(census_data, f, indent=2)
        print(f"  Saved to {out_path}")
        return census_data

    except Exception as e:
        print(f"  FAILED to fetch Census data: {e}")
        return {}


def ingest(census_key=None):
    """Run full ingestion pipeline."""
    key = census_key or os.environ.get("CENSUS_API_KEY")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    fdic = fetch_fdic_branches()
    cfpb = fetch_cfpb_complaints()
    census = fetch_census_acs(key)

    print(f"\nIngestion complete.")
    print(f"  FDIC branches: {len(fdic)} counties")
    print(f"  CFPB complaints: {len(cfpb)} states")
    print(f"  Census ACS: {len(census)} counties")
    print("Run `python -m src.clean` to process and merge data.")


if __name__ == "__main__":
    key = None
    if "--census-key" in sys.argv:
        idx = sys.argv.index("--census-key")
        if idx + 1 < len(sys.argv):
            key = sys.argv[idx + 1]
    ingest(key)
