import json
import time
from typing import Dict, List

import requests
import utils

ENTERGY_ENDPOINTS = {
    "county": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyLouisiana/county",
    "zipcode": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyLouisiana/zip",
}

REQUIRED_COUNTY_FIELDS = {"state", "county", "customersServed", "customersAffected", "lastUpdatedTime"}
REQUIRED_ZIPCODE_FIELDS = {"state", "zip", "customersServed", "customersAffected", "lastUpdatedTime"}

MAX_RETRIES = 3
BACKOFF_BASE = 2  # seconds


def get(url: str) -> list:
    """Fetch JSON data from the API with retry and exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise ValueError(f"Expected list, got {type(data).__name__}")
            return data
        except (requests.RequestException, ValueError) as e:
            wait = BACKOFF_BASE ** attempt
            print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def validate(data: List[Dict], required_fields: set, label: str) -> List[Dict]:
    """Validate response data has expected structure and fields."""
    if not data:
        raise ValueError(f"{label}: API returned empty list")

    missing = required_fields - set(data[0].keys())
    if missing:
        raise ValueError(f"{label}: Missing required fields: {missing}")

    # Filter out any records missing required fields
    valid = [item for item in data if required_fields.issubset(item.keys())]
    if len(valid) < len(data):
        print(f"  Warning: {len(data) - len(valid)} records dropped (missing fields)")

    return valid


def enrich(data: List[Dict]) -> str:
    """Add timestamps to the data."""
    for item in data:
        item["lastUpdatedTime"] = utils.from_epoch(item["lastUpdatedTime"])
        item["_loaded_at"] = utils.today()
    return json.dumps(data, indent=4)


def to_file(filename: str, data: str) -> None:
    """Write the data to a JSON file."""
    if not filename.endswith(".json"):
        filename += ".json"
    with open(filename, "w") as f:
        f.write(data)


if __name__ == "__main__":
    required_fields = {
        "county": REQUIRED_COUNTY_FIELDS,
        "zipcode": REQUIRED_ZIPCODE_FIELDS,
    }

    success = 0
    for key, url in ENTERGY_ENDPOINTS.items():
        print(f"Fetching {key}...")
        try:
            data = get(url)
            data = validate(data, required_fields[key], key)
            enriched = enrich(data)
            to_file(f"entergy_outages_{key}", enriched)
            print(f"  Wrote {key}: {len(data)} records")
            success += 1
        except Exception as e:
            print(f"  ERROR for {key}: {e}")

    print(f"Done. {success}/{len(ENTERGY_ENDPOINTS)} endpoints succeeded.")
