"""Fetch Utah SNOTEL station metadata and daily AWDB observations as CSV.

Example: python scripts/fetch_awdb_snotel.py --start 2026-10-01 --end 2026-10-31
The AWDB service is public and does not require credentials.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_ROOT = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1"
DEFAULT_ELEMENTS = ("WTEQ", "SNWD")
DAILY_COLUMNS = [
    "observation_date", "station_triplet", "station_id", "station_name", "state",
    "network", "station_latitude", "station_longitude", "station_elevation_ft",
    "data_timezone_offset_hours", "element_code", "element_ordinal", "duration",
    "value", "value_mm", "unit", "original_value", "original_unit", "data_source",
    "central_tendency_type", "flags_json", "observation_timestamp",
    "source_begin_date", "source_end_date", "source_url", "retrieved_at_utc",
]


def request_json(endpoint: str, params: dict[str, str], attempts: int = 4) -> tuple[object, str]:
    url = f"{API_ROOT}/{endpoint}?{urlencode(params)}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "SnowMap/1.0"})
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=90) as response:
                return json.load(response), url
        except (HTTPError, URLError, TimeoutError) as error:
            if isinstance(error, HTTPError) and error.code < 500 and error.code != 429:
                raise RuntimeError(f"AWDB returned HTTP {error.code} for {endpoint}") from error
            if attempt + 1 == attempts:
                raise RuntimeError(f"AWDB request failed after {attempts} attempts ({endpoint})") from error
            time.sleep(2 ** attempt)
    raise AssertionError("unreachable")


def csv_value(value: object) -> str:
    return "" if value is None else str(value)


def millimeters(value: object, unit: object) -> str:
    """Convert recognized inch values to mm while retaining the source value/unit."""
    if value is None or str(unit or "").strip().lower() not in {"in", "inch", "inches"}:
        return ""
    try:
        return format(float(value) * 25.4, ".10g")
    except (TypeError, ValueError):
        return ""


def fetch(start: date, end: date, output: Path, metadata_output: Path,
          elements: tuple[str, ...], include_inactive: bool) -> int:
    station_params = {
        "state": "UT", "network": "SNTL", "elements": ",".join(elements),
        "durations": "DAILY", "returnStationElements": "true",
        "returnForecastPointMetadata": "false", "returnReservoirMetadata": "false",
        "activeOnly": str(not include_inactive).lower(),
    }
    stations_obj, stations_url = request_json("stations", station_params)
    if not isinstance(stations_obj, list):
        raise ValueError("AWDB stations response was not a JSON array")
    stations = [s for s in stations_obj if isinstance(s, dict) and s.get("stationTriplet")
                and str(s.get("stateCode", "")).upper() == "UT"
                and str(s.get("networkCode", "")).upper() == "SNTL"]
    if not stations:
        raise ValueError("AWDB returned no Utah SNOTEL stations")
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(json.dumps({"source_url": stations_url,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(), "stations": stations},
        indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    retrieved = datetime.now(timezone.utc).isoformat()
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=DAILY_COLUMNS)
        writer.writeheader()
        triplets = [str(s["stationTriplet"]) for s in stations]
        # AWDB currently limits a request by station-element count. Batch stations
        # conservatively and keep the response grouped by triplet.
        for offset in range(0, len(triplets), 100):
            batch = triplets[offset:offset + 100]
            params = {
                "stationTriplets": ",".join(batch), "elements": ",".join(elements),
                "duration": "DAILY", "beginDate": start.isoformat(), "endDate": end.isoformat(),
                "returnFlags": "true", "returnOriginalValues": "true",
                "returnSuspectData": "true",
            }
            payload, data_url = request_json("data", params)
            if not isinstance(payload, list):
                raise ValueError("AWDB data response was not a JSON array")
            station_by_triplet = {str(s["stationTriplet"]): s for s in stations}
            for series in payload:
                if not isinstance(series, dict):
                    continue
                triplet = str(series.get("stationTriplet", ""))
                station = station_by_triplet.get(triplet, {})
                for item in series.get("data", []) or []:
                    if not isinstance(item, dict):
                        continue
                    se = item.get("stationElement") or {}
                    code = str(se.get("elementCode", ""))
                    if code not in elements:
                        continue
                    original_value = item.get("originalValue")
                    if isinstance(original_value, dict):
                        original_value = original_value.get("value")
                    value = item.get("value")
                    raw_date = str(item.get("date", ""))
                    observation_date = raw_date[:10]
                    try:
                        observation_date = date.fromisoformat(observation_date).isoformat()
                    except ValueError as error:
                        raise ValueError(f"Unexpected AWDB observation date: {raw_date!r}") from error
                    writer.writerow({
                        "observation_date": observation_date, "station_triplet": triplet,
                        "station_id": station.get("stationId"), "station_name": station.get("name"),
                        "state": station.get("stateCode", "UT"), "network": station.get("networkCode", "SNTL"),
                        "station_latitude": station.get("latitude"), "station_longitude": station.get("longitude"),
                        "station_elevation_ft": station.get("elevation"),
                        "data_timezone_offset_hours": station.get("dataTimeZone"),
                        "element_code": code, "element_ordinal": se.get("ordinal"),
                        "duration": se.get("durationName", "DAILY"), "value": csv_value(value),
                        "value_mm": millimeters(value, se.get("storedUnitCode")),
                        "unit": se.get("storedUnitCode"), "original_value": csv_value(original_value),
                        "original_unit": se.get("originalUnitCode"), "data_source": item.get("dataSource"),
                        "central_tendency_type": item.get("centralTendencyType"),
                        "flags_json": json.dumps(item.get("flags", []), separators=(",", ":")),
                        "observation_timestamp": raw_date, "source_begin_date": se.get("beginDate"),
                        "source_end_date": se.get("endDate"), "source_url": data_url,
                        "retrieved_at_utc": retrieved,
                    })
                    count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--output", type=Path, default=Path("data/processed/snotel_daily_awdb.csv"))
    parser.add_argument("--metadata-output", type=Path, default=Path("data/processed/snotel_stations_awdb.json"))
    parser.add_argument("--elements", default=",".join(DEFAULT_ELEMENTS), help="Comma-separated AWDB element codes")
    parser.add_argument("--include-inactive", action="store_true")
    args = parser.parse_args()
    if args.end < args.start:
        parser.error("--end must be on or after --start")
    elements = tuple(dict.fromkeys(code.strip().upper() for code in args.elements.split(",") if code.strip()))
    if not elements:
        parser.error("--elements must include at least one element code")
    count = fetch(args.start, args.end, args.output, args.metadata_output, elements, args.include_inactive)
    print(f"Daily observation rows: {count:,}")
    print(f"CSV: {args.output}")
    print(f"Station metadata: {args.metadata_output}")


if __name__ == "__main__":
    main()
