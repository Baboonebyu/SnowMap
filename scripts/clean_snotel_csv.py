"""Validate and normalize a daily SNOTEL CSV export to metric units."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path


REQUIRED_COLUMNS = {
    "date",
    "station_id",
    "station_name",
    "state",
    "network",
    "elevation_ft",
    "swe_in",
    "snow_depth_in",
    "observation_basis",
    "data_status",
    "source_url",
}
OUTPUT_COLUMNS = [
    "observation_date",
    "station_triplet",
    "station_id",
    "station_name",
    "state",
    "network",
    "station_latitude",
    "station_longitude",
    "data_timezone_offset_hours",
    "elevation_ft",
    "elevation_m",
    "swe_in",
    "swe_mm",
    "snow_depth_in",
    "snow_depth_mm",
    "observation_basis",
    "data_status",
    "source_url",
]


def optional_decimal(value: str, *, row_number: int, field: str) -> Decimal | None:
    value = value.strip()
    if not value:
        return None
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"Row {row_number}: {field} is not numeric: {value!r}") from error
    if not number.is_finite():
        raise ValueError(f"Row {row_number}: {field} must be finite")
    if number < 0:
        raise ValueError(f"Row {row_number}: {field} cannot be negative")
    return number


def metric_mm(inches: Decimal | None) -> str:
    return "" if inches is None else format(inches * Decimal("25.4"), "f")


def metric_m(feet: Decimal | None) -> str:
    return "" if feet is None else format(feet * Decimal("0.3048"), "f")


def clean(source: Path, destination: Path, metadata_path: Path | None) -> int:
    metadata_by_triplet = {}
    if metadata_path is not None:
        stations = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata_by_triplet = {
            station["stationTriplet"]: station
            for station in stations
            if station.get("stationTriplet")
        }

    seen_keys: set[tuple[str, str]] = set()
    destination.parent.mkdir(parents=True, exist_ok=True)

    with source.open("r", newline="", encoding="utf-8-sig") as input_file:
        reader = csv.DictReader(input_file)
        headers = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_COLUMNS - headers)
        if missing:
            raise ValueError(f"Input CSV is missing required columns: {', '.join(missing)}")

        with destination.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            row_count = 0

            for row_number, row in enumerate(reader, start=2):
                raw_date = (row["date"] or "").strip()
                try:
                    observation_date = date.fromisoformat(raw_date).isoformat()
                except ValueError as error:
                    raise ValueError(f"Row {row_number}: invalid ISO date {raw_date!r}") from error

                station_id = (row["station_id"] or "").strip()
                state = (row["state"] or "").strip().upper()
                network = (row["network"] or "").strip().upper()
                station_name = (row["station_name"] or "").strip()
                if not all((station_id, state, network, station_name)):
                    raise ValueError(f"Row {row_number}: station ID, state, network, and name are required")

                station_triplet = f"{station_id}:{state}:{network}"
                metadata = metadata_by_triplet.get(station_triplet)
                if metadata_path is not None and metadata is None:
                    raise ValueError(
                        f"Row {row_number}: no station metadata for {station_triplet}"
                    )
                natural_key = (observation_date, station_triplet)
                if natural_key in seen_keys:
                    raise ValueError(
                        f"Row {row_number}: duplicate station/date key {observation_date}, {station_triplet}"
                    )
                seen_keys.add(natural_key)

                elevation_ft = optional_decimal(row["elevation_ft"], row_number=row_number, field="elevation_ft")
                swe_in = optional_decimal(row["swe_in"], row_number=row_number, field="swe_in")
                snow_depth_in = optional_decimal(
                    row["snow_depth_in"], row_number=row_number, field="snow_depth_in"
                )

                writer.writerow(
                    {
                        "observation_date": observation_date,
                        "station_triplet": station_triplet,
                        "station_id": station_id,
                        "station_name": station_name,
                        "state": state,
                        "network": network,
                        "station_latitude": "" if metadata is None else metadata.get("latitude", ""),
                        "station_longitude": "" if metadata is None else metadata.get("longitude", ""),
                        "data_timezone_offset_hours": "" if metadata is None else metadata.get("dataTimeZone", ""),
                        "elevation_ft": "" if elevation_ft is None else format(elevation_ft, "f"),
                        "elevation_m": metric_m(elevation_ft),
                        "swe_in": "" if swe_in is None else format(swe_in, "f"),
                        "swe_mm": metric_mm(swe_in),
                        "snow_depth_in": "" if snow_depth_in is None else format(snow_depth_in, "f"),
                        "snow_depth_mm": metric_mm(snow_depth_in),
                        "observation_basis": (row["observation_basis"] or "").strip(),
                        "data_status": (row["data_status"] or "").strip(),
                        "source_url": (row["source_url"] or "").strip(),
                    }
                )
                row_count += 1

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=Path("data/samples/snotel_utah_2026-10-31_sample.csv"),
        help="Input daily SNOTEL CSV export",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/snotel_utah_2026-10-31.csv"),
        help="Normalized CSV output path",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=Path("data/samples/snotel_station_metadata_2026-10-31.json"),
        help="Optional AWDB station metadata response to add coordinates and time zone",
    )
    args = parser.parse_args()
    rows = clean(args.source, args.output, args.metadata)
    print(f"Cleaned rows: {rows:,}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
