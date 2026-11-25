"""Find the nearest Utah SNODAS grid cell for each SNOTEL sample row."""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path


EARTH_RADIUS_KM = 6371.0088


def distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def nearest_cells(stations_path: Path, grid_path: Path, output_path: Path) -> int:
    with stations_path.open("r", newline="", encoding="utf-8") as source:
        stations = list(csv.DictReader(source))
    with grid_path.open("r", newline="", encoding="utf-8") as source:
        grid = list(csv.DictReader(source))

    required_station_fields = {"station_triplet", "station_latitude", "station_longitude"}
    required_grid_fields = {
        "cell_id",
        "latitude",
        "longitude",
        "swe_mm",
        "snow_depth_mm",
    }
    if not stations or not required_station_fields.issubset(stations[0]):
        raise ValueError("SNOTEL input is empty or missing station coordinates")
    if not grid or not required_grid_fields.issubset(grid[0]):
        raise ValueError("SNODAS input is empty or missing required grid columns")

    nearest = {station["station_triplet"]: (float("inf"), None) for station in stations}
    station_coords = {
        station["station_triplet"]: (
            float(station["station_latitude"]),
            float(station["station_longitude"]),
        )
        for station in stations
    }

    for cell in grid:
        cell_lat = float(cell["latitude"])
        cell_lon = float(cell["longitude"])
        for triplet, (station_lat, station_lon) in station_coords.items():
            distance = distance_km(station_lat, station_lon, cell_lat, cell_lon)
            if distance < nearest[triplet][0]:
                nearest[triplet] = (distance, cell)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "observation_date",
        "station_triplet",
        "station_name",
        "station_latitude",
        "station_longitude",
        "nearest_cell_id",
        "nearest_cell_latitude",
        "nearest_cell_longitude",
        "center_distance_km",
        "snotel_swe_mm",
        "snodas_swe_mm",
        "snotel_snow_depth_mm",
        "snodas_snow_depth_mm",
        "observation_basis",
        "data_status",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=columns)
        writer.writeheader()
        for station in stations:
            distance, cell = nearest[station["station_triplet"]]
            if cell is None:
                raise ValueError(f"No SNODAS cell found for {station['station_triplet']}")
            writer.writerow(
                {
                    "observation_date": station["observation_date"],
                    "station_triplet": station["station_triplet"],
                    "station_name": station["station_name"],
                    "station_latitude": station["station_latitude"],
                    "station_longitude": station["station_longitude"],
                    "nearest_cell_id": cell["cell_id"],
                    "nearest_cell_latitude": cell["latitude"],
                    "nearest_cell_longitude": cell["longitude"],
                    "center_distance_km": f"{distance:.3f}",
                    "snotel_swe_mm": station["swe_mm"],
                    "snodas_swe_mm": cell["swe_mm"],
                    "snotel_snow_depth_mm": station["snow_depth_mm"],
                    "snodas_snow_depth_mm": cell["snow_depth_mm"],
                    "observation_basis": station["observation_basis"],
                    "data_status": station["data_status"],
                }
            )
    return len(stations)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stations",
        type=Path,
        default=Path("data/processed/snotel_utah_2026-10-31.csv"),
    )
    parser.add_argument(
        "--grid",
        type=Path,
        default=Path("data/processed/snodas_utah_20261031.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/snotel_snodas_nearest_2026-10-31.csv"),
    )
    args = parser.parse_args()
    count = nearest_cells(args.stations, args.grid, args.output)
    print(f"Stations matched: {count:,}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
