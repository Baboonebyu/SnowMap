"""Load SnowMap CSV data into BigQuery using ADC."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
CONFIG = {
    "snodas": {
        "source_schema": ROOT / "schemas" / "snodas_source.json",
        "cell_schema": ROOT / "schemas" / "snodas_cell.json",
        "data_schema": ROOT / "schemas" / "snodas_data.json",
        "cell_table": "snodas_cell",
        "default_table": "snodas_data",
    },
}

SNOTEL_SOURCE_COLUMNS = {
    "observation_date", "station_triplet", "station_id", "station_name",
    "station_latitude", "station_longitude", "station_elevation_ft",
    "element_code", "value", "value_mm", "unit", "flags_json", "retrieved_at_utc",
}
SNOTEL_ELEMENT_FIELDS = {
    "SNWD": "snow_depth",
    "WTEQ": "swe",
}
SNOTEL_TABLES = {
    "station": (ROOT / "schemas" / "snotel_station.json", ("station_triplet",), None),
    "daily": (ROOT / "schemas" / "snotel_daily.json", ("station_triplet", "observation_date"), "observation_date"),
}


def read_schema(path: Path) -> tuple[list[bigquery.SchemaField], list[str]]:
    schema_data = json.loads(path.read_text(encoding="utf-8"))
    schema = [bigquery.SchemaField(item["name"], item["type"], mode=item["mode"])
              for item in schema_data]
    return schema, [item["name"] for item in schema_data]


def load_json_rows(client: bigquery.Client, destination: str,
                   schema: list[bigquery.SchemaField], rows: list[dict[str, object]],
                   write_disposition: str) -> None:
    """Load typed rows directly into a BigQuery table or partition."""
    load_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
    )
    client.load_table_from_json(rows, destination, job_config=load_config).result()


def load_snodas_csv(client: bigquery.Client, csv_path: Path,
                    dataset: str, table_name: str,
                    cell_table: str | None = None,
                    refresh_cells: bool = False) -> int:
    config = CONFIG["snodas"]
    source_schema, columns = read_schema(config["source_schema"])
    cell_schema, _ = read_schema(config["cell_schema"])
    data_schema, _ = read_schema(config["data_schema"])
    with csv_path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        missing = set(columns) - set(reader.fieldnames or [])
        extra = set(reader.fieldnames or []) - set(columns)
        if missing or extra:
            raise ValueError(f"CSV schema mismatch; missing={sorted(missing)}, extra={sorted(extra)}")
        rows = list(reader)
    if not rows:
        print("CSV contains no observations; nothing loaded")
        return 0
    cell_rows: dict[int, dict[str, object]] = {}
    rows_by_date: dict[str, list[dict[str, object]]] = {}
    seen_daily_keys: set[tuple[str, int]] = set()
    for row_number, row in enumerate(rows, start=2):
        if not row["observation_date"]:
            raise ValueError(f"CSV row {row_number} has no observation date")
        observation_date = row["observation_date"]
        date.fromisoformat(observation_date)
        if any(not row[key] for key in ("cell_id", "grid_row", "grid_col", "latitude", "longitude")):
            raise ValueError(f"CSV row {row_number} is missing a cell key or coordinate")
        grid_row, grid_col = int(row["grid_row"]), int(row["grid_col"])
        if not 0 <= grid_row < 2**31 or not 0 <= grid_col < 2**32:
            raise ValueError(f"CSV row {row_number} has grid coordinates outside the supported ID range")
        numeric_cell_id = (grid_row << 32) + grid_col
        cell = {
            "id": numeric_cell_id,
            "lat": float(row["latitude"]),
            "long": float(row["longitude"]),
        }
        previous_cell = cell_rows.setdefault(numeric_cell_id, cell)
        if previous_cell != cell:
            raise ValueError(f"CSV row {row_number} has inconsistent metadata for cell {row['cell_id']}")
        key = (observation_date, numeric_cell_id)
        if key in seen_daily_keys:
            raise ValueError(f"CSV row {row_number} duplicates cell {row['cell_id']} on {observation_date}")
        seen_daily_keys.add(key)
        rows_by_date.setdefault(observation_date, []).append({
            "cell_id": numeric_cell_id,
            "date": observation_date,
            "snow_depth": int(row["snow_depth_mm"]) if row["snow_depth_mm"] else None,
            "swe": int(row["swe_mm"]) if row["swe_mm"] else None,
        })
    table_name = table_name or config["default_table"]
    cell_table = cell_table or config["cell_table"]
    cell_id = f"{client.project}.{dataset}.{cell_table}"
    data_id = f"{client.project}.{dataset}.{table_name}"
    cell_target = bigquery.Table(cell_id, schema=cell_schema)
    data_target = bigquery.Table(data_id, schema=data_schema)
    data_target.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    client.create_table(cell_target, exists_ok=True)
    client.create_table(data_target, exists_ok=True)
    current_cells = client.get_table(cell_id)
    if refresh_cells or current_cells.num_rows == 0:
        load_json_rows(client, cell_id, cell_schema, list(cell_rows.values()),
                       bigquery.WriteDisposition.WRITE_TRUNCATE)
    else:
        print(f"Cell table already has {current_cells.num_rows:,} rows; leaving it unchanged")

    for observation_date, daily_rows in sorted(rows_by_date.items()):
        partition_id = observation_date.replace("-", "")
        load_json_rows(client, f"{data_id}${partition_id}", data_schema,
                       daily_rows, bigquery.WriteDisposition.WRITE_TRUNCATE)
    return len(rows)


def nullable_float(value: str, label: str) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"Invalid numeric value for {label}: {value!r}") from error


def rounded_millimeters(value: str, value_mm: str, unit: str,
                        row_number: int, element: str) -> int | None:
    if not value:
        return None
    normalized_unit = unit.strip().lower()
    try:
        if normalized_unit in {"in", "inch", "inches"}:
            millimeters = Decimal(value) * Decimal("25.4")
        elif normalized_unit in {"mm", "millimeter", "millimeters"}:
            millimeters = Decimal(value)
        elif value_mm:
            millimeters = Decimal(value_mm)
        else:
            raise ValueError(
                f"CSV row {row_number} has {element} value {value!r} with unsupported unit {unit!r}"
            )
    except InvalidOperation as error:
        raise ValueError(f"CSV row {row_number} has invalid {element} value {value!r}") from error
    return int(millimeters.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def load_snotel_csv(client: bigquery.Client, csv_path: Path,
                    dataset: str, station_table: str,
                    daily_table: str, refresh_stations: bool = False) -> int:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        missing = SNOTEL_SOURCE_COLUMNS - set(reader.fieldnames or [])
        extra = set(reader.fieldnames or []) - SNOTEL_SOURCE_COLUMNS
        if missing or extra:
            raise ValueError(f"CSV schema mismatch; missing={sorted(missing)}, extra={sorted(extra)}")
        source_rows = list(reader)
    if not source_rows:
        print("CSV contains no observations; nothing loaded")
        return 0

    station_rows: dict[str, dict[str, object]] = {}
    daily_rows: dict[tuple[str, str], dict[str, object]] = {}
    seen_elements: set[tuple[str, str, str]] = set()
    for row_number, row in enumerate(source_rows, start=2):
        triplet = row["station_triplet"].strip()
        observation_date = row["observation_date"].strip()
        element = row["element_code"].strip().upper()
        if not triplet:
            raise ValueError(f"CSV row {row_number} is missing station_triplet")
        try:
            date.fromisoformat(observation_date)
        except ValueError as error:
            raise ValueError(f"CSV row {row_number} has invalid observation date {observation_date!r}") from error
        if element not in SNOTEL_ELEMENT_FIELDS:
            raise ValueError(f"CSV row {row_number} has unsupported SNOTEL element {element!r}; expected SNWD or WTEQ")
        if not row["retrieved_at_utc"]:
            raise ValueError(f"CSV row {row_number} has no retrieved_at_utc provenance")

        station = {
            "station_triplet": triplet,
            "station_id": row["station_id"] or None,
            "station_name": row["station_name"] or None,
            "station_latitude": nullable_float(row["station_latitude"], "station_latitude"),
            "station_longitude": nullable_float(row["station_longitude"], "station_longitude"),
            "station_elevation_ft": nullable_float(row["station_elevation_ft"], "station_elevation_ft"),
        }
        previous_station = station_rows.setdefault(triplet, station)
        if previous_station != station:
            raise ValueError(f"CSV row {row_number} has inconsistent metadata for station {triplet}")

        key = (triplet, observation_date)
        daily = daily_rows.setdefault(key, {
            "station_triplet": triplet,
            "observation_date": observation_date,
            "snow_depth_mm": None,
            "swe_mm": None,
            "retrieved_at_utc": row["retrieved_at_utc"],
        })
        if daily["retrieved_at_utc"] != row["retrieved_at_utc"]:
            raise ValueError(f"CSV row {row_number} has inconsistent run provenance for {triplet} on {observation_date}")

        metric = SNOTEL_ELEMENT_FIELDS[element]
        metric_key = (triplet, observation_date, element)
        if metric_key in seen_elements:
            raise ValueError(f"CSV row {row_number} duplicates {element} for {triplet} on {observation_date}")
        seen_elements.add(metric_key)
        daily[f"{metric}_mm"] = rounded_millimeters(
            row["value"], row["value_mm"], row["unit"], row_number, element
        )
        daily[f"{metric}_flags_json"] = row["flags_json"] or None

    # Fill nullable fields for observations where one of the two elements is absent.
    _, daily_columns = read_schema(SNOTEL_TABLES["daily"][0])
    daily_rows_list = [{column: row.get(column) for column in daily_columns}
                       for row in daily_rows.values()]
    station_schema, _ = read_schema(SNOTEL_TABLES["station"][0])
    daily_schema, _ = read_schema(SNOTEL_TABLES["daily"][0])
    station_id = f"{client.project}.{dataset}.{station_table}"
    daily_id = f"{client.project}.{dataset}.{daily_table}"
    station_target = bigquery.Table(station_id, schema=station_schema)
    daily_target = bigquery.Table(daily_id, schema=daily_schema)
    daily_target.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="observation_date",
    )
    client.create_table(station_target, exists_ok=True)
    client.create_table(daily_target, exists_ok=True)

    # Load station metadata once; refresh it only when explicitly requested.
    current_station_table = client.get_table(station_id)
    if refresh_stations or current_station_table.num_rows == 0:
        load_json_rows(client, station_id, station_schema,
                       list(station_rows.values()), bigquery.WriteDisposition.WRITE_TRUNCATE)
    else:
        print(f"Station table already has {current_station_table.num_rows:,} rows; leaving it unchanged")

    # Daily data replaces only dates present in this CSV.
    rows_by_date: dict[str, list[dict[str, object]]] = {}
    for row in daily_rows_list:
        rows_by_date.setdefault(str(row["observation_date"]), []).append(row)
    for observation_date, rows in sorted(rows_by_date.items()):
        partition_id = observation_date.replace("-", "")
        load_json_rows(client, f"{daily_id}${partition_id}", daily_schema,
                       rows, bigquery.WriteDisposition.WRITE_TRUNCATE)
    return len(daily_rows_list)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=("snodas", "snotel"))
    parser.add_argument("csv", type=Path)
    parser.add_argument("--project", required=True, help="BigQuery project ID; auth uses Application Default Credentials")
    parser.add_argument("--dataset", required=True, help="Existing BigQuery dataset ID")
    parser.add_argument("--table", help="SNODAS daily data table name")
    parser.add_argument("--cell-table", default="snodas_cell", help="SNODAS cell metadata table")
    parser.add_argument("--refresh-cells", action="store_true",
                        help="Replace the cell metadata snapshot, even when the table already has rows")
    parser.add_argument("--station-table", default="snotel_station", help="SNOTEL station metadata table")
    parser.add_argument("--daily-table", default="snotel_daily", help="SNOTEL station/date observations table")
    parser.add_argument("--refresh-stations", action="store_true",
                        help="Replace the station metadata snapshot, even when the table already has rows")
    args = parser.parse_args()
    client = bigquery.Client(project=args.project)
    if args.source == "snotel":
        count = load_snotel_csv(client, args.csv, args.dataset,
                                args.station_table, args.daily_table,
                                args.refresh_stations)
        print(f"Loaded {count:,} station-date rows into {args.project}.{args.dataset}.{args.daily_table}")
        print(f"Station metadata table: {args.project}.{args.dataset}.{args.station_table}")
    else:
        table_name = args.table or CONFIG[args.source]["default_table"]
        count = load_snodas_csv(client, args.csv, args.dataset, table_name,
                                args.cell_table, args.refresh_cells)
        print(f"Loaded {count:,} cell-date rows into {args.project}.{args.dataset}.{table_name}")
        print(f"Cell metadata table: {args.project}.{args.dataset}.{args.cell_table}")


if __name__ == "__main__":
    main()
