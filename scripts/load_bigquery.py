"""Load SnowMap CSV data into BigQuery using ADC."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from pathlib import Path
import uuid

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
CONFIG = {
    "snodas": {
        "schema": ROOT / "schemas" / "snodas_daily.json",
        "key": ("observation_date", "cell_id"),
        "partition": "observation_date",
        "default_table": "snodas_daily",
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
                    dataset: str, table_name: str) -> int:
    config = CONFIG["snodas"]
    schema, columns = read_schema(config["schema"])
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
    for row_number, row in enumerate(rows, start=2):
        if not row[config["partition"]]:
            raise ValueError(f"CSV row {row_number} has no observation date")
        date.fromisoformat(row[config["partition"]])
        if any(not row[key] for key in config["key"]):
            raise ValueError(f"CSV row {row_number} is missing a natural key field")
    table_id = f"{client.project}.{dataset}.{table_name}"
    target = bigquery.Table(table_id, schema=schema)
    target.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=config["partition"],
    )
    client.create_table(target, exists_ok=True)
    stage_id = f"{client.project}.{dataset}._snowmap_stage_{uuid.uuid4().hex}"
    load_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
    )
    try:
        with csv_path.open("rb") as source_file:
            client.load_table_from_file(source_file, stage_id, job_config=load_config).result()
        on_clause = " AND ".join(f"T.`{field}` = S.`{field}`" for field in config["key"])
        update_columns = [field for field in columns if field not in config["key"]]
        update_clause = ", ".join(f"`{field}` = S.`{field}`" for field in update_columns)
        insert_columns = ", ".join(f"`{field}`" for field in columns)
        insert_values = ", ".join(f"S.`{field}`" for field in columns)
        sql = f"""
            MERGE `{table_id}` T
            USING `{stage_id}` S
              ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
        client.query(sql).result()
    finally:
        client.delete_table(stage_id, not_found_ok=True)
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
    parser.add_argument("--table", help="Destination table name (SNODAS daily table only)")
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
        count = load_snodas_csv(client, args.csv, args.dataset, table_name)
        print(f"Merged {count:,} rows into {args.project}.{args.dataset}.{table_name}")


if __name__ == "__main__":
    main()
