"""Idempotently merge a normalized SnowMap CSV into BigQuery using ADC."""

from __future__ import annotations

import argparse
import csv
import json
import uuid
from datetime import date
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
CONFIG = {
    "snodas": {
        "schema": ROOT / "schemas" / "snodas_daily.json",
        "key": ("observation_date", "cell_id"),
        "partition": "observation_date",
        "default_table": "snodas_daily",
    },
    "snotel": {
        "schema": ROOT / "schemas" / "snotel_daily.json",
        "key": ("observation_date", "station_triplet", "element_code"),
        "partition": "observation_date",
        "default_table": "snotel_daily",
    },
}


def load_csv(client: bigquery.Client, source: str, csv_path: Path,
             dataset: str, table_name: str) -> int:
    config = CONFIG[source]
    schema_data = json.loads(config["schema"].read_text(encoding="utf-8"))
    schema = [bigquery.SchemaField(item["name"], item["type"], mode=item["mode"])
              for item in schema_data]
    columns = [item["name"] for item in schema_data]
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

    stage_name = f"_snowmap_stage_{uuid.uuid4().hex}"
    stage_id = f"{client.project}.{dataset}.{stage_name}"
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=CONFIG)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--project", required=True, help="BigQuery project ID; auth uses Application Default Credentials")
    parser.add_argument("--dataset", required=True, help="Existing BigQuery dataset ID")
    parser.add_argument("--table", help="Destination table name")
    args = parser.parse_args()
    table_name = args.table or CONFIG[args.source]["default_table"]
    client = bigquery.Client(project=args.project)
    count = load_csv(client, args.source, args.csv, args.dataset, table_name)
    print(f"Merged {count:,} rows into {args.project}.{args.dataset}.{table_name}")


if __name__ == "__main__":
    main()
