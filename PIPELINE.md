# SnowMap data pipeline

## SNOTEL backfills

Fetches active Utah SNOTEL stations and daily SWE (`WTEQ`) and snow depth (`SNWD`) for an inclusive date range. The station snapshot is written as JSON and source observations are written as one row per station, date, and element.

```bash
python scripts/fetch_awdb_snotel.py --start 2026-10-01 --end 2026-10-31
```

The observations CSV contains only the date, station identifiers and location, element code, source value/unit needed for conversion, quality flags, and retrieval time. `value_mm` is populated when AWDB's stored unit is inches. A separate station metadata JSON snapshot is also written but is not loaded into BigQuery. Use `--include-inactive` to include inactive stations. The BigQuery loader accepts the two ERD elements `WTEQ` and `SNWD`; additional elements can be fetched for separate analysis but must be removed or handled separately before loading.

## BigQuery tables

Create a BigQuery dataset in advance. `schemas/snotel_station.json` and `schemas/snotel_daily.json` define the ERD's two SNOTEL tables. `snotel_station` has one row per station, keyed by `station_triplet`, with station ID, name, coordinates, and elevation. `snotel_daily` has one row per station and date, keyed by `(station_triplet, observation_date)` and partitioned by `observation_date`. Each daily row stores snow depth and SWE rounded to whole millimeters, their quality flags, and the retrieval time. It uses load jobs rather than DML: station metadata loads when the station table is empty and stays unchanged on later runs; pass `--refresh-stations` to replace it. Each run overwrites only daily date partitions present in the CSV, preserving other dates and making overlapping reruns idempotent.

SNODAS follows the ERD with two tables. `snodas_cell` stores one row per grid cell (`id`, latitude, and longitude); its stable numeric ID is `grid_row * 2^32 + grid_col`. `snodas_data` stores one row per cell and date (`cell_id`, `date`, snow depth, and SWE) and is partitioned by `date`. The loader uses BigQuery load jobs: it populates the cell table when empty and replaces only the date partitions included in the CSV. Reruns therefore update daily observations without DML. Pass `--refresh-cells` to replace the cell metadata snapshot. `schemas/snodas_source.json` describes the extractor CSV, while `schemas/snodas_cell.json` and `schemas/snodas_data.json` describe the ERD-aligned destination tables.

Install the BigQuery client and authenticate with Application Default Credentials (for example, `gcloud auth application-default login` locally). No service-account key belongs in this repository.

```bash
python -m pip install -r requirements.txt
python scripts/load_bigquery.py snotel data/processed/snotel_daily_awdb.csv --project snowmap-515822 --dataset snowmap
python scripts/load_bigquery.py snodas data/processed/snodas_utah_20261031.csv --project YOUR_PROJECT --dataset YOUR_DATASET
```

The destination dataset must already exist. The SNOTEL loader expects the long-form AWDB CSV from `fetch_awdb_snotel.py`; its exact source headers are validated before loading. The SNODAS loader expects the existing extractor's eight-column CSV and creates or updates `snodas_cell` and `snodas_data`.

To update values or flags for a date range, fetch that range again and load the resulting CSV. For example, to refresh February 2025:

```bash
python scripts/fetch_awdb_snotel.py --start 2025-02-01 --end 2025-02-28
python scripts/load_bigquery.py snotel data/processed/snotel_daily_awdb.csv --project snowmap-515822 --dataset snowmap
```

The loader replaces those daily date partitions, so the same command can be rerun without duplicating rows. Dates outside the requested range remain in the table.

## Data caveats

- NRCS observations may be provisional and can be revised. Keep the flags and rerun recent date ranges to refresh them.
- AWDB `dataTimeZone` is a fixed offset from GMT; the returned observation date remains the source's local daily date.
- The station metadata file is a latest-run snapshot. The SNOTEL station table carries station attributes from the observations CSV, while each daily row retains its AWDB retrieval provenance.
- This step does not download SNODAS archive files or schedule jobs. The current SNODAS extractor remains a one-archive command; its output can be loaded using the command above.
