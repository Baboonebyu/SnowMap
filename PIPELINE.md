# SnowMap data pipeline

## SNOTEL backfills

Fetches active Utah SNOTEL stations and daily SWE (`WTEQ`) and snow depth (`SNWD`) for an inclusive date range. The station snapshot is written as JSON and observations are written as one row per station, date, and element.

```powershell
python scripts/fetch_awdb_snotel.py --start 2026-10-01 --end 2026-10-31
```

The CSV retains the AWDB value and unit, original value/unit when supplied, station coordinates and elevation, data timezone offset, observation timestamp, data source, flags, element duration, and source/retrieval provenance. `value_mm` is populated when AWDB's stored unit is inches; the original value and unit remain alongside it. Other units keep `value_mm` empty. The station metadata JSON is a snapshot for the same run. Use `--include-inactive` to include inactive stations, or `--elements WTEQ,SNWD,PREC` to request additional AWDB elements.

## BigQuery tables

Create a BigQuery dataset in advance. The schemas under `schemas/` define `snodas_daily` and `snotel_daily`; observations are partitioned by `observation_date`. Natural keys are `(observation_date, cell_id)` and `(observation_date, station_triplet, element_code)`. The loader stages each CSV and uses a `MERGE`, replacing existing rows with fresh source values on repeated runs.

Install the BigQuery client and authenticate with Application Default Credentials (for example, `gcloud auth application-default login` locally). No service-account key belongs in this repository.

```powershell
python -m pip install -r requirements.txt
python scripts/load_bigquery.py snotel data/processed/snotel_daily_awdb.csv --project YOUR_PROJECT --dataset YOUR_DATASET
python scripts/load_bigquery.py snodas data/processed/snodas_utah_20261031.csv --project YOUR_PROJECT --dataset YOUR_DATASET
```

The destination dataset must already exist. The SNOTEL loader expects the long-form AWDB CSV from `fetch_awdb_snotel.py`. The SNODAS loader expects the existing extractor's eight-column CSV. The loader validates exact CSV headers before submitting a BigQuery job.

## Data caveats

- NRCS observations may be provisional and can be revised. Keep the flags and rerun recent date ranges to refresh them.
- AWDB `dataTimeZone` is a fixed offset from GMT; the returned observation date remains the source's local daily date.
- The station metadata file is a latest-run snapshot, while the SNOTEL observation table carries station attributes as seen during each fetch.
- This step does not download SNODAS archive files or schedule jobs. The current SNODAS extractor remains a one-archive command; its output can be loaded using the command above.
