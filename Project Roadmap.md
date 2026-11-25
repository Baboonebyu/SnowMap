# SnowMap Project Roadmap

## Project goal

Make Utah snow and water data easier to explore. The first version will collect, clean, and store SNODAS and SNOTEL observations in Google BigQuery, then display useful summaries and maps on a website.

## Big picture

```text
NOAA SNODAS --\
              +--> Collect --> Clean and validate --> BigQuery --> Website/API --> Snow maps and summaries
NRCS SNOTEL --/                                             ^
                                                           |
                                                    Scheduled refreshes
```

SNODAS and SNOTEL describe snow in different ways. SNODAS is a daily gridded model product; SNOTEL is a set of measurements from monitoring stations. Keep their source data and schemas distinct, then combine them for display or analysis where it makes sense.

## Proposed first-version scope

- Utah-focused data, with an explicit Utah boundary or grid-cell selection method.
- SNODAS snow water equivalent (SWE) and snow depth.
- Daily SNOTEL SWE, snow depth, and precipitation, subject to availability at each station.
- A repeatable Python ingestion process that can backfill a date range and update recent dates safely.
- BigQuery tables for source observations and location metadata.
- A website map with a date-specific SNODAS snow heatmap and clickable circles for SNOTEL stations.

Start with these products and a small date range. Add other variables, historical normals, forecasts, and advanced map features after the pipeline is reliable.

## Work plan

### 1. Confirm requirements and data definitions

- Choose the first website questions to support, such as “Where is snowpack highest today?” and “How has SWE changed over time?”
- Define the geographic boundary for Utah, including how to handle cells that only partly intersect the state.
- Define which SNODAS value controls the initial heatmap (SWE or snow depth) and how users switch between them.
- Decide which station fields appear when a SNOTEL circle is clicked, and whether the popup links to a longer time series.
- Choose the initial date range and refresh frequency.
- Confirm which units the website will use and document source units and conversions.
- Decide whether the first release includes all Utah SNOTEL stations or a selected subset.

**Deliverable:** a short data dictionary and a written list of initial products, locations, and date ranges.

### 2. Explore and prototype each source

**SNODAS**

- Identify and verify the download location for the daily product files needed by the project.
- Download a representative day and inspect its product header, grid dimensions, coordinate reference system, scale, units, and missing-value representation.
- Confirm how to map Utah geographic boundaries to the SNODAS grid and produce stable cell IDs.
- Prototype extracting SWE and snow depth for Utah only.

The NOHRSC archive documents daily gridded products and a nested archive layout. The product files include a text header and a big-endian binary raster, so the parser must use the file metadata rather than assume the raster's coordinate system or units. See [NOHRSC archived data](https://www.nohrsc.noaa.gov/archived_data/) and [archive format instructions](https://www.nohrsc.noaa.gov/archived_data/instructions.html).

**SNOTEL**

- Use the NRCS AWDB REST web service to list Utah SNOTEL stations and retrieve their metadata.
- Inspect available station elements and select the daily elements and units needed for the first version.
- Download a small date range for a few stations and inspect missing values, flags, timestamps, and revisions.

NRCS describes AWDB as a public source of machine-readable station and observation data. See the [NRCS Snow Survey program](https://www.nrcs.usda.gov/programs-initiatives/sswsf-snow-survey-and-water-supply-forecasting-program) and [AWDB Web Service User Guide](https://www.nrcs.usda.gov/sites/default/files/2023-03/AWDB%20Web%20Service%20User%20Guide.pdf).

**Deliverable:** small sample files or dataframes with source metadata and parsing assumptions recorded.

### 3. Define storage and data contracts

Use separate location and observation tables for the two sources. A starting BigQuery layout:

| Table | Grain | Example fields |
|---|---|---|
| `snodas_cells` | One row per stable grid cell | `cell_id`, latitude, longitude, elevation if available, geometry/grid reference |
| `snodas_daily` | One row per cell, date, and product | `observation_date`, `cell_id`, `product`, value, unit, source/version metadata |
| `snotel_stations` | One row per station | `station_id`, name, latitude, longitude, elevation, state, network, active dates if available |
| `snotel_daily` | One row per station, date, and element | `observation_date`, `station_id`, `element`, value, unit, data flags, source metadata |

Document the natural key for each observation table. Likely keys are `(observation_date, cell_id, product)` for SNODAS and `(observation_date, station_id, element)` for SNOTEL. Partition observation tables by date. Retain source units, normalized units, quality flags where provided, and an ingestion timestamp. Agree on final field names and types before building the loader.

**Deliverable:** version-controlled schema definitions and a concise data dictionary.

### 4. Build the ingestion pipeline

Organize the Python code into clear stages:

1. **Discover and fetch:** request the required source files and API responses for a date range.
2. **Parse:** unpack SNODAS products and read the raster using its header; parse AWDB station observations and metadata.
3. **Transform:** select Utah cells/stations, normalize dates and units, and reshape records to the agreed schemas.
4. **Validate:** check schemas, duplicate keys, expected date coverage, plausible coordinate ranges, missingness, and known source flags. Report anomalies without silently discarding source values.
5. **Load:** write records to BigQuery with explicit schemas and retryable jobs.
6. **Record run status:** capture source, requested date range, row counts, completion time, and errors, without logging credentials or other secrets.

Make the process idempotent: rerunning a date range should update or replace the same logical records rather than create duplicates. Recent records may be revised by their source, so plan a small rolling re-fetch window as well as historical backfills.

The official [BigQuery Python client documentation](https://docs.cloud.google.com/python/docs/reference/bigquery/latest) covers client setup and loading data. Authentication should use Application Default Credentials locally and an attached service identity in the deployed environment; do not put service-account keys or tokens in the repository.

**Deliverable:** a command-line pipeline that can run for one date, a date range, or the configured recent refresh window.

### 5. Validate the stored data

- Compare a sample of extracted SNODAS values and coordinates with the source raster and metadata.
- Compare selected SNOTEL station values with NRCS reports for the same dates and elements.
- Check that repeated runs do not introduce duplicate natural keys.
- Check date gaps and distinguish missing source data from parser or download failures.
- Run representative BigQuery queries for map tiles, daily summaries, and station time series.
- Document data limitations, update timing, units, and provenance for users.

**Deliverable:** a validated initial data set and a repeatable checklist for future source or schema changes.

### 6. Schedule and operate the pipeline

- Choose a runtime and scheduler. A small GCP deployment using Cloud Run Jobs and Cloud Scheduler is a natural fit because data is already being written to BigQuery; a local scheduled job can be used during development.
- Configure runtime identity with only the access needed to read source data and write to the target BigQuery dataset.
- Set timeouts, retries, and useful failure reporting.
- Choose a schedule after checking source publication timing; avoid assuming the newest date is available immediately.
- Keep ingestion logs and run status so missed dates can be detected and backfilled.

**Deliverable:** an unattended scheduled run with a documented recovery procedure.

### 7. Build the website and data access layer

- Make the SNODAS grid the colored heatmap layer, with a date selector, SWE/depth choice, and legend that states the units.
- Draw each SNOTEL location as a circle above the SNODAS layer. Clicking a circle should show the station name, observation date, SWE, snow depth, and data status; a later enhancement can add a station time series.
- Keep the station circles visually distinct from the SNODAS color scale, and make the station layer easy to toggle when it obscures the heatmap.
- Decide how the site will read BigQuery-backed data. A small API/service layer can constrain queries, cache common responses, and keep database access out of the browser.
- Serve the large SNODAS grid through raster/map tiles or another tiled representation instead of sending every cell as a large GeoJSON response for each map view. Serve the much smaller SNOTEL station set and selected-date observations as point data.
- Show units, source, observation date, and relevant caveats alongside the visualizations.
- Cache stable or frequently requested tiles and station responses where it improves load time and controls query cost.

**Deliverable:** a usable first website backed by the stored data, followed by incremental map and comparison features.

### 8. Deploy, monitor, and improve

- Deploy the site and ingestion process with separate development and production configuration.
- Monitor scheduled runs, API errors, page performance, and BigQuery usage.
- Add data freshness indicators and clear error behavior when the newest data is unavailable.
- Gather feedback and prioritize additional variables, historical comparisons, and visualization improvements.

**Deliverable:** a maintained public-facing SnowMap experience with a clear path for adding data sources and features.

## Initial implementation order

1. Inspect one SNODAS day and a few SNOTEL stations.
2. Finalize units, date conventions, Utah selection, and BigQuery schemas.
3. Implement and validate each source adapter separately.
4. Add repeatable BigQuery loads and a date-range backfill command.
5. Run the pipeline on a small sample, then an initial historical range.
6. Schedule routine updates and monitor freshness.
7. Build the first map and station views on top of stable tables.

## Decisions to make during the first phase

- What is the initial historical date range?
- Which Utah boundary dataset will define the SNODAS clipping rule?
- Should SNODAS be represented as one row per cell/product or one wider row per cell/date with SWE and depth columns? The long form is easier to extend to more products; the wide form can be simpler for a narrow first map.
- Which daily SNOTEL elements and units are needed at launch?
- How often should the pipeline run, given the data publication schedule?
- Which API and hosting approach best fits the website's first release?

## Data-source and platform references

- [NOHRSC SNODAS archived data](https://www.nohrsc.noaa.gov/archived_data/)
- [NOHRSC archive format instructions](https://www.nohrsc.noaa.gov/archived_data/instructions.html)
- [NRCS Snow Survey and Water Supply Forecasting Program](https://www.nrcs.usda.gov/programs-initiatives/sswsf-snow-survey-and-water-supply-forecasting-program)
- [NRCS AWDB Web Service User Guide](https://www.nrcs.usda.gov/sites/default/files/2023-03/AWDB%20Web%20Service%20User%20Guide.pdf)
- [Google BigQuery Python client](https://docs.cloud.google.com/python/docs/reference/bigquery/latest)
