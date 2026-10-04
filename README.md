# agentbench-taxi-pipeline

A reproducible, incremental data pipeline over the
[NYC TLC yellow taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
built on Python + DuckDB.

## Layout

```
src/taxi_pipeline/
  config.py     paths, month helpers, DQ thresholds
  db.py         DuckDB connection helper
  ingest.py     download raw Parquet + load raw_trips / zones
  transform.py  cleaned trips table + daily/hourly/zone aggregates
  dq.py         data-quality checks and the dq_results audit table
  report.py     matplotlib charts + reports/report.md
  pipeline.py   transform / report / run-all orchestration
  cli.py        the taxi-pipeline entry point
tests/          unit tests over synthetic Parquet fixtures
```

Generated output (all gitignored):

```
data/raw/               downloaded Parquet + taxi_zone_lookup.csv
data/warehouse.duckdb   persistent DuckDB warehouse
reports/report.md       the report
reports/charts/*.png    embedded charts
reports/exports/*.parquet   aggregate tables, exported for releases
```

## Quick start

```bash
python3 -m venv .venv
./.venv/bin/pip install -e ".[dev]"

./.venv/bin/taxi-pipeline run-all 2024-01 2024-02 2024-03
```

## Commands

| Command | What it does |
| ------- | ------------ |
| `ingest MONTH` | Download one month of yellow-taxi Parquet into `data/raw` (skipping files already present) and load it into `raw_trips`. |
| `transform` | Rebuild the cleaned `trips` table and the aggregates, then run the data-quality gate. `--month YYYY-MM` (repeatable) limits which partitions are re-cleaned. |
| `report` | Write `reports/report.md` with the chart PNGs, the month-by-month table and the DQ results. |
| `run-all MONTH...` | `ingest` each month, then `transform`, then `report`. |

Exit codes: `0` success, `1` pipeline error (e.g. nothing ingested yet),
`2` a data-quality check failed.

## The model

**`raw_trips`** — every TLC column, plus a leading `month` partition column.
The table is logically partitioned on `month`: re-ingesting a month deletes that
partition before reloading it, so `ingest` is idempotent and months are
independent.

**`trips`** — the cleaned layer. A raw row reaches it only if it has a positive
fare, a positive distance, a positive duration, a pickup inside the requested
month, and no NULL in a key column; exact duplicate rows are collapsed. The
layer adds `trip_duration_minutes`, `pickup_date` and `pickup_hour`.

**Aggregates** — `daily_trips` (per month and calendar day), `hourly_fare_tip`
(per hour of day, with tip as a percentage of fare) and `top_zones` (joined to
the official taxi zone lookup).

## Data-quality gate

`transform` runs these checks, records every result in `dq_results`, and exits
non-zero if any fail:

| Check | Rule |
| ----- | ---- |
| `survival_rate` | ≥ 95% of a month's raw rows survive cleaning |
| `no_nulls_in_key_columns` | no NULLs in pickup/dropoff time, PU/DO location, fare or distance |
| `known_pickup_zones` | every `PULocationID` matches a row in `zones` |
| `month_over_month_change` | consecutive months differ in trip count by < 40% |

## Development

```bash
./.venv/bin/ruff check .
./.venv/bin/ruff format --check .
./.venv/bin/pytest
```

Tests generate their own synthetic TLC-shaped Parquet fixtures
(`tests/fixtures/build.py`) and never touch the network. They include fixtures
built to fail each DQ check.

## CI

- **`ci.yml` / `lint-and-test`** — ruff + pytest on every push and PR.
- **`ci.yml` / `pipeline`** — manual dispatch and a weekly schedule; runs
  `run-all` for one month (default `2024-01`), caches `data/raw` with
  `actions/cache` keyed on the month, and uploads `reports/` as an artifact.
- **`release.yml`** — on a `v*` tag, runs the pipeline for 2024-01..2024-03 and
  attaches `report.md`, the chart PNGs and the aggregate Parquet exports to a
  GitHub Release.
