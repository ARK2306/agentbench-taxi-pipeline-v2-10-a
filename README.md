# agentbench-taxi-pipeline

A reproducible, incremental data pipeline over the
[NYC TLC yellow taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
built on Python + DuckDB.

## Layout

```
src/taxi_pipeline/      pipeline package
  config.py             paths, month helpers, DQ thresholds
  db.py                 DuckDB connection helper
  ingest.py             download raw Parquet + load raw_trips
tests/                  unit tests over synthetic Parquet fixtures
data/raw/               downloaded Parquet + zone lookup (gitignored)
data/warehouse.duckdb   persistent DuckDB warehouse (gitignored)
reports/                generated report + charts (gitignored)
```

## Quick start

```bash
python3 -m venv .venv
./.venv/bin/pip install -e ".[dev]"

./.venv/bin/taxi-pipeline ingest 2024-01
```

`ingest MONTH` downloads one month of yellow-taxi Parquet from the official TLC
CloudFront endpoint into `data/raw` (skipping files already present) and loads it
into the `raw_trips` table of `data/warehouse.duckdb`. The table is logically
partitioned on a `month` column and a re-ingest replaces that month's partition,
so the command is idempotent.

## Development

```bash
./.venv/bin/ruff check .
./.venv/bin/ruff format --check .
./.venv/bin/pytest
```
