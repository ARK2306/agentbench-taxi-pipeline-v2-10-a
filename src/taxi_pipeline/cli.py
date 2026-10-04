"""Command line entry point for the taxi pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from .config import default_paths, validate_month
from .dq import DataQualityError
from .ingest import ingest_month
from .pipeline import PipelineError, report, run_all, transform


def _month_arg(value: str) -> str:
    try:
        return validate_month(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taxi-pipeline",
        description="Incremental pipeline over the NYC TLC yellow taxi trip data.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="download one month of raw data and load it")
    ingest.add_argument("month", type=_month_arg, help="month to ingest, as YYYY-MM")
    ingest.add_argument(
        "--force-download",
        action="store_true",
        help="re-download even if the Parquet file is already in data/raw",
    )

    transform_cmd = sub.add_parser(
        "transform",
        help="build the cleaned trips table and aggregates, then run data-quality checks",
    )
    transform_cmd.add_argument(
        "--month",
        dest="months",
        action="append",
        type=_month_arg,
        help="limit cleaning to this month (repeatable); defaults to every ingested month",
    )

    sub.add_parser("report", help="write reports/report.md with charts and DQ results")

    run_all_cmd = sub.add_parser("run-all", help="ingest, transform and report in one go")
    run_all_cmd.add_argument("months", nargs="+", type=_month_arg, help="months as YYYY-MM")
    run_all_cmd.add_argument(
        "--force-download",
        action="store_true",
        help="re-download even if the Parquet files are already in data/raw",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    paths = default_paths()

    try:
        if args.command == "ingest":
            rows = ingest_month(args.month, paths, force_download=args.force_download)
            print(f"ingested {args.month}: {rows:,} raw rows")
        elif args.command == "transform":
            summary = transform(paths, args.months)
            for month, (raw_rows, clean_rows) in summary["cleaned"].items():
                print(f"transformed {month}: {clean_rows:,} clean of {raw_rows:,} raw rows")
            print(f"aggregates: {summary['aggregates']}")
            print(f"data-quality: {len(summary['checks'])} checks passed")
        elif args.command == "report":
            print(f"wrote {report(paths)}")
        elif args.command == "run-all":
            print(f"wrote {run_all(args.months, paths, force_download=args.force_download)}")
        else:
            raise AssertionError(f"unhandled command {args.command!r}")
    except DataQualityError as exc:
        print(f"data-quality failure: {exc}", file=sys.stderr)
        return 2
    except PipelineError as exc:
        print(f"pipeline error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
