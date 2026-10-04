"""Command line entry point for the taxi pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from .config import default_paths, validate_month
from .ingest import ingest_month


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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    paths = default_paths()

    if args.command == "ingest":
        rows = ingest_month(args.month, paths, force_download=args.force_download)
        print(f"ingested {args.month}: {rows:,} raw rows")
        return 0

    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    sys.exit(main())
