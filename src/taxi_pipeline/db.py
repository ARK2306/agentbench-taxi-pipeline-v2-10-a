"""DuckDB connection helper."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb


@contextmanager
def connect(warehouse: Path, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the persistent warehouse, creating its parent directory if needed."""
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(warehouse), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()
