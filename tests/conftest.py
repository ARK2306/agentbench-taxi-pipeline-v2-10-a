"""Shared fixtures: synthetic Parquet inputs and a scratch warehouse."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.config import Paths  # noqa: E402


@pytest.fixture
def paths(tmp_path: Path) -> Paths:
    p = Paths(tmp_path)
    p.ensure()
    return p
