"""Render charts, the month-by-month comparison and the Markdown report."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402

from .config import Paths  # noqa: E402

log = logging.getLogger(__name__)

AGGREGATE_TABLES = ("daily_trips", "hourly_fare_tip", "top_zones")

INK = "#1f2933"
ACCENT = "#2f6f9f"
ACCENT_ALT = "#c86b3c"
GRID = "#d7dde3"


def _style_axes(ax) -> None:
    ax.set_facecolor("white")
    ax.grid(True, axis="y", color=GRID, linewidth=0.7)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK, labelsize=9)
    ax.yaxis.label.set_color(INK)
    ax.xaxis.label.set_color(INK)
    ax.title.set_color(INK)


def chart_trips_per_day(conn, out_path: Path) -> Path:
    """Line chart of cleaned trips per calendar day across every ingested month."""
    rows = conn.execute(
        "SELECT pickup_date, sum(trip_count) FROM daily_trips GROUP BY 1 ORDER BY 1"
    ).fetchall()
    dates = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=150)
    ax.plot(dates, counts, color=ACCENT, linewidth=1.6)
    ax.fill_between(dates, counts, color=ACCENT, alpha=0.12)
    ax.set_title("Yellow taxi trips per day", fontsize=13, pad=12, loc="left")
    ax.set_ylabel("trips")
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}k"))
    _style_axes(ax)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    return out_path


def chart_hourly_fare_tip(conn, out_path: Path) -> Path:
    """Average fare (bars) against tip percentage (line) by pickup hour."""
    rows = conn.execute(
        "SELECT pickup_hour, avg_fare, tip_pct FROM hourly_fare_tip ORDER BY pickup_hour"
    ).fetchall()
    hours = [row[0] for row in rows]
    fares = [row[1] for row in rows]
    tips = [row[2] for row in rows]

    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=150)
    ax.bar(hours, fares, color=ACCENT, width=0.72, label="avg fare ($)")
    ax.set_xlabel("pickup hour")
    ax.set_ylabel("average fare ($)")
    ax.set_xticks(range(0, 24))
    ax.set_title("Average fare and tip percentage by hour of day", fontsize=13, pad=12, loc="left")
    _style_axes(ax)

    ax2 = ax.twinx()
    ax2.plot(
        hours,
        tips,
        color=ACCENT_ALT,
        linewidth=2.0,
        marker="o",
        markersize=3.5,
        label="tip % of fare",
    )
    ax2.set_ylabel("tip as % of fare", color=INK)
    ax2.tick_params(colors=INK, labelsize=9)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color(GRID)
    ax2.grid(False)

    handles = ax.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
    labels = ax.get_legend_handles_labels()[1] + ax2.get_legend_handles_labels()[1]
    ax.legend(handles, labels, loc="upper left", frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    return out_path


def chart_top_zones(conn, out_path: Path, limit: int = 10) -> Path:
    """Horizontal bar chart of the busiest pickup zones."""
    rows = conn.execute(
        "SELECT zone, borough, trip_count FROM top_zones ORDER BY trip_count DESC LIMIT ?",
        [limit],
    ).fetchall()
    labels = [f"{row[0]} ({row[1]})" for row in rows][::-1]
    counts = [row[2] for row in rows][::-1]

    fig, ax = plt.subplots(figsize=(10, 4.8), dpi=150)
    ax.barh(labels, counts, color=ACCENT, height=0.68)
    ax.set_xlabel("trips")
    ax.set_title(f"Top {limit} pickup zones", fontsize=13, pad=12, loc="left")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}k"))
    _style_axes(ax)
    ax.grid(True, axis="x", color=GRID, linewidth=0.7)
    ax.grid(False, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    return out_path


def month_comparison(conn) -> list[tuple]:
    """One row per ingested month: raw/clean counts, survival, fare and tip stats."""
    return conn.execute(
        """
        SELECT
            r.month,
            r.raw_rows,
            coalesce(c.clean_rows, 0)                                   AS clean_rows,
            coalesce(c.clean_rows, 0) * 1.0 / nullif(r.raw_rows, 0)     AS survival_rate,
            c.avg_fare,
            c.avg_distance,
            c.avg_duration,
            c.tip_pct,
            c.total_revenue
        FROM (SELECT month, count(*) AS raw_rows FROM raw_trips GROUP BY month) r
        LEFT JOIN (
            SELECT
                month,
                count(*)                   AS clean_rows,
                avg(fare_amount)           AS avg_fare,
                avg(trip_distance)         AS avg_distance,
                avg(trip_duration_minutes) AS avg_duration,
                100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0) AS tip_pct,
                sum(total_amount)          AS total_revenue
            FROM trips GROUP BY month
        ) c ON c.month = r.month
        ORDER BY r.month
        """
    ).fetchall()


def dq_rows(conn):
    """Rows of the most recent DQ run, or an empty list if none were recorded."""
    tables = {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
    if "dq_results" not in tables:
        return []
    return conn.execute(
        """
        SELECT check_name, scope, passed, observed, threshold, details, run_at
        FROM dq_results
        WHERE run_at = (SELECT max(run_at) FROM dq_results)
        ORDER BY scope, check_name
        """
    ).fetchall()


def export_aggregates(conn, out_dir: Path) -> list[Path]:
    """Write each aggregate table out as a Parquet file for release attachment."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for table in AGGREGATE_TABLES:
        destination = out_dir / f"{table}.parquet"
        conn.execute(
            f"COPY {table} TO '{destination.as_posix()}' (FORMAT PARQUET)"  # noqa: S608
        )
        written.append(destination)
    return written


def _fmt(value, spec: str = ",.2f", dash: str = "-") -> str:
    return dash if value is None else format(value, spec)


def render_report(conn, paths: Paths) -> Path:
    """Write ``reports/report.md`` with embedded charts, comparison table and DQ results."""
    paths.ensure()
    charts = {
        "trips_per_day": chart_trips_per_day(conn, paths.charts / "trips_per_day.png"),
        "hourly_fare_tip": chart_hourly_fare_tip(conn, paths.charts / "hourly_fare_tip.png"),
        "top_zones": chart_top_zones(conn, paths.charts / "top_zones.png"),
    }
    export_aggregates(conn, paths.exports)

    comparison = month_comparison(conn)
    checks = dq_rows(conn)
    months = [row[0] for row in comparison]

    lines: list[str] = [
        "# NYC Yellow Taxi — pipeline report",
        "",
        f"Months ingested: **{', '.join(months) if months else 'none'}**",
        "",
        "Source: NYC TLC yellow taxi trip records, loaded into DuckDB by `taxi-pipeline`.",
        "All figures below come from the cleaned `trips` table unless stated otherwise.",
        "",
        "## Trips per day",
        "",
        f"![Trips per day](charts/{charts['trips_per_day'].name})",
        "",
        "## Average fare and tip percentage by hour",
        "",
        f"![Average fare and tip percentage by hour](charts/{charts['hourly_fare_tip'].name})",
        "",
    ]

    hourly = conn.execute(
        """
        SELECT pickup_hour, trip_count, avg_fare, tip_pct
        FROM hourly_fare_tip ORDER BY pickup_hour
        """
    ).fetchall()
    if hourly:
        lines += [
            "| Hour | Trips | Avg fare | Tip % of fare |",
            "| ---: | ----: | -------: | ------------: |",
        ]
        lines += [
            f"| {hour:02d} | {count:,} | ${_fmt(fare)} | {_fmt(tip)}% |"
            for hour, count, fare, tip in hourly
        ]
        lines.append("")

    lines += [
        "## Top 10 pickup zones",
        "",
        f"![Top 10 pickup zones](charts/{charts['top_zones'].name})",
        "",
        "| # | Zone | Borough | Trips | Avg fare | Total revenue |",
        "| -: | ---- | ------- | ----: | -------: | ------------: |",
    ]
    top = conn.execute(
        """
        SELECT zone, borough, trip_count, avg_fare, total_revenue
        FROM top_zones ORDER BY trip_count DESC LIMIT 10
        """
    ).fetchall()
    lines += [
        f"| {rank} | {zone} | {borough} | {count:,} | ${_fmt(fare)} | ${_fmt(revenue, ',.0f')} |"
        for rank, (zone, borough, count, fare, revenue) in enumerate(top, start=1)
    ]

    lines += [
        "",
        "## Month-by-month comparison",
        "",
        "| Month | Raw rows | Clean rows | Survival | Avg fare | Avg distance (mi) "
        "| Avg duration (min) | Tip % | Total revenue |",
        "| ----- | -------: | ---------: | -------: | -------: | ----------------: "
        "| -----------------: | ----: | ------------: |",
    ]
    for month, raw, clean, survival, fare, distance, duration, tip, revenue in comparison:
        lines.append(
            f"| {month} | {raw:,} | {clean:,} | {_fmt(survival, '.2%')} | ${_fmt(fare)} "
            f"| {_fmt(distance)} | {_fmt(duration, ',.1f')} | {_fmt(tip)}% "
            f"| ${_fmt(revenue, ',.0f')} |"
        )

    lines += ["", "## Data-quality checks", ""]
    if checks:
        run_at = checks[0][6]
        passed = sum(1 for row in checks if row[2])
        lines += [
            f"Run at {run_at:%Y-%m-%d %H:%M:%S} UTC — **{passed}/{len(checks)} passed**.",
            "",
            "| Check | Scope | Result | Observed | Threshold | Details |",
            "| ----- | ----- | ------ | -------: | --------: | ------- |",
        ]
        for name, scope, ok, observed, threshold, details, _ in checks:
            mark = "PASS" if ok else "FAIL"
            if name in ("survival_rate", "month_over_month_change"):
                observed_text = _fmt(observed, ".2%")
                threshold_text = _fmt(threshold, ".0%")
            else:
                observed_text = _fmt(observed, ",.0f")
                threshold_text = _fmt(threshold, ",.0f")
            lines.append(
                f"| `{name}` | {scope} | {mark} | {observed_text} | {threshold_text} | {details} |"
            )
    else:
        lines.append("_No data-quality results recorded yet — run `taxi-pipeline transform`._")

    lines += [
        "",
        "## Aggregate exports",
        "",
        "Parquet copies of the aggregate tables are written to `reports/exports/`:",
        "",
    ]
    lines += [f"- `exports/{table}.parquet`" for table in AGGREGATE_TABLES]
    lines.append("")

    report_path = paths.reports / "report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("wrote %s", report_path)
    return report_path
