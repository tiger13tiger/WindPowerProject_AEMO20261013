"""Run the data quality checks over silver, and publish the results.

Each check is a SQL model in sql/quality/ returning one row per finding, with
the columns (trading_date, subject, observed, message). A check with no rows
passed. This module decides what each finding means:

  hard  the data is wrong in a way that corrupts anything built on it.
        The run exits non-zero so a workflow stops rather than publishing.
  soft  the data is surprising but plausibly real. Logged as a warning, and
        recorded for the dashboard, but the run continues.

The split matters because a pipeline that fails on everything unusual gets
muted, and the NEM is genuinely full of unusual: negative prices, farms over
nameplate, units offline for days. Only a contradiction in the data itself —
a duplicated grain, a day lying about its own completeness, an impossible
capacity factor — is worth stopping for.

Results go to data/gold/quality.parquet, rewritten in full each run. It is an
aggregate for the dashboard, not a record to accumulate, and it is small
enough that a rewrite costs nothing.

Usage:
    python scripts/check_quality.py            # check, write, exit 1 on hard
    python scripts/check_quality.py --no-write # report only
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("check_quality")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
GOLD_DIR: Final = REPO_ROOT / "data" / "gold"
SQL_DIR: Final = REPO_ROOT / "sql" / "quality"
REGISTRY: Final = REPO_ROOT / "data" / "reference" / "wind_duids.csv"

INTERVALS_PER_DAY: Final = 288

# Two hours of five-minute intervals.
MAX_MISSING_INTERVALS: Final = 24

# 1.05 allows for a farm genuinely out-rating its nameplate without tolerating
# a stale registry. The floor is slightly below zero because an idle wind farm
# draws auxiliary power: Cullerin Range on 2026-08-31 generated nothing all day
# (peak 0.0 MW) and drew up to 0.09 MW across 98 intervals, for -0.31 MWh and a
# capacity factor of -0.000427 — one farm-day in 4,712. That is a parked farm,
# not the registry or sign-convention fault this check is hard for, and 0.09 of
# 30 MW is 0.3% of nameplate. An inverted sign would mirror a real generation
# profile and land near -0.1 to -0.5, so -0.01 separates the two cleanly.
CF_MIN: Final = -0.01
CF_MAX: Final = 1.05

# Prices outside this band are real but rare enough to be worth a look.
PRICE_MAX: Final = 5_000.0
PRICE_MIN: Final = -1_000.0

HARD: Final = "hard"
SOFT: Final = "soft"

# DuckDB rejects named parameters a query does not use, so each model is given
# exactly the ones it mentions. "$/MWh" in a message string does not match,
# because a parameter name must start with a letter.
PARAM_RE: Final = re.compile(r"\$([a-z][a-z0-9_]*)")


@dataclass(frozen=True)
class Check:
    """One quality check: a SQL model, a severity, and what it needs to run."""

    name: str
    severity: str
    sql_file: str
    needs: tuple[str, ...]  # "wind", "price", "registry"


CHECKS: Final = (
    Check("duplicate_interval_duid", HARD, "duplicate_interval_duid.sql", ("wind",)),
    Check("incomplete_day_marked_complete", HARD, "incomplete_day_marked_complete.sql", ("wind",)),
    Check("capacity_factor_out_of_range", HARD, "capacity_factor_out_of_range.sql", ("wind",)),
    Check("duid_missing_too_long", SOFT, "duid_missing_too_long.sql", ("wind", "registry")),
    Check("above_capacity_rows", SOFT, "above_capacity_rows.sql", ("wind",)),
    Check("price_out_of_band", SOFT, "price_out_of_band.sql", ("price",)),
)


class QualityError(Exception):
    """The checks could not be run."""


@dataclass(frozen=True)
class Finding:
    """One thing a check objected to."""

    check: str
    severity: str
    trading_date: object
    subject: str | None
    observed: float | None
    message: str


@dataclass(frozen=True)
class Result:
    """The outcome of one check."""

    check: Check
    status: str  # pass | fail | warn | skip
    findings: tuple[Finding, ...]
    note: str = ""


def available(silver_dir: Path, registry: Path) -> set[str]:
    """Return which inputs exist, so a check with nothing to read is skipped."""
    present: set[str] = set()
    if any((silver_dir / "wind_output").glob("*.parquet")):
        present.add("wind")
    if any((silver_dir / "price").glob("*.parquet")):
        present.add("price")
    if registry.exists():
        present.add("registry")
    return present


def run_check(
    con: duckdb.DuckDBPyConnection,
    check: Check,
    sql_dir: Path,
    params: dict[str, object],
    present: set[str],
) -> Result:
    """Run one check and classify its findings."""
    missing = [need for need in check.needs if need not in present]
    if missing:
        return Result(check, "skip", (), f"no {', '.join(missing)} data")

    path = sql_dir / check.sql_file
    if not path.exists():
        raise QualityError(f"missing quality model: {path}")

    sql = path.read_text()
    used = {name: params[name] for name in set(PARAM_RE.findall(sql)) if name in params}
    rows = con.execute(sql, used).fetchall()
    findings = tuple(
        Finding(
            check=check.name,
            severity=check.severity,
            trading_date=row[0],
            subject=row[1],
            observed=None if row[2] is None else float(row[2]),
            message=str(row[3]),
        )
        for row in rows
    )

    if not findings:
        return Result(check, "pass", ())
    return Result(check, "fail" if check.severity == HARD else "warn", findings)


def log_result(result: Result) -> None:
    """Log exactly one line per check, whatever happened."""
    name = result.check.name
    count = len(result.findings)
    if result.status == "skip":
        LOG.info("SKIP  %-32s %s", name, result.note)
    elif result.status == "pass":
        LOG.info("PASS  %-32s no findings", name)
    elif result.status == "warn":
        LOG.warning("WARN  %-32s %d finding(s); first: %s", name, count, result.findings[0].message)
    else:
        LOG.error("FAIL  %-32s %d finding(s); first: %s", name, count, result.findings[0].message)


def write_table(results: list[Result], destination: Path, checked_at: datetime) -> int:
    """Write the quality table, one row per finding plus one per clean check."""
    rows: list[tuple] = []
    for result in results:
        if result.findings:
            rows.extend(
                (
                    checked_at,
                    f.check,
                    f.severity,
                    result.status,
                    f.trading_date,
                    f.subject,
                    f.observed,
                    f.message,
                )
                for f in result.findings
            )
        else:
            # A clean check still earns a row: a dashboard needs to show that a
            # check ran and found nothing, which is different from not running.
            rows.append(
                (
                    checked_at,
                    result.check.name,
                    result.check.severity,
                    result.status,
                    None,
                    None,
                    None,
                    result.note or "no findings",
                )
            )

    destination.parent.mkdir(parents=True, exist_ok=True)
    literal = str(destination).replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE quality (
                checked_at TIMESTAMPTZ,
                check_name VARCHAR,
                severity VARCHAR,
                status VARCHAR,
                trading_date DATE,
                subject VARCHAR,
                observed DOUBLE,
                message VARCHAR
            )
            """
        )
        con.executemany("INSERT INTO quality VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.execute(
            "COPY (SELECT * FROM quality ORDER BY severity, check_name, trading_date, subject) "
            f"TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return len(rows)


def check_all(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    registry: Path = REGISTRY,
    sql_dir: Path = SQL_DIR,
    write: bool = True,
) -> list[Result]:
    """Run every check. Returns one Result per check, in declaration order."""
    present = available(silver_dir, registry)
    params: dict[str, object] = {
        "wind_glob": str(silver_dir / "wind_output" / "*.parquet"),
        "price_glob": str(silver_dir / "price" / "*.parquet"),
        "registry": str(registry),
        "intervals_per_day": INTERVALS_PER_DAY,
        "max_missing_intervals": MAX_MISSING_INTERVALS,
        "cf_min": CF_MIN,
        "cf_max": CF_MAX,
        "price_max": PRICE_MAX,
        "price_min": PRICE_MIN,
    }

    results: list[Result] = []
    con = duckdb.connect()
    try:
        for check in CHECKS:
            result = run_check(con, check, sql_dir, params, present)
            log_result(result)
            results.append(result)
    finally:
        con.close()

    hard = sum(len(r.findings) for r in results if r.check.severity == HARD)
    soft = sum(len(r.findings) for r in results if r.check.severity == SOFT)
    LOG.info("%d hard finding(s), %d soft finding(s)", hard, soft)

    if write:
        written = write_table(results, gold_dir / "quality.parquet", datetime.now(UTC))
        LOG.info("wrote %s (%d rows)", gold_dir / "quality.parquet", written)

    return results


def has_hard_failure(results: list[Result]) -> bool:
    """Return whether any hard check found something."""
    return any(r.check.severity == HARD and r.findings for r in results)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run data quality checks over silver.")
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--sql-dir", type=Path, default=SQL_DIR)
    parser.add_argument("--no-write", action="store_true", help="report without writing the table")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        results = check_all(
            silver_dir=args.silver_dir,
            gold_dir=args.gold_dir,
            registry=args.registry,
            sql_dir=args.sql_dir,
            write=not args.no_write,
        )
    except QualityError as exc:
        LOG.error("%s", exc)
        return 1

    if has_hard_failure(results):
        LOG.error("hard quality failures: refusing to pass this run")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
