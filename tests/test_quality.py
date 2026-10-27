"""Tests for the data quality checks.

Every check is exercised twice: once on data it must object to, and once on
data it must stay quiet about. A check only tested against clean data proves
nothing — it would pass just as happily if its WHERE clause were inverted.
"""

from datetime import date, datetime, timedelta

import duckdb
import pytest
from check_quality import (
    HARD,
    MAX_MISSING_INTERVALS,
    SOFT,
    check_all,
    has_hard_failure,
)
from nemweb import NEM_TZ

DAY = date(2026, 10, 12)

WIND_DDL = """
CREATE OR REPLACE TABLE wind (
    trading_date DATE, interval_start_utc TIMESTAMPTZ, interval_start_nem TIMESTAMP,
    interval_end_utc TIMESTAMPTZ, interval_end_nem TIMESTAMP, duid VARCHAR,
    station_name VARCHAR, region VARCHAR, capacity_mw DOUBLE, mw DOUBLE,
    is_negative BOOLEAN, is_above_capacity BOOLEAN, source_file VARCHAR,
    intervals_present SMALLINT, is_complete BOOLEAN
)
"""

PRICE_DDL = """
CREATE OR REPLACE TABLE price (
    trading_date DATE, interval_start_utc TIMESTAMPTZ, interval_start_nem TIMESTAMP,
    interval_end_utc TIMESTAMPTZ, interval_end_nem TIMESTAMP, region VARCHAR,
    rrp DOUBLE, is_negative BOOLEAN, intervention INTEGER, apcflag INTEGER,
    marketsuspendedflag INTEGER, source_file VARCHAR,
    intervals_present SMALLINT, is_complete BOOLEAN
)
"""

REGISTRY = """duid,station_name,region,fuel_type,capacity_mw,in_scada
WFA1,Alpha Wind Farm,SA1,Wind,100.0,true
WFB1,Beta Wind Farm,VIC1,Wind,100.0,true
WFOFF1,Offline Wind Farm,VIC1,Wind,20.0,false
"""


def _write(ddl, table, rows, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(ddl)
        con.executemany(
            f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows
        )
        con.execute(f"COPY {table} TO '{destination}' (FORMAT PARQUET)")
    finally:
        con.close()


def wind_rows(
    duids=("WFA1",),
    intervals=288,
    mw=10.0,
    capacity=100.0,
    is_complete=True,
    intervals_present=None,
    above_capacity=False,
    skip_last=0,
):
    """Build silver wind rows. mw/capacity sets the capacity factor directly."""
    present = intervals if intervals_present is None else intervals_present
    rows = []
    for duid in duids:
        for i in range(intervals - (skip_last if duid == duids[-1] else 0)):
            start_nem = datetime(DAY.year, DAY.month, DAY.day) + timedelta(minutes=5 * i)
            start_utc = start_nem.replace(tzinfo=NEM_TZ)
            rows.append(
                (
                    DAY, start_utc, start_nem,
                    start_utc + timedelta(minutes=5), start_nem + timedelta(minutes=5),
                    duid, f"{duid} Farm", "SA1", capacity, mw,
                    mw < 0, above_capacity, "f.zip", present, is_complete,
                )
            )
    return rows


def price_rows(rrp=50.0, intervals=288, region="SA1"):
    rows = []
    for i in range(intervals):
        start_nem = datetime(DAY.year, DAY.month, DAY.day) + timedelta(minutes=5 * i)
        start_utc = start_nem.replace(tzinfo=NEM_TZ)
        rows.append(
            (
                DAY, start_utc, start_nem,
                start_utc + timedelta(minutes=5), start_nem + timedelta(minutes=5),
                region, rrp, rrp < 0, 0, 0, 0, "p.zip", intervals, True,
            )
        )
    return rows


def run(tmp_path, wind=None, price=None, registry=REGISTRY):
    """Lay out a silver tree and run every check against it."""
    silver = tmp_path / "silver"
    if wind:
        _write(WIND_DDL, "wind", wind, silver / "wind_output" / f"date={DAY}.parquet")
    if price:
        _write(PRICE_DDL, "price", price, silver / "price" / f"date={DAY}.parquet")

    registry_path = tmp_path / "wind_duids.csv"
    registry_path.write_text(registry)

    results = check_all(
        silver_dir=silver,
        gold_dir=tmp_path / "gold",
        registry=registry_path,
        write=True,
    )
    return {r.check.name: r for r in results}


# --- hard: duplicate grain ------------------------------------------------


def test_duplicate_grain_fails(tmp_path):
    rows = wind_rows()
    rows.append(rows[10])  # the same (interval_start, duid) twice
    result = run(tmp_path, wind=rows)["duplicate_interval_duid"]

    assert result.status == "fail"
    assert result.check.severity == HARD
    assert "duplicate grain: 2 rows for WFA1" in result.findings[0].message


def test_clean_grain_passes(tmp_path):
    assert run(tmp_path, wind=wind_rows())["duplicate_interval_duid"].status == "pass"


def test_same_interval_different_duids_is_not_a_duplicate(tmp_path):
    result = run(tmp_path, wind=wind_rows(duids=("WFA1", "WFB1")))["duplicate_interval_duid"]
    assert result.status == "pass"


# --- hard: a day lying about its completeness -----------------------------


def test_short_day_marked_complete_fails(tmp_path):
    result = run(tmp_path, wind=wind_rows(intervals=100, is_complete=True))[
        "incomplete_day_marked_complete"
    ]
    assert result.status == "fail"
    assert "100/288" in result.findings[0].message


def test_sealed_short_day_passes(tmp_path):
    # The legitimate short day: flagged incomplete, so nothing is being claimed.
    result = run(
        tmp_path, wind=wind_rows(intervals=100, is_complete=False, intervals_present=100)
    )["incomplete_day_marked_complete"]
    assert result.status == "pass"


def test_full_day_marked_complete_passes(tmp_path):
    result = run(tmp_path, wind=wind_rows())["incomplete_day_marked_complete"]
    assert result.status == "pass"


# --- hard: capacity factor ------------------------------------------------


def test_capacity_factor_above_limit_fails(tmp_path):
    # mw/capacity is the capacity factor when output is flat: 110/100 = 1.10.
    result = run(tmp_path, wind=wind_rows(mw=110.0, capacity=100.0))[
        "capacity_factor_out_of_range"
    ]
    assert result.status == "fail"
    assert result.findings[0].observed == pytest.approx(1.10, abs=1e-6)


def test_inverted_sign_capacity_factor_fails(tmp_path):
    # -5 of 100 MW is -0.05: a real generation profile with its sign flipped,
    # which is the registry or convention fault this check is hard for.
    result = run(tmp_path, wind=wind_rows(mw=-5.0, capacity=100.0))[
        "capacity_factor_out_of_range"
    ]
    assert result.status == "fail"
    assert result.findings[0].observed < 0


def test_a_parked_farm_drawing_auxiliary_power_passes(tmp_path):
    """Negative, but by a rounding error's worth, and real.

    Cullerin Range on 2026-08-31 generated nothing all day and drew up to
    0.09 MW of auxiliary load against 30 MW registered — one farm-day in 4,712,
    a capacity factor of -0.000427. Failing the whole run on that is the
    pipeline CLAUDE.md warns about, so the floor sits just below zero and this
    pins which side of it a parked farm falls on.
    """
    result = run(tmp_path, wind=wind_rows(mw=-0.09, capacity=30.0))[
        "capacity_factor_out_of_range"
    ]
    assert result.status == "pass"


def test_capacity_factor_at_the_limit_passes(tmp_path):
    # 1.05 exactly is allowed; the check is strictly greater than.
    result = run(tmp_path, wind=wind_rows(mw=105.0, capacity=100.0))[
        "capacity_factor_out_of_range"
    ]
    assert result.status == "pass"


def test_normal_capacity_factor_passes(tmp_path):
    result = run(tmp_path, wind=wind_rows(mw=35.0, capacity=100.0))[
        "capacity_factor_out_of_range"
    ]
    assert result.status == "pass"


def test_part_day_is_scaled_not_penalised(tmp_path):
    # A sealed half-day at full output is capacity factor 1.0, not 0.5.
    result = run(
        tmp_path,
        wind=wind_rows(intervals=144, mw=100.0, capacity=100.0, is_complete=False,
                       intervals_present=144),
    )["capacity_factor_out_of_range"]
    assert result.status == "pass"


# --- soft: a farm missing too long ----------------------------------------


def test_duid_missing_more_than_two_hours_warns(tmp_path):
    # WFB1 is short by 25 intervals: just over two hours.
    rows = wind_rows(duids=("WFA1", "WFB1"), skip_last=MAX_MISSING_INTERVALS + 1)
    result = run(tmp_path, wind=rows)["duid_missing_too_long"]

    assert result.status == "warn"
    assert result.check.severity == SOFT
    assert result.findings[0].subject == "WFB1"
    assert result.findings[0].observed == MAX_MISSING_INTERVALS + 1


def test_duid_missing_exactly_two_hours_passes(tmp_path):
    # The boundary itself is tolerated: the check is strictly greater than.
    rows = wind_rows(duids=("WFA1", "WFB1"), skip_last=MAX_MISSING_INTERVALS)
    assert run(tmp_path, wind=rows)["duid_missing_too_long"].status == "pass"


def test_non_scheduled_duid_absent_is_not_a_finding(tmp_path):
    # WFOFF1 has in_scada = false and never appears; that is expected.
    result = run(tmp_path, wind=wind_rows(duids=("WFA1", "WFB1")))["duid_missing_too_long"]
    assert result.status == "pass"


# --- soft: above capacity -------------------------------------------------


def test_above_capacity_rows_warn(tmp_path):
    rows = wind_rows(mw=150.0, capacity=100.0, above_capacity=True)
    result = run(tmp_path, wind=rows)["above_capacity_rows"]

    assert result.status == "warn"
    assert result.findings[0].observed == 288


def test_no_above_capacity_rows_passes(tmp_path):
    assert run(tmp_path, wind=wind_rows())["above_capacity_rows"].status == "pass"


# --- soft: price band -----------------------------------------------------


def test_price_above_the_band_warns(tmp_path):
    result = run(tmp_path, wind=wind_rows(), price=price_rows(rrp=6000.0))["price_out_of_band"]
    assert result.status == "warn"
    assert result.findings[0].observed == 6000.0


def test_price_below_the_floor_warns(tmp_path):
    result = run(tmp_path, wind=wind_rows(), price=price_rows(rrp=-2000.0))["price_out_of_band"]
    assert result.status == "warn"


@pytest.mark.parametrize("rrp", [5000.0, -1000.0, 0.0, 300.0])
def test_prices_inside_the_band_pass(tmp_path, rrp):
    result = run(tmp_path, wind=wind_rows(), price=price_rows(rrp=rrp))["price_out_of_band"]
    assert result.status == "pass"


# --- exit status and the published table ----------------------------------


def test_hard_finding_fails_the_run(tmp_path):
    rows = wind_rows()
    rows.append(rows[0])
    results = list(run(tmp_path, wind=rows).values())
    assert has_hard_failure(results) is True


def test_soft_findings_alone_do_not_fail_the_run(tmp_path):
    rows = wind_rows(mw=50.0, capacity=100.0, above_capacity=True)
    results = list(run(tmp_path, wind=rows).values())

    assert any(r.status == "warn" for r in results)
    assert has_hard_failure(results) is False


def test_clean_data_fails_nothing(tmp_path):
    # Both scheduled farms must be present: a registry farm absent all day is
    # itself a finding, as test_duid_missing_more_than_two_hours_warns shows.
    results = list(
        run(tmp_path, wind=wind_rows(duids=("WFA1", "WFB1")), price=price_rows()).values()
    )
    assert has_hard_failure(results) is False
    assert all(r.status in {"pass", "skip"} for r in results)


def test_quality_table_records_every_check(tmp_path):
    run(tmp_path, wind=wind_rows(), price=price_rows())
    table = tmp_path / "gold" / "quality.parquet"

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT check_name FROM read_parquet('{table}')").fetchall()
        names = {r[0] for r in rows}
        statuses = con.execute(
            f"SELECT DISTINCT status FROM read_parquet('{table}')"
        ).fetchall()
    finally:
        con.close()

    # A clean check still earns a row, so the dashboard can distinguish
    # "ran and found nothing" from "did not run".
    assert len(names) == 6
    assert {s[0] for s in statuses} <= {"pass", "warn", "fail", "skip"}


def test_quality_table_carries_findings(tmp_path):
    rows = wind_rows()
    rows.append(rows[5])
    run(tmp_path, wind=rows)
    table = tmp_path / "gold" / "quality.parquet"

    con = duckdb.connect()
    try:
        row = con.execute(
            f"""SELECT severity, status, subject, message FROM read_parquet('{table}')
                WHERE check_name = 'duplicate_interval_duid'"""
        ).fetchone()
    finally:
        con.close()

    assert row[0] == HARD
    assert row[1] == "fail"
    assert row[2] == "WFA1"


def test_checks_skip_when_their_input_is_absent(tmp_path):
    results = run(tmp_path, wind=wind_rows())  # no price silver at all
    assert results["price_out_of_band"].status == "skip"
    assert "price" in results["price_out_of_band"].note
