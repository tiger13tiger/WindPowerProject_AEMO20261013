"""Tests for the baseline scorer and the split readiness gate.

The baselines are the only part of the forecasting phase that can be measured
today, so they are also the only part whose tests can check real behaviour
rather than a refusal path. Two things are worth pinning: that every predictor
is scored on the same rows, and that P5MIN is joined on issue time.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from baselines import PREDICTORS, _f
from build_features import MIN_DAYS, MIN_SPIKE_EPISODES, Readiness, materialise
from metrics import regression, trimmed_mae


def ready(**kwargs) -> Readiness:
    base = Readiness(region="X", days=MIN_DAYS, rows=1000,
                     spike_intervals=100, spike_episodes=MIN_SPIKE_EPISODES, reasons=())
    return replace(base, **kwargs)


# --- the split gate -------------------------------------------------------


def test_the_mean_model_does_not_wait_for_spikes():
    """The flaw this split fixes.

    SA1 and VIC1 have had zero spike episodes in nine days. Gating their
    mean-price model on spikes they may never see would park two of the three
    large wind regions indefinitely, for a model that does not use spikes.
    """
    r = ready(spike_episodes=0, spike_intervals=0)
    assert r.ready_mean is True
    assert r.ready_spike is False


def test_the_spike_model_needs_both():
    assert ready(days=MIN_DAYS - 1).ready_spike is False
    assert ready(spike_episodes=MIN_SPIKE_EPISODES - 1).ready_spike is False
    assert ready().ready_spike is True


def test_neither_is_ready_without_history():
    r = ready(days=1, spike_episodes=0)
    assert r.ready_mean is False and r.ready_spike is False


def test_ready_remains_the_strictest_reading():
    """Existing callers asking `ready` should keep getting the strict answer."""
    assert ready(spike_episodes=0).ready is False
    assert ready().ready is True


def test_mean_reasons_mention_only_days():
    r = Readiness("X", 9, 100, 0, 0,
                  ("9 days of history, need 60", "0 spike episodes, need 30"))
    assert r.mean_reasons == ("9 days of history, need 60",)
    assert len(r.spike_reasons) == 2


# --- the scorer -----------------------------------------------------------


def test_p5min_is_absent_from_the_120_minute_horizon():
    """AEMO's P5MIN run reaches 55 minutes, so it is no comparator at 120.

    Emitting one would mean averaging eleven of twenty-four intervals and
    calling it a forecast of the window. PREDISPATCH covers 120 instead.
    """
    assert "p5min" in PREDICTORS[60]
    assert "p5min" not in PREDICTORS[120]


def test_every_horizon_scores_the_same_three_computable_baselines():
    for horizon in (60, 120):
        assert {"persistence", "yesterday", "climatology"} <= set(PREDICTORS[horizon])


def test_decimal_values_are_coerced_for_the_stdlib_metrics():
    """DuckDB returns DECIMAL for some aggregates; metrics.py is plain floats."""
    from decimal import Decimal
    assert _f(Decimal("1.5")) == 1.5
    assert isinstance(_f(Decimal("1.5")), float)
    assert _f(None) is None


def test_median_absolute_error_separates_a_tail_from_a_bias():
    """Why MedAE was added, with the shape that motivated it.

    Nine of 1,854 Queensland rows, where P5MIN forecast a market-cap price that
    never arrived, carried 112 of its 123 MAE. The mean calls that forecast
    useless; the median shows the other 1,845 rows were fine.
    """
    predicted = [10.0] * 99 + [10_000.0]
    actual = [10.0] * 100
    reg = regression(predicted, actual)
    assert reg.mae == pytest.approx(99.9, abs=0.1)   # the tail dominates
    assert reg.medae == pytest.approx(0.0)           # the typical row is exact
    assert reg.n == 100


def test_predispatch_is_the_120_minute_aemo_comparator():
    assert "predispatch" in PREDICTORS[120]
    assert "predispatch" not in PREDICTORS[60]


def test_trimmed_mae_drops_only_the_tail():
    predicted = [12.0] * 99 + [10_000.0]
    actual = [10.0] * 100
    assert trimmed_mae(predicted, actual) == pytest.approx(2.0)   # the 99% that are typical
    assert regression(predicted, actual).mae > 100                  # the untrimmed tail is kept


# --- the PREDISPATCH comparator -------------------------------------------

SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ml"
T0 = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)


def _minutes(m: int) -> datetime:
    return T0 + timedelta(minutes=m)


def predispatch_frame(tmp_path: Path, runs: list[tuple[str, int, list[tuple[int, float]]]]):
    """Write a price grid and predispatch runs; return the comparator rows by start minute.

    runs: (run id, minutes after T0 it was published, [(period end minute, rrp), ...]).
    """
    con = duckdb.connect()
    con.execute("CREATE TABLE price (region VARCHAR, interval_start_utc TIMESTAMPTZ)")
    con.executemany("INSERT INTO price VALUES ('SA1', ?)",
                    [(_minutes(m),) for m in range(0, 180, 5)])
    con.execute("""CREATE TABLE pd (predispatchseqno VARCHAR, lastchanged TIMESTAMPTZ,
                   datetime TIMESTAMPTZ, regionid VARCHAR, intervention INTEGER, rrp DOUBLE,
                   source_file VARCHAR, ingested_at TIMESTAMPTZ)""")
    for run_id, issued, periods in runs:
        con.executemany("INSERT INTO pd VALUES (?, ?, ?, 'SA1', 0, ?, 'f', ?)",
                        [(run_id, _minutes(issued), _minutes(end), rrp, _minutes(issued))
                         for end, rrp in periods])
    price, pd = tmp_path / "price.parquet", tmp_path / "pd.parquet"
    con.execute(f"COPY price TO '{price}' (FORMAT PARQUET)")
    con.execute(f"COPY pd TO '{pd}' (FORMAT PARQUET)")
    materialise(con, SQL_DIR / "predispatch_baseline.sql",
                {"price_glob": str(price), "predispatch_glob": str(pd), "spike_threshold": 300},
                tmp_path / "out.parquet")
    rows = con.execute("""SELECT epoch(interval_start_utc - ?::TIMESTAMPTZ)::INT // 60,
                          pd_run_id, pd_mean_120, pd_spike_120, pd_intervals_120 FROM _m""",
                       [T0]).fetchall()
    return {r[0]: r[1:] for r in rows}


RUN1 = ("R1", 1, [(30, 10.0), (60, 20.0), (90, 30.0), (120, 40.0)])
RUN2 = ("R2", 31, [(60, 100.0), (90, 100.0), (120, 100.0), (150, 400.0)])


def test_predispatch_maps_intervals_to_half_hours_and_carries_the_last_forward(tmp_path):
    rows = predispatch_frame(tmp_path, [RUN1, RUN2])
    run, mean, spike, covered = rows[0]
    # Target interval ends 00:10 .. 02:05: five in the 00:30 period, six each in the
    # next three, and 02:05 beyond the run, filled with its last period's price.
    assert run == "R1"
    assert covered == 23
    assert mean == pytest.approx((5 * 10 + 6 * 20 + 6 * 30 + 6 * 40 + 40) / 24)
    assert spike == 0.0


def test_predispatch_uses_only_runs_published_by_the_forecast_time(tmp_path):
    rows = predispatch_frame(tmp_path, [RUN1, RUN2])
    # Row 00:25 is a forecast made at 00:30; R2 is published at 00:31.
    assert rows[25][0] == "R1"
    # Row 00:30 is made at 00:35, after R2 appeared.
    assert rows[30][0] == "R2"
    assert rows[30][2] == 1.0  # R2 calls a spike inside the window


def test_predispatch_rows_with_too_little_of_the_window_forecast_are_dropped(tmp_path):
    rows = predispatch_frame(tmp_path, [RUN1])
    # Only R1 exists. Row 00:30's window ends 00:40 .. 02:35 and R1 reaches
    # 02:00: 17 of 24. Row 00:45's ends 00:55 .. 02:50: 14, below the floor.
    assert rows[30][3] == 17
    assert 45 not in rows
