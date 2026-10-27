"""The trainer's guards: what may be a feature, and what each row must carry.

sklearn lives in the `ml` extra, which the hourly ingest does not install, so
this module skips rather than fails when it is absent.
"""

import pytest

pytest.importorskip("sklearn")

import duckdb
import train_forecast as tf


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """`_features` and the joined `scored` view, shaped like the real ones.

    Built the way `baselines.assemble` builds them — the feature model on its
    own, then the comparators joined on — because that separation is the thing
    under test.
    """
    con = duckdb.connect()
    con.execute("""
        CREATE TEMP TABLE _features AS SELECT
            'NSW1' AS region,
            '2026-10-01 00:00:00+00'::TIMESTAMPTZ AS interval_start_utc,
            '2026-10-01 10:00:00+10'::TIMESTAMPTZ AS interval_start_nem,
            '2026-10-01'::DATE AS trading_date,
            50.0 AS rrp,
            60.0 AS y_mean_60, 61.0 AS y_mean_120,
            0.0 AS y_spike_60, 0.0 AS y_spike_120,
            12 AS y_intervals_60, 24 AS y_intervals_120,
            49.0 AS rrp_lag_5, 7.0 AS wind_mw
    """)
    con.execute("""
        CREATE TEMP VIEW scored AS SELECT f.*,
            51.0 AS persist_mean_60, 0.0 AS persist_spike_60,
            52.0 AS persist_mean_120, 0.0 AS persist_spike_120,
            53.0 AS yest_mean_60, 0.0 AS yest_spike_60,
            54.0 AS yest_mean_120, 0.0 AS yest_spike_120,
            55.0 AS clim_mean_rrp, 0.0 AS clim_spike_rate,
            56.0 AS p5_mean_60, 0.0 AS p5_spike_60, 11 AS p5_intervals_60,
            57.0 AS pd_mean_120, 0.0 AS pd_spike_120, 24 AS pd_intervals_120
        FROM _features f
    """)
    yield con
    con.close()


def test_keys_and_targets_are_not_features(con: duckdb.DuckDBPyConnection) -> None:
    features = tf.feature_columns(con)
    for name in ("region", "interval_start_utc", "trading_date", "interval_start_nem"):
        assert name not in features
    assert not [f for f in features if f.startswith("y_")]
    assert "rrp_lag_5" in features and "wind_mw" in features


def test_the_models_own_comparators_are_not_features(con: duckdb.DuckDBPyConnection) -> None:
    """Beating a baseline must not be achievable by reading that baseline.

    Every comparator is derived from history the features already carry, so
    including one would not teach the model anything new — it would just turn
    "the model beat persistence" into "the model learned to copy it".
    """
    features = tf.feature_columns(con)
    for column in ("persist_mean_60", "yest_mean_120", "clim_mean_rrp", "p5_mean_60"):
        assert column not in features


def test_a_new_baselines_bookkeeping_cannot_become_a_feature(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """The regression that prompted reading `_features` instead of `scored`.

    `pd_intervals_120` says how many of the 24 target intervals AEMO actually
    forecast. It is knowable at `t`, so it is not leakage and no leakage test
    saw it — but it is a comparator's own bookkeeping, and it reached the model
    as a feature because it matched no name on a hand-kept exclusion list.
    Anything a join adds must stay out without being named.
    """
    features = tf.feature_columns(con)
    assert "pd_intervals_120" not in features
    assert "p5_intervals_60" not in features
    assert not [f for f in features if f.startswith(("pd_", "p5_"))]


def test_interval_t_own_price_stays_a_feature(con: duckdb.DuckDBPyConnection) -> None:
    """`rrp` is admissible, and the leakage rule turns on it.

    A row keyed `t` is a forecast issued at the end of `t`, predicting `t+5`
    onward, so `t`'s own settled price is a fact by then. It is also the base
    the model's predicted change is added to, so dropping it would break the
    regressor rather than merely weaken it.
    """
    assert "rrp" in tf.feature_columns(con)


def test_the_regressor_is_scored_on_the_loss_it_optimises() -> None:
    """Squared error chases $15,000 spikes and loses the quiet 99%."""
    assert tf.REGRESSOR_KWARGS["loss"] == "absolute_error"


def test_the_classifier_takes_no_regression_loss() -> None:
    """Sharing one kwargs dict passed `absolute_error` to the classifier."""
    assert "loss" not in tf.CLASSIFIER_KWARGS
    assert tf.CLASSIFIER_KWARGS["early_stopping"] is False


def test_provisional_geometry_is_the_smaller_one() -> None:
    for key, full in tf.FULL_GEOMETRY.items():
        assert tf.PROVISIONAL_GEOMETRY[key] < full


def test_every_row_carries_the_same_columns() -> None:
    """Both row builders must agree, or the Parquet schema depends on the data."""
    mean = tf._regression_row("NSW1", 60, "mean", 0, "model", [1.0, 2.0], [1.5, 2.5])
    spike = tf._classification_row("NSW1", 60, 0, "model", [0.1, 0.9], [0.0, 1.0])
    assert mean.metrics.keys() == spike.metrics.keys()
    assert mean.metrics["mae"] is not None and mean.metrics["brier"] is None
    assert spike.metrics["brier"] is not None and spike.metrics["mae"] is None
