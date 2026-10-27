"""Walk-forward training for the regional price forecast.

    python scripts/train_forecast.py                 # train, score, write metrics
    python scripts/train_forecast.py --provisional    # run on short history, labelled
    python scripts/train_forecast.py --no-write       # report only

Four targets, as PLAN 4.2 sets them out: mean price and spike probability, at
60 and 120 minutes. Mean and spike are separate models because price is heavy
enough in the tail that one estimator minimising absolute error learns to
ignore the spikes, and the spikes are the only intervals a battery is paid for.

**The gate and the provisional path.** `build_features.py` refuses below 60
days and 30 spike episodes. That 60 is not a round number: PLAN 4.5 advances
origins a fortnight at a time from at least 28 days of training, so two honest
folds need 28 + 2 x 14 days plus purge and embargo either side. Rather than
lower the gate — which would dress a one-fold number up as a measurement —
`--provisional` cuts weekly folds instead, runs on what exists, and stamps
every output row `provisional = true` alongside the geometry it used. A reader
can then tell a provisional MAE from a reportable one without reading the code.

**Leakage.** Three things do the work, and all three are load-bearing:

- `interval_start_utc` keys a forecast *issued at the end of* that interval, so
  the row's own `rrp` is a legitimate feature. CLAUDE.md pins this reading:
  measured across 5,695 rows carrying `LASTCHANGED`, a dispatch result is
  written about six seconds after its interval begins, so interval `t`'s price
  is a fact by the time `t` ends.
- Folds come from `walkforward.py`, which purges training rows whose target
  window crosses the cutoff and embargoes the first `horizon` minutes of the
  test period. Targets overlap by 23 of 24 intervals at 120 minutes, so an
  ordinary split leaks almost everything.
- The estimator is fitted inside the fold and nowhere else. It is also chosen
  partly for that: `HistGradientBoosting*` needs neither a scaler nor an
  imputer, so there is no fitted preprocessing step to leak through. One fewer
  surface beats a comment promising to be careful.

**Two choices here were made with a provisional fold's numbers in view**, and
saying so matters more than the numbers do. The loss and the change-based
target were both picked after seeing the model lose to persistence, so their
effect as measured on that fold is not a blind out-of-sample result. Both are
defensible a priori — match the loss to the reported metric, and do not ask a
tree ensemble to rediscover the identity function — and both must be re-checked
once real folds exist. Recorded because a reader cannot otherwise tell.

Baselines are re-scored on each fold's own test rows rather than taken from
`baselines.py`, which reports pooled figures. PLAN 4.6 asks for per fold, never
only pooled, and a model that beats persistence overall while losing on three
folds of four has not beaten it.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

import duckdb
import pandas as pd
from baselines import PREDICTORS, assemble
from build_features import (
    MIN_DAYS,
    MIN_SPIKE_EPISODES,
    FeatureError,
    Readiness,
    readiness,
)
from metrics import classification, regression
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from walkforward import Fold, NotEnoughHistory, folds

LOG: Final = logging.getLogger("train_forecast")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
OUT_DIR: Final = REPO_ROOT / "data" / "ml"

HORIZONS: Final = (60, 120)

# PLAN 4.5's geometry, and the arithmetic behind MIN_DAYS.
FULL_GEOMETRY: Final = {"min_train_days": 28, "test_days": 14, "step_days": 14}
# The smallest geometry that still carries a full purge and embargo. Measured
# against the data rather than chosen: at 14.4 days, 14d train + 7d test needs
# 21.2 days and cuts no fold at all, so a fortnight of history would report
# nothing. 7 + 5 cuts one fold today, two at about 19 days and three at 24.
# It is thin — a 5-day test holds some 60 non-overlapping 2-hour windows — and
# that is the whole reason every row it produces is stamped provisional.
PROVISIONAL_GEOMETRY: Final = {"min_train_days": 7, "test_days": 5, "step_days": 5}

# Columns that are keys or targets. Everything else in features.sql is a
# feature, derived by exclusion so a column added to the SQL arrives here
# without an edit — the alternative is a hand-kept list that silently stops
# matching the model it claims to describe.
NOT_FEATURES: Final = frozenset(
    {"region", "interval_start_utc", "interval_start_nem", "trading_date"}
)

# No early stopping: its internal validation split is drawn at random from the
# training fold, which is harmless for leakage but makes the fit — and so the
# metrics file, which is rewritten whole every run — differ run to run for no
# reason. Metrics are rounded for the same reason build_gold.py rounds.
COMMON_KWARGS: Final = {
    "max_iter": 300,
    "learning_rate": 0.05,
    "early_stopping": False,
    "random_state": 0,
}
# Absolute error, not the default squared error. The target is a mean price in
# a market that reaches $15,000, and squared error spends the whole model
# chasing those few intervals: on the first provisional fold it put NSW1's MAE
# at 96.9 against persistence's 9.97, and simply switching the loss took it to
# 31.1. Spikes are the spike classifier's job, by PLAN 4.2's own split.
REGRESSOR_KWARGS: Final = {**COMMON_KWARGS, "loss": "absolute_error"}
# The classifier has no `loss` of that name; log loss is its only option and
# the right one, being what Brier and the reliability curve assess.
CLASSIFIER_KWARGS: Final = dict(COMMON_KWARGS)


class NotReady(Exception):
    """The data cannot support an honest run, and no flag was given to say so."""


@dataclass(frozen=True)
class Scored:
    """One predictor's score on one fold, for one region and horizon."""

    region: str
    horizon_minutes: int
    target: str
    fold: int
    predictor: str
    metrics: dict


def _view_columns(con: duckdb.DuckDBPyConnection, relation: str = "scored") -> list[str]:
    """Column names of a relation in the assembled connection."""
    return [d[0] for d in con.execute(f"SELECT * FROM {relation} LIMIT 0").description]


def feature_columns(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Every feature column, taken from `features.sql`'s own output.

    Read from `_features` rather than from the joined `scored` view, so that
    what counts as a feature is decided by the feature model and nothing else.
    Everything a join adds — the computed baselines, P5MIN, PREDISPATCH — is a
    comparator by construction and cannot become an input by being added
    upstream.

    An earlier version filtered `scored` against a hand-kept list of baseline
    column names. It worked until the PREDISPATCH baseline arrived: its
    coverage counter `pd_intervals_120` matched no name on the list and was
    fed to the model as a 34th feature. Nothing about that was future
    information, so no leakage test caught it; it was simply a baseline's
    bookkeeping being treated as a market signal, and it moved NSW1's
    120-minute MAE enough to make the figure unreadable. A list that has to be
    extended whenever a baseline is added is the wrong shape for this.
    """
    names = _view_columns(con, "_features")
    features = [n for n in names if n not in NOT_FEATURES and not n.startswith("y_")]
    # The joined view must still carry them, or `_frame` would select a column
    # that is not there and fail per region rather than once, here.
    missing = set(features) - set(_view_columns(con))
    if missing:
        raise FeatureError(f"features absent from the joined grid: {sorted(missing)}")
    return features


def span(con: duckdb.DuckDBPyConnection) -> tuple[datetime, datetime]:
    """The first and last interval in the assembled view.

    Read as epoch seconds and rebuilt with an explicit UTC tzinfo rather than
    let DuckDB hand back a TIMESTAMPTZ: that conversion wants `pytz`, which is
    a dependency this project has no other use for. Timestamps stay tz-aware
    either way, which is what CLAUDE.md asks for.
    """
    row = con.execute(
        "SELECT epoch(min(interval_start_utc)), epoch(max(interval_start_utc)) FROM scored"
    ).fetchone()
    if row is None or row[0] is None:
        raise FeatureError("no feature rows to train on")
    return (
        datetime.fromtimestamp(float(row[0]), tz=UTC),
        datetime.fromtimestamp(float(row[1]), tz=UTC),
    )


def cut_folds(con: duckdb.DuckDBPyConnection, provisional: bool) -> list[Fold]:
    """Fold geometry for this run, and the reason it is the one being used."""
    first, last = span(con)
    geometry = PROVISIONAL_GEOMETRY if provisional else FULL_GEOMETRY
    return folds(first, last, horizon=timedelta(minutes=max(HORIZONS)), **geometry)


def _frame(
    con: duckdb.DuckDBPyConnection,
    region: str,
    horizon: int,
    columns: list[str],
    comparators: list[str],
) -> pd.DataFrame:
    """Test-window-complete rows for one region, ordered in time.

    The interval comes back as epoch seconds, for the reason `span` gives, and
    because fold membership is then an exact integer comparison rather than a
    timezone-aware one done inside pandas.
    """
    need = 12 if horizon == 60 else 24
    # Features and comparators both, kept apart by name: the comparators are
    # scored on the same test rows as the model and must never reach `fit`.
    selected = ", ".join(f'"{c}"' for c in [*columns, *comparators])
    return con.execute(f"""
        SELECT epoch(interval_start_utc) AS t_epoch, {selected},
               y_mean_{horizon} AS y_mean, y_spike_{horizon} AS y_spike
        FROM scored
        WHERE region = ? AND y_intervals_{horizon} = {need}
        ORDER BY interval_start_utc
    """, [region]).df()


def train_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    horizon: int,
    fold_list: list[Fold],
    features: list[str],
    with_spike: bool,
) -> list[Scored]:
    """Fit and score every fold for one region and horizon."""
    comparators = sorted(
        {c for pair in PREDICTORS[horizon].values() for c in pair}
        & set(_view_columns(con))
    )
    frame = _frame(con, region, horizon, features, comparators)
    if frame.empty:
        LOG.warning("%s %dmin: no complete target windows", region, horizon)
        return []

    window = timedelta(minutes=horizon).total_seconds()
    out: list[Scored] = []
    for fold in fold_list:
        when = frame["t_epoch"]
        # The purge, as walkforward.purged states it: a training row may be kept
        # only if its whole target window closes before the cutoff.
        train = frame[
            (when >= fold.train_start.timestamp())
            & (when + window <= fold.cutoff.timestamp())
        ]
        test = frame[
            (when >= fold.test_start.timestamp()) & (when < fold.test_end.timestamp())
        ]
        if len(train) < 100 or test.empty:
            LOG.info(
                "%s %dmin fold %d: %d train / %d test rows, skipped",
                region, horizon, fold.index, len(train), len(test),
            )
            continue

        x_train = train[features].to_numpy(dtype="float64")
        x_test = test[features].to_numpy(dtype="float64")
        actual_mean = [float(v) for v in test["y_mean"]]
        actual_spike = [float(v) for v in test["y_spike"]]

        # **The model predicts the change from the current price, not the
        # price.** Price over two hours is close to a random walk, and a tree
        # ensemble cannot represent the identity it would need to compete with
        # persistence on the level: it approximates y = x in steps and is flat
        # outside the range it trained on, so a quiet fortnight teaches it to
        # cap its own forecasts. Predicting `y_mean - rrp` and adding `rrp`
        # back makes persistence the thing the model corrects rather than the
        # thing it has to rediscover, and it is why the comparison against
        # persistence is a fair test rather than a foregone one.
        model = HistGradientBoostingRegressor(**REGRESSOR_KWARGS)
        model.fit(
            x_train,
            (train["y_mean"] - train["rrp"]).to_numpy(dtype="float64"),
        )
        predicted = [
            float(base + delta)
            for base, delta in zip(test["rrp"], model.predict(x_test), strict=True)
        ]
        out.append(_regression_row(
            region, horizon, "mean", fold.index, "model", predicted, actual_mean
        ))

        for label, (mean_col, _) in PREDICTORS[horizon].items():
            if mean_col not in test:
                continue
            comparator = test[["y_mean", mean_col]].dropna()
            if comparator.empty:
                continue
            out.append(_regression_row(
                region, horizon, "mean", fold.index, label,
                [float(v) for v in comparator[mean_col]],
                [float(v) for v in comparator["y_mean"]],
            ))

        if with_spike and any(v > 0.5 for v in actual_spike):
            spike = HistGradientBoostingClassifier(**CLASSIFIER_KWARGS)
            spike.fit(x_train, train["y_spike"].to_numpy(dtype="float64") > 0.5)
            probability = [float(p[1]) for p in spike.predict_proba(x_test)]
            out.append(_classification_row(
                region, horizon, fold.index, "model", probability, actual_spike
            ))
            for label, (_, spike_col) in PREDICTORS[horizon].items():
                if spike_col not in test:
                    continue
                comparator = test[["y_spike", spike_col]].dropna()
                if comparator.empty:
                    continue
                out.append(_classification_row(
                    region, horizon, fold.index, label,
                    [float(v) for v in comparator[spike_col]],
                    [float(v) for v in comparator["y_spike"]],
                ))
    return out


def _regression_row(
    region: str, horizon: int, target: str, fold: int,
    predictor: str, predicted: list[float], actual: list[float],
) -> Scored:
    r = regression(predicted, actual)
    return Scored(region, horizon, target, fold, predictor, {
        "n": r.n,
        "mae": round(r.mae, 4),
        "medae": round(r.medae, 4),
        "rmse": round(r.rmse, 4),
        "bias": round(r.bias, 4),
        "n_spike": r.n_spike,
        "mae_spike": None if r.mae_spike is None else round(r.mae_spike, 4),
        "brier": None,
        "pr_auc": None,
        "base_rate": None,
    })


def _classification_row(
    region: str, horizon: int, fold: int,
    predictor: str, predicted: list[float], actual: list[float],
) -> Scored:
    c = classification(predicted, actual)
    return Scored(region, horizon, "spike", fold, predictor, {
        "n": c.n,
        "mae": None,
        "medae": None,
        "rmse": None,
        "bias": None,
        "n_spike": sum(1 for a in actual if a > 0.5),
        "mae_spike": None,
        "brier": round(c.brier, 6),
        "pr_auc": round(c.pr_auc, 6),
        "base_rate": round(c.base_rate, 6),
    })


def run(provisional: bool = False) -> tuple[list[Scored], list[Fold], list[Readiness]]:
    """Train and score every region and horizon. Returns rows, folds, readiness."""
    con = assemble()
    try:
        # `readiness` reads a view named `_m`, which `assemble` leaves pointing
        # at the last model it materialised. Point it at the joined grid
        # instead: those are the rows that can actually be trained on, so the
        # day and episode counts describe the run rather than a staging table.
        con.execute("CREATE OR REPLACE TEMP VIEW _m AS SELECT * FROM scored")
        state = readiness(con)

        trainable = [r for r in state if r.ready_mean or provisional]
        if not trainable:
            short = ", ".join(f"{r.region} {r.days}d" for r in state)
            raise NotReady(
                f"no region has {MIN_DAYS} days of history ({short}). "
                "Re-run with --provisional to train on what exists and have "
                "every row of the output labelled as provisional."
            )

        try:
            fold_list = cut_folds(con, provisional)
        except NotEnoughHistory as exc:
            hint = "" if provisional else " Try --provisional for shorter folds."
            raise NotReady(f"{exc}{hint}") from exc

        features = feature_columns(con)
        LOG.info(
            "%d features, %d folds, %s geometry",
            len(features), len(fold_list), "provisional" if provisional else "full",
        )

        out: list[Scored] = []
        for r in trainable:
            for horizon in HORIZONS:
                out.extend(train_region(
                    con, r.region, horizon, fold_list, features,
                    with_spike=r.spike_episodes >= MIN_SPIKE_EPISODES,
                ))
        return out, fold_list, state
    finally:
        con.close()


def report(rows: list[Scored], fold_list: list[Fold], provisional: bool) -> None:
    """Per fold, never only pooled — PLAN 4.6."""
    if provisional:
        print("\n  PROVISIONAL: short history, short folds. Not a measured track record.")
    plural = "" if len(fold_list) == 1 else "s"
    print(f"  {len(fold_list)} fold{plural}, "
          f"{fold_list[0].train_days:.1f}d train .. {fold_list[-1].test_end:%Y-%m-%d}")

    for target in ("mean", "spike"):
        subset = [r for r in rows if r.target == target]
        if not subset:
            print(f"\n  {target}: nothing scored "
                  f"({'no positives in any test fold' if target == 'spike' else 'no folds'})")
            continue
        metric = "mae" if target == "mean" else "pr_auc"
        # Bias beside the headline metric, because on short history it is the
        # number that explains the headline: the first provisional fold put
        # NSW1's 120-minute MAE at 84 with a median error of 19, the gap being
        # a +71 systematic offset learned from one week's price level.
        second = "bias" if target == "mean" else "brier"
        print(f"\n  {target} — by fold")
        print(f"  {'region':7} {'hzn':>4} {'fold':>4} {'predictor':12} {'n':>7} "
              f"{metric:>9} {second:>9}")
        for r in subset:
            cells = " ".join(
                "        -" if r.metrics[m] is None else f"{r.metrics[m]:9.4f}"
                for m in (metric, second)
            )
            print(f"  {r.region:7} {r.horizon_minutes:>4} {r.fold:>4} "
                  f"{r.predictor:12} {r.metrics['n']:>7} {cells}")


def write(rows: list[Scored], provisional: bool, out_dir: Path = OUT_DIR) -> Path:
    """Rewrite the metrics table in full, as gold is rewritten.

    Metrics are an assessment of the current model, not a historical fact the
    way an issued forecast is, so CLAUDE.md puts them in the rewrite-in-full
    tier. Keeping every run's numbers forever would also mean a provisional row
    outliving the reason it was provisional.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    destination = out_dir / "metrics.parquet"
    records = [
        {
            "region": r.region,
            "horizon_minutes": r.horizon_minutes,
            "target": r.target,
            "fold": r.fold,
            "predictor": r.predictor,
            "provisional": provisional,
            **r.metrics,
        }
        for r in rows
    ]
    con = duckdb.connect()
    try:
        con.execute("SET threads TO 1")
        con.register("records", pd.DataFrame.from_records(records))
        literal = str(destination).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM records ORDER BY region, horizon_minutes, target, "
            f"fold, predictor) TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--provisional", action="store_true",
        help="train on short history with weekly folds, and label every output row",
    )
    parser.add_argument("--no-write", action="store_true", help="report only")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    try:
        rows, fold_list, _ = run(provisional=args.provisional)
    except (NotReady, FeatureError) as exc:
        print(f"\n  {exc}\n")
        return 1

    if not rows:
        print("\n  no fold had enough rows to train on\n")
        return 1

    report(rows, fold_list, args.provisional)
    if not args.no_write:
        destination = write(rows, args.provisional)
        LOG.info("wrote %s (%d rows)", destination, len(rows))
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
