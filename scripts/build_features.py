"""Build the ML feature table from silver, and say whether it can train a model.

    python scripts/build_features.py            # write features, report readiness
    python scripts/build_features.py --check    # report readiness only

Features are **derived and never committed** (CLAUDE.md): a 50-column table is
about 200 MB a year against 17 MB for the predictions it would support, and it
is reproducible from silver by definition. `data/ml/features/` is gitignored.

The readiness check is the other half of this script, and the more important
half today. A model trained on four days of data will produce numbers, and the
numbers will be meaningless; `ready()` says so with the counts, and
`train_forecast.py` refuses rather than proceeds — or, given
`--provisional`, runs anyway and labels every row of its output. Spike models are
the binding constraint — SA1 can go a week without a single interval above
$300, and a classifier cannot learn from zero positives.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("build_features")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
SQL_DIR: Final = REPO_ROOT / "sql" / "ml"
OUT_DIR: Final = REPO_ROOT / "data" / "ml" / "features"

PARAM_RE: Final = re.compile(r"\$([a-z][a-z0-9_]*)")

SPIKE_THRESHOLD: Final = 300.0

# What a fold needs before training on it means anything. PLAN 4.5 cuts 14-day
# test periods from at least 28 days of training, with purge and embargo either
# side of the cutoff; below roughly 60 days there is no honest first fold.
MIN_DAYS: Final = 60
# A rare-event classifier needs positives, not rows. Fewer than this and the
# model is fitting noise, however many intervals it has seen.
MIN_SPIKE_EPISODES: Final = 30


class FeatureError(Exception):
    """Features could not be built."""


@dataclass(frozen=True)
class Readiness:
    """Whether a region has enough history to train on, and why not."""

    region: str
    days: int
    rows: int
    spike_intervals: int
    spike_episodes: int
    reasons: tuple[str, ...]

    @property
    def ready_mean(self) -> bool:
        """Enough history to train the mean-price model.

        Spikes are irrelevant here. A region can go a season without an interval
        above $300 and still have a perfectly trainable mean-price model — South
        Australia and Victoria are both at zero episodes after nine days, and
        gating their mean model on spikes they may never see would park two of
        the three large wind regions indefinitely for no reason.
        """
        return self.days >= MIN_DAYS

    @property
    def ready_spike(self) -> bool:
        """Enough history *and* enough positives to train the spike classifier."""
        return self.ready_mean and self.spike_episodes >= MIN_SPIKE_EPISODES

    @property
    def ready(self) -> bool:
        """Ready for everything. Kept as the strictest reading of the two."""
        return self.ready_spike

    @property
    def mean_reasons(self) -> tuple[str, ...]:
        return tuple(r for r in self.reasons if "days" in r)

    @property
    def spike_reasons(self) -> tuple[str, ...]:
        return self.reasons


def materialise(
    con: duckdb.DuckDBPyConnection, path: Path, params: dict[str, object], destination: Path
) -> int:
    """Run the model straight to Parquet and expose it as ``_m``. Returns rows.

    Written first and read back rather than held in memory: DuckDB will not
    prepare a parameterised CREATE VIEW, and a year of five-minute rows across
    five regions is half a million rows wide enough that materialising through
    Python would be the one part of this pipeline that needs a big machine.
    """
    if not path.exists():
        raise FeatureError(f"missing ML model: {path}")
    sql = path.read_text()
    used = {name: params[name] for name in set(PARAM_RE.findall(sql)) if name in params}

    destination.parent.mkdir(parents=True, exist_ok=True)
    literal = str(destination).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)", used)
    con.execute(f"CREATE OR REPLACE TEMP VIEW _m AS SELECT * FROM read_parquet('{literal}')")
    (rows,) = con.execute("SELECT count(*) FROM _m").fetchone()
    return int(rows)


def readiness(con: duckdb.DuckDBPyConnection) -> list[Readiness]:
    """Judge each region against MIN_DAYS and MIN_SPIKE_EPISODES.

    An episode, not an interval: a two-hour price event is one thing that
    happened, and counting its 24 intervals as 24 examples is how a rare-event
    model convinces itself it has data.
    """
    rows = con.execute(f"""
        WITH marked AS (
            SELECT region, trading_date, interval_start_utc,
                   rrp > {SPIKE_THRESHOLD} AS hot,
                   row_number() OVER (PARTITION BY region ORDER BY interval_start_utc)
                 - row_number() OVER (PARTITION BY region, rrp > {SPIKE_THRESHOLD}
                                      ORDER BY interval_start_utc) AS episode
            FROM _m
        )
        SELECT region,
               count(DISTINCT trading_date) AS days,
               count(*) AS rows,
               count(*) FILTER (WHERE hot) AS spike_intervals,
               count(DISTINCT CASE WHEN hot THEN episode END) AS spike_episodes
        FROM marked GROUP BY region ORDER BY region
    """).fetchall()

    out = []
    for region, days, n, intervals, episodes in rows:
        reasons = []
        if days < MIN_DAYS:
            reasons.append(f"{days} days of history, need {MIN_DAYS}")
        if episodes < MIN_SPIKE_EPISODES:
            reasons.append(f"{episodes} spike episodes, need {MIN_SPIKE_EPISODES}")
        out.append(Readiness(region, days, n, intervals, episodes, tuple(reasons)))
    return out


def build(
    silver_dir: Path = SILVER_DIR,
    sql_dir: Path = SQL_DIR,
    out_dir: Path = OUT_DIR,
    write: bool = True,
) -> tuple[int, list[Readiness]]:
    """Build features and judge readiness. Returns ``(rows, readiness)``."""
    params: dict[str, object] = {
        "price_glob": str(silver_dir / "price" / "*.parquet"),
        "wind_glob": str(silver_dir / "wind_output" / "*.parquet"),
        "spike_threshold": SPIKE_THRESHOLD,
    }
    for name in ("price", "wind_output"):
        if not any((silver_dir / name).glob("*.parquet")):
            raise FeatureError(f"no silver {name} found under {silver_dir}")

    con = duckdb.connect()
    scratch: tempfile.TemporaryDirectory | None = None
    try:
        if write:
            destination = out_dir / "features.parquet"
        else:
            scratch = tempfile.TemporaryDirectory()
            destination = Path(scratch.name) / "features.parquet"

        rows = materialise(con, sql_dir / "features.sql", params, destination)
        state = readiness(con)
        if write:
            LOG.info(
                "wrote %s (%d rows, %d bytes)", destination.name, rows,
                destination.stat().st_size,
            )
    finally:
        con.close()
        if scratch is not None:
            scratch.cleanup()
    return rows, state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--sql-dir", type=Path, default=SQL_DIR)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--check", action="store_true", help="report readiness, write nothing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        rows, state = build(args.silver_dir, args.sql_dir, args.out_dir, write=not args.check)
    except FeatureError as exc:
        LOG.error("%s", exc)
        return 1

    print(f"\n{rows:,} feature rows\n")
    print(f"  {'region':8} {'days':>5} {'rows':>7} {'spike int':>10} {'episodes':>9}  "
          f"{'mean model':18} spike model")
    for r in state:
        short_days = f"+{MIN_DAYS - r.days} days"
        short_eps = f"+{MIN_SPIKE_EPISODES - r.spike_episodes} episodes"
        mean_status = "ready" if r.ready_mean else short_days
        if r.ready_spike:
            spike_status = "ready"
        elif r.ready_mean:
            spike_status = short_eps
        else:
            spike_status = f"{short_days}, {short_eps}"
        print(f"  {r.region:8} {r.days:>5} {r.rows:>7,} {r.spike_intervals:>10} "
              f"{r.spike_episodes:>9}  {mean_status:18} {spike_status}")

    # Reported separately because they gate different models and come free at
    # different times: days arrive on a schedule, spike episodes do not.
    if not any(r.ready_mean for r in state):
        print(
            f"\nNo region can train a mean-price model yet: a fold needs {MIN_DAYS} days.",
            file=sys.stderr,
        )
    if not any(r.ready_spike for r in state):
        print(
            f"No region can train a spike model yet: that also needs "
            f"{MIN_SPIKE_EPISODES} spike episodes, and episodes arrive in bursts "
            "rather than on a schedule.\nThe harness is built and waiting; "
            "train_forecast.py will refuse until this passes.",
            file=sys.stderr,
        )
    # Not an error: "not enough data yet" is the expected state for months, and
    # a red CI run every hour would train everyone to ignore it.
    return 0


if __name__ == "__main__":
    sys.exit(main())
