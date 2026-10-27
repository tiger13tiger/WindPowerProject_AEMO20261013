"""Score the forecasts a model has to beat, on the data we already have.

    python scripts/baselines.py              # report, and write the metrics
    python scripts/baselines.py --no-write   # report only

PLAN 4.6 requires every model be reported against persistence, same-time
yesterday and AEMO's own P5MIN, and says plainly that failing to beat P5MIN is
a finding rather than something to hide. None of those four needs training, so
all of them can be measured today — the 60-day gate on `train_forecast.py` does
not apply here, and knowing the bar before building the thing meant to clear it
is worth more than the thing itself at this stage.

Scored only on rows whose target window is complete, and every predictor is
scored on the same rows. A mean over eleven of twelve intervals is a different
quantity from the target, and comparing predictors over different row sets is
worse still: P5MIN bronze begins two days after silver price, so the first
version of this scored persistence over 12,100 rows and P5MIN over 9,310 and
called the result a comparison.

**AEMO's comparator is P5MIN at 60 minutes and PREDISPATCH at 120.** A P5MIN
run reaches 55 minutes past its own name, so it covers eleven of the twelve
intervals in the 60-minute target and is no forecast of the 120-minute one.
PREDISPATCH forecasts half-hours and reaches about two hours, which covers 17
to 23 of the 24 intervals; see `sql/ml/predispatch_baseline.sql` for how the
rest are filled using only what was published by the forecast time.

**MAE is reported with a trimmed twin.** `mae_trim99` drops the largest 1% of
absolute errors. On short history a single phantom spike in AEMO's own
forecast can own most of the MAE; both numbers are kept, see metrics.py.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Final

import duckdb
from build_features import SPIKE_THRESHOLD, FeatureError, materialise
from metrics import classification, regression, trimmed_mae

LOG: Final = logging.getLogger("baselines")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
BRONZE_DIR: Final = REPO_ROOT / "data" / "bronze"
SQL_DIR: Final = REPO_ROOT / "sql" / "ml"
OUT_DIR: Final = REPO_ROOT / "data" / "ml"

# predictor label -> (mean column, spike column) per horizon.
PREDICTORS: Final = {
    60: {
        "persistence": ("persist_mean_60", "persist_spike_60"),
        "yesterday": ("yest_mean_60", "yest_spike_60"),
        "climatology": ("clim_mean_rrp", "clim_spike_rate"),
        "p5min": ("p5_mean_60", "p5_spike_60"),
    },
    120: {
        "persistence": ("persist_mean_120", "persist_spike_120"),
        "yesterday": ("yest_mean_120", "yest_spike_120"),
        "climatology": ("clim_mean_rrp", "clim_spike_rate"),
        "predispatch": ("pd_mean_120", "pd_spike_120"),
    },
}


def _f(value: object) -> float | None:
    """DuckDB returns DECIMAL for some aggregates; metrics.py is stdlib floats."""
    return None if value is None else float(value)


def _load(con: duckdb.DuckDBPyConnection, name: str, path: Path) -> None:
    """Copy a written Parquet into a temp TABLE, not a view.

    Two reasons it is a table and not a view. `materialise` leaves its result in
    a temp view called `_m` and replaces it on the next call, so a view defined
    over `_m` is lazy and three of them all resolve to whichever model ran last
    — which is how the first version of this scored the baselines against
    themselves. And a view over a file keeps the file alive, which would stop
    the scratch directory being thrown away.
    """
    literal = str(path).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS "
                f"SELECT * FROM read_parquet('{literal}')")


def assemble(
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
    sql_dir: Path = SQL_DIR,
) -> duckdb.DuckDBPyConnection:
    """Join features, the computed baselines and P5MIN onto one grid.

    The intermediate Parquet goes to a temporary directory, not under data/.
    Only `data/ml/features/` is gitignored, so a scratch directory beside it
    would have been committed — derived files, in a repository whose size is a
    hard budget.
    """
    params: dict[str, object] = {
        "price_glob": str(silver_dir / "price" / "*.parquet"),
        "wind_glob": str(silver_dir / "wind_output" / "*.parquet"),
        "p5min_glob": str(bronze_dir / "p5min" / "**" / "*.parquet"),
        "predispatch_glob": str(bronze_dir / "predispatch_price" / "**" / "*.parquet"),
        "spike_threshold": SPIKE_THRESHOLD,
    }
    for name in ("price", "wind_output"):
        if not any((silver_dir / name).glob("*.parquet")):
            raise FeatureError(f"no silver {name} found under {silver_dir}")

    con = duckdb.connect()
    # Deterministic aggregation, as build_gold does and for the same reason.
    con.execute("SET threads TO 1")
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp)
        materialise(con, sql_dir / "features.sql", params, scratch / "features.parquet")
        _load(con, "_features", scratch / "features.parquet")
        materialise(con, sql_dir / "baselines.sql", params, scratch / "baselines.parquet")
        _load(con, "_baselines", scratch / "baselines.parquet")

        if any((bronze_dir / "p5min").glob("**/*.parquet")):
            materialise(con, sql_dir / "p5min_baseline.sql", params,
                        scratch / "p5min.parquet")
            _load(con, "_p5", scratch / "p5min.parquet")
            have_p5 = True
        else:
            have_p5 = False

        if any((bronze_dir / "predispatch_price").glob("**/*.parquet")):
            materialise(con, sql_dir / "predispatch_baseline.sql", params,
                        scratch / "predispatch.parquet")
            _load(con, "_pd", scratch / "predispatch.parquet")
            have_pd = True
        else:
            have_pd = False

    if not have_p5:
        LOG.warning("no p5min bronze; AEMO's own forecast will not be scored")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _p5 AS "
            "SELECT region, interval_start_utc, NULL::DOUBLE AS p5_mean_60, "
            "NULL::DOUBLE AS p5_spike_60, 0 AS p5_intervals_60 "
            "FROM _features WHERE false"
        )

    if not have_pd:
        LOG.warning("no predispatch bronze; the 120-minute AEMO comparator is missing")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _pd AS "
            "SELECT region, interval_start_utc, NULL::DOUBLE AS pd_mean_120, "
            "NULL::DOUBLE AS pd_spike_120, 0 AS pd_intervals_120 "
            "FROM _features WHERE false"
        )

    con.execute("""
        CREATE OR REPLACE TEMP VIEW scored AS
        SELECT f.*, b.* EXCLUDE (region, interval_start_utc, trading_date),
               p.p5_mean_60, p.p5_spike_60, p.p5_intervals_60,
               d.pd_mean_120, d.pd_spike_120, d.pd_intervals_120
        FROM _features f
        JOIN _baselines b USING (region, interval_start_utc)
        LEFT JOIN _p5 p USING (region, interval_start_utc)
        LEFT JOIN _pd d USING (region, interval_start_utc)
    """)
    return con


def score(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """One row per (region, horizon, predictor), plus an all-regions row."""
    regions = [r[0] for r in con.execute(
        "SELECT DISTINCT region FROM scored ORDER BY region").fetchall()]

    out: list[dict] = []
    for horizon, predictors in PREDICTORS.items():
        need = 12 if horizon == 60 else 24
        # Every predictor is scored on the SAME rows: those where all of them
        # are available. P5MIN bronze starts two days after silver price, so
        # scoring each on whatever it happened to cover compared persistence
        # over 12,100 rows against P5MIN over 9,310 — different days, and so a
        # different market. Restricting to the intersection costs rows and buys
        # a comparison that means something.
        available = " AND ".join(
            f"{mean} IS NOT NULL" for mean, _ in predictors.values()
        )
        for region in [*regions, None]:
            where = f"y_intervals_{horizon} = {need} AND {available}"
            if region is not None:
                where += f" AND region = '{region}'"
            for label, (mean_col, spike_col) in predictors.items():
                rows = con.execute(f"""
                    SELECT {mean_col}, y_mean_{horizon}, {spike_col}, y_spike_{horizon}
                    FROM scored WHERE {where}
                """).fetchall()
                if not rows:
                    continue
                predicted, actual = [_f(r[0]) for r in rows], [_f(r[1]) for r in rows]
                reg = regression(predicted, actual)
                cls = classification([_f(r[2]) for r in rows], [_f(r[3]) for r in rows])
                out.append({
                    "region": region or "ALL",
                    "horizon_minutes": horizon,
                    "predictor": label,
                    "n": reg.n,
                    "mae": round(reg.mae, 4),
                    "medae": round(reg.medae, 4),
                    "mae_trim99": round(trimmed_mae(predicted, actual), 4),
                    "rmse": round(reg.rmse, 4),
                    "bias": round(reg.bias, 4),
                    "n_spike": reg.n_spike,
                    "mae_spike": None if reg.mae_spike is None else round(reg.mae_spike, 4),
                    "mae_quiet": None if reg.mae_quiet is None else round(reg.mae_quiet, 4),
                    "brier": round(cls.brier, 6),
                    "pr_auc": round(cls.pr_auc, 6),
                    "base_rate": round(cls.base_rate, 6),
                })
    return out


def report(rows: list[dict]) -> None:
    print(f"\n  {'region':7} {'hzn':>4} {'predictor':12} {'n':>7} {'MAE':>9} {'MAE99':>8} "
          f"{'MedAE':>8} {'MAE spike':>10} {'Brier':>8} {'PR-AUC':>7}")
    print(f"  {'-' * 7} {'-' * 4} {'-' * 12} {'-' * 7} {'-' * 9} {'-' * 8} {'-' * 8} "
          f"{'-' * 10} {'-' * 8} {'-' * 7}")
    for row in rows:
        spike = "-" if row["mae_spike"] is None else f"{row['mae_spike']:10.2f}"
        print(f"  {row['region']:7} {row['horizon_minutes']:>4} {row['predictor']:12} "
              f"{row['n']:>7,} {row['mae']:>9.2f} {row['mae_trim99']:>8.2f} "
              f"{row['medae']:>8.2f} {spike:>10} "
              f"{row['brier']:>8.4f} {row['pr_auc']:>7.3f}")

    # The comparison PLAN 4.6 actually asks for, stated rather than left to be
    # read off the table.
    for horizon in sorted({r["horizon_minutes"] for r in rows}):
        band = [r for r in rows if r["region"] == "ALL" and r["horizon_minutes"] == horizon]
        if not band:
            continue
        best = min(band, key=lambda r: r["mae"])
        print(f"\n  {horizon} min: best baseline is {best['predictor']} "
              f"at MAE {best['mae']:.2f} (median {best['medae']:.2f})")
        aemo = next((r for r in band if r["predictor"] in ("p5min", "predispatch")), None)
        if aemo is None:
            print(f"          no AEMO comparator at {horizon} min")
        else:
            print(f"          a model must beat {aemo['predictor']}: MAE {aemo['mae']:.2f}, "
                  f"trimmed {aemo['mae_trim99']:.2f}, median {aemo['medae']:.2f}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--sql-dir", type=Path, default=SQL_DIR)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--no-write", action="store_true", help="report only")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        con = assemble(args.silver_dir, args.bronze_dir, args.sql_dir)
    except FeatureError as exc:
        LOG.error("%s", exc)
        return 1
    try:
        rows = score(con)
    finally:
        con.close()

    if not rows:
        LOG.warning("no complete target windows yet; nothing to score")
        return 0

    report(rows)
    if not args.no_write:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        destination = args.out_dir / "baseline_metrics.json"
        destination.write_text(json.dumps(rows, indent=2) + "\n")
        LOG.info("wrote %s (%d rows)", destination.name, len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
