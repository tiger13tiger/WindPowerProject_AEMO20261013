# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this project is

An end-to-end Australian wind generation data platform, built as a portfolio
piece. It ingests two AEMO 5-minute feeds — dispatch SCADA (MW per unit) and
dispatch price (RRP per region) — keeps only wind farm units from silver
onward, and refines them through bronze / silver / gold Parquet layers. Joining
output to price is what turns generation volume into revenue, capture price and
negative-price exposure. A static dashboard and a question-to-SQL assistant
sit on top, and a forecasting phase predicts regional price for a battery
arbitrage backtest.

The subjects of study are **wind farms and batteries**. Solar, coal, gas, hydro
and everything else appear only as regional totals used as features — never
modelled per unit, never broken out. The registry therefore classifies units
rather than filtering to wind.

The author is a data engineer (ADF, Synapse, Databricks medallion) with a prior
career as a wind turbine maintenance engineer, moving into AI engineering. The
repo should read as production-minded work, not a notebook dump.

## Goals

1. Reliable, idempotent ingestion of AEMO DISPATCHSCADA and DISPATCHIS on a
   schedule.
2. A clean medallion architecture that someone can follow end to end.
3. Small repo: wind DUIDs only from silver onward, write-once bronze files.
4. A static dashboard on GitHub Pages, built from the gold layer.
5. A Q&A assistant that turns a question into read-only SQL over the gold
   tables, via any OpenAI-compatible model API.
6. A regional price forecast (mean and spike probability over 60 and 120
   minutes) evaluated against AEMO's own forecast, and a battery arbitrage
   backtest that turns it into dollars.

## Stack and constraints

- Python 3.11. DuckDB for all SQL and Parquet I/O. Parquet + zstd on disk.
- Runtime is GitHub Actions free tier only. No Azure, no paid services, no
  always-on compute, no external database.
- Data is committed back into this repository. Treat repo size as a hard budget.
- Dependencies: keep the list short. The runtime pipeline is `duckdb` and
  `httpx` only. Spreadsheet parsing for the reference registry lives in the
  `reference` extra (`pandas`, `openpyxl`, `xlrd`, `python-calamine`), which the
  hourly ingest never installs. Ask before adding anything else.

## Layer contract

| Layer  | Content | Committed | Grain |
|--------|---------|-----------|-------|
| raw    | Downloaded `PUBLIC_*.zip` | No (gitignored) | per 5-min file |
| bronze | All DUIDs and regions, source columns unchanged | Yes | one file per closed NEM hour |
| silver | Wind only. Typed, deduplicated, validated, UTC + NEM local time | Yes | one file per day |
| gold   | Aggregates and dashboard-ready tables | Yes | whole-table rewrite |

Rules that follow from this:

- Bronze is a faithful copy of the source: every DUID, columns untouched, no
  business logic. The wind filter belongs at the silver boundary, so bronze
  stays replayable if the DUID registry changes.
- Bronze and silver files are immutable once written. Never rewrite one — a
  rewritten file is a new blob in git history forever, and a file rewritten
  every run makes the repository grow without bound. Gold is small enough
  (hundreds of KB) to rewrite in full each run.
- Bronze is written one file per closed NEM hour, never per five-minute
  interval. Measured on live cardinality (517 DUIDs), hour files cost 293 MB
  and 17,520 files a year against 702 MB and 210,240 — the Parquet footer and
  the DUID dictionary are paid once an hour instead of 288 times a day.
- An hour is closed only once AEMO has published an interval in a later hour,
  and an hour is always written in full. Never write a partial hour: the rest
  of it arriving would force a rewrite. Never consult the clock for this — the
  directory listing says how far AEMO has got.
- Do not add a compaction step. Merging small files into large ones leaves both
  in git history permanently, so compacting after the fact always costs more
  than writing at the right grain to begin with.
- A silver day is written once, when bronze holds all 288 intervals for it, or
  when it is sealed: bronze has observed intervals more than two days past the
  day's end, so AEMO's two-day retention means the missing files are gone. A
  sealed day is written as it stands, with `intervals_present` and
  `is_complete` on every row. Judge this from the data's own latest interval,
  never the wall clock — a clock seals days during an ingestion outage, writing
  gaps that were still fillable.
- The archive backfill is the one exception. `Archive/` holds a rolling year
  of what `Current/` dropped, so a seal's premise — the missing files are gone
  — stops being true once the backfill has supplied them. `backfill_archive.py`
  removes a silver day only if it says of itself `is_complete = false`, and the
  next transform rewrites it whole. No other code removes a silver file.
- Every write is idempotent and keyed off a watermark: each source keeps its
  own at `data/bronze/<source>/_ingested.txt`, one file name per line, appended
  only after a successful Parquet write. Re-running a job must not duplicate
  rows or change existing files.
- Sources are independent. `scada` failing must never block `price`, and vice
  versa — AEMO retains only about two days, so a blocked source loses data
  permanently.

## Conventions

- Type hints on every function signature, including return types. Prefer
  `pathlib.Path` over `str` for paths, and `datetime` with explicit tzinfo —
  never a naive datetime.
- Comments are concise and explain *why*. No comment that restates the code, no
  decorative banners. Docstrings on public functions, one line unless the
  behaviour is genuinely subtle.
- Business logic that is naturally SQL lives in `sql/`, loaded and executed by
  thin Python wrappers. Do not hand-roll in pandas what DuckDB can express.
- Tests use `pytest`. Unit tests run against small fixtures checked into
  `tests/fixtures/`, never against the network. Mark anything that needs
  network `@pytest.mark.network` and keep it out of the default run.
- Formatting and linting with `ruff`. Line length 100.
- Module layout: one responsibility per module, no cross-layer imports
  (`gold.py` must not import `bronze.py`). Adding a NEMWeb report means
  declaring a `ReportSpec` in its own `scripts/ingest_*.py`, never editing
  `scripts/nemweb.py` with report-specific branching.
- Log with `logging`, not `print`, in anything that runs in a workflow.

## Data quality

- Checks live in `sql/quality/`, one model per check, each returning one row
  per finding as (trading_date, subject, observed, message). `check_quality.py`
  decides severity; the SQL never does.
- Hard checks fail the run with a non-zero exit: the data contradicts itself and
  anything built on it would be wrong. Soft checks warn and continue: the data
  is surprising but plausibly real.
- Put a check in the soft tier unless a finding means something downstream is
  definitely incorrect. The NEM is full of genuinely strange but real values —
  negative prices, farms over nameplate, units offline for days — and a
  pipeline that fails on all of them gets ignored.
- Results are rewritten in full to `data/gold/quality.parquet` each run,
  including a row for every check that found nothing, so a dashboard can tell
  "ran and passed" from "did not run".

## Workflows

- `ingest.yml` runs the whole pipeline hourly: the `ingest` job writes bronze,
  then the `transform` job runs silver, quality and gold. `transform` depends on
  `ingest` with `if: always()`, because the two are independent — a download
  that failed for one source must not stop silver being rebuilt from the bronze
  already committed.
- Quality gates gold, not silver. The check runs with `continue-on-error` so
  silver is still committed, gold is skipped on a hard failure, and a final step
  ends the run red so the failure cannot pass unnoticed.
- `backfill.yml` is manual, one month per run, and holds the same
  `ingest-bronze` concurrency group as the hourly ingest, because both append
  to the bronze watermarks. It writes bronze in the live layout and nothing
  downstream: the next hourly transform builds the silver and gold.
- Concurrency is declared at workflow level only. A job asking for the same
  group as its own workflow queues behind its own run until it times out.
- Data commits carry `[skip ci]`, which suppresses every workflow for that push.
  Anything that must run after a data commit chains off `workflow_run`, not off
  the push — that is why `pages.yml` watches the ingest workflow finishing.

## Machine learning

Planned, not built — see PLAN.md section 4. These rules apply the moment any
of it exists.

- **Features come from silver, never from gold.** Gold's finest grain is
  hourly and the rest is daily; the targets and most features are five-minute.
  An hourly mean has already averaged the spike away, and the spike is the only
  interval a battery is paid for. Gold stays the dashboard's source and the
  commercial context, not the model's input.
- The unit of prediction is (region, interval). Price is regional; wind and
  battery output are features, never targets.
- Mean price and spike probability are separate models. Price is heavy-tailed
  enough that one model optimising mean absolute error learns to ignore the
  spikes, which are the only intervals a battery is paid for.
- Features are **derived, never committed**. A 50-column feature table costs
  about 200 MB a year against 17 MB for the predictions, and it is reproducible
  from silver by definition. Rebuild it in the workflow and gitignore it, as
  `docs/data` already is. Model files are build artefacts, not data.
- Model outputs live in `data/ml/`. Predictions are **write-once per day**,
  like silver: a forecast issued at 14:05 is a historical fact, and rewriting it
  destroys the only evidence the model was not retrofitted. Metrics and backtest
  results are rewritten in full, like gold.
- The `ml` extra (`scikit-learn`, `lightgbm`, `pandas`, `numpy`) is installed
  by the ML workflow and nothing else. The hourly ingest keeps installing plain
  `.`; these wheels are tens of megabytes.
- Every model must be reported against persistence, same-time-yesterday and
  AEMO's own `P5MIN` forecast, per fold, never only pooled. If it cannot beat
  `P5MIN`, say so plainly — that is a finding, not a failure to hide.

## Leakage rules

A leaked feature produces a model that looks superb and earns nothing, so
these are not negotiable:

- A feature for interval `t` may use only data whose interval ends at or before
  `t`, with one interval of slack for publication lag — data up to `t − 5 min`.
  **`t` is the first interval being predicted, not the row key.** A feature row
  keyed `t` in `sql/ml/features.sql` is a forecast issued at the end of
  interval `t` and predicting `t+5` onward, so interval `t`'s own price is a
  feature. Measured, not assumed: across 5,695 rows carrying AEMO's
  `LASTCHANGED`, a dispatch result is written about 6 seconds after the
  interval it governs begins and never before, so interval `t`'s price is a
  fact by the time `t` ends. Stating which `t` is meant matters because both
  readings are self-consistent and only one of them is implemented.
- Forecasts are keyed by **issue time, never valid time**. Every forecast row
  carries `issued_at`, and a feature may use it only where `issued_at <= t`.
  Joining on valid time alone hands the model the answer.
- Weather is admissible only from a forecast issued before `t`. Observed or
  reanalysis weather at `t` is not available at `t` in reality.
- Scalers, imputation, encodings and feature selection are fitted inside the
  training fold only.
- Walk-forward validation with **purge and embargo**: drop training rows whose
  target window crosses the fold cutoff, and skip the first `horizon` minutes of
  the test period. Targets overlap, so an ordinary split is not enough.
- Backtest policy thresholds are tuned inside the training fold. Tuning them on
  the test period is leakage wearing a different hat.
- Trades settle at the actual price of the interval, never the forecast.
- The registry is a slowly changing dimension: using today's capacities for
  older features tells the model which farms were later expanded.

## Security

- Never commit secrets, API keys, or tokens, and never add a credential to a
  workflow file, a config file, a test fixture, or a commit message. A
  credential needed locally goes in `.env`, which is gitignored.
- **No GitHub Actions secret is configured, and none should need to be.** The
  pipeline takes public, unauthenticated AEMO feeds and writes files; CI runs
  the assistant against recorded replies. A fork clones and goes green with no
  setup, which is worth protecting.
- The assistant is the one component that can call out, to an
  OpenAI-compatible endpoint named by `ASSISTANT_PROVIDER`. Its key
  (`DEEPSEEK_API_KEY` by default) is read from the environment at runtime, is
  optional, and must stay optional: every code path has to work without it.

## Working style

- Keep changes scoped to what was asked. No speculative abstraction.
- Before adding a dependency, a workflow, or a new data layer, say what it buys
  and what it costs in repo size or runtime minutes.
- When touching ingestion, state the effect on git history growth.
