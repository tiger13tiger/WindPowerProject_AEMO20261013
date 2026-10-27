# Plan

Repository layout, design decisions, and build order. Sections 1 and 2 describe
the repository as built; section 3 marks what is done and what remains.

## 1. Repository structure

```
.
├── .github/
│   └── workflows/
│       ├── ingest.yml              # hourly: fetch both feeds → bronze → commit
│       ├── reference.yml           # manual: refresh the wind DUID registry
│       ├── tests.yml               # CI: ruff + pytest on every push and PR
│       └── pages.yml               # deploy docs/ when gold changes
│
├── scripts/
│   ├── nemweb.py                   # shared: list, download, parse, watermark, write
│   ├── cli.py                      # shared argparse entry point
│   ├── ingest_scada.py             # ReportSpec: Dispatch_SCADA → bronze/scada
│   ├── ingest_dispatchis.py        # ReportSpec: DispatchIS   → bronze/price,
│   │                               #   regionsum, interconnector (one download)
│   ├── ingest_p5min.py             # ReportSpec: P5MIN        → bronze/p5min
│   ├── ingest_predispatch.py       # ReportSpec: PredispatchIS → bronze/predispatch_price,
│   │                               #   predispatch_region (one download)
│   ├── inspect_report.py           # print a report's real headers; writes nothing
│   ├── refresh_reference.py        # wind DUID registry from the AEMO list
│   ├── build_silver.py             # wind filter, typing, dedup, validation
│   ├── check_quality.py            # quality checks, hard fail / soft warn
│   └── build_gold.py               # run the gold SQL, write aggregate tables
│
├── sql/
│   ├── silver/wind_output.sql       # wind filter, timestamps, flags
│   ├── silver/price.sql             # regional price, pricing run only
│   ├── quality/*.sql                # one model per quality check
│   └── gold/
│       ├── generation_5min.sql     # wide, dashboard-ready interval table
│       ├── farm_daily.sql          # MWh, capacity factor, peak per farm per day
│       ├── farm_revenue_daily.sql  # MWh x RRP: revenue and realised price
│       ├── capture_price.sql       # capture price vs time-weighted average price
│       ├── negative_price.sql      # intervals generating into a negative price
│       ├── state_hourly.sql        # MW by state and NEM region
│       └── fleet_monthly.sql       # fleet totals and records
│
├── data/
│   ├── reference/
│   │   ├── wind_duids.csv          # DUID → station, region, fuel, capacity, in_scada
│   │   └── wind_duids.meta.json    # source, retrieval date, counts, exclusions
│   ├── bronze/
│   │   ├── scada/_ingested.txt     # watermark, one source file name per line
│   │   ├── scada/date=YYYY-MM-DD/  # hour=HH.parquet, write-once
│   │   ├── price/_ingested.txt
│   │   └── price/date=YYYY-MM-DD/  # hour=HH.parquet
│   ├── silver/date=YYYY-MM-DD.parquet
│   └── gold/*.parquet
│
├── docs/                           # GitHub Pages site (static, built from gold)
│   ├── index.html
│   ├── assets/
│   └── data/                       # small JSON extracts the page fetches
│
├── assistant/
│   ├── schema.md                   # table and column descriptions given to the model
│   ├── prompts/system.md           # the rules the generated SQL must obey
│   ├── provider.py                 # any OpenAI-compatible endpoint, over httpx
│   ├── guard.py                    # allowlist + sandbox for generated SQL
│   ├── qa.py                       # NL question → DuckDB SQL → answer
│   ├── eval.py                     # score the model against the question set
│   └── evals/                      # questions.json + recorded replies
│
├── tests/
│   ├── fixtures/                   # trimmed AEMO CSVs, committed
│   ├── test_parser.py              # C/I/D nested-CSV parsing
│   ├── test_bronze.py              # hour selection, writes, watermark
│   ├── test_listing.py             # directory listing and file names
│   ├── test_price.py               # DISPATCH_PRICE specifics
│   └── test_reference.py           # wind DUID registry
│
├── CLAUDE.md
├── README.md
├── PLAN.md
├── pyproject.toml                  # deps, ruff and pytest config
├── .gitignore
└── LICENSE
```

## 2. Key decisions

**DuckDB instead of Spark or pandas.** The whole dataset is a few hundred MB.
DuckDB reads and writes Parquet natively, runs the aggregations in SQL, and
needs no cluster — which matters because the only compute available is a
GitHub Actions runner. It also keeps the transformation logic as portable SQL
rather than framework-specific code.

**Raw zips are never committed.** They are downloaded into the runner's
workspace, parsed, and discarded. AEMO's `Current/` directory only retains about
two days of files, so the watermark plus the bronze layer is the durable record.

**Wind filtering happens at the silver boundary, not in bronze.** Bronze keeps
every DUID exactly as published, so it stays replayable when the wind DUID
registry changes — a new farm can be backfilled from existing bronze files
instead of re-downloading from a directory that only retains two days. The cost
is repo size: all ~500 DUIDs is roughly 150k rows per day against 23k for wind
alone, or very roughly 150 MB a year of committed Parquet rather than 35 MB.
That is affordable at the hour grain below, and if history growth ever becomes
the binding constraint, moving the filter into the bronze write is the lever to
pull.

**Spot price is ingested alongside generation, not instead of it.** Generation
alone measures volume. Joined to the regional reference price it measures
economics: revenue per farm, capture price against the time-weighted average,
and how often a farm is generating into a negative price — which is the
commercially interesting question for Australian wind, and the one a former
turbine engineer is well placed to ask. Both feeds are the same shape, so the
second source cost one `ReportSpec`, not a second pipeline.

**Bronze keeps intervention runs.** When AEMO intervenes it publishes both a
physical and a pricing run for the same interval. Discarding one is a business
decision, so bronze keeps both and silver resolves it.

**FCAS price columns are not ingested.** They are eight separate ancillary
service markets, irrelevant to wind energy revenue, and would quadruple the
price table's width. This is the one place bronze is not perfectly faithful,
and it is a one-way door: history written before the columns are added will not
carry them.

**Immutable files, because git history is append-only.** The expensive mistake
here would be rewriting the current day's Parquet file on every run — 288
rewrites a day, each one a permanent blob. Instead bronze writes one file per
closed UTC hour and never touches it again, so history grows at roughly the rate
of the data itself. Gold tables are the exception: they are small enough
(hundreds of KB) that a full rewrite each run is cheaper than the complexity of
incremental merging.

**Hourly schedule, not every 5 minutes.** One run per hour picks up 12 source
files, produces one bronze file, and makes one commit. That is 24 commits a day
instead of 288, well inside the free tier, and the data is at most an hour
behind. Latency is not what this project is demonstrating.

**One Parquet file per closed hour, and no compaction.** Measured on live
cardinality (517 DUIDs, 5 regions), the three candidate grains cost:

| Grain | Size/year | Files/year | Rewrites |
|-------|-----------|------------|----------|
| per 5-min interval | 702 MB | 210,240 | none |
| **per hour** | **293 MB** | **17,520** | **none** |
| per day | 227 MB | 730 | one per run |

Hour files cost 58% less than interval files because the Parquet footer and the
DUID dictionary are paid once an hour instead of 288 times a day. Daily files
are smaller still, but only reachable by rewriting the day file on every run,
which costs about 2.5 GB a year of superseded blobs — worse than either
alternative — or by running once a day and risking permanent loss against
AEMO's two-day retention.

Compaction was the original plan and is now explicitly rejected: merging small
files into large ones leaves *both* permanently in git history, so it always
costs more than writing at the right grain in the first place. The lever is
choosing the grain, not cleaning up afterwards.

**An hour is written only once it is closed, and only in full.** AEMO having
published an interval in a later hour is what proves no further file can land
in this one; the clock is never consulted, because a late or early run would
then corrupt the boundary. A partial hour is never written, since the rest of
it arriving would force exactly the rewrite this design exists to avoid. The
`limit` input therefore rounds down to whole hours.

**Idempotency through a watermark.** `data/bronze/<source>/_ingested.txt` holds
one source file name per line, appended only after the Parquet write succeeds.
Each run lists the AEMO directory, subtracts what the watermark already has, and
processes the difference. This makes re-runs safe and automatically backfills gaps from a
failed or skipped run — which will happen, because GitHub's scheduled runs are
not guaranteed to fire on time.

**The DUID registry is checked in, refreshed by hand.** Wind farm metadata
comes from AEMO's NEM Registration and Exemption List, an Excel file published
irregularly. Scraping it on every run would be fragile and slow, so it lives in
`data/reference/wind_duids.csv` and `reference.yml` refreshes it on manual
dispatch — a new DUID entering the pipeline gets a human look at the diff.

Fuel source alone cannot identify a wind farm: AEMO files Hornsdale Power
Reserve, a battery, under fuel source "Wind" because it sits beside the wind
farm. The technology and dispatch type columns are what separate a turbine from
a battery, and excluded units are named in the meta sidecar so the judgement is
reviewable.

The registry also carries `in_scada`, true when the DUID appears in committed
bronze SCADA. Twelve registered wind farms are non-scheduled and never appear in
DISPATCHSCADA at all; without the flag, quality checks cannot tell a
non-scheduled unit from a genuine ingestion gap.

**Capacity factor needs registered capacity**, which is why the registry is a
dependency of the gold layer and not just a filter list.

**Timestamps are stored twice.** AEMO publishes `SETTLEMENTDATE` in NEM time
(AEST, no daylight saving, UTC+10). Silver stores an explicit UTC timestamp for
correctness and the original NEM local timestamp for anything user-facing.
Nothing in the codebase handles a naive datetime.

**The dashboard is static.** GitHub Pages cannot run server-side code, so the
Pages workflow exports small JSON slices from gold into `docs/data/` and the
page renders them client-side. No API, nothing to keep running.

**The assistant generates SQL, it does not read the data.** The model is given
the gold schema and the question, returns a read-only DuckDB query, and the
query runs locally against the Parquet files. Token usage stays flat regardless
of how much history accumulates, the data never leaves the machine, and every
answer is traceable to a query that can be inspected — which is the only way to
trust a number a language model had a hand in.

## 3. Build steps

**Phase 0 — Foundations**
1. `pyproject.toml` with Python 3.11, `duckdb`, `httpx`; ruff and pytest config.
2. `.gitignore` (raw data, `data/bronze` scratch, `.env`, `__pycache__`, `.duckdb`).
3. `config.py`: paths, NEM timezone, layer schemas, size budget constants.
4. `tests.yml` running ruff and pytest, so CI is green before any logic exists.

**Phase 1 — Reference data** — *script and workflow done, CSV pending a run*
5. `scripts/refresh_reference.py`: download the NEM Registration and Exemption
   List, locate the generator sheet and header row by name, filter to wind, and
   write `wind_duids.csv` plus a provenance sidecar. ✓
6. `reference.yml`: manual dispatch, since the list changes only on
   registration or retirement and a new DUID deserves a human look. ✓
7. Load-with-validation helper for the silver join.

**Phase 2 — Ingestion and bronze** — *done, see `scripts/`*
7. List the AEMO directory, parse file names to interval timestamps, download
   and unpack with a timeout. ✓
8. Watermark in `data/bronze/_ingested.txt`, one source file name per line, so
   re-runs skip what is already ingested. ✓
9. Parse the nested AEMO CSV (`C`/`I`/`D` row markers, headers tracked per
   table) and write one Parquet file per source file, partitioned by NEM date. ✓
10. Parser and bronze tests against CSV fixtures, including a multi-table
    report and a corrupt file. ✓
11. Second source: `ingest_dispatchis.py` for DispatchIS `DISPATCH_PRICE`, sharing
    the same core through a `ReportSpec`. ✓

**Phase 3 — Silver and quality** — *silver done, quality outstanding*
11. `scripts/build_silver.py` + `sql/silver/`: restate AEMO's interval-ending
    timestamps as start and end in UTC and NEM local, date by interval start,
    deduplicate on (interval_start, duid), join the registry, and flag negative
    and above-capacity values rather than dropping them. ✓
12. Price silver on the same timestamps, keeping the pricing run where an
    interval carries both. ✓
13. Day completeness and sealing, so a permanently incomplete day cannot block
    silver forever. ✓
14. `scripts/check_quality.py` + `sql/quality/`: six checks over silver,
    hard ones failing the run and soft ones warning, published to
    `data/gold/quality.parquet` for the dashboard. ✓

**Phase 4 — Gold** — *done*
15. Five gold models run by `scripts/build_gold.py` as a full rewrite:
    `farm_daily`, `farm_revenue_daily`, `capture_price`, `region_hourly` and
    `latest_snapshot`. ✓
16. `latest_snapshot` reads silver plus the newest bronze hours, filtered to
    wind in memory, so the dashboard is current even before a silver day
    closes. ✓
17. Tests asserting exact totals against a fixed fixture day. ✓

**Phase 5 — Automation** — *ingest.yml and tests.yml done*
15. `ingest.yml`: hourly cron at :07, both ingesters, commit with `[skip ci]`,
    concurrency group so runs cannot overlap, rebase-and-retry on a push race. ✓
16. `tests.yml`: ruff and pytest on Python 3.11 for every push and PR. ✓
17. A one-off backfill run to seed history from whatever `Current/` holds
    (~576 intervals, about 48 hour files per source), plus a note on extending
    it from AEMO's archive. ✓
18. `transform` job in `ingest.yml`: silver → quality → gold after every
    ingest, gated so a hard quality failure stops gold without losing silver.
    `pages.yml` chains off the ingest workflow finishing, because the data
    commits carry `[skip ci]` and would otherwise suppress it. ✓

**Phase 6 — Dashboard** — *done*
18. `scripts/export_dashboard.py` + `sql/dashboard/`: eight JSON slices from
    gold into `docs/data/`, about 50 KB in total. ✓
19. `docs/index.html`: plain HTML and Chart.js from a CDN, no build step.
    Fleet tiles, regional output, capacity factor trend, best and worst farms,
    revenue and negative-price exposure, capture ratio, capture detail,
    quality table. ✓
20. `pages.yml` deploys `docs/` whenever gold changes, regenerating the JSON
    rather than committing it. ✓
21. `docs/farm.html`: one farm at a time, picked from a dropdown. Its
    scorecard, the hour-of-day output shape against the regional price shape,
    the five-minute profile of the most recent complete day, its daily record,
    and its rank in the fleet. ✓

    **Why the per-farm detail reads silver, not gold.** The subject of the page
    is the five-minute shape, which gold has already averaged to hourly, and
    the whole point is *when* a farm generates against when its region pays.
    Putting it in gold is not an option either: gold is committed and rewritten
    every run, so 96 farms × 288 intervals would add megabytes to git history
    hourly. `docs/data/` is gitignored and rebuilt at deploy time, so the files
    cost deploy seconds and nothing in the repository.

    **Why one file per farm.** All farms in one slice is 2.4 MB today and
    36 MB after a year — it works now and breaks silently as data accumulates.
    One file per farm is ~21 KB, and a visitor downloads only the farm they
    picked. The hour-of-day shape is windowed to 14 days to keep it that size
    as history grows.

    Developer-facing measures throughout: capture price, capture ratio against
    the regional time-weighted average, revenue per MW installed per day, and
    negative-price share. Capacity factor alone ranks a merchant project wrong
    — the live data already shows farms with the fleet's best capacity factors
    earning negative revenue.

**Phase 7 — Assistant** — *done*
20. `assistant/schema.md` and `assistant/prompts/system.md`: the gold tables,
    their units, and the handful of things that are easy to get wrong (capacity
    factor is a fraction; rank on revenue not yield; exclude incomplete days). ✓
21. `qa.py`: question → SQL → execute read-only → print the answer **with the
    query that produced it**. A number whose query is hidden is a number nobody
    can check, and this one was written by a language model. ✓
22. `evals/questions.json`: 22 questions, run by `eval.py` in CI. ✓

    **Provider-neutral, over any OpenAI-compatible endpoint.** DeepSeek by
    default, selectable with `ASSISTANT_PROVIDER`. That shape is worth more
    than any one vendor: `POST /chat/completions` is served by nearly everyone,
    so `httpx` — already a dependency — is the whole client and the assistant
    adds **nothing to install**.

    **Two independent defences on generated SQL** (`guard.py`), because either
    alone is a single point of failure. A statement allowlist: exactly one
    statement, SELECT or WITH only, with a splitter that does not mistake a
    semicolon inside a string literal for a second statement. And a sandbox:
    gold is copied into an in-memory database and `enable_external_access` is
    switched off, which DuckDB will not let a later statement switch back on.
    So a query that defeats the allowlist still cannot read a file, ATTACH a
    database, COPY data out or INSTALL an extension.

    **The repository needs no credential.** Model replies are recorded under
    `evals/recordings/`, keyed by a hash of the exact prompt and model, and
    served in place of a request. CI runs the eval with no key and no network;
    a fork goes green with no setup. `--record` writes recordings when a key is
    present.

    **The eval compares results, not SQL text.** Two correct queries for the
    same question differ in alias, join order and whitespace, so scoring
    strings would score style. Each question carries a reference query and the
    model passes when its query returns the same values. With no recordings the
    run reports that the model was not scored rather than claiming a number —
    and still re-runs every reference query against the live gold schema, which
    is what stops the question set rotting as that schema moves.

**Phase 8 — Polish**
23. Architecture diagram and a data dictionary in the README.
24. Repo size report in the run log, and a documented pruning strategy for when
    history growth becomes a problem.

**Phase 9 — Forecasting and battery arbitrage** — *see section 4*

B2 (bronze feature sources) — *done*. PredispatchIS `Current` retains about
**15 days**, not the two days the other reports hold: the first run backfilled
2026-10-01 to 2026-10-15 unprompted. Worth knowing for a phase whose binding
constraint is history.

 `ingest_dispatchis.py` writes `price`,
`regionsum` and `interconnector` from one download; `ingest_p5min.py` writes
`p5min`; `ingest_predispatch.py` writes `predispatch_price` and
`predispatch_region` from one download. Every column name was read off a live
file by `scripts/inspect_report.py`, which the "Inspect AEMO report" workflow
runs on demand and which writes nothing.
25. Extend the registry from wind-only to a typed unit list, so batteries are
    classified rather than discarded, and solar and other fuels can be rolled
    up to regional totals. ✓ `refresh_reference.py` now also writes
    `units.csv` (every DUID → wind, battery, solar or other) and
    `battery_duids.csv` (with `station_id`, which pairs an older battery's
    generator and load DUIDs). `wind_duids.csv` is unchanged. Battery is
    decided before wind because AEMO files Hornsdale Power Reserve under fuel
    "Wind", and every bidirectional unit in the NEM is a battery. `storage_mwh`
    is read under five plausible spellings, none yet confirmed on the live
    file; a refresh that matches none says so.
25a. Archive backfill, `scripts/backfill_archive.py` + `backfill.yml`. ✓
    `Archive/` holds a rolling year of daily bundles, each a zip of the same
    five-minute zips the live ingest reads, so the backfill feeds them through
    the live parser and writer. A bundle covers a market day (04:05 to 04:00),
    which splits hour 04 across two bundles; the bundles either side of the
    range are read so every hour is written whole. Proved on the real archive
    before merge: one day gave 288 files per bundle and exactly 288 x 5 price
    rows. Scope decided for now: the latest three months (Aug to mid-Oct 2026),
    about 85 MB, with the rest of the year a later decision. The archive held
    2025-10-16 onward on 2026-10-27; the first half of October 2025 is gone.
    P5MIN is archived too and is backfilled; PREDISPATCH is not, because its
    `Current/` keeps fifteen days and its bundles are the largest.
25b. Baselines kept current, with an AEMO comparator at both horizons. ✓
    `baselines.py` runs in the hourly transform, so `baseline_metrics.json`
    can no longer go stale (the committed one had been generated before the
    P5MIN coverage fix in the same PR). `predispatch_baseline.sql` is the
    120-minute comparator, joined on publication time and carrying a run's
    last period over the 1-7 intervals its two-hour reach falls short. Every
    row also reports `mae_trim99`: on 15 Oct 2026 P5MIN forecast QLD1 near
    $23,200 for an hour that settled near $90, and nine rows carried 85% of
    QLD1's P5MIN MAE. Trimmed, AEMO beats persistence at both horizons on
    the data so far (60 min 9.34 vs 9.83; 120 min 13.42 vs 13.81); untrimmed,
    it loses. Both are reported because both are true.
26. New bronze source: `P5MIN`, AEMO's own five-minute forecast, as the
    baseline to beat and as a feature.
27. `scripts/build_features.py` + `sql/ml/features.sql`: one row per region per
    interval, 43 columns, derived and not committed. ✓
28. `scripts/train_forecast.py`: walk-forward training for four targets, with
    the purge and embargo described in section 4. ✓ **Built, and refusing by
    default** — the 60-day gate holds, and `--provisional` runs on short
    history with every output row stamped `provisional`. On the first such run
    (14.4 days, one fold, 6.9 days of training) the mean model reached parity
    with persistence in QLD1 and VIC1 and was marginally ahead in SA1, while
    NSW1 at 120 minutes came in at MAE 84 against persistence's 12 — a median
    error of 19 and a **+71 bias**, which is the model applying one week's
    price level to the next. That is the gate earning its keep rather than a
    bug, and the reason the number is labelled instead of lowered. Spike
    models scored nothing: no test fold held a positive.
29. `scripts/backtest_battery.py`: arbitrage simulation driven by the forecast,
    measured against perfect foresight and a no-forecast threshold rule. ✓
30. Dashboard panels for forecast error against the baselines, spike
    reliability, and backtest revenue.
   **These go on their own Forecast page, not onto the existing dashboard,
   and the track record leads rather than the current forecast — see
   section 7, which also covers the restructure this is the trigger for.**


## 4. Forecasting and battery arbitrage

Plan only. Nothing in this section is built.

### 4.1 Scope

The project studies **wind farms and batteries**. Solar, coal, gas, hydro and
everything else enter only as **regional totals used as features** — never
modelled, never broken out per unit. That keeps the subject matter narrow
enough to say something real about, while still letting the model see the
supply stack that actually sets the price.

Everything the model sees comes from **silver** (section 4.3), which is the
last layer at five-minute grain.

This forces one change upstream. The registry today answers "is this a wind
farm?" and discards everything else; Hornsdale Power Reserve is excluded by
name. It must instead answer "what kind of unit is this?" — `wind`, `battery`,
or `other` — because a battery is now a subject and the rest is now a feature.
The existing exclusion logic becomes a classification, and `in_scada` stays as
it is.

### 4.2 Targets

Price is regional, so the unit of prediction is **(region, interval)**. Wind
and battery output are features, not targets. **SA1 first**: the most
wind-exposed region, the most volatile price, and the one where a battery has
the clearest case. Then the other four, as separate models rather than one
pooled model, until there is evidence pooling helps.

For a forecast made at interval `t`, over a window of the next `h` minutes
(`h` in {60, 120}, i.e. the next 12 and 24 dispatch intervals):

| Target | Definition | Type |
|--------|-----------|------|
| `y_mean_60` | mean RRP over `t+5 … t+60` | regression |
| `y_mean_120` | mean RRP over `t+5 … t+120` | regression |
| `y_spike_60` | any interval in `t+5 … t+60` above $300/MWh | binary |
| `y_spike_120` | any interval in `t+5 … t+120` above $300/MWh | binary |

Mean and spike are deliberately separate problems. Price is heavy-tailed
enough that a model tuned on mean absolute error learns to ignore spikes, and
a spike is exactly what a battery is paid for. One model optimising MAE would
be excellent at the 95% of intervals that do not matter commercially.

### 4.3 Features

**Features are built from silver, never from gold.**

Gold's finest grain is hourly (`region_hourly`); everything else in it is
daily. The targets in 4.2 are defined on five-minute dispatch intervals, and
most of the features below are five-minute lags, ramps and spike counts.
Those cannot be recovered from an hourly mean — the averaging has already
removed the spike, which is the single thing a battery is paid for. Silver is
the last layer that still holds one row per five-minute interval:

| Layer | Finest grain | Usable as a feature source |
|-------|--------------|---------------------------|
| `silver/wind_output`, `silver/price` | 5 minutes | **yes** |
| `gold/region_hourly` | 1 hour | no — spikes already averaged away |
| `gold/farm_daily`, `farm_revenue_daily`, `capture_price` | 1 day | no |

This is not a flaw in gold. Gold aggregates deliberately, for human reading;
it is simply the wrong input for a model predicting five-minute prices.

Gold keeps two roles in this phase: as context when interpreting results
(`capture_price` says what wind actually earned, which is the commercial
backdrop to a forecast), and as the dashboard's source, which is where the
model's own outputs are published alongside it.

Every feature is **a function of the (region, interval) grid at or before `t`**.

**Price history** — lags at 5, 10, 15, 30, 60, 120 minutes; rolling mean,
standard deviation, min and max over 30, 60 and 240 minutes; count of intervals
above $300 in the last 60 and 240 minutes; time since the last spike.

**Wind** — regional wind MW; wind as a share of registered wind capacity;
5, 30 and 60-minute ramp; the same for the region's neighbours, since an
interconnector makes a neighbour's wind a local price signal.

**Battery** — regional battery MW, signed, so charging and discharging are
distinguishable. ~~A crude proxy for available state of charge, which AEMO
does not publish directly.~~ **Corrected 2026-10-15:** AEMO does publish it.
`DISPATCH_REGIONSUM` carries `BDU_ENERGY_STORAGE` (regional battery energy in
MWh), `BDU_INITIAL_ENERGY_STORAGE`, `BDU_MIN_AVAIL`/`BDU_MAX_AVAIL` and
`BDU_CLEAREDMW_GEN`/`BDU_CLEAREDMW_LOAD`. The proxy is unnecessary: state of
charge is a measured regional quantity, and charge/discharge arrive already
split by direction. Found by inspecting a live file, not assumed.

**Other supply, as regional totals only** — solar MW, and non-renewable MW
as a single figure. The point is the residual: demand minus wind minus solar
is what the dispatchable stack has to cover, and it predicts price far better
than any of its parts.

**Demand and system** — regional demand, its ramp, and net interconnector flow.

**Calendar** — interval of day and day of week, each as sine/cosine pairs so
midnight is adjacent to 23:55; public holiday flag; month.

**AEMO's own forecast** — the `P5MIN` run issued at or before `t`: forecast
RRP for each interval in the window, and forecast regional demand. This is both
the strongest feature and the baseline to beat.

**Weather (optional, later)** — forecast wind speed at hub height for the
region's main wind corridors. Deferred, not because it would not help but
because it introduces a second source with a genuinely harder leakage problem
(section 4.4) and the model should first be shown to work without it.

### 4.4 Leakage rules

**Measured, 2026-10-15.** The rules below assumed one interval of slack for
publication lag. That assumption is now partly measured and partly still open.
AEMO stamps its own write time into the forecast reports, and the file name
carries the publication instant, so the lag is observable rather than guessed
— once the file-name stamp is read as NEM local time, which is the whole trick:
parsed naively it reads as UTC and every figure comes out exactly ten hours
wrong.

| Report | AEMO writes → publishes | Published vs the run it is named for | Lead to the interval forecast | Published late |
|---|---|---|---|---|
| `P5MIN` (32,040 rows) | 2.3 s | **1.95–4.47 min early** | 2–60 min | **0** |
| `PredispatchIS` (13,520 rows) | 9.8 s | — (no run timestamp) | 11–119 min | **0** |

Two things follow. Gating a `P5MIN` feature on `RUN_DATETIME <= t` is
**conservative**: the run is on the server about four minutes before the time
it is named for, so the rule gives away a little information rather than
leaking any. And `PredispatchIS`'s nearest period is only **11 minutes** ahead
at its tightest, not the half hour its cadence suggests — a backtest that
assumes a half-hour horizon from it is assuming lead time that does not exist.

**Still open: the dispatch reports.** `ingested_at` cannot answer this. It
records when *this pipeline* ran — a median of ten hours after the interval,
because bronze is written an hour at a time — and says nothing about AEMO. The
one live sample seen during inspection had `LASTCHANGED` 09:45:09 against
`SETTLEMENTDATE` 09:50:00, which would mean a dispatch result is published at
roughly its interval's *start*, nearly five minutes before the stamp it
carries, because dispatch is solved forward. One sample is not a measurement,
so `LASTCHANGED` is now ingested on all three dispatch tables and this can be
settled from committed data in a few days.



These are the rules the whole phase stands on. A leaked feature produces a
model that looks superb and earns nothing.

1. **A feature at `t` may use only data whose interval ends at or before
   `t`.** The grid is interval-ending, so the interval labelled `t` is already
   complete and is admissible.

2. **Allow one interval of publication lag.** A dispatch interval ending at
   `t` is not on NEMWeb at `t`. Features use data up to `t − 5 min` unless a
   measured publication delay proves otherwise. Costing one interval of
   information is cheaper than discovering the whole backtest was optimistic.

3. **Forecasts are keyed by issue time, never by valid time.** Every
   forecast row carries `issued_at` and `valid_for`, and a feature may use a
   row only where `issued_at <= t`. Joining on `valid_for` alone is the
   classic way to hand a model tomorrow's answer. This applies to `P5MIN`,
   pre-dispatch and any weather forecast.

4. **Weather is admissible only from a forecast issued before `t`.** Observed
   weather at `t` is not available at `t` in practice, and analysis-grade
   reanalysis is not available at all in real time. Store the issue time or do
   not store it.

5. **The registry is a slowly changing dimension.** Using today's capacities
   to build 2024 features tells the model which farms were later expanded.
   Either snapshot the registry with effective dates, or restrict training to
   the period since the current registry was valid, and say which.

6. **Fit every transformation inside the fold.** Scalers, imputation values,
   target encodings and feature selection are fitted on training rows only.

7. **Purge and embargo** (section 4.5), because a target window straddling
   the fold boundary leaks the test period into training.

8. **No feature derived from the target window**, including "number of
   intervals since the last spike" computed with hindsight.

A test asserts rule 1 mechanically: for a sample of rows, rebuild each feature
using only data up to `t` and assert equality with the stored value. A feature
that fails cannot be explained away.

### 4.5 Walk-forward validation

Random k-fold on a time series is meaningless here, and ordinary
train/test splitting is not enough either, because the targets overlap.

- **Rolling origin.** Fold `k` trains on everything up to `cutoff_k` and tests
  on the following 14 days. Origins advance 14 days at a time.
- **Purge.** Drop training rows whose target window extends past `cutoff_k`.
  With a 120-minute horizon that is the last 24 training intervals.
- **Embargo.** Skip the first 120 minutes of the test period, so no test
  feature overlaps a training target.
- **Expanding, not sliding, window** to start. The NEM changes structurally —
  new batteries, new transmission — so a sliding window may prove better; that
  is an experiment, not an assumption.
- **Report per fold, never only pooled.** A single pooled number hides a model
  that worked until a market change and then stopped.

### 4.6 Baselines

The model must beat all three, reported side by side at every horizon:

1. **Persistence** — the forecast mean is the current RRP; spike probability is
   1 if the current interval is above $300. Trivially cheap and surprisingly
   hard to beat over 60 minutes.
2. **Same time yesterday** — the mean over the same clock window 24 hours
   earlier. Captures the daily shape that persistence misses.
3. **AEMO `P5MIN`** — the market operator's own forecast for the same window.
   This is the honest bar. If the model cannot beat it, the correct conclusion
   is that AEMO's forecast is good, and the write-up says so.

For spike probability, a fourth: **climatology**, the historical spike rate for
that interval-of-day, which is what any calibrated model must beat to be worth
its complexity.

**Metrics.** Regression: MAE and RMSE, reported both overall and split by
whether the window contained a spike, because an overall MAE is dominated by
the rare intervals. Classification: Brier score, precision-recall AUC (not
ROC AUC — spikes are rare enough that ROC flatters everything), and a
reliability curve, since a probability that is not calibrated is useless to
the dispatch rule in 4.7. The decisive metric is section 4.7's revenue.

### 4.7 Battery arbitrage backtest

A forecast is only worth what a decision made from it earns.

**The battery.** A price-taking unit, parameterised: power MW, energy MWh,
round-trip efficiency (0.85 to start), state-of-charge limits. Default to a
realistic SA1 unit. Price-taking is an assumption worth stating: a large
battery moves the price it trades into, and this backtest will not model that,
so results overstate what a large unit would earn.

**The policy.** Charge when the forecast mean over the next window is in the
bottom quantile of recent prices and state of charge allows; discharge when
the forecast mean is in the top quantile or forecast spike probability exceeds
a threshold. Thresholds tuned **inside the training fold only** — tuning them
on the test period is leakage wearing a different hat.

**Settlement.** Every trade settles at the *actual* RRP of the interval it
occurs in, never the forecast. Charging pays, discharging earns, and
round-trip losses are taken on the charge leg.

**Comparators.**

| Strategy | What it establishes |
|----------|--------------------|
| Perfect foresight | the ceiling: all the money a perfect forecast could earn |
| Forecast-driven policy | what this model is worth |
| Fixed price threshold, no forecast | how much of that is the forecast rather than the rule |
| Do nothing | zero |

Reported as $/MW/day and as a share of the perfect-foresight ceiling, per fold.
The second number is the one that matters: "captured 34% of the theoretical
maximum" says more than a dollar figure that depends on how volatile the test
period happened to be.

### 4.8 Repo size

Measured on a worst case of 1,440 region-intervals a day and 50 float columns,
using random values, which do not compress — real lag and rolling features
correlate heavily and would do better:

| Artefact | Per day | Per year | Committed? |
|----------|---------|----------|-----------|
| Features (50 cols) | 551 KB | **201 MB** | **No — derived** |
| Predictions (4 targets) | 46 KB | **17 MB** | Yes |
| Model files | — | ~60 MB if monthly | **No** |
| `P5MIN` bronze | 312 KB | ~110 MB hour-grain / ~82 MB day-grain | Yes |

Committing features would cost more than the entire existing pipeline and push
the repository past GitHub's 1 GB recommendation inside two years. They are
reproducible from silver by definition, so they are rebuilt in the workflow
and gitignored — the same decision already taken for `docs/data`.

Model files are build artefacts, not data: kept as workflow artefacts, with
only metrics and predictions committed. A model worth keeping gets attached to
a release.

**ML therefore adds about 32 MB a year**, taking the project to roughly
330 MB a year. The budget decision stands.

**Bronze grain for the feature sources (measured 2026-10-14).** The four new
bronze tables are far thinner per interval than Dispatch SCADA: 5 regions or
6 interconnectors, against 517 DUIDs. At that cardinality an hour file is
mostly Parquet footer — the fixed cost is about 1.5 KB empty and 2.3 KB of a
2.5 KB file holding 60 rows.

Five tables, not four: PredispatchIS keeps its regional prices and its regional
solution in separate tables (`PREDISPATCH_REGION_PRICES` and
`PREDISPATCH_REGION_SOLUTION`), so each becomes its own bronze table.

**Superseded. Measured on committed data, 2026-10-15.** The table that stood
here was built from synthetic rows — smooth sine waves across five regions —
and it was wrong, badly on one side. Such values compress far better than real
AEMO data when a day of them is concatenated into one file, so the day-grain
column was optimistic by roughly ten times. Real numbers, taking one full day
of each table as committed and rewriting it as a single Parquet file:

| Source | Hour files | Hour grain | Day grain | Saving |
|--------|-----------|-----------|----------|--------|
| `regionsum` | 19 | 108 KB/day | 49 KB/day | 2.2x |
| `interconnector` | 19 | 72 KB/day | 32 KB/day | 2.2x |
| `p5min` | 24 | 312 KB/day | 225 KB/day | 1.4x |
| `predispatch_price` | 24 | 68 KB/day | 9 KB/day | 7.6x |
| `predispatch_region` | 24 | 108 KB/day | 24 KB/day | 4.5x |
| **Total** | | **233 MB/yr** | **118 MB/yr** | **2.0x** |

The shape of the argument survives and its force does not. Day files still win
everywhere, and win most where rows per hour are fewest — 7.6x on the 40-row
predispatch price table against 1.4x on the 720-row P5MIN. But the saving is
2x overall, not 12x, and hour grain costs 233 MB a year rather than 136.

Trimming P5MIN's horizon is not the alternative lever it looks like: keeping 6
of its 12 intervals saves only 1.5x, because the per-file footer and the
repeated run and interval timestamps dominate, and a 30-minute horizon cannot
serve the 60-minute target in 4.2. P5MIN's ~110 MB a year is close to
irreducible at this grain.

This is the same arithmetic the layer contract already does for bronze, giving
the opposite answer: hour files won for SCADA because 6,118 rows amortise the
footer, and lose here because 40 to 720 rows do not. Day files keep the
write-once property as long as a day is written only once closed, which is the
rule silver already follows. The contract currently says hour, so these sources
are built at hour grain until that decision is taken.

At hour grain these five tables cost about 233 MB a year, which is more than
the entire existing pipeline and takes the project to roughly 470 MB a year. At
day grain they cost 118 MB. Neither lands inside the 32 MB a year section 4.8
budgeted for the whole ML phase, so that budget needs revisiting whichever
grain is chosen. Nothing about the data changes between the two — only the
number of Parquet footers written.

They are ingested at hour grain in the meantime rather than held back, because
the two costs are not symmetric: AEMO retains about two days, so an hour not
ingested is gone permanently, while an hour written at the dear grain is 370 KB
a day of history that a later decision can stop adding to.

Model outputs get their own tree, because they do not all behave the same way:

```
data/ml/
  predictions/date=YYYY-MM-DD.parquet   write-once, like silver
  metrics.parquet                        full rewrite, like gold
  backtest.parquet                       full rewrite, like gold
```

Predictions are **write-once**. A forecast issued at 14:05 yesterday is a
historical fact, and rewriting it would destroy the only honest record of what
the model actually said at the time — the difference between a backtest and a
retrofit. Metrics and backtest results are recomputed across all history on
every run, so they are rewritten in full, exactly as gold is.

Features sit outside this tree entirely: derived, gitignored, rebuilt in the
workflow.

The flow, end to end:

```
silver ──► features (derived, gitignored) ──► model ──► data/ml/predictions
                                                   └──► data/ml/metrics
  gold ──────────────────────────────────────────────► dashboard ◄──────┘
```

### 4.9 Dependencies

A new `ml` extra, installed by the ML workflow and by nothing else:

```toml
ml = ["scikit-learn>=1.5", "lightgbm>=4.5", "pandas>=2.2", "numpy>=2.0"]
```

Gradient boosting because the problem is tabular, non-linear, and small enough
to train on a free runner in minutes; a regularised linear model is the first
thing to try and the thing to fall back to if boosting does not clearly beat it.
These are large wheels — scikit-learn alone is tens of megabytes — which is
precisely why the hourly ingest must keep installing plain `.` and never see
them.

## 5. Curtailment: the open piece, and why it is open

The one metric from the dashboard review that is not built. It is not built
because publishing it would have meant publishing a number that cannot be
defended, and the reason is worth writing down rather than rediscovering.

### Why it matters

A developer looking at a wind farm wants to separate three things that all look
identical in SCADA output:

- the wind did not blow,
- the wind blew and the farm was told to back off because the price had gone
  negative (**economic** curtailment — the operator's own choice, and a
  decision a battery or a different offtake structure can change),
- the wind blew and AEMO capped the farm for a network or system constraint
  (**network** curtailment — not the operator's choice, and a material risk
  that a capacity factor hides entirely).

Capacity factor cannot tell them apart. Nothing currently on the dashboard can.

### What already exists

Everything needed is ingested:

| Piece | Where |
|---|---|
| Actual output per farm, 5-minute | `data/silver/wind_output/` |
| AEMO's unconstrained wind availability per region | `ss_wind_uigf` in `data/bronze/regionsum/` |
| Regional price, to split economic from network | `data/silver/price/` |

### The blocker, measured

Comparing regional wind SCADA against `SS_WIND_UIGF` looks like it gives
curtailment directly. It does not, because the two cover **different
populations**: `SS_WIND_UIGF` is the availability forecast for
**semi-scheduled** units only, while the registry in
`data/reference/wind_duids.csv` includes older scheduled and non-scheduled
farms as well.

Measured over the record as it stood:

```
region  avg actual  avg UIGF   apparent shortfall
VIC1      1374 MW    1533 MW        13.9%
SA1        730 MW     818 MW        11.7%
TAS1       166 MW     102 MW        actual ABOVE forecast availability
```

TAS is the proof. Actual output cannot exceed the availability forecast for the
same units, so the populations plainly differ — Tasmania's older farms are not
semi-scheduled and are therefore absent from the UIGF while present in the
SCADA sum. VIC and SA may well be showing real curtailment, but the figure
cannot be separated from the population mismatch until the registry is fixed.

### The fix, in order

**Correction.** An earlier version of this section said the registry already
parsed the column needed and merely failed to write it. That was wrong, and the
mistake is worth recording because it is an easy one to repeat:
`refresh_reference.py` parses **`dispatch_type`**, which holds *Generator /
Load / Bidirectional* — that is why `is_generation()` consults it to reject
bidirectional units. The scheduling classification — *Scheduled /
Semi-Scheduled / Non-Scheduled* — is a **different column**, and it was not
parsed at all. Two columns, similar names, unrelated meanings.

1. **Parse and persist the scheduling classification.** Done: `schedule_type`
   is now read via `OPTIONAL_COLUMN_HINTS` and written to `wind_duids.csv`
   alongside `dispatch_type`. The candidate spellings
   (`schedule type`, `classification`, `scheduled status`, …) are **unverified
   against the live file**, because aemo.com.au is unreachable from the
   development sandbox. A refresh run logs the headers it actually saw when
   none match, and warns loudly when the column comes back empty, which is how
   the true spelling gets confirmed.

   **This step needs a human to run the manual `reference.yml` workflow.** The
   committed `wind_duids.csv` has the new columns only once that runs.
2. **Filter the comparison** to semi-scheduled DUIDs only, on both sides, then
   re-measure the three regions above. Expect TAS to stop being impossible.
   Until that re-measurement exists there is no defensible curtailment number,
   which is the whole reason this section exists rather than a metric.
3. **Then** build the metric: `greatest(uigf - actual, 0)` per region per
   interval, split by the price in that interval — negative or near-zero price
   means economic, a price above zero with output held down means the
   constraint was not the operator's choice.

### Two traps to carry into step 3

- **UIGF is a forecast, not a measurement.** Small positive differences are
  forecast error, not curtailment. The metric needs a tolerance — a percentage
  of availability, not an absolute MW — and the tolerance has to be stated on
  the chart, because the result moves with it.
- **The registry is a slowly changing dimension** (CLAUDE.md, leakage rules).
  Using today's dispatch types and capacities for older intervals will
  misattribute curtailment at farms that were later expanded or reclassified.
  For the dashboard this is tolerable if said out loud; for anything feeding
  the ML phase it is not.

### Sizing

One gold model at (region, trading_date) or (region, hour) grain, a few KB a
day, plus one dashboard panel. The registry change is a column. The work is
mostly in step 2 — confirming the populations now match — rather than in the
SQL.

## 6. Repo size: the budget, and pruning when it matters

CLAUDE.md calls repo size a hard budget and names no number, which makes it
impossible to act on. The number is **1 GB**: GitHub recommends staying under
it, and a portfolio repository is something people clone, so the practical
ceiling sits below the technical one.

`python scripts/repo_size.py` prints the position every transform run. It
converts bytes per day across seven sources at three grains into the only
figure that drives a decision — months of runway — and it is a report, never a
gate: a red hourly run over a slow-moving number gets ignored within a week.

### Where the growth actually is

Measured per source against each source's own observed days:

```
  bronze/scada               191 MB/yr      silver/wind_output      40 MB/yr
  bronze/p5min                91 MB/yr      bronze/price            30 MB/yr
  bronze/regionsum            43 MB/yr      bronze/interconnector    29 MB/yr
  bronze/predispatch_region   36 MB/yr      bronze/predispatch_price 23 MB/yr
  gold (per full rewrite)     45 MB/yr      silver/price             7 MB/yr
                                            ---------------------------------
                                            total                  534 MB/yr
```

Two things follow. Write-once layers cost their size once, so a year of them is
a year of bytes. Gold costs its size *every time it changes* — 125 KB rewritten
hourly would be 1.1 GB a year against 45 MB at one rewrite a day, which is why
making gold byte-stable was worth doing and why it is reported per rewrite.

### Pruning options, cheapest first

**1. Do nothing.** At 534 MB/yr with 14 MB on disk there are roughly 23 months
of runway. This is the correct answer today, and the report exists so the
answer can be revisited on evidence rather than on anxiety.

**2. Prune forecast history to a rolling window.** `p5min` and the two
`predispatch` tables are about 150 MB/yr — 28% of all growth — and *nothing
consumes them yet*. They exist for the ML phase, and forecasts lose value with
age in a way that actuals do not: a P5MIN run from 2026 will not help a model
trained in 2028, and walk-forward backtesting uses a bounded window regardless.
Keeping 90 days rather than everything is the cheapest real lever and costs no
analysis. Note it cannot be done by deleting files — git history is append-only,
so pruning means not ingesting beyond the window, or accepting that the old
blobs stay in history and only the working tree shrinks.

**3. Day-grain bronze for `scada`.** The largest single line at 191 MB/yr.
Measured earlier at roughly half the cost at day grain, against hour grain's
one advantage: an hour can be sealed as soon as AEMO publishes into the next
one, where a day cannot be sealed until the day ends. Costs a layer-contract
change plus day-sealing logic, and the cost grows with the history there is to
migrate — so if this is ever going to happen, earlier is cheaper.

**4. Stop committing bronze for derived regional sources.** `regionsum` and
`interconnector` are regional totals, 72 MB/yr together, and silver could hold
what gold needs. This breaks the replayability that bronze exists for, so it
is a real trade and not a free saving.

**5. Last resort: a separate data repository**, or a history rewrite. Both
break every existing clone and every permalink. Only worth it if the project
outlives the budget by years rather than months.

### What would change the picture

A second ML source, a finer grain, or a new unit class in the registry. Each of
those should be costed with `repo_size.py` before it is merged, not after —
the report takes a second to run and the alternative is noticing at 900 MB.

## 7. Dashboard information architecture: restructure when the forecast lands

Recorded now because the trigger for acting on it is months away, and by then
the reasoning will be gone.

### The diagnosis

The dashboard has two pages — "Fleet overview" and "Farm detail" — organised by
**data grain**: all farms, or one farm. That is how the pipeline thinks. It is
not how a reader arrives. Nobody opens this asking for fleet-grain things; they
open it asking *can I build here*, *is my asset underperforming*, or *what is my
exposure next week*, and those are three different people at a gentailer.

The symptom is countable. Fleet overview carries **21 panels**, because it was
the only fleet-level page and every metric added since the dashboard review
went there by default. It is not a section, it is a bucket. Performance,
structural risk, portfolio construction and pipeline health sit interleaved
with nothing separating them.

Specific mis-placements, as of the curtailment work:

- *Negative-price exposure* sits among the performance panels and is a risk
  measure.
- *Does the wind arrive when demand does* sits between correlation and
  curtailment, belonging to neither.
- *Do these farms generate at the same time* answers a portfolio-construction
  question quite unlike anything around it.
- *Data quality* is pipeline health rather than analysis and occupies a full
  panel.
- *Revenue and capture detail* largely duplicates two panels above it.

### The structure to move to

Organised by the question and named for the reader:

| Section | Reader | Holds |
|---|---|---|
| status strip (not a page) | operations | fleet MW now, latest interval, quality dot |
| **Performance** | asset management | output, capacity factor, revenue, capture price, the farm league table |
| **Revenue risk** | origination, portfolio | cannibalisation and its solar control, revenue concentration, negative-price runs and the longest events, demand shape, curtailment |
| **Portfolio** | origination | the regional correlation matrix, and the per-farm most/least correlated lists currently stranded on the farm page |
| **Farm detail** | asset management | unchanged — it is the one page that already knows who it is for |
| **Forecast** | trading, dispatch | see below |

Revenue risk is the analytically distinctive part of this project and is
currently below the fold on a 21-panel page.

**"Fleet overview" is also the wrong name.** To a gentailer "fleet" means *their*
fleet; this is the whole NEM. "Market" or "National" stops an EnergyAustralia
reader expecting to see their own assets.

### Where the ML outputs go, and in what order

A **separate Forecast page**, for three reasons that do not apply to anything
else on the dashboard:

- Everything else is measurement; this is prediction. Shown beside history
  without error bars and a track record, a forecast is read as fact.
- Its reader is trading and dispatch, not origination.
- Its cadence differs: issued continuously, scored after the fact.

The order within the page matters more than the placement:

1. **The track record first** — forecast against actual against the baselines,
   MAE and MedAE and spike discrimination, per region and per horizon. After
   `scripts/baselines.py` this is also the interesting content: persistence at
   MAE 10.61 is a hard bar, and P5MIN at 37.45 looks useless until its PR-AUC
   of 0.696 against persistence's 0.342 is beside it.
2. **The current forecast**, with its recent error attached.
3. **The battery backtest** — dollars, and capture ratio against the
   perfect-foresight ceiling.

A forecast page that shows predictions above its own accuracy is the fastest
way to lose a trader's trust. Scorecard first and the page stays credible even
when the model loses — which PLAN 4.6 already requires be reported plainly.

The backtest arguably answers an origination question rather than a trading
one, but it is meaningless without the forecast driving it, so it stays here and
is cross-linked from Portfolio instead.

### Sequencing

**Do not restructure before the forecast exists.** The fifth page is needed then
anyway, and restructuring earlier moves the same panels twice.

Two things are worth doing before then, and both are small: move *Data quality*
to a footer strip, and rename *Fleet overview*.
