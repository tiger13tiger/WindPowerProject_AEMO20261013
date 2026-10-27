-- ML: AEMO's half-hourly pre-dispatch, as the 120-minute baseline a model must beat.
--
-- P5MIN reaches 55 minutes, so it cannot speak for the 120-minute target.
-- PREDISPATCH can: it forecasts half-hour periods, and bronze keeps the nearest
-- four of each run (NEAR_HORIZON in ingest_predispatch.py).
--
-- **Joined on issue time, never valid time.** `lastchanged` is when AEMO
-- published the run. A feature row keyed `t` may use anything known by the end
-- of interval `t`, which is `t + 5 min`, so the admissible run is the latest
-- one with lastchanged <= interval_start_utc + 5 min.
--
-- **How far a run reaches, measured.** Across 1,246 runs a run is published
-- about 28 minutes before its first period ends and its fourth period ends
-- 113-119 minutes after publication. Runs come every 30 minutes, so the latest
-- admissible run covers the 120-minute target (interval ends t+10 .. t+125) up
-- to somewhere between t+88 and t+125 - that is, 17 to 24 of its 24 intervals.
-- The rest are filled with the run's last period price, which is what anyone
-- reading pre-dispatch at `t` would assume, and uses nothing they could not
-- see. `pd_intervals_120` says how many intervals AEMO actually forecast.
--
-- **Periods are interval-ending, like every AEMO timestamp here.** Period
-- `datetime` D covers the six dispatch intervals ending D-25 .. D, so the
-- period covering a dispatch interval ending at e ends at e rounded up to the
-- half hour.
--
-- Parameters: $predispatch_glob, $price_glob, $spike_threshold

WITH grid AS (
    SELECT region, interval_start_utc
    FROM read_parquet($price_glob)
),

periods AS (
    SELECT
        predispatchseqno AS run_id,
        regionid AS region,
        datetime AS period_end,
        rrp,
        max(lastchanged) OVER (PARTITION BY predispatchseqno) AS issued_at
    FROM read_parquet($predispatch_glob)
    WHERE intervention = 0
    QUALIFY row_number() OVER (
        PARTITION BY predispatchseqno, regionid, datetime
        ORDER BY ingested_at DESC, source_file DESC
    ) = 1
),

runs AS (
    SELECT region, run_id, any_value(issued_at) AS issued_at, max(period_end) AS last_end,
           arg_max(rrp, period_end) AS last_rrp
    FROM periods
    GROUP BY region, run_id
),

admissible AS (
    SELECT g.region, g.interval_start_utc, r.run_id, r.last_end, r.last_rrp
    FROM grid g
    ASOF JOIN runs r
        ON r.region = g.region
       AND r.issued_at <= g.interval_start_utc + INTERVAL 5 MINUTE
),

-- The 24 target intervals, by their end time: t+10 .. t+125.
targets AS (
    SELECT a.*, a.interval_start_utc + INTERVAL 5 MINUTE * (k + 2) AS interval_end
    FROM admissible a, range(24) AS steps(k)
),

priced AS (
    SELECT
        t.region,
        t.interval_start_utc,
        t.run_id,
        p.rrp IS NOT NULL AS covered,
        coalesce(p.rrp, t.last_rrp) AS rrp
    FROM targets t
    LEFT JOIN periods p
        ON p.run_id = t.run_id
       AND p.region = t.region
       AND p.period_end = time_bucket(INTERVAL 30 MINUTE, t.interval_end - INTERVAL 5 MINUTE)
                          + INTERVAL 30 MINUTE
)

SELECT
    region,
    interval_start_utc,
    any_value(run_id) AS pd_run_id,
    avg(rrp) AS pd_mean_120,
    max(CASE WHEN rrp > $spike_threshold THEN 1.0 ELSE 0.0 END) AS pd_spike_120,
    count(*) FILTER (WHERE covered) AS pd_intervals_120
FROM priced
GROUP BY region, interval_start_utc
-- A run AEMO forecast less than two thirds of is a different quantity from the
-- target, the same reason P5MIN rows short of eleven intervals are dropped. In
-- steady operation this never bites (the minimum is 17); it removes rows where
-- an ingestion gap left only an old run admissible.
HAVING count(*) FILTER (WHERE covered) >= 16
ORDER BY region, interval_start_utc
