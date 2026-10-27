-- HARD: daily capacity factor must lie within [-0.01, 1.05].
--
-- Capacity factor is daily MWh divided by what the farm could have produced
-- flat out. Materially below zero means a farm consumed more than it generated
-- across a whole day, which is not a curtailment story but a sign the registry
-- capacity or the MW sign convention is wrong. Above 1.05 means output exceeded
-- nameplate all day, so the registered capacity is almost certainly stale.
-- Both are registry faults rather than data faults, and both corrupt every
-- comparison between farms.
--
-- The floor is not zero, because an idle farm legitimately draws auxiliary
-- power and the resulting capacity factor is negative by a rounding error's
-- worth. See CF_MIN in check_quality.py for the farm-day that set it. Failing
-- the run on -0.000427 would be the pipeline CLAUDE.md warns about: one that
-- rejects genuinely strange but real NEM values until nobody trusts it.
--
-- Computed from silver here. When gold's farm_daily exists this should read
-- that table instead, so the published figure is the one being checked.
--
-- Parameters: $wind_glob, $cf_min, $cf_max

WITH per_farm_day AS (
    SELECT
        trading_date,
        duid,
        any_value(station_name) AS station_name,
        any_value(capacity_mw) AS capacity_mw,
        -- Five-minute readings in MW: twelve of them make an MWh.
        sum(mw) / 12.0 AS mwh,
        count(DISTINCT interval_start_utc) AS intervals
    FROM read_parquet($wind_glob)
    WHERE capacity_mw IS NOT NULL AND capacity_mw > 0
    GROUP BY trading_date, duid
),

with_factor AS (
    SELECT
        *,
        -- Scaled by the intervals actually present, so a sealed part-day is
        -- not reported as a low capacity factor for a reason that is not real.
        mwh / (capacity_mw * (intervals * 5.0 / 60.0)) AS capacity_factor
    FROM per_farm_day
    WHERE intervals > 0
)

SELECT
    trading_date,
    duid AS subject,
    capacity_factor AS observed,
    'capacity factor ' || round(capacity_factor, 3) || ' for ' || station_name
        || ' (' || round(mwh, 1) || ' MWh against ' || capacity_mw || ' MW)' AS message
FROM with_factor
WHERE capacity_factor < $cf_min OR capacity_factor > $cf_max
ORDER BY trading_date, duid
