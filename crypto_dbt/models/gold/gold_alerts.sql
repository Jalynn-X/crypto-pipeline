-- Gold layer: price alerts based on 5-min % change
-- Thresholds defined here — easy to tune without touching Flink
{% set drop_threshold   = -2.0 %}
{% set spike_threshold  =  2.0 %}

WITH price_changes AS (
    SELECT
        pair,
        price,
        ingested_at,
        event_date,
        -- Compare to price from ~5 ticks ago (approx 5 min at 1 tick/min)
        LAG(price, 5) OVER (
            PARTITION BY pair
            ORDER BY ingested_at
        ) AS price_5min_ago
    FROM {{ ref('silver_prices') }}
),

with_pct AS (
    SELECT
        pair,
        ingested_at                                   AS alert_time,
        price                                         AS current_price,
        price_5min_ago,
        ROUND((price / price_5min_ago - 1) * 100, 4) AS percent_change,
        event_date
    FROM price_changes
    WHERE price_5min_ago IS NOT NULL
      AND price_5min_ago > 0
)

SELECT
    pair,
    alert_time,
    current_price,
    price_5min_ago,
    percent_change,
    CASE
        WHEN percent_change <= {{ drop_threshold }}  THEN 'DROP'
        WHEN percent_change >= {{ spike_threshold }} THEN 'SPIKE'
    END AS alert_type,
    event_date
FROM with_pct
WHERE percent_change <= {{ drop_threshold }}
   OR percent_change >= {{ spike_threshold }}