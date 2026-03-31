-- Gold layer: 5-minute OHLC candles per pair
-- Standard candlestick format for trading dashboards
WITH windowed AS (
    SELECT
        pair,
        -- Bucket each tick into a 5-minute window
        TIMESTAMP_TRUNC(ingested_at, MINUTE) AS minute_bucket,
        price,
        volume_24h,
        ingested_at,
        event_date,
        -- Identify first and last tick in each 5-min window
        ROW_NUMBER() OVER (
            PARTITION BY pair, TIMESTAMP_TRUNC(ingested_at, MINUTE)
            ORDER BY ingested_at ASC
        ) AS tick_asc,
        ROW_NUMBER() OVER (
            PARTITION BY pair, TIMESTAMP_TRUNC(ingested_at, MINUTE)
            ORDER BY ingested_at DESC
        ) AS tick_desc
    FROM {{ ref('silver_prices') }}
)

SELECT
    pair,
    minute_bucket                                AS window_start,
    TIMESTAMP_ADD(minute_bucket, INTERVAL 5 MINUTE) AS window_end,
    MAX(CASE WHEN tick_asc  = 1 THEN price END)  AS open,
    MAX(price)                                   AS high,
    MIN(price)                                   AS low,
    MAX(CASE WHEN tick_desc = 1 THEN price END)  AS close,
    SUM(volume_24h)                              AS total_volume,
    COUNT(*)                                     AS tick_count,
    event_date
FROM windowed
GROUP BY pair, minute_bucket, event_date