-- Silver layer: clean, typed, deduplicated price records
-- Stored as a real BigQuery table, partitioned by date, clustered by pair
WITH deduped AS (
    SELECT
        pair,
        price,
        volume_24h,
        high_24h,
        low_24h,
        open_price,
        trade_count,
        ingested_at,
        event_date,
        -- Row number for deduplication
        ROW_NUMBER() OVER (
            PARTITION BY pair, ingested_at
            ORDER BY ingested_at
        ) AS row_num
    FROM {{ ref('stg_bronze_raw') }}
)

SELECT
    pair,
    price,
    volume_24h,
    high_24h,
    low_24h,
    open_price,
    trade_count,
    ingested_at,
    event_date,
    -- How many seconds since last update for this pair
    TIMESTAMP_DIFF(
        ingested_at,
        LAG(ingested_at) OVER (
            PARTITION BY pair ORDER BY ingested_at
        ),
        SECOND
    ) AS seconds_since_last_tick
FROM deduped
WHERE row_num = 1  -- remove duplicates