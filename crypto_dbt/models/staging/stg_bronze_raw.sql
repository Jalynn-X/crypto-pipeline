-- Flatten raw JSON from bronze external table into typed columns
-- This is the ONLY place we touch the raw JSON structure
-- All downstream models use this staging view
SELECT
    pair,
    -- Current price (last trade price)
    CAST(JSON_VALUE(data, '$.c[0]')  AS FLOAT64)  AS price,
    -- 24h volume
    CAST(JSON_VALUE(data, '$.v[0]')  AS FLOAT64)  AS volume_24h,
    -- 24h high
    CAST(JSON_VALUE(data, '$.h[0]')  AS FLOAT64)  AS high_24h,
    -- 24h low
    CAST(JSON_VALUE(data, '$.l[0]')  AS FLOAT64)  AS low_24h,
    -- opening price
    CAST(JSON_VALUE(data, '$.o')     AS FLOAT64)  AS open_price,
    -- number of trades
    CAST(JSON_VALUE(data, '$.t[0]')  AS INT64)    AS trade_count,
    -- ingestion timestamp
    CAST(ingestion_time              AS TIMESTAMP) AS ingested_at,
    -- date partition column
    DATE(ingestion_time)                           AS event_date
FROM {{ source('crypto', 'bronze_raw') }}
WHERE pair IS NOT NULL
  AND data IS NOT NULL
  AND ingestion_time IS NOT NULL