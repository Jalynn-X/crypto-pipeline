{{ config(materialized='view') }}

SELECT
    pair,
    -- Last trade price (this is your "current price" / close price)
    CAST(JSON_VALUE(data, '$.c[0]') AS FLOAT64)    AS price,
    CAST(JSON_VALUE(data, '$.c[1]') AS FLOAT64)    AS last_trade_volume,
    -- Volume
    CAST(JSON_VALUE(data, '$.v[0]') AS FLOAT64)    AS volume_today,
    CAST(JSON_VALUE(data, '$.v[1]') AS FLOAT64)    AS volume_24h,
    -- VWAP (volume weighted average price)
    CAST(JSON_VALUE(data, '$.p[0]') AS FLOAT64)    AS vwap_today,
    CAST(JSON_VALUE(data, '$.p[1]') AS FLOAT64)    AS vwap_24h,
    -- Trade count
    CAST(JSON_VALUE(data, '$.t[0]') AS INT64)      AS trades_today,
    CAST(JSON_VALUE(data, '$.t[1]') AS INT64)      AS trades_24h,
    -- Price range today
    CAST(JSON_VALUE(data, '$.l[0]') AS FLOAT64)    AS low_today,
    CAST(JSON_VALUE(data, '$.l[1]') AS FLOAT64)    AS low_24h,
    CAST(JSON_VALUE(data, '$.h[0]') AS FLOAT64)    AS high_today,
    CAST(JSON_VALUE(data, '$.h[1]') AS FLOAT64)    AS high_24h,
    -- Open price (today midnight UTC)
    CAST(JSON_VALUE(data, '$.o')    AS FLOAT64)    AS open_today,
    -- Ask and bid
    CAST(JSON_VALUE(data, '$.a[0]') AS FLOAT64)    AS ask_price,
    CAST(JSON_VALUE(data, '$.a[1]') AS INT64)      AS ask_whole_lot_volume,
    CAST(JSON_VALUE(data, '$.b[0]') AS FLOAT64)    AS bid_price,
    CAST(JSON_VALUE(data, '$.b[1]') AS INT64)      AS bid_whole_lot_volume,
    -- Derived: bid-ask spread (market liquidity indicator)
    ROUND(
        CAST(JSON_VALUE(data, '$.a[0]') AS FLOAT64) -
        CAST(JSON_VALUE(data, '$.b[0]') AS FLOAT64),
        4
    )                                              AS bid_ask_spread,
    -- Timestamps
    CAST(ingestion_time AS TIMESTAMP)              AS ingested_at,
    DATE(ingestion_time)                           AS event_date
FROM {{ source('crypto', 'bronze_raw') }}
WHERE pair IS NOT NULL
  AND data IS NOT NULL
  AND ingestion_time IS NOT NULL