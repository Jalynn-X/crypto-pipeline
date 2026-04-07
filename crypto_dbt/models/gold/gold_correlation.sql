
WITH btc AS (
    SELECT ingested_at, price AS btc_price
    FROM {{ ref('silver_prices') }}
    WHERE pair = 'XBTUSD'
),
eth AS (
    SELECT ingested_at, price AS eth_price
    FROM {{ ref('silver_prices') }}
    WHERE pair = 'ETHUSD'
)

SELECT
    b.ingested_at,
    b.btc_price,
    e.eth_price,
    -- Normalized prices (index to 100 at start)
    b.btc_price / FIRST_VALUE(b.btc_price) OVER (
        ORDER BY b.ingested_at
    ) * 100                                 AS btc_indexed,
    e.eth_price / FIRST_VALUE(e.eth_price) OVER (
        ORDER BY e.ingested_at
    ) * 100                                 AS eth_indexed,
    DATE(b.ingested_at)                     AS event_date
FROM btc b
JOIN eth e
    ON TIMESTAMP_DIFF(b.ingested_at, e.ingested_at, SECOND) 
       BETWEEN -30 AND 30