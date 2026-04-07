{{
    config(
        materialized='incremental',
        unique_key=['ingested_at', 'pair'],
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        cluster_by=['pair']
    )
}}

WITH raw_data AS (
    SELECT
        pair,
        price,
        last_trade_volume,
        volume_today,
        volume_24h,
        vwap_today,
        vwap_24h,
        trades_today,
        trades_24h,
        low_today,
        low_24h,
        high_today,
        high_24h,
        open_today,
        ask_price,
        bid_price,
        bid_ask_spread,
        ingested_at,
        event_date
    FROM {{ ref('stg_bronze_raw') }}

    {% if is_incremental() %}
        -- Only pull data newer than the latest record in the target table
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pair, ingested_at 
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM raw_data
),

final_processing AS (
    SELECT
        * EXCEPT(row_num),
        -- Correctly calculating lag within the current batch
        LAG(ingested_at) OVER (
            PARTITION BY pair ORDER BY ingested_at
        ) AS previous_ingested_at
    FROM deduped
    WHERE row_num = 1
)

SELECT
    *,
    TIMESTAMP_DIFF(ingested_at, previous_ingested_at, SECOND) AS seconds_since_last_tick
FROM final_processing