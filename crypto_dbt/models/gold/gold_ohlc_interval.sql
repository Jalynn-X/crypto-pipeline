{{
    config(
        materialized='incremental',
        unique_key=['pair', 'window_start'],
        incremental_strategy='merge',
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        cluster_by=['pair']
    )
}}

{% set interval_minutes = 30 %}

WITH source AS (
    SELECT *
    FROM {{ ref('silver_prices') }}

    {% if is_incremental() %}
        WHERE ingested_at >= (
            SELECT DATETIME_SUB(
                COALESCE(MAX(window_start), '1900-01-01'),
                -- Look back twice the interval to capture lagging ticks
                INTERVAL {{ interval_minutes * 2 }} MINUTE
            )
            FROM {{ this }}
        )
    {% endif %}
),

bucketed AS (
    SELECT
        *,
        -- Explicitly truncating to the 30-min start
        TIMESTAMP_SECONDS(
            DIV(UNIX_SECONDS(CAST(ingested_at AS TIMESTAMP)), {{ interval_minutes * 60 }}) * {{ interval_minutes * 60 }}
        ) AS time_bucket
    FROM source
),

windowed AS (
    SELECT
        pair,
        time_bucket,
        price,
        volume_24h,
        ingested_at,
        event_date,
        ROW_NUMBER() OVER (
            PARTITION BY pair, time_bucket
            ORDER BY ingested_at ASC
        ) AS tick_asc,
        ROW_NUMBER() OVER (
            PARTITION BY pair, time_bucket
            ORDER BY ingested_at DESC
        ) AS tick_desc
    FROM bucketed
),

final_metrics AS (
    SELECT
        pair,
        time_bucket                                             AS window_start,
        TIMESTAMP_ADD(
            time_bucket, INTERVAL {{ interval_minutes }} MINUTE
        )                                                       AS window_end,
        MAX(CASE WHEN tick_asc  = 1 THEN price END)            AS open_price,
        MAX(price)                                             AS high_price,
        MIN(price)                                             AS low_price,
        MAX(CASE WHEN tick_desc = 1 THEN price END)            AS close_price,
        MAX(CASE WHEN tick_desc = 1 THEN volume_24h END)       AS volume_at_close,
        COUNT(*)                                               AS tick_count,
        event_date
    FROM windowed
    GROUP BY pair, time_bucket, event_date
)

SELECT
    *,
    -- Use the aliases from final_metrics for cleaner calculations
    ROUND((close_price / NULLIF(open_price, 0) - 1) * 100, 4) AS pct_change,
    CASE
        WHEN close_price > open_price THEN 'UP'
        WHEN close_price < open_price THEN 'DOWN'
        ELSE 'FLAT'
    END AS direction
FROM final_metrics