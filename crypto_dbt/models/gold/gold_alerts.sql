{{
    config(
        materialized='incremental',
        unique_key=['pair', 'alert_time'],
        incremental_strategy='merge',
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        cluster_by=['pair', 'alert_type']
    )
}}

{% set drop_threshold   = -1.0 %}
{% set spike_threshold  =  1.0 %}
{% set lookback_minutes = 30   %}
{% set max_gap_minutes  = 120  %}

WITH silver AS (
    SELECT *
    FROM {{ ref('silver_prices') }}

    {% if is_incremental() %}
        WHERE ingested_at >= (
            SELECT DATETIME_SUB(
                COALESCE(MAX(alert_time), '1900-01-01'),
                INTERVAL {{ lookback_minutes + 5 }} MINUTE
            )
            FROM {{ this }}
        )
    {% endif %}
),

price_with_past AS (
    SELECT
        curr.pair,
        curr.price                                          AS current_price,
        curr.ingested_at,
        curr.event_date,
        past.price                                          AS past_price,
        past.ingested_at                                    AS past_ingested_at,
        TIMESTAMP_DIFF(
            curr.ingested_at,
            past.ingested_at,
            MINUTE
        )                                                   AS minutes_apart
    FROM silver curr                                        -- 👈 renamed current → curr
    LEFT JOIN silver past
        ON  curr.pair = past.pair
        AND past.ingested_at BETWEEN
            TIMESTAMP_SUB(
                curr.ingested_at,
                INTERVAL {{ lookback_minutes + max_gap_minutes }} MINUTE
            )
            AND
            TIMESTAMP_SUB(
                curr.ingested_at,
                INTERVAL {{ lookback_minutes - 2 }} MINUTE
            )
),

closest_past AS (
    SELECT
        pair,
        current_price,
        ingested_at,
        event_date,
        past_price,
        past_ingested_at,
        minutes_apart,
        ROW_NUMBER() OVER (
            PARTITION BY pair, ingested_at
            ORDER BY ABS(minutes_apart - {{ lookback_minutes }}) ASC
        ) AS rank
    FROM price_with_past
    WHERE past_price IS NOT NULL
),

price_changes AS (
    SELECT
        pair,
        ingested_at                                         AS alert_time,
        current_price,
        past_price                                          AS reference_price,
        past_ingested_at                                    AS reference_time,
        minutes_apart                                       AS actual_lookback_minutes,
        ROUND(
            (current_price / NULLIF(past_price, 0) - 1) * 100,
            4
        )                                                   AS percent_change,
        event_date,
        CASE
            WHEN minutes_apart > {{ lookback_minutes + 5 }}
            THEN TRUE ELSE FALSE
        END                                                 AS is_gap_comparison
    FROM closest_past
    WHERE rank = 1
)

SELECT
    pair,
    alert_time,
    current_price,
    reference_price,
    reference_time,
    actual_lookback_minutes,
    percent_change,
    CASE
        WHEN percent_change <= {{ drop_threshold }}  THEN 'DROP'
        WHEN percent_change >= {{ spike_threshold }} THEN 'SPIKE'
    END                                                     AS alert_type,
    is_gap_comparison,
    event_date
FROM price_changes
WHERE percent_change <= {{ drop_threshold }}
   OR percent_change >= {{ spike_threshold }}
