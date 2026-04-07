{{
    config(
        materialized='incremental',
        unique_key=['pair', 'ingested_at'],
        incremental_strategy='merge',
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        cluster_by=['pair']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('silver_prices') }}

    {% if is_incremental() %}
        WHERE ingested_at > (
            SELECT COALESCE(MAX(ingested_at), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    pair,
    ingested_at,
    price,
    vwap_24h,
    ROUND(price - vwap_24h, 4)              AS price_vs_vwap,
    ROUND((price / vwap_24h - 1) * 100, 4) AS pct_from_vwap,
    CASE
        WHEN price > vwap_24h THEN 'ABOVE_VWAP'
        WHEN price < vwap_24h THEN 'BELOW_VWAP'
        ELSE 'AT_VWAP'
    END                                     AS vwap_position,
    event_date
FROM source