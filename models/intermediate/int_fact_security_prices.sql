-- models/intermediate/int_fact_security_prices.sql
-- Silver fact: Security prices with USD conversion and derived metrics

{{
    config(
        materialized='incremental',
        unique_key=['security_id', 'price_date'],
        incremental_strategy='merge'
    )
}}

WITH prices AS (
    SELECT * FROM {{ ref('stg_security_prices') }}
),

fx_rates AS (
    SELECT * FROM {{ ref('int_dim_fx_rates') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['p.price_id']) }} AS price_sk,
    p.price_id,
    p.security_id,
    p.price_date,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume,
    p.currency,
    -- USD conversion
    CASE
        WHEN p.currency = 'USD' THEN p.close_price
        ELSE p.close_price * COALESCE(fx.exchange_rate, 1)
    END AS close_price_usd,
    COALESCE(fx.exchange_rate, 1) AS fx_rate_to_usd,
    p.daily_range,
    p.daily_return_pct,
    p.price_source AS source,
    TO_NUMBER(TO_CHAR(p.price_date, 'YYYYMMDD')) AS price_date_key,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM prices p
LEFT JOIN fx_rates fx
    ON p.currency = fx.from_currency
    AND fx.to_currency = 'USD'
    AND fx.rate_date = p.price_date

{% if is_incremental() %}
WHERE p._loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
