-- models/intermediate/int_fact_transactions.sql
-- Silver fact: Enriched transactions with USD conversion

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

fx_rates AS (
    SELECT * FROM {{ ref('int_dim_fx_rates') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} AS transaction_sk,
    t.transaction_id,
    t.portfolio_id,
    t.security_id,
    t.transaction_date,
    t.settlement_date,
    t.transaction_type,
    t.quantity,
    t.price,
    t.transaction_amount,
    t.currency,
    -- USD conversion
    CASE
        WHEN t.currency = 'USD' THEN t.transaction_amount
        ELSE t.transaction_amount * COALESCE(fx.exchange_rate, 1)
    END AS amount_usd,
    CASE
        WHEN t.currency = 'USD' THEN t.price
        ELSE t.price * COALESCE(fx.exchange_rate, 1)
    END AS price_usd,
    COALESCE(fx.exchange_rate, 1) AS fx_rate_to_usd,
    t.broker,
    t.trader_id,
    t.status,
    t.settlement_days,
    t.created_at,
    t.updated_at,
    TO_NUMBER(TO_CHAR(t.transaction_date, 'YYYYMMDD')) AS transaction_date_key,
    TO_NUMBER(TO_CHAR(t.settlement_date, 'YYYYMMDD')) AS settlement_date_key,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM transactions t
LEFT JOIN fx_rates fx
    ON t.currency = fx.from_currency
    AND fx.to_currency = 'USD'
    AND fx.rate_date = t.transaction_date

{% if is_incremental() %}
WHERE t._loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
