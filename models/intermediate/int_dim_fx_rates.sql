-- models/intermediate/int_dim_fx_rates.sql
-- Silver dimension: FX rates with inverse rates

{{
    config(
        materialized='table',
        unique_key=['from_currency', 'to_currency', 'rate_date']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['rate_id']) }} AS fx_sk,
    rate_id,
    from_currency,
    to_currency,
    rate_date,
    exchange_rate,
    inverse_rate,
    rate_source AS source,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ ref('stg_fx_rates') }}
