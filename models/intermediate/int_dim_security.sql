-- models/intermediate/int_dim_security.sql
-- Silver dimension: Conformed security master

{{
    config(
        materialized='table',
        unique_key='security_id'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['security_id']) }} AS security_sk,
    security_id,
    isin,
    cusip,
    ticker,
    security_name,
    asset_class,
    currency,
    country_code,
    sector,
    market_cap_category,
    exchange,
    CURRENT_TIMESTAMP()     AS effective_from,
    '9999-12-31'::TIMESTAMP AS effective_to,
    TRUE                    AS is_current,
    CURRENT_TIMESTAMP()     AS _loaded_at
FROM {{ ref('stg_securities') }}
