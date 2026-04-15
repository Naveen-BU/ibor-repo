-- models/intermediate/int_dim_portfolio.sql
-- Silver dimension: Conformed portfolio master

{{
    config(
        materialized='table',
        unique_key='portfolio_id'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['portfolio_id']) }} AS portfolio_sk,
    portfolio_id,
    portfolio_name,
    portfolio_type,
    strategy,
    base_currency,
    inception_date,
    portfolio_manager,
    status,
    benchmark_index,
    risk_profile,
    CURRENT_TIMESTAMP()     AS effective_from,
    '9999-12-31'::TIMESTAMP AS effective_to,
    TRUE                    AS is_current,
    CURRENT_TIMESTAMP()     AS _loaded_at
FROM {{ ref('stg_portfolios') }}
