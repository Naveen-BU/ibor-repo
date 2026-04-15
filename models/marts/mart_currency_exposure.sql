-- models/marts/mart_currency_exposure.sql
-- Gold: Multi-currency risk exposure per portfolio

{{
    config(
        materialized='table',
        tags=['gold', 'risk']
    )
}}

WITH positions AS (
    SELECT * FROM {{ ref('mart_position_valuation') }}
)

SELECT
    portfolio_id,
    portfolio_name,
    holding_currency,
    COUNT(DISTINCT security_id)                           AS num_positions,
    SUM(market_value_local)                               AS total_mv_local,
    SUM(market_value_usd)                                 AS total_mv_usd,
    ROUND(
        SUM(market_value_usd) /
        NULLIF(SUM(SUM(market_value_usd)) OVER (PARTITION BY portfolio_id), 0) * 100, 2
    )                                                     AS pct_of_portfolio,
    SUM(unrealized_pnl_usd)                               AS total_unrealized_pnl_usd,
    ROUND(AVG(return_pct), 2)                             AS avg_return_pct
FROM positions
GROUP BY portfolio_id, portfolio_name, holding_currency
