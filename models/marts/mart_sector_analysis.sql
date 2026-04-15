-- models/marts/mart_sector_analysis.sql
-- Gold: Sector allocation and performance analysis

{{
    config(
        materialized='table',
        tags=['gold', 'reporting']
    )
}}

WITH positions AS (
    SELECT * FROM {{ ref('mart_position_valuation') }}
)

SELECT
    portfolio_id,
    portfolio_name,
    sector,
    asset_class,
    COUNT(DISTINCT security_id)                           AS num_holdings,
    SUM(net_quantity)                                     AS total_shares,
    SUM(market_value_usd)                                 AS sector_mv_usd,
    ROUND(
        SUM(market_value_usd) /
        NULLIF(SUM(SUM(market_value_usd)) OVER (PARTITION BY portfolio_id), 0) * 100, 2
    )                                                     AS sector_weight_pct,
    SUM(unrealized_pnl_usd)                               AS sector_pnl_usd,
    ROUND(AVG(return_pct), 2)                             AS avg_return_pct
FROM positions
GROUP BY portfolio_id, portfolio_name, sector, asset_class
