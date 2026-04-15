-- models/marts/mart_risk_metrics.sql
-- Gold: Portfolio-level risk metrics

{{
    config(
        materialized='table',
        tags=['gold', 'risk']
    )
}}

WITH trade_stats AS (
    SELECT
        portfolio_id,
        COUNT(DISTINCT transaction_date) AS trading_days,
        STDDEV(amount_usd)               AS volatility_amount,
        AVG(amount_usd)                  AS avg_daily_amount,
        MAX(amount_usd)                  AS max_daily_amount,
        MIN(amount_usd)                  AS min_daily_amount
    FROM {{ ref('int_fact_transactions') }}
    GROUP BY portfolio_id
),

position_stats AS (
    SELECT
        portfolio_id,
        SUM(market_value_usd)            AS total_aum_usd,
        COUNT(*)                         AS total_positions,
        SUM(unrealized_pnl_usd)          AS total_unrealized_pnl,
        MAX(market_value_usd) /
            NULLIF(SUM(market_value_usd), 0) * 100 AS top_holding_pct,
        COUNT(DISTINCT holding_currency) AS num_currencies
    FROM {{ ref('mart_position_valuation') }}
    GROUP BY portfolio_id
),

portfolios AS (
    SELECT * FROM {{ ref('int_dim_portfolio') }} WHERE is_current = TRUE
)

SELECT
    ts.portfolio_id,
    p.portfolio_name,
    p.risk_profile,
    p.strategy,
    ts.trading_days,
    ps.total_aum_usd,
    ps.total_positions,
    ps.total_unrealized_pnl,
    ROUND(ts.volatility_amount, 2)      AS trade_amount_volatility,
    ROUND(ts.avg_daily_amount, 2)       AS avg_trade_size_usd,
    ROUND(ts.max_daily_amount, 2)       AS max_trade_size_usd,
    ROUND(ps.top_holding_pct, 2)        AS top_holding_concentration_pct,
    ps.num_currencies                   AS currency_diversification
FROM trade_stats ts
JOIN portfolios p ON ts.portfolio_id = p.portfolio_id
LEFT JOIN position_stats ps ON ts.portfolio_id = ps.portfolio_id
