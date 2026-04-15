-- models/marts/mart_portfolio_summary.sql
-- Gold: Portfolio-level summary with AUM and trade metrics

{{
    config(
        materialized='table',
        tags=['gold', 'reporting']
    )
}}

WITH portfolios AS (
    SELECT * FROM {{ ref('int_dim_portfolio') }}
    WHERE is_current = TRUE
),

transactions AS (
    SELECT * FROM {{ ref('int_fact_transactions') }}
)

SELECT
    p.portfolio_id,
    p.portfolio_name,
    p.portfolio_type,
    p.strategy,
    p.base_currency,
    p.portfolio_manager,
    p.risk_profile,
    p.benchmark_index,
    p.inception_date,
    COUNT(DISTINCT t.security_id)          AS num_securities,
    COUNT(DISTINCT t.transaction_id)       AS total_trades,
    SUM(t.quantity)                         AS total_shares,
    SUM(t.transaction_amount)              AS total_invested_local,
    SUM(t.amount_usd)                      AS total_invested_usd,
    ROUND(AVG(t.price_usd), 2)             AS avg_price_usd,
    MIN(t.transaction_date)                AS first_trade_date,
    MAX(t.transaction_date)                AS last_trade_date,
    COUNT(DISTINCT t.broker)               AS num_brokers,
    ROUND(AVG(t.settlement_days), 1)       AS avg_settlement_days
FROM portfolios p
LEFT JOIN transactions t ON p.portfolio_id = t.portfolio_id
GROUP BY ALL
