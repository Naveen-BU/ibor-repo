-- models/marts/mart_position_valuation.sql
-- Gold: Current holdings valued at latest prices with P&L

{{
    config(
        materialized='table',
        tags=['gold', 'reporting']
    )
}}

WITH holdings AS (
    SELECT
        t.portfolio_id,
        t.security_id,
        t.currency,
        SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.quantity ELSE -t.quantity END) AS net_quantity,
        SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.transaction_amount ELSE -t.transaction_amount END) AS cost_basis_local,
        SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.amount_usd ELSE -t.amount_usd END) AS cost_basis_usd
    FROM {{ ref('int_fact_transactions') }} t
    WHERE t.status = 'SETTLED'
    GROUP BY t.portfolio_id, t.security_id, t.currency
    HAVING net_quantity > 0
),

latest_prices AS (
    SELECT security_id, close_price, close_price_usd, price_date, currency
    FROM {{ ref('int_fact_security_prices') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY price_date DESC) = 1
),

portfolios AS (
    SELECT * FROM {{ ref('int_dim_portfolio') }} WHERE is_current = TRUE
),

securities AS (
    SELECT * FROM {{ ref('int_dim_security') }} WHERE is_current = TRUE
)

SELECT
    h.portfolio_id,
    p.portfolio_name,
    p.portfolio_type,
    h.security_id,
    s.ticker,
    s.security_name,
    s.asset_class,
    s.sector,
    s.country_code,
    h.currency                                            AS holding_currency,
    h.net_quantity,
    h.cost_basis_local,
    h.cost_basis_usd,
    lp.close_price                                        AS current_price,
    lp.close_price_usd                                    AS current_price_usd,
    lp.price_date                                         AS price_as_of,
    ROUND(h.net_quantity * lp.close_price, 2)             AS market_value_local,
    ROUND(h.net_quantity * lp.close_price_usd, 2)         AS market_value_usd,
    ROUND(h.net_quantity * lp.close_price_usd - h.cost_basis_usd, 2) AS unrealized_pnl_usd,
    CASE
        WHEN h.cost_basis_usd > 0
        THEN ROUND((h.net_quantity * lp.close_price_usd - h.cost_basis_usd) / h.cost_basis_usd * 100, 2)
        ELSE 0
    END                                                   AS return_pct
FROM holdings h
JOIN portfolios p ON h.portfolio_id = p.portfolio_id
JOIN securities s ON h.security_id = s.security_id
LEFT JOIN latest_prices lp ON h.security_id = lp.security_id
