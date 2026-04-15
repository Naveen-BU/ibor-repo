-- models/marts/mart_trading_activity.sql
-- Gold: Broker analysis and trade flow metrics

{{
    config(
        materialized='table',
        tags=['gold', 'reporting']
    )
}}

SELECT
    t.broker,
    t.transaction_type,
    t.currency,
    YEAR(t.transaction_date)                              AS trade_year,
    MONTH(t.transaction_date)                             AS trade_month,
    MONTHNAME(t.transaction_date)                         AS month_name,
    COUNT(*)                                              AS trade_count,
    SUM(t.quantity)                                       AS total_quantity,
    SUM(t.amount_usd)                                    AS total_amount_usd,
    ROUND(AVG(t.amount_usd), 2)                          AS avg_trade_size_usd,
    ROUND(AVG(t.settlement_days), 1)                     AS avg_settlement_days,
    COUNT(CASE WHEN t.status = 'SETTLED' THEN 1 END)     AS settled_count,
    COUNT(CASE WHEN t.status != 'SETTLED' THEN 1 END)    AS pending_count
FROM {{ ref('int_fact_transactions') }} t
GROUP BY ALL
