-- tests/assert_settlement_date_after_trade_date.sql
-- Custom test: Settlement date must be on or after transaction date

SELECT
    transaction_id,
    transaction_date,
    settlement_date
FROM {{ ref('int_fact_transactions') }}
WHERE settlement_date < transaction_date
