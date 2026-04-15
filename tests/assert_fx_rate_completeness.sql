-- tests/assert_fx_rate_completeness.sql
-- Custom test: Each currency pair should have a valid rate

SELECT
    t.transaction_id,
    t.currency,
    t.transaction_date,
    t.fx_rate_to_usd
FROM {{ ref('int_fact_transactions') }} t
WHERE t.currency != 'USD'
  AND (t.fx_rate_to_usd IS NULL OR t.fx_rate_to_usd = 1)