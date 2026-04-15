-- tests/assert_price_reasonableness.sql
-- Custom test: Security prices must be reasonable.
-- Checks: close_price > 0, close_price <= 1,000,000, high_price >= low_price

SELECT
    price_id,
    security_id,
    price_date,
    close_price,
    high_price,
    low_price,
    CASE
        WHEN close_price <= 0           THEN 'Non-positive close price'
        WHEN close_price > 1000000      THEN 'Close price exceeds 1M'
        WHEN high_price < low_price     THEN 'High price below low price'
    END AS issue
FROM {{ ref('int_fact_security_prices') }}
WHERE close_price <= 0
   OR close_price > 1000000
   OR high_price < low_price
