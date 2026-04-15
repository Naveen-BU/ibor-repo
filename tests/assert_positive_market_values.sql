-- tests/assert_positive_market_values.sql
-- Custom test: All position market values should be positive

SELECT
    portfolio_id,
    security_id,
    market_value_usd
FROM {{ ref('mart_position_valuation') }}
WHERE market_value_usd IS NOT NULL
  AND market_value_usd < 0
