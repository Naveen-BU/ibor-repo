-- tests/assert_no_orphan_transactions.sql
-- Custom test: Ensure no transactions reference non-existent portfolios or securities

SELECT
    t.transaction_id,
    t.portfolio_id,
    t.security_id,
    'Missing portfolio' AS issue
FROM {{ ref('int_fact_transactions') }} t
LEFT JOIN {{ ref('int_dim_portfolio') }} p ON t.portfolio_id = p.portfolio_id
WHERE p.portfolio_id IS NULL

UNION ALL

SELECT
    t.transaction_id,
    t.portfolio_id,
    t.security_id,
    'Missing security' AS issue
FROM {{ ref('int_fact_transactions') }} t
LEFT JOIN {{ ref('int_dim_security') }} s ON t.security_id = s.security_id
WHERE s.security_id IS NULL