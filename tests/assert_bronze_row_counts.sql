-- tests/assert_bronze_row_counts.sql
-- Custom test: All Bronze source tables must have at least 1 row.
-- Returns a row for each empty table (test fails if any rows returned).

{% set bronze_tables = [
    'raw_fx_rates',
    'raw_portfolio_master',
    'raw_security_prices',
    'raw_security_reference',
    'raw_transactions'
] %}

{% for tbl in bronze_tables %}
SELECT
    '{{ tbl }}' AS table_name,
    COUNT(*)    AS row_count
FROM {{ source('bronze', tbl) }}
HAVING COUNT(*) = 0
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
