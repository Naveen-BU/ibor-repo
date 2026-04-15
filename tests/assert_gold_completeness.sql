-- tests/assert_gold_completeness.sql
-- Custom test: All Gold mart tables must have at least 1 row.
-- Returns a row for each empty mart (test fails if any rows returned).

{% set gold_models = [
    'mart_portfolio_summary',
    'mart_position_valuation',
    'mart_currency_exposure',
    'mart_sector_analysis',
    'mart_risk_metrics',
    'mart_trading_activity'
] %}

{% for model in gold_models %}
SELECT
    '{{ model }}' AS model_name,
    COUNT(*)      AS row_count
FROM {{ ref(model) }}
HAVING COUNT(*) = 0
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
