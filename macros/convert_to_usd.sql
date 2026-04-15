-- macros/convert_to_usd.sql
-- Reusable macro for currency conversion to USD

{% macro convert_to_usd(amount_column, currency_column, date_column) %}
    CASE
        WHEN {{ currency_column }} = 'USD' THEN {{ amount_column }}
        ELSE {{ amount_column }} * COALESCE(
            (SELECT fx.exchange_rate
             FROM {{ ref('int_dim_fx_rates') }} fx
             WHERE fx.from_currency = {{ currency_column }}
               AND fx.to_currency = 'USD'
               AND fx.rate_date = {{ date_column }}
             LIMIT 1),
            1
        )
    END
{% endmacro %}
