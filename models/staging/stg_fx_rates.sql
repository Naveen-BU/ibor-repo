-- models/staging/stg_fx_rates.sql
-- Staging model: Clean and type-cast raw FX rates

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_fx_rates') }}
),

cleaned AS (
    SELECT
        TRIM(rate_id)                               AS rate_id,
        UPPER(TRIM(from_currency))                  AS from_currency,
        UPPER(TRIM(to_currency))                    AS to_currency,
        TRY_TO_DATE(rate_date, 'YYYY-MM-DD')        AS rate_date,
        TRY_TO_NUMBER(exchange_rate, 18, 6)          AS exchange_rate,
        TRIM(source)                                AS rate_source,
        _loaded_at,
        _source_file
    FROM source
    WHERE rate_id IS NOT NULL
      AND TRY_TO_NUMBER(exchange_rate, 18, 6) IS NOT NULL
      AND TRY_TO_NUMBER(exchange_rate, 18, 6) > 0
)

SELECT
    *,
    ROUND(1.0 / exchange_rate, 6)                   AS inverse_rate
FROM cleaned
