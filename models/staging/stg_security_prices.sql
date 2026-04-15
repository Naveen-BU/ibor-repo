-- models/staging/stg_security_prices.sql
-- Staging model: Clean and type-cast security prices

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_security_prices') }}
),

cleaned AS (
    SELECT
        TRIM(price_id)                               AS price_id,
        TRIM(security_id)                            AS security_id,
        TRY_TO_DATE(price_date, 'YYYY-MM-DD')        AS price_date,
        TRY_TO_NUMBER(open_price, 18, 6)              AS open_price,
        TRY_TO_NUMBER(high_price, 18, 6)              AS high_price,
        TRY_TO_NUMBER(low_price, 18, 6)               AS low_price,
        TRY_TO_NUMBER(close_price, 18, 6)             AS close_price,
        TRY_TO_NUMBER(volume, 18, 0)                  AS volume,
        UPPER(TRIM(currency))                        AS currency,
        TRIM(source)                                 AS price_source,
        TRY_TO_TIMESTAMP_NTZ(created_at)              AS created_at,
        _loaded_at,
        _source_file
    FROM source
    WHERE price_id IS NOT NULL
      AND security_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY security_id, price_date ORDER BY _loaded_at DESC) = 1
)

SELECT
    *,
    high_price - low_price                           AS daily_range,
    CASE
        WHEN open_price > 0
        THEN ROUND((close_price - open_price) / open_price * 100, 4)
        ELSE 0
    END                                              AS daily_return_pct
FROM cleaned
