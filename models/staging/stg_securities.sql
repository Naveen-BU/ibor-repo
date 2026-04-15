-- models/staging/stg_securities.sql
-- Staging model: Clean security reference master

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_security_reference') }}
),

cleaned AS (
    SELECT
        TRIM(security_id)                            AS security_id,
        TRIM(isin)                                   AS isin,
        TRIM(cusip)                                  AS cusip,
        TRIM(ticker)                                 AS ticker,
        TRIM(security_name)                          AS security_name,
        TRIM(asset_class)                            AS asset_class,
        UPPER(TRIM(currency))                        AS currency,
        UPPER(TRIM(country))                         AS country_code,
        TRIM(sector)                                 AS sector,
        TRIM(market_cap_category)                    AS market_cap_category,
        UPPER(TRIM(exchange))                        AS exchange,
        _loaded_at,
        _source_file
    FROM source
    WHERE security_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY _loaded_at DESC) = 1
)

SELECT * FROM cleaned