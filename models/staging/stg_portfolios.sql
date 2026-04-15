-- models/staging/stg_portfolios.sql
-- Staging model: Clean portfolio master data

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_portfolio_master') }}
),

cleaned AS (
    SELECT
        TRIM(portfolio_id)                           AS portfolio_id,
        TRIM(portfolio_name)                         AS portfolio_name,
        TRIM(portfolio_type)                         AS portfolio_type,
        TRIM(strategy)                               AS strategy,
        UPPER(TRIM(base_currency))                   AS base_currency,
        TRY_TO_DATE(inception_date, 'YYYY-MM-DD')    AS inception_date,
        TRIM(portfolio_manager)                      AS portfolio_manager,
        UPPER(TRIM(status))                          AS status,
        TRIM(benchmark_index)                        AS benchmark_index,
        TRIM(risk_profile)                           AS risk_profile,
        _loaded_at,
        _source_file
    FROM source
    WHERE portfolio_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY portfolio_id ORDER BY _loaded_at DESC) = 1
)

SELECT * FROM cleaned