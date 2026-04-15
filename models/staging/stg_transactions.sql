-- models/staging/stg_transactions.sql
-- Staging model: Clean and type-cast transactions

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_transactions') }}
),

cleaned AS (
    SELECT
        TRIM(transaction_id)                         AS transaction_id,
        TRIM(portfolio_id)                           AS portfolio_id,
        TRIM(security_id)                            AS security_id,
        TRY_TO_DATE(transaction_date, 'YYYY-MM-DD')  AS transaction_date,
        TRY_TO_DATE(settlement_date, 'YYYY-MM-DD')   AS settlement_date,
        UPPER(TRIM(transaction_type))                AS transaction_type,
        TRY_TO_NUMBER(quantity, 18, 4)                AS quantity,
        TRY_TO_NUMBER(price, 18, 6)                   AS price,
        TRY_TO_NUMBER(transaction_amount, 18, 4)      AS transaction_amount,
        UPPER(TRIM(currency))                        AS currency,
        TRIM(broker)                                 AS broker,
        TRIM(trader_id)                              AS trader_id,
        UPPER(TRIM(status))                          AS status,
        TRY_TO_TIMESTAMP_NTZ(created_at)              AS created_at,
        TRY_TO_TIMESTAMP_NTZ(updated_at)              AS updated_at,
        _loaded_at,
        _source_file
    FROM source
    WHERE transaction_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _loaded_at DESC) = 1
)

SELECT
    *,
    DATEDIFF('day', transaction_date, settlement_date) AS settlement_days
FROM cleaned
