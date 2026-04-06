
WITH 

CRYPTO AS (
    SELECT 
        TICKER,
        DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        'CRYPTO' AS ASSET_TYPE,
        QUOTE_CURRENCY AS CURRENCY,
        INGESTED_AT
    FROM {{ref('stg_crypto') }}
),

STOCKS AS (
    SELECT 
        TICKER,
        DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        'STOCKS' AS ASSET_TYPE, 
        'USD' AS CURRENCY,
        INGESTED_AT
    FROM {{ref ('stg_stocks') }}
),

FINAL AS (
    SELECT 
        DATE,
        TICKER,
        CURRENCY,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        ASSET_TYPE, 
        INGESTED_AT
    FROM CRYPTO

    UNION ALL

    SELECT 
        DATE,
        TICKER,
        CURRENCY,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        ASSET_TYPE, 
        INGESTED_AT
    FROM STOCKS
)

SELECT
    *
FROM FINAL