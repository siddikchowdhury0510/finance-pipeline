--pulls from raw BigQuery table
WITH SOURCE AS (
    SELECT * FROM {{ source('finance_raw', 'raw_crypto') }}
),

-- uses row number to rank duplicates
DEDUPED AS (
    SELECT
        TICKER,
        DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        BASE_CURRENCY,
        QUOTE_CURRENCY,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY TICKER, DATE 
            ORDER BY INGESTED_AT DESC
        ) AS ROW_NUM 
    FROM SOURCE

),

--casts every column to the correct type and filter to only keep row num = 1, 
--this is the deduplication we needed due to the duplicates in ingestion.
FINAL AS (
    SELECT 
        TICKER,
        CAST(DATE AS TIMESTAMP) AS DATE,
        CAST(OPEN AS FLOAT64) AS OPEN,
        CAST(HIGH AS FLOAT64) AS HIGH,
        CAST(LOW AS FLOAT64) AS LOW,
        CAST(CLOSE AS FLOAT64) AS CLOSE,
        CAST(VOLUME AS FLOAT64) AS VOLUME,
        CAST(QUOTE_CURRENCY AS STRING) AS QUOTE_CURRENCY,
        CAST(BASE_CURRENCY AS STRING) AS BASE_CURRENCY,
        INGESTED_AT
    FROM DEDUPED
    WHERE ROW_NUM = 1
)

SELECT 
    * 
FROM FINAL