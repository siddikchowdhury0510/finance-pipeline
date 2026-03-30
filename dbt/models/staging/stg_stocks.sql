--pulls from raw BigQuery table
WITH SOURCE AS (
    SELECT * FROM {{ source('finance_raw', 'raw_stocks') }}
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
        ADJ_CLOSE,
        ADJ_OPEN,
        ADJ_HIGH,
        ADJ_LOW,
        DIV_CASH,
        SPLIT_FACTOR,
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
        CAST(VOLUME AS INT64) AS VOLUME,
        CAST(ADJ_CLOSE AS FLOAT64) AS ADJ_CLOSE,
        CAST(ADJ_OPEN AS FLOAT64) AS ADJ_OPEN,
        CAST(ADJ_HIGH AS FLOAT64) AS ADJ_HIGH,
        CAST(ADJ_LOW AS FLOAT64) AS ADJ_LOW,
        CAST(DIV_CASH AS FLOAT64) AS DIV_CASH,
        CAST(SPLIT_FACTOR AS FLOAT64) AS SPLIT_FACTOR,
        INGESTED_AT 
    FROM DEDUPED
    WHERE ROW_NUM = 1
)

SELECT 
    * 
FROM FINAL