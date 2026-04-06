WITH SOURCE AS (
    SELECT * FROM {{ source('finance_raw', 'raw_macro')}}
),

DEDUPED AS (
    SELECT
        DATE,
        INDICATOR,
        VALUE,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY INDICATOR, DATE
            ORDER BY INGESTED_AT DESC
        ) AS ROW_NUM
    FROM SOURCE      

),

FINAL AS (
    SELECT
        CAST(DATE AS TIMESTAMP) AS DATE,
        CAST(INDICATOR AS STRING) AS INDICATOR,
        CAST(VALUE AS FLOAT64) AS VALUE,
        INGESTED_AT
    FROM DEDUPED
    where ROW_NUM= 1 
    AND VALUE IS NOT NULL -- remove historic NULL values

)

SELECT
    *
FROM FINAL