WITH

MACRO AS (
    SELECT
        DATE,
        INDICATOR,
        VALUE
    FROM {{ ref('stg_macro')}}
    
),

FINAL AS (
    SELECT
        DATE,
        UPPER(INDICATOR) AS INDICATOR,
        ROUND(VALUE, 2) AS VALUE,
        CURRENT_TIMESTAMP() AS LAST_UPDATED
    FROM MACRO
)

SELECT
*
FROM FINAL