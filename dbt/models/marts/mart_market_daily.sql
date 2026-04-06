WITH

MARKET_DAILY AS (
    SELECT
        DATE,
        TICKER,
        CURRENCY,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        ASSET_TYPE
    FROM {{ ref('int_market_daily')}}
),

FINAL AS (
    SELECT
        DATE,
        TICKER,
        ASSET_TYPE,
        CURRENCY,
        ROUND(OPEN, 2) AS OPEN,
        ROUND(CLOSE,2) AS CLOSE,
        ROUND((CLOSE - OPEN), 2) AS PRICE_CHANGE,
        ROUND((CLOSE-OPEN) / OPEN * 100, 2) AS PRICE_CHANGE_PCT,
        ROUND(HIGH, 2) AS HIGH,
        ROUND(LOW,2) AS LOW,
        ROUND((HIGH - LOW), 2) AS DAILY_RANGE,
        ROUND(VOLUME/ 1000000, 2) AS VOLUME_M,
        CURRENT_TIMESTAMP() AS LAST_UPDATED
    FROM MARKET_DAILY

)

SELECT
*
FROM FINAL