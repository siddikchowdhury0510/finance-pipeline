-- Returns rows where any price column is negative
-- Test passes if zero rows returned

SELECT
    *
FROM {{ ref('stg_crypto') }}
WHERE CLOSE < 0
    OR OPEN < 0
    OR HIGH < 0
    OR LOW < 0