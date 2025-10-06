{{ config(materialized='table') }}

WITH unique_rates AS (
    SELECT DISTINCT rate_code_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE rate_code_id IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY rate_code_id) AS rate_code_sk,
    rate_code_id,
    CASE rate_code_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Other'
    END AS rate_code_desc
FROM unique_rates