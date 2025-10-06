{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY vendor_id) AS vendor_sk,
    vendor_id,
    CASE vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc'
        ELSE 'Other'
    END AS vendor_name
FROM (
    SELECT DISTINCT vendor_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)