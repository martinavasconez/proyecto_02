{{ config(materialized='table') }}

WITH unique_payments AS (
    SELECT DISTINCT payment_type_desc
    FROM {{ ref('stg_taxi_trips') }}
    WHERE payment_type_desc IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY payment_type_desc) AS payment_type_sk,
    payment_type_desc
FROM unique_payments