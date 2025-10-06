{{ config(materialized='table') }}

SELECT 
    ROW_NUMBER() OVER (ORDER BY service_type) AS service_type_sk,
    service_type,
    CASE service_type
        WHEN 'yellow' THEN 'Yellow Taxi'
        WHEN 'green' THEN 'Green Taxi'
        ELSE 'Other'
    END AS service_type_desc
FROM (
    SELECT DISTINCT service_type
    FROM {{ ref('stg_taxi_trips') }}
    WHERE service_type IS NOT NULL
)