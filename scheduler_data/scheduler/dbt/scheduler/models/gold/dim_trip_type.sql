{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY trip_type) AS trip_type_sk,
    trip_type,
    CASE trip_type
        WHEN 1 THEN 'Street-hail'
        WHEN 2 THEN 'Dispatch'
        ELSE 'Unknown'
    END AS trip_type_desc
FROM (
    SELECT DISTINCT trip_type
    FROM {{ ref('stg_taxi_trips') }}
    WHERE trip_type IS NOT NULL
)