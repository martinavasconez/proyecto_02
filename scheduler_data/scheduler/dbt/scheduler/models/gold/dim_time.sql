{{ config(materialized='table') }}

WITH times AS (
    SELECT DISTINCT CAST(pickup_datetime AS TIME) AS time_of_day
    FROM {{ ref('stg_taxi_trips') }}
    UNION
    SELECT DISTINCT CAST(dropoff_datetime AS TIME)
    FROM {{ ref('stg_taxi_trips') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY time_of_day) AS time_sk,
    time_of_day,
    EXTRACT(HOUR FROM time_of_day) AS hour,
    CASE 
        WHEN EXTRACT(HOUR FROM time_of_day) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM time_of_day) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM time_of_day) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_period,
    CASE 
        WHEN EXTRACT(HOUR FROM time_of_day) BETWEEN 6 AND 18 THEN 'Day'
        ELSE 'Night' 
    END AS day_night
FROM times