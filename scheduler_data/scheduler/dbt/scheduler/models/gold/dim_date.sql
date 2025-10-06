{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_sk,
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DOW FROM full_date) AS day_of_week,
    CASE EXTRACT(DOW FROM full_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name
FROM (
    SELECT DISTINCT DATE(pickup_datetime) AS full_date
    FROM {{ ref('stg_taxi_trips') }}
    UNION
    SELECT DISTINCT DATE(dropoff_datetime)
    FROM {{ ref('stg_taxi_trips') }}
)