{{ config(materialized='table') }}

SELECT
    zone_id AS zone_sk,
    borough,
    zone,
    COALESCE(borough, 'Unknown') AS borough_clean
FROM {{ ref('stg_taxi_zones') }}