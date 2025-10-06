{{ config(materialized='view') }}

WITH yellow AS (
    SELECT
        'yellow' AS service_type,
        TPEP_PICKUP_DATETIME AS pickup_datetime,
        TPEP_DROPOFF_DATETIME AS dropoff_datetime,
        PASSENGER_COUNT AS passenger_count,
        TRIP_DISTANCE AS trip_distance,
        FARE_AMOUNT AS fare_amount,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT AS tip_amount,
        TOLLS_AMOUNT AS tolls_amount,
        IMPROVEMENT_SURCHARGE,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        CBD_CONGESTION_FEE,
        RATECODEID AS rate_code_id,
        PAYMENT_TYPE AS payment_type,
        VENDORID AS vendor_id,
        PULOCATIONID AS pu_location_id,
        DOLOCATIONID AS do_location_id,
        NULL AS trip_type
    FROM {{ source('bronze', 'YELLOW_TAXIS') }}
),

green AS (
    SELECT
        'green' AS service_type,
        LPEP_PICKUP_DATETIME AS pickup_datetime,
        LPEP_DROPOFF_DATETIME AS dropoff_datetime,
        PASSENGER_COUNT AS passenger_count,
        TRIP_DISTANCE AS trip_distance,
        FARE_AMOUNT AS fare_amount,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT AS tip_amount,
        TOLLS_AMOUNT AS tolls_amount,
        IMPROVEMENT_SURCHARGE,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        CBD_CONGESTION_FEE,
        RATECODEID AS rate_code_id,
        PAYMENT_TYPE AS payment_type,
        VENDORID AS vendor_id,
        PULOCATIONID AS pu_location_id,
        DOLOCATIONID AS do_location_id,
        TRIP_TYPE AS trip_type
    FROM {{ source('bronze', 'GREEN_TAXIS') }}
),

unioned AS (
    SELECT * FROM yellow
    UNION ALL
    SELECT * FROM green
),

cleaned AS (
    SELECT
        service_type,
        CAST(pickup_datetime  AS TIMESTAMP) AS pickup_datetime,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        CASE WHEN passenger_count < 0 THEN NULL ELSE passenger_count END AS passenger_count,
        CASE WHEN trip_distance   < 0 THEN NULL ELSE trip_distance   END AS trip_distance,
        CASE WHEN fare_amount     < 0 THEN NULL ELSE fare_amount     END AS fare_amount,

        tip_amount,
        tolls_amount,
        extra,
        mta_tax,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,

        CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided Trip'
            ELSE 'Other'
        END AS payment_type_desc,

        rate_code_id,
        vendor_id,
        pu_location_id,
        do_location_id,
        trip_type
    FROM unioned
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND trip_distance >= 0
      AND fare_amount >= 0
      AND DATEDIFF('hour', pickup_datetime, dropoff_datetime) < 24
)

SELECT
    c.*,
    PU.borough  AS pickup_borough,
    PU.zone     AS pickup_zone,
    DO_.borough AS dropoff_borough,
    DO_.zone    AS dropoff_zone
FROM cleaned c
LEFT JOIN {{ ref('stg_taxi_zones') }} PU  ON c.pu_location_id = PU.zone_id
LEFT JOIN {{ ref('stg_taxi_zones') }} DO_ ON c.do_location_id = DO_.zone_id
