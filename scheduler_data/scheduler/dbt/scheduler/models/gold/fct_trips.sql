{{ config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
) }}

WITH base AS (
    SELECT
        MD5(CONCAT(
            CAST(pickup_datetime AS VARCHAR),
            CAST(pu_location_id AS VARCHAR),
            CAST(do_location_id AS VARCHAR),
            CAST(vendor_id AS VARCHAR)
        )) AS trip_id,
        
        DATE(pickup_datetime) AS pickup_date,
        pickup_datetime,
        dropoff_datetime,
        
        vendor_id,
        rate_code_id,
        payment_type_desc,
        service_type,
        trip_type,
        pu_location_id,
        do_location_id,
        
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        extra,
        mta_tax,
        improvement_surcharge,
        congestion_surcharge,
        COALESCE(airport_fee, 0) AS airport_fee,
        COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee,
        
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) AS trip_duration_min,
        CASE 
            WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 0
            THEN (trip_distance / (DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0))
            ELSE 0
        END AS mph
        
    FROM {{ ref('stg_taxi_trips') }}
    
    WHERE 1=1
      -- Filtros de calidad
      AND DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 0
      AND DATEDIFF('minute', pickup_datetime, dropoff_datetime) < 1440  
      AND trip_distance > 0
      AND trip_distance < 100  
      AND fare_amount >= 0
      AND fare_amount < 1000  
    
    {% if is_incremental() %}
      AND pickup_datetime > (SELECT COALESCE(MAX(pickup_datetime), '2000-01-01'::TIMESTAMP) FROM {{ this }})
    {% endif %}
    
    {% if var('process_month', none) %}
      AND DATE_TRUNC('month', pickup_datetime) = '{{ var("process_month") }}'::DATE
    {% endif %}
)

SELECT

    b.trip_id,
    
    d.date_sk AS pickup_date_sk,
    d.date_sk AS dropoff_date_sk,  -
    EXTRACT(HOUR FROM b.pickup_datetime) AS pickup_time_sk,  
    EXTRACT(HOUR FROM b.dropoff_datetime) AS dropoff_time_sk, 
    b.pu_location_id AS pu_zone_sk,
    b.do_location_id AS do_zone_sk,
    v.vendor_sk,
    r.rate_code_sk,
    p.payment_type_sk,
    s.service_type_sk,
    COALESCE(tt.trip_type_sk, -1) AS trip_type_sk,
    
    b.pickup_datetime,
    b.dropoff_datetime,
    
    EXTRACT(YEAR FROM b.pickup_date) AS pickup_year,
    EXTRACT(MONTH FROM b.pickup_date) AS pickup_month,
    EXTRACT(DAY FROM b.pickup_date) AS pickup_day,
    EXTRACT(DOW FROM b.pickup_date) AS pickup_dow,
    EXTRACT(HOUR FROM b.pickup_datetime) AS pickup_hour,
    
    b.passenger_count,
    b.trip_distance,
    b.fare_amount,
    b.tip_amount,
    b.tolls_amount,
    b.extra,
    b.mta_tax,
    b.improvement_surcharge,
    b.congestion_surcharge,
    b.airport_fee,
    b.cbd_congestion_fee,
    
    (b.fare_amount + b.tip_amount + b.tolls_amount + b.extra + b.mta_tax +
     b.improvement_surcharge + b.congestion_surcharge + b.airport_fee + b.cbd_congestion_fee
    ) AS total_amount,
    
    CASE 
        WHEN b.fare_amount > 0 
        THEN (b.tip_amount / b.fare_amount) * 100
        ELSE 0
    END AS tip_percentage,
    
    b.trip_duration_min,
    b.mph

FROM base b
LEFT JOIN {{ ref('dim_date') }} d ON b.pickup_date = d.full_date
LEFT JOIN {{ ref('dim_vendor') }} v ON b.vendor_id = v.vendor_id
LEFT JOIN {{ ref('dim_rate_code') }} r ON b.rate_code_id = r.rate_code_id
LEFT JOIN {{ ref('dim_payment_type') }} p ON b.payment_type_desc = p.payment_type_desc
LEFT JOIN {{ ref('dim_service_type') }} s ON b.service_type = s.service_type
LEFT JOIN {{ ref('dim_trip_type') }} tt ON b.trip_type = tt.trip_type