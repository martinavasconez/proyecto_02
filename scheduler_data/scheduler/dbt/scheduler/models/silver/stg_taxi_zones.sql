{{ config(materialized='view') }}

select
    "LocationID"    as zone_id,
    "Borough"       as borough,
    "Zone"          as zone,
    "service_zone"  as service_zone
from {{ source('bronze', 'TAXI_ZONES') }}