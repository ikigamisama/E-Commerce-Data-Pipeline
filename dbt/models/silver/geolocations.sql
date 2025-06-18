{{ config(
    materialized='table',
    tags=['silver', 'geolocations']
) }}

SELECT DISTINCT
    UPPER(TRIM(geolocation_city)) AS geolocation_city,
    UPPER(TRIM(geolocation_region)) AS geolocation_region,
    geolocation_lat,
    geolocation_lng,
    CURRENT_TIMESTAMP AS processed_at
FROM {{ source('bronze', 'raw_geolocations') }}
WHERE geolocation_city IS NOT NULL
  AND geolocation_region IS NOT NULL
  AND geolocation_lat IS NOT NULL 
  AND geolocation_lng IS NOT NULL
  AND geolocation_lat BETWEEN -90 AND 90
  AND geolocation_lng BETWEEN -180 AND 180
