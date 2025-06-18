

SELECT
    ROW_NUMBER() OVER (ORDER BY geolocation_city, geolocation_region) AS geo_key,
    geolocation_city,
    geolocation_region,
    geolocation_lat,
    geolocation_lng,
    processed_at
FROM "e_commerce"."silver"."geolocations"