{{ config(
    materialized='table',
    tags=['gold', 'dimensions', 'products']
) }}

SELECT
    product_id,
    product_name,
    product_category_name,
    product_description,
    product_price,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    CASE
        WHEN product_price <= 50 THEN 'LOW'
        WHEN product_price <= 200 THEN 'MEDIUM'
        WHEN product_price <= 500 THEN 'HIGH'
        ELSE 'PREMIUM'
    END AS price_range,
    CASE
        WHEN product_weight_g <= 500 THEN 'LIGHT'
        WHEN product_weight_g <= 2000 THEN 'MEDIUM'
        WHEN product_weight_g <= 5000 THEN 'HEAVY'
        ELSE 'VERY_HEAVY'
    END AS weight_category,
    CASE
        WHEN product_photos_qty = 0 THEN 'NO_PHOTOS'
        WHEN product_photos_qty <= 3 THEN 'FEW_PHOTOS'
        WHEN product_photos_qty <= 6 THEN 'GOOD_PHOTOS'
        ELSE 'MANY_PHOTOS'
    END AS photo_quality,
    processed_at
FROM {{ ref('products') }}
