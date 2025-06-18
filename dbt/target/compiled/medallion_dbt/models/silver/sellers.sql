

SELECT
    seller_id,
    DATE(seller_signup_date) AS seller_signup_date,
    UPPER(TRIM(seller_category_specialization)) AS seller_category_specialization,
    COALESCE(seller_rating, 0) AS seller_rating,
    UPPER(TRIM(seller_fulfillment_type)) AS seller_fulfillment_type,
    UPPER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,
    CASE
        WHEN COALESCE(seller_rating, 0) >= 4.5 THEN 'EXCELLENT'
        WHEN COALESCE(seller_rating, 0) >= 4.0 THEN 'GOOD'
        WHEN COALESCE(seller_rating, 0) >= 3.0 THEN 'AVERAGE'
        ELSE 'POOR'
    END AS seller_rating_category,
    CURRENT_TIMESTAMP AS processed_at
FROM "e_commerce"."bronze"."raw_sellers"
WHERE seller_id IS NOT NULL
  AND seller_signup_date IS NOT NULL