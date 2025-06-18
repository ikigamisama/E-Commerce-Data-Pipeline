{{ config(
    materialized='table',
    tags=['gold', 'dimensions', 'sellers']
) }}

SELECT
    seller_id,
    seller_category_specialization,
    seller_rating,
    seller_rating_category,
    seller_fulfillment_type,
    seller_city,
    seller_state,
    seller_signup_date,

    {% if target.name == 'dev' %}
        DATEDIFF('day', CAST(seller_signup_date AS DATE), CURRENT_DATE) AS days_active,
        CASE
            WHEN DATEDIFF('day', CAST(seller_signup_date AS DATE), CURRENT_DATE) <= 90 THEN 'NEW_SELLER'
            WHEN DATEDIFF('day', CAST(seller_signup_date AS DATE), CURRENT_DATE) <= 365 THEN 'RECENT_SELLER'
            ELSE 'ESTABLISHED_SELLER'
        END AS seller_tenure,
    {% elif target.name == 'prod' %}
        (CURRENT_DATE - seller_signup_date) AS days_active,
        CASE
            WHEN CURRENT_DATE - seller_signup_date <= 90 THEN 'NEW_SELLER'
            WHEN CURRENT_DATE - seller_signup_date <= 365 THEN 'RECENT_SELLER'
            ELSE 'ESTABLISHED_SELLER'
        END AS seller_tenure,
    {% endif %}

    CURRENT_TIMESTAMP AS processed_at

FROM {{ ref('sellers') }}
