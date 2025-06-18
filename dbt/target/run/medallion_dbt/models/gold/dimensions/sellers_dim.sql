
  
    

  create  table "e_commerce"."gold"."sellers_dim__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    seller_id,
    seller_category_specialization,
    seller_rating,
    seller_rating_category,
    seller_fulfillment_type,
    seller_city,
    seller_state,
    seller_signup_date,

    
        (CURRENT_DATE - seller_signup_date) AS days_active,
        CASE
            WHEN CURRENT_DATE - seller_signup_date <= 90 THEN 'NEW_SELLER'
            WHEN CURRENT_DATE - seller_signup_date <= 365 THEN 'RECENT_SELLER'
            ELSE 'ESTABLISHED_SELLER'
        END AS seller_tenure,
    

    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."silver"."sellers"
  );
  