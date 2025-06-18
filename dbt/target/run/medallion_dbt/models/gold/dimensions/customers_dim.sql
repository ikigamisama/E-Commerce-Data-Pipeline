
  
    

  create  table "e_commerce"."gold"."customers_dim__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    customer_id,
    customer_unique_id,
    customer_name,
    customer_age,
    customer_gender,
    customer_city,
    customer_region,
    signup_date,
    
    -- Age group classification
    CASE
        WHEN customer_age BETWEEN 18 AND 25 THEN 'YOUNG_ADULT'
        WHEN customer_age BETWEEN 26 AND 35 THEN 'ADULT'
        WHEN customer_age BETWEEN 36 AND 50 THEN 'MIDDLE_AGED'
        WHEN customer_age > 50 THEN 'SENIOR'
        ELSE 'UNKNOWN'
    END AS age_group,

    -- Tenure logic per warehouse
    
        (CURRENT_DATE - signup_date::DATE) AS days_since_signup,
        CASE
            WHEN (CURRENT_DATE - signup_date::DATE) <= 30 THEN 'NEW'
            WHEN (CURRENT_DATE - signup_date::DATE) <= 365 THEN 'RECENT'
            ELSE 'ESTABLISHED'
        END AS customer_tenure,
    

    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."silver"."customers"
  );
  