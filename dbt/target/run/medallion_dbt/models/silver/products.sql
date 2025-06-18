
  
    
    

    create  table
      "e_commerce"."silver"."products__dbt_tmp"
  
    as (
      

SELECT
    product_id,
    UPPER(TRIM(product_category_name)) AS product_category_name,
    TRIM(product_description) AS product_description,
    product_price,
    COALESCE(product_photos_qty, 0) AS product_photos_qty,
    COALESCE(product_weight_g, 0) AS product_weight_g,
    COALESCE(product_length_cm, 0) AS product_length_cm,
    COALESCE(product_height_cm, 0) AS product_height_cm,
    COALESCE(product_width_cm, 0) AS product_width_cm,
    TRIM(product_name) AS product_name,
    CASE 
        WHEN product_length_cm > 0 AND product_height_cm > 0 AND product_width_cm > 0
        THEN product_length_cm * product_height_cm * product_width_cm
        ELSE NULL
    END AS product_volume_cm3,
    CURRENT_TIMESTAMP AS processed_at
FROM "e_commerce"."bronze"."raw_products"
WHERE product_id IS NOT NULL
  AND product_name IS NOT NULL
    );
  
  