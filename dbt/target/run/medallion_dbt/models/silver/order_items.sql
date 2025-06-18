
  
    
    

    create  table
      "e_commerce"."silver"."order_items__dbt_tmp"
  
    as (
      

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    COALESCE(discount_pct, 0) AS discount_pct,

    
        COALESCE(coupon_applied, 'NONE') AS coupon_applied,
    

    price * (1 - COALESCE(discount_pct, 0) / 100.0) AS discounted_price,
    price * (1 - COALESCE(discount_pct, 0) / 100.0) + freight_value AS total_item_cost,
    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."bronze"."raw_order_items"
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_id IS NOT NULL
  AND price > 0
    );
  
  