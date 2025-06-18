
  
    

  create  table "e_commerce"."gold"."order_items_facts__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,

    
        DATE_TRUNC('day', o.order_purchase_timestamp::timestamp) AS order_date,
    

    -- Item financial metrics
    oi.price AS item_price,
    oi.freight_value,
    oi.discount_pct,
    oi.discounted_price,
    oi.total_item_cost,

    -- Derived metrics
    oi.price - oi.discounted_price AS discount_amount,
    CASE 
        WHEN oi.price > 0 
        THEN (oi.price - oi.discounted_price) / oi.price * 100
        ELSE 0 
    END AS effective_discount_pct,

    -- Shipping analysis
    CASE
        WHEN oi.freight_value = 0 THEN 'FREE_SHIPPING'
        WHEN oi.freight_value <= 10 THEN 'LOW_SHIPPING'
        WHEN oi.freight_value <= 30 THEN 'MEDIUM_SHIPPING'
        ELSE 'HIGH_SHIPPING'
    END AS shipping_category,

    -- Coupon usage
    (oi.coupon_applied IS NOT NULL AND CAST(oi.coupon_applied AS VARCHAR) NOT IN ('NONE', '')) AS coupon_used,

    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."silver"."order_items" oi
JOIN "e_commerce"."silver"."orders" o 
    ON oi.order_id = o.order_id
  );
  