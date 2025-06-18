
  
    

  create  table "e_commerce"."gold"."seller_performance_facts__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    s.seller_id,
    
        DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)  AS month_year,
    

    -- Volume metrics
    COUNT(DISTINCT oi.order_id) AS orders_count,
    COUNT(oi.order_item_id) AS items_sold,
    COUNT(DISTINCT oi.product_id) AS unique_products_sold,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    
    -- Revenue metrics
    SUM(oi.price) AS gross_revenue,
    SUM(oi.discounted_price) AS net_revenue,
    SUM(oi.freight_value) AS freight_revenue,
    SUM(oi.total_item_cost) AS total_revenue,
    
    -- Performance metrics
    AVG(oi.price) AS avg_item_price,
    AVG(oi.discount_pct) AS avg_discount_pct,
    
    -- Delivery performance
    COUNT(CASE WHEN o.delivery_performance = 'ON_TIME' THEN 1 END) AS on_time_deliveries,
    COUNT(CASE WHEN o.delivery_performance = 'LATE' THEN 1 END) AS late_deliveries,
    
    CASE 
        WHEN COUNT(CASE WHEN o.delivery_performance IN ('ON_TIME', 'LATE') THEN 1 END) > 0
        THEN COUNT(CASE WHEN o.delivery_performance = 'ON_TIME' THEN 1 END) * 100.0 / 
             COUNT(CASE WHEN o.delivery_performance IN ('ON_TIME', 'LATE') THEN 1 END)
        ELSE NULL
    END AS on_time_delivery_rate,
    
    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."silver"."sellers" s
JOIN "e_commerce"."silver"."order_items" oi 
    ON s.seller_id = oi.seller_id
JOIN "e_commerce"."silver"."orders" o 
    ON oi.order_id = o.order_id
GROUP BY 
    s.seller_id,
    
        DATE_TRUNC('month', o.order_purchase_timestamp::timestamp) 
    
  );
  