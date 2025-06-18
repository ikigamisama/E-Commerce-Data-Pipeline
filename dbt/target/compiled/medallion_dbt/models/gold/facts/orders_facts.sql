

SELECT
    o.order_id,
    o.customer_id,

    
        DATE_TRUNC('day', o.order_purchase_timestamp::timestamp) AS order_date,
    

    o.order_status,
    o.delivery_performance,
    o.delivery_days,

    -- Aggregated order metrics
    COUNT(oi.order_item_id) AS total_items,
    SUM(oi.price) AS gross_item_value,
    SUM(oi.freight_value) AS total_freight,
    SUM(oi.discount_pct * oi.price / 100.0) AS total_discount_amount,
    SUM(oi.discounted_price) AS net_item_value,
    SUM(oi.total_item_cost) AS total_order_value,

    -- Seller metrics
    COUNT(DISTINCT oi.seller_id) AS unique_sellers,

    -- Product metrics
    COUNT(DISTINCT oi.product_id) AS unique_products,

    -- Calculated metrics
    CASE 
        WHEN COUNT(oi.order_item_id) > 0 
        THEN SUM(oi.total_item_cost) / COUNT(oi.order_item_id)
        ELSE 0 
    END AS avg_item_value,

    CASE 
        WHEN SUM(oi.price) > 0 
        THEN SUM(oi.discount_pct * oi.price / 100.0) / SUM(oi.price) * 100
        ELSE 0 
    END AS avg_discount_pct,

    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."silver"."orders" o
LEFT JOIN "e_commerce"."silver"."order_items" oi 
    ON o.order_id = oi.order_id

GROUP BY 
    o.order_id,
    o.customer_id,

    
        DATE_TRUNC('day', o.order_purchase_timestamp::timestamp),
    

    o.order_status,
    o.delivery_performance,
    o.delivery_days