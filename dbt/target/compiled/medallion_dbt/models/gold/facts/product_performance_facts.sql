

WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_category_name,

        
            DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)  AS month_year,
        

        COUNT(DISTINCT oi.order_id) AS orders_count,
        COUNT(oi.order_item_id) AS items_sold,
        COUNT(DISTINCT oi.seller_id) AS unique_sellers,
        COUNT(DISTINCT o.customer_id) AS unique_customers,

        SUM(oi.price) AS gross_revenue,
        SUM(oi.discounted_price) AS net_revenue,
        SUM(oi.total_item_cost) AS total_revenue,

        AVG(oi.price) AS avg_selling_price,
        MIN(oi.price) AS min_selling_price,
        MAX(oi.price) AS max_selling_price,
        AVG(oi.discount_pct) AS avg_discount_pct
    FROM "e_commerce"."silver"."products" p
    JOIN "e_commerce"."silver"."order_items" oi 
        ON p.product_id = oi.product_id
    JOIN "e_commerce"."silver"."orders" o 
        ON oi.order_id = o.order_id
    GROUP BY 
        p.product_id,
        p.product_category_name,
        
            DATE_TRUNC('month', o.order_purchase_timestamp::timestamp) 
        
),
category_totals AS (
    SELECT
        product_category_name,
        month_year,
        SUM(net_revenue) AS category_net_revenue,
        SUM(items_sold) AS category_volume
    FROM product_sales
    GROUP BY product_category_name, month_year
)

SELECT 
    ps.*,
    ps.net_revenue * 100.0 / ct.category_net_revenue AS category_revenue_share,
    ps.items_sold * 100.0 / ct.category_volume AS category_volume_share,
    CURRENT_TIMESTAMP AS processed_at

FROM product_sales ps
JOIN category_totals ct 
    ON ps.product_category_name = ct.product_category_name
   AND ps.month_year = ct.month_year