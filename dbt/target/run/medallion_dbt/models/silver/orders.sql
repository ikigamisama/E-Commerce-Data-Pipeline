
  
    
    

    create  table
      "e_commerce"."silver"."orders__dbt_tmp"
  
    as (
      

SELECT
    order_id,
    customer_id,
    UPPER(TRIM(order_status)) AS order_status,
    order_purchase_timestamp,
    order_estimated_delivery_date,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    CASE 
        WHEN order_delivered_customer_date IS NOT NULL 
        
            THEN DATE_DIFF('day', CAST(order_delivered_customer_date AS DATE), CAST(order_purchase_timestamp AS DATE))
        
        ELSE NULL
    END AS delivery_days,
    CASE
        WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND order_delivered_customer_date <= order_estimated_delivery_date THEN 'ON_TIME'
        WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND order_delivered_customer_date > order_estimated_delivery_date THEN 'LATE'
        ELSE 'PENDING'
    END AS delivery_performance,
    CURRENT_TIMESTAMP AS processed_at
FROM "e_commerce"."bronze"."raw_orders"
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_status IS NOT NULL
  AND order_purchase_timestamp IS NOT NULL
    );
  
  