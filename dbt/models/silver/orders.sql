{{ config(
    materialized='table',
    tags=['silver', 'orders']
) }}

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
        {% if target.name == 'dev' %}
            THEN DATE_DIFF('day', CAST(order_delivered_customer_date AS DATE), CAST(order_purchase_timestamp AS DATE))
        {% elif target.name == 'prod' %}
            THEN DATE_PART('day', order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp)
        {% endif %}
        ELSE NULL
    END AS delivery_days,
    CASE
        WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND order_delivered_customer_date <= order_estimated_delivery_date THEN 'ON_TIME'
        WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND order_delivered_customer_date > order_estimated_delivery_date THEN 'LATE'
        ELSE 'PENDING'
    END AS delivery_performance,
    CURRENT_TIMESTAMP AS processed_at
FROM {{ source('bronze', 'raw_orders') }}
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_status IS NOT NULL
  AND order_purchase_timestamp IS NOT NULL
