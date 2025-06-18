

SELECT
    p.order_id,
    p.payment_type,
    p.payment_category,
    p.payment_status,
    
    -- Payment aggregations
    COUNT(*) AS payment_count,
    SUM(p.payment_value) AS total_payment_value,
    AVG(p.payment_value) AS avg_payment_value,
    MAX(p.payment_installments) AS max_installments,
    AVG(p.payment_installments) AS avg_installments,
    
    -- Payment analysis
    CASE 
        WHEN COUNT(*) = 1 THEN 'SINGLE_PAYMENT_METHOD'
        ELSE 'MULTIPLE_PAYMENT_METHODS'
    END AS payment_complexity,
    
    CURRENT_TIMESTAMP AS processed_at

FROM "e_commerce"."silver"."payments" p
GROUP BY 
    p.order_id,
    p.payment_type,
    p.payment_category,
    p.payment_status