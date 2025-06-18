{{ config(
    materialized='table',
    tags=['silver', 'payments']
) }}

SELECT
    order_id,
    payment_sequential,
    UPPER(TRIM(payment_type)) AS payment_type,
    COALESCE(payment_installments, 1) AS payment_installments,
    payment_value,
    UPPER(TRIM(payment_status)) AS payment_status,
    CASE
        WHEN COALESCE(payment_installments, 1) = 1 THEN 'SINGLE_PAYMENT'
        WHEN COALESCE(payment_installments, 1) BETWEEN 2 AND 6 THEN 'SHORT_TERM_INSTALLMENT'
        WHEN COALESCE(payment_installments, 1) > 6 THEN 'LONG_TERM_INSTALLMENT'
        ELSE 'UNKNOWN'
    END AS payment_category,
    CURRENT_TIMESTAMP AS processed_at
FROM {{ source('bronze', 'raw_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0
  AND payment_status IS NOT NULL
