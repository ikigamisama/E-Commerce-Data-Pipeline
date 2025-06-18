{{ config(
    materialized='table',
    tags=['silver', 'customers']
) }}

SELECT
    customer_id,
    customer_unique_id,
    UPPER(TRIM(customer_name)) AS customer_name,
    customer_age,
    UPPER(TRIM(customer_gender)) AS customer_gender,
    UPPER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_region)) AS customer_region,

    {% if target.name == 'dev' %}
        CAST(customer_signup_date AS DATE) AS signup_date,
    {% elif target.name == 'prod' %}
        customer_signup_date::DATE AS signup_date,
    {% endif %}

    CURRENT_TIMESTAMP AS processed_at

FROM {{ source('bronze', 'raw_customers') }}
WHERE customer_id IS NOT NULL
  AND customer_age BETWEEN 18 AND 100
  AND customer_signup_date IS NOT NULL
