{{ config(
    materialized='table',
    tags=['gold', 'dimensions', 'dates']
) }}

{% if target.name == 'dev' %}
    WITH date_spine AS (
        SELECT 
            CAST('{{ var("start_date") }}' AS DATE) + CAST(i AS INTEGER) AS date_day
        FROM range(0, 3653) AS r(i)
        WHERE (CAST('{{ var("start_date") }}' AS DATE) + CAST(i AS INTEGER)) <= CAST('{{ var("end_date") }}' AS DATE)
    )

    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        CAST(STRFTIME('%w', date_day) AS INTEGER) + 1 AS day_of_week,  -- 1 = Sunday
        CAST(STRFTIME('%j', date_day) AS INTEGER) AS day_of_year,
        CAST(STRFTIME('%W', date_day) AS INTEGER) AS week_of_year,
        CAST(((CAST(STRFTIME('%m', date_day) AS INTEGER) - 1) / 3 + 1) AS INTEGER) AS quarter,
        STRFTIME('%Y-%m', date_day) AS year_month,
        STRFTIME('%Y', date_day) || '-Q' || CAST(((CAST(STRFTIME('%m', date_day) AS INTEGER) - 1) / 3 + 1) AS INTEGER) AS year_quarter,
        STRFTIME('%A', date_day) AS day_name,
        STRFTIME('%B', date_day) AS month_name,
        CASE 
            WHEN STRFTIME('%w', date_day) IN ('0', '6') THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        CASE 
            WHEN STRFTIME('%w', date_day) BETWEEN '1' AND '5' THEN TRUE 
            ELSE FALSE 
        END AS is_weekday,
        CASE 
            WHEN STRFTIME('%d', date_day) = '01' THEN TRUE 
            ELSE FALSE 
        END AS is_month_start,
        CASE 
            WHEN DATE_TRUNC('month', date_day) + INTERVAL 1 MONTH - INTERVAL 1 DAY = date_day THEN TRUE 
            ELSE FALSE 
        END AS is_month_end
    FROM date_spine

{% elif target.name == 'prod' %}
    -- PostgreSQL branch remains unchanged
    WITH date_spine AS (
        SELECT 
            generate_series(
                '{{ var("start_date") }}'::DATE,
                '{{ var("end_date") }}'::DATE,
                INTERVAL '1 day'
            ) AS date_day
    )

    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(DOW FROM date_day) + 1 AS day_of_week,  -- 1 = Monday, 7 = Sunday
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'YYYY-"Q"Q') AS year_quarter,
        TRIM(TO_CHAR(date_day, 'Day')) AS day_name,
        TRIM(TO_CHAR(date_day, 'Month')) AS month_name,
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        CASE 
            WHEN EXTRACT(DOW FROM date_day) BETWEEN 1 AND 5 THEN TRUE 
            ELSE FALSE 
        END AS is_weekday,
        CASE 
            WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_month_start,
        CASE 
            WHEN date_day = (date_trunc('month', date_day) + INTERVAL '1 MONTH' - INTERVAL '1 day')::DATE THEN TRUE 
            ELSE FALSE 
        END AS is_month_end
    FROM date_spine
{% endif %}