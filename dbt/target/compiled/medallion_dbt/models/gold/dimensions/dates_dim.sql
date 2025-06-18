


    -- PostgreSQL branch remains unchanged
    WITH date_spine AS (
        SELECT 
            generate_series(
                '2020-01-01'::DATE,
                '2025-12-31'::DATE,
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
