WITH RECURSIVE dates AS (
    SELECT DATE('2005-01-01') AS full_date
    UNION ALL
    SELECT DATE(full_date + INTERVAL 1 DAY)
    FROM dates
    WHERE full_date < DATE('2050-12-31')
)
INSERT INTO dim_date_daily (
    date_sk,
    full_date,
    day_since_2005,
    month_since_2005,
    day_of_week,
    calendar_month,
    calendar_year,
    calendar_year_month,
    day_of_month,
    day_of_year,
    week_of_year_sunday,
    year_week_sunday,
    week_sunday_start,
    week_of_year_monday,
    year_week_monday,
    week_monday_start,
    holiday,
    day_type
)
SELECT
    YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date) AS date_sk,
    full_date,
    DATEDIFF(full_date, '2005-01-01') AS day_since_2005,
    TIMESTAMPDIFF(MONTH, '2005-01-01', full_date) AS month_since_2005,
    DAYNAME(full_date) AS day_of_week,
    MONTHNAME(full_date) AS calendar_month,
    YEAR(full_date) AS calendar_year,
    CONCAT(YEAR(full_date), '-', LPAD(MONTH(full_date), 2, '0')) AS calendar_year_month,
    DAY(full_date) AS day_of_month,
    DAYOFYEAR(full_date) AS day_of_year,
    WEEK(full_date, 7) AS week_of_year_sunday,
    CONCAT(YEAR(full_date), '-', LPAD(WEEK(full_date, 7), 2, '0')) AS year_week_sunday,
    DATE_ADD(full_date, INTERVAL(1 - DAYOFWEEK(full_date)) DAY) AS week_sunday_start,
    WEEK(full_date, 1) AS week_of_year_monday,
    CONCAT(YEAR(full_date), '-', LPAD(WEEK(full_date, 1), 2, '0')) AS year_week_monday,
    DATE_ADD(full_date, INTERVAL(1 - IF(DAYOFWEEK(full_date) = 1, 7, DAYOFWEEK(full_date) - 2)) DAY) AS week_monday_start,
    CASE
        WHEN MONTH(full_date) = 12 AND DAY(full_date) = 25 THEN 'Christmas'
        WHEN MONTH(full_date) = 1 AND DAY(full_date) = 1 THEN 'New Year'
        WHEN MONTH(full_date) = 7 AND DAY(full_date) = 4 THEN 'Independence Day'
        ELSE 'None'
    END AS holiday,
    CASE
        WHEN DAYOFWEEK(full_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM dates;