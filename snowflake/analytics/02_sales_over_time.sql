-- =============================================================================
-- Hourly sales, daily sales, sales trends.
-- All times are STORE-LOCAL (Asia/Kuala_Lumpur): LOCAL_HOUR and DATE_KEY come from the
-- business date derived in STAGING, never from UTC.
-- =============================================================================

-- Hourly sales profile (average revenue per trading hour, last 28 days)
SELECT
    f.LOCAL_HOUR,
    SUM(f.NET_AMOUNT) / COUNT(DISTINCT f.DATE_KEY)          AS avg_revenue_per_day_myr,
    COUNT(DISTINCT f.TRANSACTION_ID) / COUNT(DISTINCT f.DATE_KEY) AS avg_transactions_per_day
FROM FACT_SALES f
JOIN DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
WHERE d.FULL_DATE >= DATEADD(DAY, -28, CURRENT_DATE())
GROUP BY f.LOCAL_HOUR
ORDER BY f.LOCAL_HOUR;

-- Daily sales with day-over-day and week-over-week change
WITH daily AS (
    SELECT d.FULL_DATE, d.DAY_NAME, d.IS_WEEKEND,
           SUM(f.NET_AMOUNT) AS revenue,
           COUNT(DISTINCT f.TRANSACTION_ID) AS transactions
    FROM FACT_SALES f
    JOIN DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
    GROUP BY 1, 2, 3
)
SELECT
    FULL_DATE, DAY_NAME, IS_WEEKEND, revenue, transactions,
    revenue / NULLIF(LAG(revenue) OVER (ORDER BY FULL_DATE), 0) - 1     AS dod_change,
    revenue / NULLIF(LAG(revenue, 7) OVER (ORDER BY FULL_DATE), 0) - 1  AS wow_change
FROM daily
ORDER BY FULL_DATE DESC;

-- Sales trend: 7-day moving average by region
WITH daily_region AS (
    SELECT d.FULL_DATE, s.REGION, SUM(f.NET_AMOUNT) AS revenue
    FROM FACT_SALES f
    JOIN DIM_DATE d  ON d.DATE_KEY = f.DATE_KEY
    JOIN DIM_STORE s ON s.STORE_KEY = f.STORE_KEY
    GROUP BY 1, 2
)
SELECT
    FULL_DATE, REGION, revenue,
    AVG(revenue) OVER (PARTITION BY REGION ORDER BY FULL_DATE
                       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_moving_avg
FROM daily_region
ORDER BY REGION, FULL_DATE;

-- Weekend vs weekday uplift per store format
SELECT
    s.STORE_FORMAT,
    AVG(IFF(d.IS_WEEKEND, daily_revenue, NULL)) / NULLIF(AVG(IFF(NOT d.IS_WEEKEND, daily_revenue, NULL)), 0) - 1
        AS weekend_uplift
FROM (
    SELECT f.STORE_KEY, f.DATE_KEY, SUM(f.NET_AMOUNT) AS daily_revenue
    FROM FACT_SALES f GROUP BY 1, 2
) x
JOIN DIM_DATE d  ON d.DATE_KEY = x.DATE_KEY
JOIN DIM_STORE s ON s.STORE_KEY = x.STORE_KEY
GROUP BY s.STORE_FORMAT;
