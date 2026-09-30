-- =============================================================================
-- Store performance: productivity (revenue per sqm), vs regional average, margin.
-- =============================================================================

WITH store_30d AS (
    SELECT
        s.STORE_ID, s.STORE_NAME, s.REGION, s.STORE_FORMAT, s.SIZE_SQM,
        SUM(f.NET_AMOUNT)                   AS revenue,
        SUM(f.NET_AMOUNT - f.COST_AMOUNT)   AS gross_margin,
        COUNT(DISTINCT f.TRANSACTION_ID)    AS transactions,
        COUNT(DISTINCT f.DATE_KEY)          AS trading_days
    FROM FACT_SALES f
    JOIN DIM_STORE s ON s.STORE_KEY = f.STORE_KEY
    JOIN DIM_DATE d  ON d.DATE_KEY = f.DATE_KEY
    WHERE d.FULL_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    STORE_ID, STORE_NAME, REGION, STORE_FORMAT,
    revenue,
    revenue / NULLIF(trading_days, 0)                                AS revenue_per_day,
    revenue / NULLIF(SIZE_SQM, 0)                                    AS revenue_per_sqm,
    gross_margin / NULLIF(revenue, 0)                                AS gross_margin_pct,
    revenue / NULLIF(transactions, 0)                                AS avg_basket_value,
    revenue / AVG(revenue) OVER (PARTITION BY REGION) - 1            AS vs_region_avg,
    RANK() OVER (PARTITION BY STORE_FORMAT ORDER BY revenue / NULLIF(SIZE_SQM, 0) DESC) AS productivity_rank_in_format
FROM store_30d
ORDER BY revenue_per_sqm DESC;
