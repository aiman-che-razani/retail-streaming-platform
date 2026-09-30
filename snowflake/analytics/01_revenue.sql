-- =============================================================================
-- Revenue: total, by outlet, by product, by category (as-was vs as-is).
-- Run as RETAIL_ANALYST on RETAIL_ANALYTICS_WH:
--   USE ROLE RETAIL_ANALYST; USE WAREHOUSE RETAIL_ANALYTICS_WH; USE SCHEMA RETAIL_DEV.ANALYTICS;
-- Revenue = SUM(NET_AMOUNT) (after line discounts). Filters on DATE_KEY prune micro-partitions.
-- =============================================================================

-- Total revenue, transactions and units for the last 30 days
SELECT
    SUM(f.NET_AMOUNT)                    AS revenue_myr,
    COUNT(DISTINCT f.TRANSACTION_ID)     AS transactions,
    SUM(f.QUANTITY)                      AS units_sold
FROM FACT_SALES f
JOIN DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
WHERE d.FULL_DATE >= DATEADD(DAY, -30, CURRENT_DATE());

-- Revenue by outlet (with region and format) and share of total
SELECT
    s.STORE_ID, s.STORE_NAME, s.REGION, s.STORE_FORMAT,
    SUM(f.NET_AMOUNT)                                              AS revenue_myr,
    RATIO_TO_REPORT(SUM(f.NET_AMOUNT)) OVER ()                     AS share_of_total,
    RANK() OVER (ORDER BY SUM(f.NET_AMOUNT) DESC)                  AS revenue_rank
FROM FACT_SALES f
JOIN DIM_STORE s ON s.STORE_KEY = f.STORE_KEY
GROUP BY s.STORE_ID, s.STORE_NAME, s.REGION, s.STORE_FORMAT
ORDER BY revenue_myr DESC;

-- Revenue by product (top 20)
SELECT
    p.PRODUCT_ID,
    ANY_VALUE(pc.PRODUCT_NAME)           AS product_name,   -- current (SCD1) name
    SUM(f.NET_AMOUNT)                    AS revenue_myr,
    SUM(f.QUANTITY)                      AS units_sold
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON p.PRODUCT_KEY = f.PRODUCT_KEY
JOIN VW_DIM_PRODUCT_CURRENT pc ON pc.PRODUCT_ID = p.PRODUCT_ID
GROUP BY p.PRODUCT_ID
ORDER BY revenue_myr DESC
LIMIT 20;

-- Revenue by category: AS-WAS (category at the time of sale, via the fact's SCD2 key)
-- versus AS-IS (today's category for every historical sale). They differ exactly when
-- products were recategorised - this is why DIM_PRODUCT tracks category as SCD Type 2.
WITH as_was AS (
    SELECT p.CATEGORY, p.SUBCATEGORY, SUM(f.NET_AMOUNT) AS revenue
    FROM FACT_SALES f JOIN DIM_PRODUCT p ON p.PRODUCT_KEY = f.PRODUCT_KEY
    GROUP BY 1, 2
),
as_is AS (
    SELECT pc.CATEGORY, pc.SUBCATEGORY, SUM(f.NET_AMOUNT) AS revenue
    FROM FACT_SALES f JOIN VW_DIM_PRODUCT_CURRENT pc ON pc.PRODUCT_ID = f.PRODUCT_ID
    GROUP BY 1, 2
)
SELECT
    COALESCE(w.CATEGORY, i.CATEGORY)       AS category,
    COALESCE(w.SUBCATEGORY, i.SUBCATEGORY) AS subcategory,
    w.revenue                              AS revenue_as_was,
    i.revenue                              AS revenue_as_is,
    COALESCE(i.revenue, 0) - COALESCE(w.revenue, 0) AS restatement_delta
FROM as_was w
FULL OUTER JOIN as_is i ON i.CATEGORY = w.CATEGORY AND i.SUBCATEGORY = w.SUBCATEGORY
ORDER BY category, subcategory;
