-- =============================================================================
-- Units sold, top-selling and underperforming products.
-- =============================================================================

-- Top 10 products by units in each category (last 30 days)
WITH product_units AS (
    SELECT pc.CATEGORY, pc.PRODUCT_ID, pc.PRODUCT_NAME,
           SUM(f.QUANTITY) AS units_sold, SUM(f.NET_AMOUNT) AS revenue
    FROM FACT_SALES f
    JOIN DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
    JOIN VW_DIM_PRODUCT_CURRENT pc ON pc.PRODUCT_ID = f.PRODUCT_ID
    WHERE d.FULL_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
    GROUP BY 1, 2, 3
)
SELECT *
FROM product_units
QUALIFY ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY units_sold DESC) <= 10
ORDER BY CATEGORY, units_sold DESC;

-- Underperforming products: ACTIVE products in the bottom decile of their category by units
-- sold (last 30 days), including products with ZERO sales (left join from the dimension -
-- an inner join from the fact would silently hide products that never sold).
WITH active_products AS (
    SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUBCATEGORY, LIST_PRICE
    FROM VW_DIM_PRODUCT_CURRENT
    WHERE IS_ACTIVE AND NOT IS_INFERRED AND PRODUCT_ID NOT IN ('-1', '-2')
),
sales_30d AS (
    SELECT f.PRODUCT_ID, SUM(f.QUANTITY) AS units, SUM(f.NET_AMOUNT) AS revenue,
           MAX(f.EVENT_TS_LOCAL) AS last_sold_at
    FROM FACT_SALES f
    JOIN DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
    WHERE d.FULL_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
    GROUP BY f.PRODUCT_ID
),
ranked AS (
    SELECT a.*, COALESCE(s.units, 0) AS units_30d, COALESCE(s.revenue, 0) AS revenue_30d, s.last_sold_at,
           NTILE(10) OVER (PARTITION BY a.CATEGORY ORDER BY COALESCE(s.units, 0)) AS decile_in_category
    FROM active_products a
    LEFT JOIN sales_30d s ON s.PRODUCT_ID = a.PRODUCT_ID
)
SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUBCATEGORY, units_30d, revenue_30d, last_sold_at
FROM ranked
WHERE decile_in_category = 1
ORDER BY CATEGORY, units_30d;

-- Price-change impact: units per day before vs after each SCD2 price change
WITH price_versions AS (
    SELECT PRODUCT_ID, PRODUCT_KEY, LIST_PRICE, EFFECTIVE_FROM_UTC, EFFECTIVE_TO_UTC,
           LAG(LIST_PRICE) OVER (PARTITION BY PRODUCT_ID ORDER BY VERSION_NUMBER) AS previous_price
    FROM DIM_PRODUCT
    WHERE NOT IS_INFERRED AND PRODUCT_ID NOT IN ('-1', '-2')
)
SELECT
    v.PRODUCT_ID, v.previous_price, v.LIST_PRICE AS new_price, v.EFFECTIVE_FROM_UTC AS changed_at,
    SUM(f.QUANTITY) / NULLIF(DATEDIFF(DAY, v.EFFECTIVE_FROM_UTC, LEAST(v.EFFECTIVE_TO_UTC, SYSDATE())), 0)
        AS units_per_day_at_new_price
FROM price_versions v
LEFT JOIN FACT_SALES f ON f.PRODUCT_KEY = v.PRODUCT_KEY
WHERE v.previous_price IS NOT NULL AND v.previous_price <> v.LIST_PRICE
GROUP BY 1, 2, 3, 4
ORDER BY changed_at DESC;
