-- =============================================================================
-- Inventory position, low stock, turnover, stock movements.
-- CLOSING_ON_HAND is SEMI-ADDITIVE: it may be summed across stores/products at ONE point in
-- time, but never across days (summing 30 daily closing balances is meaningless). Over a
-- period, use the average (or the end-of-period value).
-- =============================================================================

-- Current inventory position by store and category (latest snapshot date)
SELECT REGION, STORE_NAME, CATEGORY,
       SUM(CLOSING_ON_HAND) AS units_on_hand,
       SUM(CLOSING_VALUE)   AS stock_value_at_cost_myr
FROM VW_INVENTORY_POSITION
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Low-stock products: below reorder point (< 3 days of cover at the 7-day sales rate)
SELECT STORE_ID, STORE_NAME, PRODUCT_ID, PRODUCT_NAME, CATEGORY,
       CLOSING_ON_HAND, AVG_DAILY_UNITS_SOLD_7D,
       CLOSING_ON_HAND / NULLIF(AVG_DAILY_UNITS_SOLD_7D, 0) AS days_of_cover
FROM VW_INVENTORY_POSITION
WHERE IS_BELOW_REORDER_POINT
ORDER BY days_of_cover NULLS FIRST, AVG_DAILY_UNITS_SOLD_7D DESC
LIMIT 100;

-- Inventory turnover (units) per category over the last 30 days:
--   turnover = units sold in period / AVERAGE units on hand in period
-- The average comes from the daily periodic snapshot - the reason that fact table exists.
WITH period AS (
    SELECT sn.PRODUCT_KEY, sn.STORE_KEY, sn.DATE_KEY, sn.CLOSING_ON_HAND, sn.UNITS_SOLD
    FROM FACT_INVENTORY_SNAPSHOT sn
    JOIN DIM_DATE d ON d.DATE_KEY = sn.DATE_KEY
    WHERE d.FULL_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
),
per_category AS (
    SELECT p.CATEGORY,
           SUM(pe.UNITS_SOLD)                                         AS units_sold,
           -- average over days of the (store x product) total per day
           AVG(daily_on_hand)                                         AS avg_units_on_hand
    FROM (
        SELECT PRODUCT_KEY, DATE_KEY, SUM(CLOSING_ON_HAND) AS daily_on_hand, SUM(UNITS_SOLD) AS UNITS_SOLD
        FROM period GROUP BY PRODUCT_KEY, DATE_KEY
    ) pe
    JOIN DIM_PRODUCT p ON p.PRODUCT_KEY = pe.PRODUCT_KEY
    GROUP BY p.CATEGORY
)
SELECT CATEGORY, units_sold, avg_units_on_hand,
       units_sold / NULLIF(avg_units_on_hand, 0)          AS turnover_30d,
       30 / NULLIF(units_sold / NULLIF(avg_units_on_hand, 0), 0) AS days_of_inventory
FROM per_category
ORDER BY turnover_30d DESC;

-- Stock movements by type and reason (shrinkage analysis), last 30 days
SELECT
    f.MOVEMENT_TYPE,
    COALESCE(f.REASON_CODE, 'n/a')  AS reason_code,
    COUNT(*)                        AS movements,
    SUM(f.QUANTITY_DELTA)           AS net_units,
    SUM(f.MOVEMENT_VALUE)           AS net_value_myr
FROM FACT_INVENTORY f
JOIN DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
WHERE d.FULL_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1, net_units;

-- Shrinkage rate per store: units lost to damage/expiry/shrinkage per 1,000 units sold
SELECT
    s.STORE_ID, s.STORE_NAME,
    -SUM(IFF(f.REASON_CODE IN ('DAMAGED', 'EXPIRED', 'SHRINKAGE'), f.QUANTITY_DELTA, 0)) AS units_lost,
    -SUM(IFF(f.MOVEMENT_TYPE = 'SALE', f.QUANTITY_DELTA, 0))                          AS units_sold,
    1000 * units_lost / NULLIF(units_sold, 0)                                         AS lost_per_1000_sold
FROM FACT_INVENTORY f
JOIN DIM_STORE s ON s.STORE_KEY = f.STORE_KEY
GROUP BY 1, 2
ORDER BY lost_per_1000_sold DESC;
