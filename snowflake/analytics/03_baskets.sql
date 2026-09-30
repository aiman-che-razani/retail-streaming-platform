-- =============================================================================
-- Basket analysis. FACT_SALES is at LINE grain, so basket metrics first aggregate to the
-- transaction (the degenerate dimension TRANSACTION_ID), then average across baskets.
-- Averaging line amounts directly would give "average line value", a different metric.
-- =============================================================================

-- Average basket value and size, by store
WITH baskets AS (
    SELECT f.TRANSACTION_ID, f.STORE_KEY,
           SUM(f.NET_AMOUNT) AS basket_value,
           SUM(f.QUANTITY)   AS basket_units,
           COUNT(*)          AS basket_lines
    FROM FACT_SALES f
    GROUP BY f.TRANSACTION_ID, f.STORE_KEY
)
SELECT
    s.STORE_ID, s.STORE_NAME,
    COUNT(*)                        AS transactions,
    AVG(b.basket_value)             AS avg_basket_value_myr,
    MEDIAN(b.basket_value)          AS median_basket_value_myr,
    AVG(b.basket_units)             AS avg_units_per_basket,
    AVG(b.basket_lines)             AS avg_lines_per_basket
FROM baskets b
JOIN DIM_STORE s ON s.STORE_KEY = b.STORE_KEY
GROUP BY s.STORE_ID, s.STORE_NAME
ORDER BY avg_basket_value_myr DESC;

-- Basket value by payment method and by loyalty status (guest vs tier held AT PURCHASE TIME)
WITH baskets AS (
    SELECT f.TRANSACTION_ID, ANY_VALUE(f.PAYMENT_METHOD) AS payment_method,
           ANY_VALUE(f.CUSTOMER_KEY) AS customer_key, SUM(f.NET_AMOUNT) AS basket_value
    FROM FACT_SALES f
    GROUP BY f.TRANSACTION_ID
)
SELECT
    b.payment_method,
    c.LOYALTY_TIER,
    COUNT(*)              AS transactions,
    AVG(b.basket_value)   AS avg_basket_value_myr
FROM baskets b
JOIN DIM_CUSTOMER c ON c.CUSTOMER_KEY = b.customer_key
GROUP BY 1, 2
ORDER BY 1, avg_basket_value_myr DESC;
