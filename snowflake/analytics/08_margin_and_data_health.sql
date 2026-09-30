-- =============================================================================
-- Gross margin, plus the analyst's view of data health.
-- =============================================================================

-- Gross margin by category and month. COST_AMOUNT uses the product cost VALID AT SALE TIME
-- (SCD2), so historical margin is not rewritten when costs change.
SELECT d.YEAR_MONTH, p.CATEGORY,
       SUM(f.NET_AMOUNT)                                   AS revenue,
       SUM(f.COST_AMOUNT)                                  AS cost,
       SUM(f.NET_AMOUNT - f.COST_AMOUNT)                   AS gross_margin,
       SUM(f.NET_AMOUNT - f.COST_AMOUNT) / NULLIF(SUM(f.NET_AMOUNT), 0) AS gross_margin_pct
FROM FACT_SALES f
JOIN DIM_DATE d    ON d.DATE_KEY = f.DATE_KEY
JOIN DIM_PRODUCT p ON p.PRODUCT_KEY = f.PRODUCT_KEY
WHERE f.COST_AMOUNT IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, gross_margin DESC;

-- How much revenue is currently attributed to inferred (late-arriving) products?
-- Non-zero is normal briefly; persistent values mean product master data is missing (SF-003).
SELECT p.IS_INFERRED, COUNT(*) AS lines, SUM(f.NET_AMOUNT) AS revenue
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON p.PRODUCT_KEY = f.PRODUCT_KEY
GROUP BY p.IS_INFERRED;
