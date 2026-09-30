-- =============================================================================
-- Customer / loyalty analysis using SCD2 history.
-- =============================================================================

-- Revenue by the loyalty tier the customer held AT THE TIME OF PURCHASE (as-was).
-- Joining on the fact's CUSTOMER_KEY gives the historical version; guests are key -2.
SELECT c.LOYALTY_TIER,
       COUNT(DISTINCT f.TRANSACTION_ID)                          AS transactions,
       SUM(f.NET_AMOUNT)                                         AS revenue_myr,
       SUM(f.NET_AMOUNT) / COUNT(DISTINCT f.TRANSACTION_ID)      AS avg_basket_myr
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON c.CUSTOMER_KEY = f.CUSTOMER_KEY
GROUP BY c.LOYALTY_TIER
ORDER BY revenue_myr DESC;

-- Tier migrations: how many customers moved between tiers, per month
WITH transitions AS (
    SELECT CUSTOMER_ID,
           LAG(LOYALTY_TIER) OVER (PARTITION BY CUSTOMER_ID ORDER BY VERSION_NUMBER) AS from_tier,
           LOYALTY_TIER AS to_tier,
           EFFECTIVE_FROM_UTC
    FROM DIM_CUSTOMER
    WHERE NOT IS_INFERRED AND CUSTOMER_ID NOT IN ('-1', '-2')
)
SELECT TO_CHAR(EFFECTIVE_FROM_UTC, 'YYYY-MM') AS month, from_tier, to_tier, COUNT(*) AS customers
FROM transitions
WHERE from_tier IS NOT NULL AND from_tier <> to_tier
GROUP BY 1, 2, 3
ORDER BY 1 DESC, customers DESC;

-- Share of sales from loyalty members vs guests, per region
SELECT s.REGION,
       SUM(IFF(f.CUSTOMER_KEY = -2, f.NET_AMOUNT, 0)) / SUM(f.NET_AMOUNT) AS guest_share,
       SUM(IFF(f.CUSTOMER_KEY <> -2, f.NET_AMOUNT, 0)) / SUM(f.NET_AMOUNT) AS member_share
FROM FACT_SALES f
JOIN DIM_STORE s ON s.STORE_KEY = f.STORE_KEY
GROUP BY s.REGION;
