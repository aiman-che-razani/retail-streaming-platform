-- =============================================================================
-- Set-level data-quality and reconciliation rules (event-contracts.md §5, SF-001..SF-008).
-- Record-level rules run in Spark; these need whole-table context.
--
-- Results are APPENDED to OPS.DQ_RESULTS (history is the point: "is SF-003 growing?").
-- The procedure records FAILs instead of raising: a reconciliation lag must not make the
-- task graph fail repeatedly and get auto-suspended. Alerting reads OPS.DQ_RESULTS through
-- `retail-reconcile` (see docs/runbooks/data-quality.md).
--
-- Grace period: rows loaded/staged in the last GRACE_MINUTES may legitimately not have
-- reached the next layer yet (15-min schedules), so they are excluded from completeness
-- checks. That is the "late-not-yet-loaded" explanation category, not a defect.
-- =============================================================================

CREATE OR REPLACE PROCEDURE {{DATABASE}}.OPS.SP_RUN_DQ_CHECKS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    run_id VARCHAR DEFAULT UUID_STRING();
    grace_minutes INTEGER DEFAULT 30;
    window_hours INTEGER DEFAULT 48;   -- checks cover recent loads only: bounded warehouse cost
    failures INTEGER DEFAULT 0;
BEGIN
    -- SF-001 (INFO): transport duplicates in RAW (same event_id loaded more than once).
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    WITH counts AS (
        SELECT 'pos_transactions' AS dataset, COUNT(*) - COUNT(DISTINCT EVENT_ID) AS dup FROM {{DATABASE}}.RAW.POS_TRANSACTIONS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
        UNION ALL SELECT 'inventory_movements', COUNT(*) - COUNT(DISTINCT EVENT_ID) FROM {{DATABASE}}.RAW.INVENTORY_MOVEMENTS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
        UNION ALL SELECT 'customer_events', COUNT(*) - COUNT(DISTINCT EVENT_ID) FROM {{DATABASE}}.RAW.CUSTOMER_EVENTS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
        UNION ALL SELECT 'product_events', COUNT(*) - COUNT(DISTINCT EVENT_ID) FROM {{DATABASE}}.RAW.PRODUCT_EVENTS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
    )
    SELECT :run_id, 'SF-001', 'Transport duplicates in RAW loaded in the last 48h (removed by STAGING)', 'INFO',
           IFF(SUM(dup) = 0, 'PASS', 'FAIL'), SUM(dup), 0, OBJECT_AGG(dataset, dup::VARIANT)
    FROM counts;

    -- SF-002 (WARN): business duplicates discarded in the last 24 hours.
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    SELECT :run_id, 'SF-002', 'Business duplicates (same business key, different event_id), 24h', 'WARN',
           IFF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*), 0,
           OBJECT_CONSTRUCT('sample_keys', ARRAY_SLICE(ARRAY_AGG(BUSINESS_KEY), 0, 10))
    FROM {{DATABASE}}.OPS.BUSINESS_DUPLICATES
    WHERE DETECTED_AT >= DATEADD(HOUR, -24, SYSDATE());

    -- SF-003 (WARN): fact rows attributed to inferred (late-arriving) dimension members.
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    WITH inferred AS (
        SELECT
            (SELECT COUNT(*) FROM {{DATABASE}}.ANALYTICS.FACT_SALES f
               JOIN {{DATABASE}}.ANALYTICS.DIM_PRODUCT p ON p.PRODUCT_KEY = f.PRODUCT_KEY
              WHERE p.IS_INFERRED) AS sales_inferred_product,
            (SELECT COUNT(*) FROM {{DATABASE}}.ANALYTICS.FACT_SALES f
               JOIN {{DATABASE}}.ANALYTICS.DIM_CUSTOMER c ON c.CUSTOMER_KEY = f.CUSTOMER_KEY
              WHERE c.IS_INFERRED) AS sales_inferred_customer,
            (SELECT COUNT(*) FROM {{DATABASE}}.ANALYTICS.DIM_PRODUCT WHERE IS_INFERRED) AS inferred_products
    )
    SELECT :run_id, 'SF-003', 'Fact rows pointing at inferred dimension members', 'WARN',
           IFF(sales_inferred_product + sales_inferred_customer = 0, 'PASS', 'FAIL'),
           sales_inferred_product + sales_inferred_customer, 0,
           OBJECT_CONSTRUCT('sales_inferred_product', sales_inferred_product,
                            'sales_inferred_customer', sales_inferred_customer,
                            'inferred_products', inferred_products)
    FROM inferred;

    -- SF-004 (ERROR): facts with unknown store/date/product (validation should make this 0).
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    WITH unknown AS (
        SELECT
            (SELECT COUNT(*) FROM {{DATABASE}}.ANALYTICS.FACT_SALES
              WHERE STORE_KEY = -1 OR DATE_KEY = -1 OR PRODUCT_KEY = -1 OR CUSTOMER_KEY = -1) AS sales,
            (SELECT COUNT(*) FROM {{DATABASE}}.ANALYTICS.FACT_INVENTORY
              WHERE STORE_KEY = -1 OR DATE_KEY = -1 OR PRODUCT_KEY = -1) AS inventory
    )
    SELECT :run_id, 'SF-004', 'Fact rows with Unknown (-1) dimension keys', 'ERROR',
           IFF(sales + inventory = 0, 'PASS', 'FAIL'), sales + inventory, 0,
           OBJECT_CONSTRUCT('fact_sales', sales, 'fact_inventory', inventory)
    FROM unknown;

    -- SF-005 (ERROR): Spark said N valid records per batch; RAW must hold exactly N distinct
    -- Kafka offsets for that batch. Counting distinct offsets makes re-loaded files harmless.
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    WITH audit AS (
        SELECT DATASET, SPARK_QUERY_ID, SPARK_BATCH_ID,
               SUM(RECORDS_VALID) AS valid, SUM(RECORDS_REJECTED) AS rejected, MIN(_LOADED_AT) AS loaded_at
        FROM (
            SELECT * FROM {{DATABASE}}.RAW.INGEST_BATCH_AUDIT
            WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
            QUALIFY ROW_NUMBER() OVER (PARTITION BY DATASET, SPARK_QUERY_ID, SPARK_BATCH_ID, KAFKA_TOPIC, KAFKA_PARTITION
                                       ORDER BY _LOADED_AT DESC) = 1
        )
        GROUP BY 1, 2, 3
    ),
    raw_counts AS (
        SELECT 'pos_transactions' AS DATASET, SPARK_QUERY_ID, SPARK_BATCH_ID,
               COUNT(DISTINCT KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET) AS loaded
        FROM {{DATABASE}}.RAW.POS_TRANSACTIONS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours - 1, SYSDATE()) GROUP BY 1, 2, 3
        UNION ALL
        SELECT 'inventory_movements', SPARK_QUERY_ID, SPARK_BATCH_ID, COUNT(DISTINCT KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
        FROM {{DATABASE}}.RAW.INVENTORY_MOVEMENTS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours - 1, SYSDATE()) GROUP BY 1, 2, 3
        UNION ALL
        SELECT 'customer_events', SPARK_QUERY_ID, SPARK_BATCH_ID, COUNT(DISTINCT KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
        FROM {{DATABASE}}.RAW.CUSTOMER_EVENTS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours - 1, SYSDATE()) GROUP BY 1, 2, 3
        UNION ALL
        SELECT 'product_events', SPARK_QUERY_ID, SPARK_BATCH_ID, COUNT(DISTINCT KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
        FROM {{DATABASE}}.RAW.PRODUCT_EVENTS
        WHERE _LOADED_AT >= DATEADD(HOUR, -:window_hours - 1, SYSDATE()) GROUP BY 1, 2, 3
    ),
    compared AS (
        SELECT a.DATASET, a.SPARK_QUERY_ID, a.SPARK_BATCH_ID, a.valid, COALESCE(r.loaded, 0) AS loaded
        FROM audit a
        LEFT JOIN raw_counts r
               ON r.DATASET = a.DATASET AND r.SPARK_QUERY_ID = a.SPARK_QUERY_ID AND r.SPARK_BATCH_ID = a.SPARK_BATCH_ID
        WHERE a.loaded_at < DATEADD(MINUTE, -:grace_minutes, SYSDATE())
    )
    SELECT :run_id, 'SF-005', 'Spark valid count = RAW loaded count per batch', 'ERROR',
           IFF(COUNT_IF(valid <> loaded) = 0, 'PASS', 'FAIL'), COUNT_IF(valid <> loaded), 0,
           OBJECT_CONSTRUCT(
               'batches_checked', COUNT(*),
               'records_valid', SUM(valid),
               'records_loaded', SUM(loaded),
               'mismatches', ARRAY_SLICE(ARRAY_AGG(IFF(valid <> loaded,
                   OBJECT_CONSTRUCT('dataset', DATASET, 'batch', SPARK_BATCH_ID, 'valid', valid, 'loaded', loaded), NULL)), 0, 20))
    FROM compared;

    -- SF-006 (ERROR): every RAW business key reaches STAGING, every staged line reaches the fact.
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    WITH raw_not_staged_pos AS (
        SELECT COUNT(DISTINCT r.EVENT:payload:transaction_id::VARCHAR) AS n
        FROM {{DATABASE}}.RAW.POS_TRANSACTIONS r
        WHERE r._LOADED_AT < DATEADD(MINUTE, -:grace_minutes, SYSDATE())
          AND r._LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
          AND NOT EXISTS (SELECT 1 FROM {{DATABASE}}.STAGING.STG_POS_TRANSACTION_LINES s
                          WHERE s.TRANSACTION_ID = r.EVENT:payload:transaction_id::VARCHAR)
    ),
    raw_not_staged_inv AS (
        SELECT COUNT(DISTINCT r.EVENT:payload:movement_id::VARCHAR) AS n
        FROM {{DATABASE}}.RAW.INVENTORY_MOVEMENTS r
        WHERE r._LOADED_AT < DATEADD(MINUTE, -:grace_minutes, SYSDATE())
          AND r._LOADED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
          AND NOT EXISTS (SELECT 1 FROM {{DATABASE}}.STAGING.STG_INVENTORY_MOVEMENTS s
                          WHERE s.MOVEMENT_ID = r.EVENT:payload:movement_id::VARCHAR)
    ),
    staged_not_in_fact AS (
        SELECT COUNT(*) AS n
        FROM {{DATABASE}}.STAGING.STG_POS_TRANSACTION_LINES s
        WHERE s._STAGED_AT < DATEADD(MINUTE, -:grace_minutes, SYSDATE())
          AND s._STAGED_AT >= DATEADD(HOUR, -:window_hours, SYSDATE())
          AND NOT EXISTS (SELECT 1 FROM {{DATABASE}}.ANALYTICS.FACT_SALES f
                          WHERE f.TRANSACTION_ID = s.TRANSACTION_ID AND f.LINE_NUMBER = s.LINE_NUMBER)
    )
    SELECT :run_id, 'SF-006', 'RAW -> STAGING -> FACT completeness', 'ERROR',
           IFF(p.n + i.n + f.n = 0, 'PASS', 'FAIL'), p.n + i.n + f.n, 0,
           OBJECT_CONSTRUCT('pos_transactions_not_staged', p.n,
                            'inventory_movements_not_staged', i.n,
                            'staged_lines_not_in_fact_sales', f.n)
    FROM raw_not_staged_pos p, raw_not_staged_inv i, staged_not_in_fact f;

    -- SF-007 (WARN): freshness - newest sale in the warehouse is more than 60 minutes old.
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    SELECT :run_id, 'SF-007', 'FACT_SALES freshness (minutes behind now)', 'WARN',
           IFF(COALESCE(DATEDIFF(MINUTE, MAX(EVENT_TS_UTC), SYSDATE()), 1e9) <= 60, 'PASS', 'FAIL'),
           DATEDIFF(MINUTE, MAX(EVENT_TS_UTC), SYSDATE()), 60,
           OBJECT_CONSTRUCT('max_event_ts_utc', MAX(EVENT_TS_UTC), 'last_loaded_at', MAX(_LOADED_AT))
    FROM {{DATABASE}}.ANALYTICS.FACT_SALES;

    -- SF-008 (WARN): the source's reported stock level should move exactly by each delta.
    INSERT INTO {{DATABASE}}.OPS.DQ_RESULTS (RUN_ID, RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, DETAILS)
    WITH ordered AS (
        SELECT STORE_ID, PRODUCT_ID, QUANTITY_DELTA, QUANTITY_ON_HAND_AFTER,
               LAG(QUANTITY_ON_HAND_AFTER) OVER (PARTITION BY STORE_ID, PRODUCT_ID
                                                 ORDER BY EVENT_TS_UTC, MOVEMENT_ID) AS previous_on_hand
        FROM {{DATABASE}}.ANALYTICS.FACT_INVENTORY
        WHERE EVENT_TS_UTC >= DATEADD(HOUR, -24, SYSDATE())
    )
    SELECT :run_id, 'SF-008', 'Inventory on-hand progression matches movement deltas (24h)', 'WARN',
           IFF(COUNT_IF(QUANTITY_ON_HAND_AFTER - previous_on_hand <> QUANTITY_DELTA) = 0, 'PASS', 'FAIL'),
           COUNT_IF(QUANTITY_ON_HAND_AFTER - previous_on_hand <> QUANTITY_DELTA), 0,
           OBJECT_CONSTRUCT('movements_checked', COUNT_IF(previous_on_hand IS NOT NULL))
    FROM ordered;

    SELECT COUNT(*) INTO :failures
    FROM {{DATABASE}}.OPS.DQ_RESULTS
    WHERE RUN_ID = :run_id AND STATUS = 'FAIL' AND SEVERITY = 'ERROR';

    RETURN 'dq run ' || run_id || ': ' || failures || ' ERROR-severity failures';
END;
$$;

-- Latest result per rule (what dashboards and `retail-reconcile` read).
CREATE OR REPLACE VIEW {{DATABASE}}.OPS.VW_DQ_LATEST COPY GRANTS COPY GRANTS AS
SELECT *
FROM {{DATABASE}}.OPS.DQ_RESULTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY CHECKED_AT DESC) = 1;
