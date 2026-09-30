-- =============================================================================
-- RAW -> STAGING procedures (data-model.md §3, §5).
--
-- Why stored procedures: each unit consumes a stream with SEVERAL statements (log business
-- duplicates + MERGE). They must commit atomically: if the MERGE failed after the log insert
-- committed, the stream offset would advance and the rows would be lost. Inside one explicit
-- transaction every read of a stream sees the same rows, and the offset only advances on
-- COMMIT. Any error -> ROLLBACK -> the next run reprocesses the same rows (FM-13).
--
-- Dedup rules:
--   * transport duplicates (same event_id) and business duplicates (same transaction_id /
--     movement_id with another event_id) collapse to ONE row. Within one run the earliest
--     event (event time, then load order) wins; across runs the event staged first wins;
--   * business duplicates are recorded in OPS.BUSINESS_DUPLICATES (rule SF-002).
-- =============================================================================

-- ----------------------------------------------------------------------------- stores (SCD1)
CREATE OR REPLACE PROCEDURE {{DATABASE}}.STAGING.SP_STAGE_STORES()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    MERGE INTO {{DATABASE}}.STAGING.STG_STORES t
    USING (
        SELECT *
        FROM {{DATABASE}}.RAW.STORE_REFERENCE_STREAM
        QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY _LOADED_AT DESC, _FILE_NAME DESC) = 1
    ) s
    ON t.STORE_ID = s.STORE_ID
    WHEN MATCHED THEN UPDATE SET
        STORE_NAME = s.STORE_NAME, CITY = s.CITY, STATE = s.STATE, REGION = s.REGION,
        STORE_FORMAT = s.STORE_FORMAT, SIZE_SQM = s.SIZE_SQM, OPENED_DATE = s.OPENED_DATE,
        TIMEZONE = COALESCE(s.TIMEZONE, 'Asia/Kuala_Lumpur'), _UPDATED_AT = SYSDATE()
    WHEN NOT MATCHED THEN INSERT
        (STORE_ID, STORE_NAME, CITY, STATE, REGION, STORE_FORMAT, SIZE_SQM, OPENED_DATE, TIMEZONE)
        VALUES (s.STORE_ID, s.STORE_NAME, s.CITY, s.STATE, s.REGION, s.STORE_FORMAT, s.SIZE_SQM,
                s.OPENED_DATE, COALESCE(s.TIMEZONE, 'Asia/Kuala_Lumpur'));
    RETURN 'stores merged: ' || SQLROWCOUNT;
END;
$$;

-- ----------------------------------------------------------------------------- POS lines
CREATE OR REPLACE PROCEDURE {{DATABASE}}.STAGING.SP_STAGE_POS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    staged INTEGER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    -- SF-002: record business duplicates before they are discarded.
    INSERT INTO {{DATABASE}}.OPS.BUSINESS_DUPLICATES
        (DATASET, BUSINESS_KEY, KEPT_EVENT_ID, DUPLICATE_EVENT_ID, DUPLICATE_EVENT_TIMESTAMP,
         KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
    WITH incoming AS (
        SELECT EVENT_ID, EVENT_TIMESTAMP, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, _LOADED_AT,
               EVENT:payload:transaction_id::VARCHAR AS BUSINESS_KEY
        FROM {{DATABASE}}.RAW.POS_TRANSACTIONS_STREAM
    ),
    first_incoming AS (
        SELECT * FROM incoming
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BUSINESS_KEY
                                   ORDER BY EVENT_TIMESTAMP, _LOADED_AT, KAFKA_PARTITION, KAFKA_OFFSET) = 1
    ),
    already_staged AS (
        SELECT DISTINCT TRANSACTION_ID AS BUSINESS_KEY, EVENT_ID
        FROM {{DATABASE}}.STAGING.STG_POS_TRANSACTION_LINES
        WHERE TRANSACTION_ID IN (SELECT BUSINESS_KEY FROM incoming)
    ),
    winners AS (
        SELECT f.BUSINESS_KEY, COALESCE(a.EVENT_ID, f.EVENT_ID) AS KEPT_EVENT_ID
        FROM first_incoming f
        LEFT JOIN already_staged a ON a.BUSINESS_KEY = f.BUSINESS_KEY
    )
    SELECT 'pos_transactions', i.BUSINESS_KEY, w.KEPT_EVENT_ID, i.EVENT_ID, i.EVENT_TIMESTAMP,
           i.KAFKA_TOPIC, i.KAFKA_PARTITION, i.KAFKA_OFFSET
    FROM incoming i
    JOIN winners w ON w.BUSINESS_KEY = i.BUSINESS_KEY
    WHERE i.EVENT_ID <> w.KEPT_EVENT_ID
      AND NOT EXISTS (SELECT 1 FROM {{DATABASE}}.OPS.BUSINESS_DUPLICATES b
                      WHERE b.DUPLICATE_EVENT_ID = i.EVENT_ID)   -- redelivery: already logged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY i.EVENT_ID ORDER BY i.KAFKA_PARTITION, i.KAFKA_OFFSET) = 1;

    MERGE INTO {{DATABASE}}.STAGING.STG_POS_TRANSACTION_LINES t
    USING (
        WITH first_events AS (
            SELECT EVENT_ID, EVENT_TIMESTAMP, EVENT, DQ_WARNINGS, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
            FROM {{DATABASE}}.RAW.POS_TRANSACTIONS_STREAM
            QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT:payload:transaction_id::VARCHAR
                                       ORDER BY EVENT_TIMESTAMP, _LOADED_AT, KAFKA_PARTITION, KAFKA_OFFSET) = 1
        ),
        lines AS (
            SELECT e.EVENT_ID, e.EVENT_TIMESTAMP, e.DQ_WARNINGS, e.KAFKA_TOPIC, e.KAFKA_PARTITION,
                   e.KAFKA_OFFSET, e.EVENT:payload AS P, l.VALUE AS L
            FROM first_events e,
                 LATERAL FLATTEN(INPUT => e.EVENT:payload:line_items) l
        ),
        typed AS (
            SELECT
                P:transaction_id::VARCHAR            AS TRANSACTION_ID,
                L:line_number::NUMBER(5,0)           AS LINE_NUMBER,
                EVENT_ID,
                P:store_id::VARCHAR                  AS STORE_ID,
                P:register_id::VARCHAR               AS REGISTER_ID,
                P:customer_id::VARCHAR               AS CUSTOMER_ID,
                IFF(P:payment_method::VARCHAR IN ('CASH', 'CARD', 'EWALLET'),
                    P:payment_method::VARCHAR, 'UNKNOWN') AS PAYMENT_METHOD,
                P:currency::VARCHAR                  AS CURRENCY,
                L:product_id::VARCHAR                AS PRODUCT_ID,
                L:quantity::NUMBER(10,0)             AS QUANTITY,
                L:unit_price::NUMBER(12,2)           AS UNIT_PRICE,
                L:discount_amount::NUMBER(12,2)      AS DISCOUNT_AMOUNT,
                P:total_amount::NUMBER(14,2)         AS TRANSACTION_TOTAL,
                EVENT_TIMESTAMP                      AS EVENT_TS_UTC,
                DQ_WARNINGS, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
            FROM lines
        )
        SELECT t.*,
               t.QUANTITY * t.UNIT_PRICE                          AS GROSS_AMOUNT,
               t.QUANTITY * t.UNIT_PRICE - t.DISCOUNT_AMOUNT      AS NET_AMOUNT,
               -- Local business time from the store's timezone (all UTC upstream).
               CONVERT_TIMEZONE('UTC', COALESCE(s.TIMEZONE, 'Asia/Kuala_Lumpur'), t.EVENT_TS_UTC) AS EVENT_TS_LOCAL
        FROM typed t
        LEFT JOIN {{DATABASE}}.STAGING.STG_STORES s ON s.STORE_ID = t.STORE_ID
    ) src
    ON t.TRANSACTION_ID = src.TRANSACTION_ID AND t.LINE_NUMBER = src.LINE_NUMBER
    -- Whole-basket semantics: a transaction already staged is never extended by lines from a
    -- later (discarded) event, even if that event had more lines.
    WHEN NOT MATCHED AND NOT EXISTS (
        SELECT 1 FROM {{DATABASE}}.STAGING.STG_POS_TRANSACTION_LINES x
        WHERE x.TRANSACTION_ID = src.TRANSACTION_ID
    ) THEN INSERT
        (TRANSACTION_ID, LINE_NUMBER, EVENT_ID, STORE_ID, REGISTER_ID, CUSTOMER_ID, PAYMENT_METHOD,
         CURRENCY, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, GROSS_AMOUNT, NET_AMOUNT,
         TRANSACTION_TOTAL, EVENT_TS_UTC, EVENT_TS_LOCAL, BUSINESS_DATE, LOCAL_HOUR, DQ_WARNINGS,
         KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
    VALUES
        (src.TRANSACTION_ID, src.LINE_NUMBER, src.EVENT_ID, src.STORE_ID, src.REGISTER_ID,
         src.CUSTOMER_ID, src.PAYMENT_METHOD, src.CURRENCY, src.PRODUCT_ID, src.QUANTITY,
         src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.GROSS_AMOUNT, src.NET_AMOUNT,
         src.TRANSACTION_TOTAL, src.EVENT_TS_UTC, src.EVENT_TS_LOCAL,
         TO_DATE(src.EVENT_TS_LOCAL), HOUR(src.EVENT_TS_LOCAL), src.DQ_WARNINGS,
         src.KAFKA_TOPIC, src.KAFKA_PARTITION, src.KAFKA_OFFSET);
    staged := SQLROWCOUNT;

    COMMIT;
    RETURN 'pos lines staged: ' || staged;
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

-- ----------------------------------------------------------------------------- inventory
CREATE OR REPLACE PROCEDURE {{DATABASE}}.STAGING.SP_STAGE_INVENTORY()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    staged INTEGER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    INSERT INTO {{DATABASE}}.OPS.BUSINESS_DUPLICATES
        (DATASET, BUSINESS_KEY, KEPT_EVENT_ID, DUPLICATE_EVENT_ID, DUPLICATE_EVENT_TIMESTAMP,
         KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
    WITH incoming AS (
        SELECT EVENT_ID, EVENT_TIMESTAMP, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, _LOADED_AT,
               EVENT:payload:movement_id::VARCHAR AS BUSINESS_KEY
        FROM {{DATABASE}}.RAW.INVENTORY_MOVEMENTS_STREAM
    ),
    first_incoming AS (
        SELECT * FROM incoming
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BUSINESS_KEY
                                   ORDER BY EVENT_TIMESTAMP, _LOADED_AT, KAFKA_PARTITION, KAFKA_OFFSET) = 1
    ),
    already_staged AS (
        SELECT MOVEMENT_ID AS BUSINESS_KEY, EVENT_ID
        FROM {{DATABASE}}.STAGING.STG_INVENTORY_MOVEMENTS
        WHERE MOVEMENT_ID IN (SELECT BUSINESS_KEY FROM incoming)
    ),
    winners AS (
        SELECT f.BUSINESS_KEY, COALESCE(a.EVENT_ID, f.EVENT_ID) AS KEPT_EVENT_ID
        FROM first_incoming f
        LEFT JOIN already_staged a ON a.BUSINESS_KEY = f.BUSINESS_KEY
    )
    SELECT 'inventory_movements', i.BUSINESS_KEY, w.KEPT_EVENT_ID, i.EVENT_ID, i.EVENT_TIMESTAMP,
           i.KAFKA_TOPIC, i.KAFKA_PARTITION, i.KAFKA_OFFSET
    FROM incoming i
    JOIN winners w ON w.BUSINESS_KEY = i.BUSINESS_KEY
    WHERE i.EVENT_ID <> w.KEPT_EVENT_ID
      AND NOT EXISTS (SELECT 1 FROM {{DATABASE}}.OPS.BUSINESS_DUPLICATES b
                      WHERE b.DUPLICATE_EVENT_ID = i.EVENT_ID)   -- redelivery: already logged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY i.EVENT_ID ORDER BY i.KAFKA_PARTITION, i.KAFKA_OFFSET) = 1;

    MERGE INTO {{DATABASE}}.STAGING.STG_INVENTORY_MOVEMENTS t
    USING (
        WITH first_events AS (
            SELECT EVENT_ID, EVENT_TIMESTAMP, EVENT, CAUSATION_ID, DQ_WARNINGS, KAFKA_TOPIC,
                   KAFKA_PARTITION, KAFKA_OFFSET
            FROM {{DATABASE}}.RAW.INVENTORY_MOVEMENTS_STREAM
            QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT:payload:movement_id::VARCHAR
                                       ORDER BY EVENT_TIMESTAMP, _LOADED_AT, KAFKA_PARTITION, KAFKA_OFFSET) = 1
        ),
        typed AS (
            SELECT
                EVENT:payload:movement_id::VARCHAR                    AS MOVEMENT_ID,
                EVENT_ID,
                EVENT:payload:store_id::VARCHAR                       AS STORE_ID,
                EVENT:payload:product_id::VARCHAR                     AS PRODUCT_ID,
                EVENT:payload:movement_type::VARCHAR                  AS MOVEMENT_TYPE,
                EVENT:payload:quantity_delta::NUMBER(10,0)            AS QUANTITY_DELTA,
                EVENT:payload:quantity_on_hand_after::NUMBER(12,0)    AS QUANTITY_ON_HAND_AFTER,
                EVENT:payload:reason_code::VARCHAR                    AS REASON_CODE,
                EVENT:payload:reference_id::VARCHAR                   AS REFERENCE_ID,
                EVENT:payload:unit_cost::NUMBER(12,2)                 AS UNIT_COST,
                CAUSATION_ID,
                EVENT_TIMESTAMP                                       AS EVENT_TS_UTC,
                DQ_WARNINGS, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
            FROM first_events
        )
        SELECT t.*,
               CONVERT_TIMEZONE('UTC', COALESCE(s.TIMEZONE, 'Asia/Kuala_Lumpur'), t.EVENT_TS_UTC) AS EVENT_TS_LOCAL
        FROM typed t
        LEFT JOIN {{DATABASE}}.STAGING.STG_STORES s ON s.STORE_ID = t.STORE_ID
    ) src
    ON t.MOVEMENT_ID = src.MOVEMENT_ID
    WHEN NOT MATCHED THEN INSERT
        (MOVEMENT_ID, EVENT_ID, STORE_ID, PRODUCT_ID, MOVEMENT_TYPE, QUANTITY_DELTA,
         QUANTITY_ON_HAND_AFTER, REASON_CODE, REFERENCE_ID, UNIT_COST, CAUSATION_ID, EVENT_TS_UTC,
         EVENT_TS_LOCAL, BUSINESS_DATE, DQ_WARNINGS, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)
    VALUES
        (src.MOVEMENT_ID, src.EVENT_ID, src.STORE_ID, src.PRODUCT_ID, src.MOVEMENT_TYPE,
         src.QUANTITY_DELTA, src.QUANTITY_ON_HAND_AFTER, src.REASON_CODE, src.REFERENCE_ID,
         src.UNIT_COST, src.CAUSATION_ID, src.EVENT_TS_UTC, src.EVENT_TS_LOCAL,
         TO_DATE(src.EVENT_TS_LOCAL), src.DQ_WARNINGS, src.KAFKA_TOPIC, src.KAFKA_PARTITION,
         src.KAFKA_OFFSET);
    staged := SQLROWCOUNT;

    COMMIT;
    RETURN 'inventory movements staged: ' || staged;
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

-- ----------------------------------------------------------------------------- customer changes (all versions)
CREATE OR REPLACE PROCEDURE {{DATABASE}}.STAGING.SP_STAGE_CUSTOMERS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    MERGE INTO {{DATABASE}}.STAGING.STG_CUSTOMER_CHANGES t
    USING (
        SELECT
            EVENT_ID,
            EVENT:payload:customer_id::VARCHAR           AS CUSTOMER_ID,
            EVENT_TYPE,
            EVENT_TIMESTAMP                              AS EVENT_TS_UTC,
            IFF(EVENT:payload:loyalty_tier::VARCHAR IN ('BASIC', 'SILVER', 'GOLD', 'PLATINUM'),
                EVENT:payload:loyalty_tier::VARCHAR, 'UNKNOWN') AS LOYALTY_TIER,
            EVENT:payload:home_store_id::VARCHAR         AS HOME_STORE_ID,
            EVENT:payload:city::VARCHAR                  AS CITY,
            EVENT:payload:state::VARCHAR                 AS STATE,
            TRY_TO_DATE(EVENT:payload:signup_date::VARCHAR) AS SIGNUP_DATE,
            EVENT:payload:birth_year::NUMBER(4,0)        AS BIRTH_YEAR,
            EVENT:payload:email_sha256::VARCHAR          AS EMAIL_SHA256,
            EVENT:payload:marketing_opt_in::BOOLEAN      AS MARKETING_OPT_IN
        FROM {{DATABASE}}.RAW.CUSTOMER_EVENTS_STREAM
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY _LOADED_AT) = 1
    ) s
    ON t.EVENT_ID = s.EVENT_ID
    WHEN NOT MATCHED THEN INSERT
        (EVENT_ID, CUSTOMER_ID, EVENT_TYPE, EVENT_TS_UTC, LOYALTY_TIER, HOME_STORE_ID, CITY, STATE,
         SIGNUP_DATE, BIRTH_YEAR, EMAIL_SHA256, MARKETING_OPT_IN)
    VALUES
        (s.EVENT_ID, s.CUSTOMER_ID, s.EVENT_TYPE, s.EVENT_TS_UTC, s.LOYALTY_TIER, s.HOME_STORE_ID,
         s.CITY, s.STATE, s.SIGNUP_DATE, s.BIRTH_YEAR, s.EMAIL_SHA256, s.MARKETING_OPT_IN);
    RETURN 'customer changes staged: ' || SQLROWCOUNT;
END;
$$;

-- ----------------------------------------------------------------------------- product changes (all versions)
CREATE OR REPLACE PROCEDURE {{DATABASE}}.STAGING.SP_STAGE_PRODUCTS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    MERGE INTO {{DATABASE}}.STAGING.STG_PRODUCT_CHANGES t
    USING (
        SELECT
            EVENT_ID,
            EVENT:payload:product_id::VARCHAR          AS PRODUCT_ID,
            EVENT_TYPE,
            EVENT_TIMESTAMP                            AS EVENT_TS_UTC,
            EVENT:payload:product_name::VARCHAR        AS PRODUCT_NAME,
            EVENT:payload:brand::VARCHAR               AS BRAND,
            EVENT:payload:category::VARCHAR            AS CATEGORY,
            EVENT:payload:subcategory::VARCHAR         AS SUBCATEGORY,
            EVENT:payload:list_price::NUMBER(12,2)     AS LIST_PRICE,
            EVENT:payload:unit_cost::NUMBER(12,2)      AS UNIT_COST,
            EVENT:payload:currency::VARCHAR            AS CURRENCY,
            EVENT:payload:unit_of_measure::VARCHAR     AS UNIT_OF_MEASURE,
            EVENT:payload:is_active::BOOLEAN           AS IS_ACTIVE
        FROM {{DATABASE}}.RAW.PRODUCT_EVENTS_STREAM
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY _LOADED_AT) = 1
    ) s
    ON t.EVENT_ID = s.EVENT_ID
    WHEN NOT MATCHED THEN INSERT
        (EVENT_ID, PRODUCT_ID, EVENT_TYPE, EVENT_TS_UTC, PRODUCT_NAME, BRAND, CATEGORY, SUBCATEGORY,
         LIST_PRICE, UNIT_COST, CURRENCY, UNIT_OF_MEASURE, IS_ACTIVE)
    VALUES
        (s.EVENT_ID, s.PRODUCT_ID, s.EVENT_TYPE, s.EVENT_TS_UTC, s.PRODUCT_NAME, s.BRAND,
         s.CATEGORY, s.SUBCATEGORY, s.LIST_PRICE, s.UNIT_COST, s.CURRENCY, s.UNIT_OF_MEASURE,
         s.IS_ACTIVE);
    RETURN 'product changes staged: ' || SQLROWCOUNT;
END;
$$;

-- ----------------------------------------------------------------------------- orchestration
-- Stores first: POS/inventory staging needs the store timezone.
CREATE OR REPLACE PROCEDURE {{DATABASE}}.STAGING.SP_STAGE_ALL()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    CALL {{DATABASE}}.STAGING.SP_STAGE_STORES();
    CALL {{DATABASE}}.STAGING.SP_STAGE_PRODUCTS();
    CALL {{DATABASE}}.STAGING.SP_STAGE_CUSTOMERS();
    CALL {{DATABASE}}.STAGING.SP_STAGE_POS();
    CALL {{DATABASE}}.STAGING.SP_STAGE_INVENTORY();
    RETURN 'staging complete';
END;
$$;
