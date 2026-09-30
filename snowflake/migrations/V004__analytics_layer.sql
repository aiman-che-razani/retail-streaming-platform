-- =============================================================================
-- V004 ANALYTICS layer: star schema (data-model.md §4, ADR-006).
--
-- Surrogate keys are deterministic: MD5_NUMBER_LOWER64(natural_key || '|' || effective_from)
-- for SCD2 dimensions, MD5_NUMBER_LOWER64(natural_key) for SCD1. Reserved members:
--   -1 = Unknown (natural key missing/invalid)   -2 = Not applicable (e.g. guest checkout)
-- Primary/foreign keys are declared for documentation and BI tools; Snowflake does not
-- enforce them (only NOT NULL), so integrity is verified by DQ rules instead.
-- =============================================================================

USE SCHEMA {{DATABASE}}.ANALYTICS;

-- ----------------------------------------------------------------------------- DIM_DATE
CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_KEY            NUMBER(8,0)  NOT NULL PRIMARY KEY,   -- YYYYMMDD
    FULL_DATE           DATE         NOT NULL,
    DAY_OF_WEEK_ISO     NUMBER(1,0)  NOT NULL,               -- 1 = Monday
    DAY_NAME            VARCHAR(3)   NOT NULL,
    DAY_OF_MONTH        NUMBER(2,0)  NOT NULL,
    DAY_OF_YEAR         NUMBER(3,0)  NOT NULL,
    ISO_WEEK            NUMBER(2,0)  NOT NULL,
    MONTH_NUMBER        NUMBER(2,0)  NOT NULL,
    MONTH_NAME          VARCHAR(3)   NOT NULL,
    YEAR_MONTH          VARCHAR(7)   NOT NULL,               -- 2026-09
    QUARTER             NUMBER(1,0)  NOT NULL,
    YEAR                NUMBER(4,0)  NOT NULL,
    IS_WEEKEND          BOOLEAN      NOT NULL,
    IS_PUBLIC_HOLIDAY   BOOLEAN      NOT NULL,
    HOLIDAY_NAME        VARCHAR
)
COMMENT = 'Grain: one row per calendar date 2020-01-01..2030-12-31, plus -1 Unknown.';

-- Populate once (versioned migration runs exactly once). Fixed-date national holidays only;
-- lunar holidays (Hari Raya, Chinese New Year, Deepavali) move each year and are not modelled.
-- Guarded so a re-run after a partial failure never duplicates rows. ROW_NUMBER, not SEQ4(),
-- because SEQ4 is not guaranteed gap-free.
INSERT INTO DIM_DATE
WITH days AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2020-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')),
    d,
    DAYOFWEEKISO(d),
    DAYNAME(d),
    DAY(d),
    DAYOFYEAR(d),
    WEEKISO(d),
    MONTH(d),
    MONTHNAME(d),
    TO_CHAR(d, 'YYYY-MM'),
    QUARTER(d),
    YEAR(d),
    DAYOFWEEKISO(d) IN (6, 7),
    TO_CHAR(d, 'MM-DD') IN ('01-01', '05-01', '08-31', '09-16', '12-25'),
    CASE TO_CHAR(d, 'MM-DD')
        WHEN '01-01' THEN 'New Year''s Day'
        WHEN '05-01' THEN 'Labour Day'
        WHEN '08-31' THEN 'National Day'
        WHEN '09-16' THEN 'Malaysia Day'
        WHEN '12-25' THEN 'Christmas Day'
    END
FROM days
WHERE d <= '2030-12-31'
  AND NOT EXISTS (SELECT 1 FROM DIM_DATE x WHERE x.FULL_DATE = days.d);

INSERT INTO DIM_DATE
SELECT -1, '1900-01-01'::DATE, 1, 'Unk', 1, 1, 1, 1, 'Unk', 'Unknown', 1, 1900, FALSE, FALSE, 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE WHERE DATE_KEY = -1);

-- ----------------------------------------------------------------------------- DIM_STORE (SCD1)
CREATE TABLE IF NOT EXISTS DIM_STORE (
    STORE_KEY       NUMBER(38,0)  NOT NULL PRIMARY KEY,
    STORE_ID        VARCHAR       NOT NULL UNIQUE,
    STORE_NAME      VARCHAR       NOT NULL,
    CITY            VARCHAR,
    STATE           VARCHAR,
    REGION          VARCHAR,
    STORE_FORMAT    VARCHAR,
    SIZE_SQM        NUMBER(10,0),
    OPENED_DATE     DATE,
    TIMEZONE        VARCHAR,
    _UPDATED_AT     TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'SCD Type 1: store attributes are overwritten (history restated under the current structure).';

INSERT INTO DIM_STORE (STORE_KEY, STORE_ID, STORE_NAME, REGION, STORE_FORMAT)
SELECT * FROM VALUES
    (-1, '-1', 'Unknown', 'Unknown', 'Unknown'),
    (-2, '-2', 'Not applicable', 'Not applicable', 'Not applicable')
WHERE NOT EXISTS (SELECT 1 FROM DIM_STORE WHERE STORE_KEY IN (-1, -2));

-- ----------------------------------------------------------------------------- DIM_PRODUCT (SCD2/1)
CREATE TABLE IF NOT EXISTS DIM_PRODUCT (
    PRODUCT_KEY         NUMBER(38,0)  NOT NULL PRIMARY KEY,
    PRODUCT_ID          VARCHAR       NOT NULL,
    -- SCD Type 1 (latest value applied to every version)
    PRODUCT_NAME        VARCHAR       NOT NULL,
    UNIT_OF_MEASURE     VARCHAR,
    -- SCD Type 2 (a change creates a new version)
    BRAND               VARCHAR       NOT NULL,
    CATEGORY            VARCHAR       NOT NULL,
    SUBCATEGORY         VARCHAR       NOT NULL,
    LIST_PRICE          NUMBER(12,2),
    UNIT_COST           NUMBER(12,2),
    IS_ACTIVE           BOOLEAN,
    -- version metadata
    EFFECTIVE_FROM_UTC  TIMESTAMP_NTZ NOT NULL,
    EFFECTIVE_TO_UTC    TIMESTAMP_NTZ NOT NULL,   -- exclusive; 9999-12-31 for the current version
    IS_CURRENT          BOOLEAN       NOT NULL,
    IS_INFERRED         BOOLEAN       NOT NULL,   -- placeholder for a late-arriving product
    VERSION_NUMBER      NUMBER(6,0)   NOT NULL,
    SOURCE_EVENT_ID     VARCHAR,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'SCD2 on brand/category/subcategory/price/cost/active; SCD1 on name/UOM. Inferred members for late-arriving products.';

INSERT INTO DIM_PRODUCT (PRODUCT_KEY, PRODUCT_ID, PRODUCT_NAME, BRAND, CATEGORY, SUBCATEGORY,
                         EFFECTIVE_FROM_UTC, EFFECTIVE_TO_UTC, IS_CURRENT, IS_INFERRED, VERSION_NUMBER)
SELECT * FROM VALUES
    (-1, '-1', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE, 1),
    (-2, '-2', 'Not applicable', 'Not applicable', 'Not applicable', 'Not applicable', '1900-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE, 1)
WHERE NOT EXISTS (SELECT 1 FROM DIM_PRODUCT WHERE PRODUCT_KEY IN (-1, -2));

-- ----------------------------------------------------------------------------- DIM_CUSTOMER (SCD2/1)
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    CUSTOMER_KEY        NUMBER(38,0)  NOT NULL PRIMARY KEY,
    CUSTOMER_ID         VARCHAR       NOT NULL,
    -- SCD Type 2
    LOYALTY_TIER        VARCHAR       NOT NULL,
    HOME_STORE_ID       VARCHAR,
    CITY                VARCHAR,
    STATE               VARCHAR,
    -- SCD Type 1 (corrections and consent must always be current)
    SIGNUP_DATE         DATE,
    AGE_BAND            VARCHAR,
    MARKETING_OPT_IN    BOOLEAN,
    HAS_EMAIL           BOOLEAN,
    -- version metadata
    EFFECTIVE_FROM_UTC  TIMESTAMP_NTZ NOT NULL,
    EFFECTIVE_TO_UTC    TIMESTAMP_NTZ NOT NULL,
    IS_CURRENT          BOOLEAN       NOT NULL,
    IS_INFERRED         BOOLEAN       NOT NULL,
    VERSION_NUMBER      NUMBER(6,0)   NOT NULL,
    SOURCE_EVENT_ID     VARCHAR,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'SCD2 on tier and location; SCD1 on consent/demographics. No direct PII (email is only a has_email flag).';

INSERT INTO DIM_CUSTOMER (CUSTOMER_KEY, CUSTOMER_ID, LOYALTY_TIER, EFFECTIVE_FROM_UTC,
                          EFFECTIVE_TO_UTC, IS_CURRENT, IS_INFERRED, VERSION_NUMBER)
SELECT * FROM VALUES
    (-1, '-1', 'Unknown', '1900-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE, 1),
    (-2, '-2', 'Guest (no loyalty account)', '1900-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE, 1)
WHERE NOT EXISTS (SELECT 1 FROM DIM_CUSTOMER WHERE CUSTOMER_KEY IN (-1, -2));

-- ----------------------------------------------------------------------------- FACT_SALES
CREATE TABLE IF NOT EXISTS FACT_SALES (
    SALES_LINE_KEY     NUMBER(38,0)  NOT NULL PRIMARY KEY,   -- hash(transaction_id, line_number)
    DATE_KEY           NUMBER(8,0)   NOT NULL REFERENCES DIM_DATE (DATE_KEY),
    STORE_KEY          NUMBER(38,0)  NOT NULL REFERENCES DIM_STORE (STORE_KEY),
    PRODUCT_KEY        NUMBER(38,0)  NOT NULL REFERENCES DIM_PRODUCT (PRODUCT_KEY),
    CUSTOMER_KEY       NUMBER(38,0)  NOT NULL REFERENCES DIM_CUSTOMER (CUSTOMER_KEY),
    -- degenerate dimensions / attributes
    TRANSACTION_ID     VARCHAR       NOT NULL,
    LINE_NUMBER        NUMBER(5,0)   NOT NULL,
    PAYMENT_METHOD     VARCHAR       NOT NULL,
    REGISTER_ID        VARCHAR,
    -- durable natural keys (re-keying, debugging)
    STORE_ID           VARCHAR       NOT NULL,
    PRODUCT_ID         VARCHAR       NOT NULL,
    CUSTOMER_ID        VARCHAR,
    -- time
    EVENT_TS_UTC       TIMESTAMP_NTZ NOT NULL,
    EVENT_TS_LOCAL     TIMESTAMP_NTZ NOT NULL,
    LOCAL_HOUR         NUMBER(2,0)   NOT NULL,
    -- measures
    QUANTITY           NUMBER(10,0)  NOT NULL,    -- additive
    UNIT_PRICE         NUMBER(12,2)  NOT NULL,    -- non-additive (average, never sum)
    GROSS_AMOUNT       NUMBER(14,2)  NOT NULL,    -- additive
    DISCOUNT_AMOUNT    NUMBER(14,2)  NOT NULL,    -- additive
    NET_AMOUNT         NUMBER(14,2)  NOT NULL,    -- additive: REVENUE
    COST_AMOUNT        NUMBER(14,2),              -- additive (NULL while product is inferred)
    -- lineage
    EVENT_ID           VARCHAR       NOT NULL,
    _LOADED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'GRAIN: one row per product line item of a completed POS transaction (transaction_id, line_number).';

-- ----------------------------------------------------------------------------- FACT_INVENTORY
CREATE TABLE IF NOT EXISTS FACT_INVENTORY (
    INVENTORY_MOVEMENT_KEY  NUMBER(38,0)  NOT NULL PRIMARY KEY,   -- hash(movement_id)
    DATE_KEY                NUMBER(8,0)   NOT NULL REFERENCES DIM_DATE (DATE_KEY),
    STORE_KEY               NUMBER(38,0)  NOT NULL REFERENCES DIM_STORE (STORE_KEY),
    PRODUCT_KEY             NUMBER(38,0)  NOT NULL REFERENCES DIM_PRODUCT (PRODUCT_KEY),
    MOVEMENT_ID             VARCHAR       NOT NULL,
    MOVEMENT_TYPE           VARCHAR       NOT NULL,
    REASON_CODE             VARCHAR,
    REFERENCE_ID            VARCHAR,
    STORE_ID                VARCHAR       NOT NULL,
    PRODUCT_ID              VARCHAR       NOT NULL,
    EVENT_TS_UTC            TIMESTAMP_NTZ NOT NULL,
    EVENT_TS_LOCAL          TIMESTAMP_NTZ NOT NULL,
    QUANTITY_DELTA          NUMBER(10,0)  NOT NULL,   -- additive (net stock change)
    QUANTITY_ON_HAND_AFTER  NUMBER(12,0)  NOT NULL,   -- SEMI-additive: never sum across time
    UNIT_COST               NUMBER(12,2),
    MOVEMENT_VALUE          NUMBER(14,2),             -- additive
    EVENT_ID                VARCHAR       NOT NULL,
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'GRAIN: one row per inventory movement event (movement_id).';

-- ----------------------------------------------------------------------------- FACT_INVENTORY_SNAPSHOT
CREATE TABLE IF NOT EXISTS FACT_INVENTORY_SNAPSHOT (
    DATE_KEY                NUMBER(8,0)   NOT NULL REFERENCES DIM_DATE (DATE_KEY),
    STORE_KEY               NUMBER(38,0)  NOT NULL REFERENCES DIM_STORE (STORE_KEY),
    PRODUCT_KEY             NUMBER(38,0)  NOT NULL REFERENCES DIM_PRODUCT (PRODUCT_KEY),
    STORE_ID                VARCHAR       NOT NULL,
    PRODUCT_ID              VARCHAR       NOT NULL,
    CLOSING_ON_HAND         NUMBER(12,0)  NOT NULL,   -- SEMI-additive
    CLOSING_VALUE           NUMBER(14,2),             -- SEMI-additive
    UNITS_RECEIVED          NUMBER(12,0)  NOT NULL,   -- additive for the day
    UNITS_SOLD              NUMBER(12,0)  NOT NULL,   -- additive for the day
    UNITS_ADJUSTED          NUMBER(12,0)  NOT NULL,   -- additive for the day (signed)
    AVG_DAILY_UNITS_SOLD_7D NUMBER(12,2)  NOT NULL,
    IS_BELOW_REORDER_POINT  BOOLEAN       NOT NULL,   -- closing < 3 days of cover
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    PRIMARY KEY (DATE_KEY, STORE_KEY, PRODUCT_KEY)
)
COMMENT = 'GRAIN: one row per store x product x business date (closing position), for pairs with any movement history.';
