-- =============================================================================
-- V003 STAGING layer (data-model.md §3): typed, flattened, deduplicated.
-- Populated by STAGING.SP_STAGE_* procedures (snowflake/transformations/R__200_staging_procedures.sql).
-- =============================================================================

USE SCHEMA {{DATABASE}}.STAGING;

-- SCD1 current state of every store (small reference table).
CREATE TABLE IF NOT EXISTS STG_STORES (
    STORE_ID       VARCHAR       NOT NULL PRIMARY KEY,
    STORE_NAME     VARCHAR,
    CITY           VARCHAR,
    STATE          VARCHAR,
    REGION         VARCHAR,
    STORE_FORMAT   VARCHAR,
    SIZE_SQM       NUMBER(10,0),
    OPENED_DATE    DATE,
    TIMEZONE       VARCHAR       NOT NULL,
    _UPDATED_AT    TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
);

-- Grain: one row per (transaction_id, line_number). First event per transaction wins.
CREATE TABLE IF NOT EXISTS STG_POS_TRANSACTION_LINES (
    TRANSACTION_ID     VARCHAR       NOT NULL,
    LINE_NUMBER        NUMBER(5,0)   NOT NULL,
    EVENT_ID           VARCHAR       NOT NULL,
    STORE_ID           VARCHAR       NOT NULL,
    REGISTER_ID        VARCHAR,
    CUSTOMER_ID        VARCHAR,                -- NULL = guest checkout
    PAYMENT_METHOD     VARCHAR       NOT NULL,   -- unknown values normalised to 'UNKNOWN'
    CURRENCY           VARCHAR       NOT NULL,
    PRODUCT_ID         VARCHAR       NOT NULL,
    QUANTITY           NUMBER(10,0)  NOT NULL,
    UNIT_PRICE         NUMBER(12,2)  NOT NULL,
    DISCOUNT_AMOUNT    NUMBER(12,2)  NOT NULL,
    GROSS_AMOUNT       NUMBER(14,2)  NOT NULL,
    NET_AMOUNT         NUMBER(14,2)  NOT NULL,
    TRANSACTION_TOTAL  NUMBER(14,2)  NOT NULL,
    EVENT_TS_UTC       TIMESTAMP_NTZ NOT NULL,
    EVENT_TS_LOCAL     TIMESTAMP_NTZ NOT NULL,
    BUSINESS_DATE      DATE          NOT NULL,
    LOCAL_HOUR         NUMBER(2,0)   NOT NULL,
    DQ_WARNINGS        ARRAY,
    KAFKA_TOPIC        VARCHAR,
    KAFKA_PARTITION    NUMBER(10,0),
    KAFKA_OFFSET       NUMBER(20,0),
    _STAGED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    PRIMARY KEY (TRANSACTION_ID, LINE_NUMBER)
)
COMMENT = 'Grain: one row per POS transaction line. First event per transaction_id wins (SF-002).';

-- Grain: one row per movement_id.
CREATE TABLE IF NOT EXISTS STG_INVENTORY_MOVEMENTS (
    MOVEMENT_ID             VARCHAR       NOT NULL PRIMARY KEY,
    EVENT_ID                VARCHAR       NOT NULL,
    STORE_ID                VARCHAR       NOT NULL,
    PRODUCT_ID              VARCHAR       NOT NULL,
    MOVEMENT_TYPE           VARCHAR       NOT NULL,
    QUANTITY_DELTA          NUMBER(10,0)  NOT NULL,
    QUANTITY_ON_HAND_AFTER  NUMBER(12,0)  NOT NULL,
    REASON_CODE             VARCHAR,
    REFERENCE_ID            VARCHAR,
    UNIT_COST               NUMBER(12,2),
    CAUSATION_ID            VARCHAR,
    EVENT_TS_UTC            TIMESTAMP_NTZ NOT NULL,
    EVENT_TS_LOCAL          TIMESTAMP_NTZ NOT NULL,
    BUSINESS_DATE           DATE          NOT NULL,
    DQ_WARNINGS             ARRAY,
    KAFKA_TOPIC             VARCHAR,
    KAFKA_PARTITION         NUMBER(10,0),
    KAFKA_OFFSET            NUMBER(20,0),
    _STAGED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'Grain: one row per inventory movement. First event per movement_id wins.';

-- ALL versions (full-state events) - the input to SCD2. Grain: one row per event_id.
CREATE TABLE IF NOT EXISTS STG_CUSTOMER_CHANGES (
    EVENT_ID          VARCHAR       NOT NULL PRIMARY KEY,
    CUSTOMER_ID       VARCHAR       NOT NULL,
    EVENT_TYPE        VARCHAR       NOT NULL,
    EVENT_TS_UTC      TIMESTAMP_NTZ NOT NULL,
    LOYALTY_TIER      VARCHAR       NOT NULL,
    HOME_STORE_ID     VARCHAR,
    CITY              VARCHAR,
    STATE             VARCHAR,
    SIGNUP_DATE       DATE,
    BIRTH_YEAR        NUMBER(4,0),
    EMAIL_SHA256      VARCHAR,
    MARKETING_OPT_IN  BOOLEAN,
    _STAGED_AT        TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
);

CREATE TABLE IF NOT EXISTS STG_PRODUCT_CHANGES (
    EVENT_ID         VARCHAR       NOT NULL PRIMARY KEY,
    PRODUCT_ID       VARCHAR       NOT NULL,
    EVENT_TYPE       VARCHAR       NOT NULL,
    EVENT_TS_UTC     TIMESTAMP_NTZ NOT NULL,
    PRODUCT_NAME     VARCHAR,
    BRAND            VARCHAR,
    CATEGORY         VARCHAR,
    SUBCATEGORY      VARCHAR,
    LIST_PRICE       NUMBER(12,2),
    UNIT_COST        NUMBER(12,2),
    CURRENCY         VARCHAR,
    UNIT_OF_MEASURE  VARCHAR,
    IS_ACTIVE        BOOLEAN,
    _STAGED_AT       TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
);

-- Work table for SCD2 rebuilds (DDL cannot run inside a transaction, so it is permanent).
CREATE TABLE IF NOT EXISTS WRK_AFFECTED_KEYS (
    ENTITY       VARCHAR NOT NULL,   -- 'PRODUCT' | 'CUSTOMER'
    NATURAL_KEY  VARCHAR NOT NULL
);

-- Streams feeding ANALYTICS incrementally (staging tables are insert-only -> append-only).
CREATE STREAM IF NOT EXISTS STG_POS_TRANSACTION_LINES_STREAM ON TABLE STG_POS_TRANSACTION_LINES APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STG_INVENTORY_MOVEMENTS_STREAM   ON TABLE STG_INVENTORY_MOVEMENTS   APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STG_CUSTOMER_CHANGES_STREAM      ON TABLE STG_CUSTOMER_CHANGES      APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STG_PRODUCT_CHANGES_STREAM       ON TABLE STG_PRODUCT_CHANGES       APPEND_ONLY = TRUE;
