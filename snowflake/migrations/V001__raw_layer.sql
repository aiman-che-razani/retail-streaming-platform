-- =============================================================================
-- V001 RAW layer (data-model.md §2, ADR-010). Applied once by the migration runner as
-- RETAIL_ADMIN. Never edit after it has been applied: add a new V### migration instead.
--
-- Conventions: all timestamps are TIMESTAMP_NTZ holding UTC; SYSDATE() returns UTC NTZ.
-- =============================================================================

USE SCHEMA {{DATABASE}}.RAW;

-- ----------------------------------------------------------------------------- load plumbing
CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET
    TYPE = PARQUET
    COMMENT = 'Spark landing-zone Parquet batches';

CREATE FILE FORMAT IF NOT EXISTS FF_STORES_CSV
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'Store reference data (simulator/reference/stores.csv)';

-- Internal named stage: storage is Snowflake-managed and encrypted. In production this would
-- be an external stage on S3/GCS/ADLS with Snowpipe auto-ingest (ADR-010).
CREATE STAGE IF NOT EXISTS LANDING_STAGE
    FILE_FORMAT = FF_PARQUET
    COMMENT = 'Loader PUTs completed landing batches here; COPY ... PURGE removes them';

-- ----------------------------------------------------------------------------- event tables
-- One layout for the four event datasets. EVENT keeps the complete envelope exactly as
-- received, so fields added by newer producers are preserved before any consumer knows them.
CREATE TABLE IF NOT EXISTS POS_TRANSACTIONS (
    KAFKA_TOPIC        VARCHAR       NOT NULL,
    KAFKA_PARTITION    NUMBER(10,0)  NOT NULL,
    KAFKA_OFFSET       NUMBER(20,0)  NOT NULL,
    KAFKA_TIMESTAMP    TIMESTAMP_NTZ,
    KAFKA_KEY          VARCHAR,
    SCHEMA_ID          NUMBER(10,0),
    EVENT_ID           VARCHAR       NOT NULL,
    EVENT_TYPE         VARCHAR       NOT NULL,
    SCHEMA_VERSION     VARCHAR       NOT NULL,
    EVENT_TIMESTAMP    TIMESTAMP_NTZ NOT NULL,
    PRODUCED_AT        TIMESTAMP_NTZ,
    PRODUCER           VARCHAR,
    CORRELATION_ID     VARCHAR,
    CAUSATION_ID       VARCHAR,
    EVENT              VARIANT       NOT NULL,
    DQ_WARNINGS        ARRAY,
    INGESTED_AT        TIMESTAMP_NTZ,
    SPARK_QUERY_ID     VARCHAR,
    SPARK_BATCH_ID     NUMBER(20,0),
    _FILE_NAME         VARCHAR       NOT NULL,
    _FILE_ROW_NUMBER   NUMBER(20,0)  NOT NULL,
    _LOADED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'RAW pos.transactions events (one row per Kafka record loaded). May contain duplicates by design (ADR-007).';

CREATE TABLE IF NOT EXISTS INVENTORY_MOVEMENTS LIKE POS_TRANSACTIONS;
ALTER TABLE INVENTORY_MOVEMENTS SET COMMENT = 'RAW inventory.events events. May contain duplicates by design.';
CREATE TABLE IF NOT EXISTS CUSTOMER_EVENTS LIKE POS_TRANSACTIONS;
ALTER TABLE CUSTOMER_EVENTS SET COMMENT = 'RAW customer.events events. May contain duplicates by design.';
CREATE TABLE IF NOT EXISTS PRODUCT_EVENTS LIKE POS_TRANSACTIONS;
ALTER TABLE PRODUCT_EVENTS SET COMMENT = 'RAW product.updates events. May contain duplicates by design.';

-- ----------------------------------------------------------------------------- rejects + audit
CREATE TABLE IF NOT EXISTS DEAD_LETTERS (
    SOURCE_TOPIC        VARCHAR       NOT NULL,
    SOURCE_PARTITION    NUMBER(10,0)  NOT NULL,
    SOURCE_OFFSET       NUMBER(20,0)  NOT NULL,
    KAFKA_TIMESTAMP     TIMESTAMP_NTZ,
    KAFKA_KEY           VARCHAR,
    ERROR_CODE          VARCHAR       NOT NULL,
    ERROR_STAGE         VARCHAR       NOT NULL,
    ERROR_MESSAGE       VARCHAR,
    EVENT_ID            VARCHAR,
    RAW_VALUE_BASE64    VARCHAR,
    RAW_VALUE_PREVIEW   VARCHAR,
    FAILED_AT           TIMESTAMP_NTZ,
    SPARK_QUERY_ID      VARCHAR,
    SPARK_BATCH_ID      NUMBER(20,0),
    _FILE_NAME          VARCHAR       NOT NULL,
    _FILE_ROW_NUMBER    NUMBER(20,0)  NOT NULL,
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'Records rejected by Spark validation (copy of the *.dlq topics for SQL analysis).';

CREATE TABLE IF NOT EXISTS INGEST_BATCH_AUDIT (
    DATASET               VARCHAR       NOT NULL,
    KAFKA_TOPIC           VARCHAR       NOT NULL,
    KAFKA_PARTITION       NUMBER(10,0)  NOT NULL,
    MIN_OFFSET            NUMBER(20,0),
    MAX_OFFSET            NUMBER(20,0),
    RECORDS_READ          NUMBER(20,0)  NOT NULL,
    RECORDS_VALID         NUMBER(20,0)  NOT NULL,
    RECORDS_REJECTED      NUMBER(20,0)  NOT NULL,
    RECORDS_WARNED        NUMBER(20,0)  NOT NULL,
    MIN_EVENT_TIMESTAMP   TIMESTAMP_NTZ,
    MAX_EVENT_TIMESTAMP   TIMESTAMP_NTZ,
    SPARK_QUERY_ID        VARCHAR       NOT NULL,
    SPARK_BATCH_ID        NUMBER(20,0)  NOT NULL,
    PROCESSED_AT          TIMESTAMP_NTZ,
    _FILE_NAME            VARCHAR       NOT NULL,
    _LOADED_AT            TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'Per Spark batch x Kafka partition accounting; source of truth for reconciliation (SF-005).';

CREATE TABLE IF NOT EXISTS STORE_REFERENCE (
    STORE_ID        VARCHAR NOT NULL,
    STORE_NAME      VARCHAR,
    CITY            VARCHAR,
    STATE           VARCHAR,
    REGION          VARCHAR,
    STORE_FORMAT    VARCHAR,
    SIZE_SQM        NUMBER(10,0),
    OPENED_DATE     DATE,
    TIMEZONE        VARCHAR,
    TRAFFIC_WEIGHT  NUMBER(6,2),
    _FILE_NAME      VARCHAR NOT NULL,
    _LOADED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'Store master data loaded from the versioned reference CSV (every load appended).';

-- ----------------------------------------------------------------------------- streams
-- An append-only stream is an offset into the table's change history. Reading it inside a
-- DML transaction returns rows added since the last consumed offset; COMMIT advances it.
-- This replaces hand-written "last loaded timestamp" bookkeeping (data-model.md §5).
CREATE STREAM IF NOT EXISTS POS_TRANSACTIONS_STREAM    ON TABLE POS_TRANSACTIONS    APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS INVENTORY_MOVEMENTS_STREAM ON TABLE INVENTORY_MOVEMENTS APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS CUSTOMER_EVENTS_STREAM     ON TABLE CUSTOMER_EVENTS     APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS PRODUCT_EVENTS_STREAM      ON TABLE PRODUCT_EVENTS      APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STORE_REFERENCE_STREAM     ON TABLE STORE_REFERENCE     APPEND_ONLY = TRUE;
