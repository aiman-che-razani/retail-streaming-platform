-- =============================================================================
-- V002 OPS tables: data-quality results and business-duplicate log.
-- =============================================================================

USE SCHEMA {{DATABASE}}.OPS;

-- One row per rule evaluation. History is kept so trends are queryable ("did SF-003 grow?").
CREATE TABLE IF NOT EXISTS DQ_RESULTS (
    RUN_ID          VARCHAR       NOT NULL,
    RULE_ID         VARCHAR       NOT NULL,
    RULE_NAME       VARCHAR       NOT NULL,
    SEVERITY        VARCHAR       NOT NULL,   -- INFO | WARN | ERROR
    STATUS          VARCHAR       NOT NULL,   -- PASS | FAIL
    OBSERVED_VALUE  NUMBER(38,4),
    THRESHOLD       NUMBER(38,4),
    DETAILS         VARIANT,
    CHECKED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'Data-quality and reconciliation rule results (SF-001..SF-008).';

-- SF-002: same business key (transaction_id / movement_id) delivered with a different event_id.
-- The first event wins in STAGING; later ones are recorded here for investigation.
CREATE TABLE IF NOT EXISTS BUSINESS_DUPLICATES (
    DATASET          VARCHAR       NOT NULL,
    BUSINESS_KEY     VARCHAR       NOT NULL,
    KEPT_EVENT_ID    VARCHAR,
    DUPLICATE_EVENT_ID VARCHAR     NOT NULL,
    DUPLICATE_EVENT_TIMESTAMP TIMESTAMP_NTZ,
    KAFKA_TOPIC      VARCHAR,
    KAFKA_PARTITION  NUMBER(10,0),
    KAFKA_OFFSET     NUMBER(20,0),
    DETECTED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
)
COMMENT = 'Business duplicates discarded by STAGING (first event per business key wins).';
