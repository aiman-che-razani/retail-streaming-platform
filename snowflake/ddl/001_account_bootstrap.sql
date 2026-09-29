-- =============================================================================
-- Account bootstrap (ADR-005). Run ONCE as ACCOUNTADMIN:
--     SNOWFLAKE_ADMIN_ROLE=ACCOUNTADMIN retail-snowflake-migrate bootstrap
-- or paste into a Snowsight worksheet after replacing the {{...}} placeholders.
-- Idempotent: safe to re-run (IF NOT EXISTS / OR REPLACE only where harmless).
--
-- Placeholders substituted by the migration runner:
--   {{DATABASE}}            e.g. RETAIL_DEV
--   {{LOADER_PUBLIC_KEY}}   RSA public key body for the loader service user
--   {{ADMIN_USER}}          the human user who will run migrations
--   {{MONTHLY_CREDIT_QUOTA}} resource monitor cap
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------- roles
-- Functional roles, all rolled up to SYSADMIN so administrators keep visibility.
CREATE ROLE IF NOT EXISTS RETAIL_ADMIN       COMMENT = 'Owns retail database objects; runs migrations';
CREATE ROLE IF NOT EXISTS RETAIL_TRANSFORMER COMMENT = 'Owns and runs Streams/Tasks transformations';
CREATE ROLE IF NOT EXISTS RETAIL_LOADER      COMMENT = 'Loader service: PUT/COPY into RAW only';
CREATE ROLE IF NOT EXISTS RETAIL_ANALYST     COMMENT = 'Read-only access to ANALYTICS';

GRANT ROLE RETAIL_LOADER      TO ROLE RETAIL_ADMIN;
GRANT ROLE RETAIL_TRANSFORMER TO ROLE RETAIL_ADMIN;
GRANT ROLE RETAIL_ANALYST     TO ROLE RETAIL_ADMIN;
GRANT ROLE RETAIL_ADMIN       TO ROLE SYSADMIN;
GRANT ROLE RETAIL_ADMIN       TO USER {{ADMIN_USER}};

-- Tasks run with the privileges of their owner; owning roles need EXECUTE TASK.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE RETAIL_TRANSFORMER;

-- ----------------------------------------------------------------------------- cost controls
-- XSMALL = 1 credit/hour, billed per second with a 60 s minimum per resume.
-- AUTO_SUSPEND = 60 stops billing one minute after the last query.
CREATE WAREHOUSE IF NOT EXISTS RETAIL_PIPELINE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 900
    COMMENT = 'COPY loads and Streams/Tasks transformations';

CREATE WAREHOUSE IF NOT EXISTS RETAIL_ANALYTICS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 600
    COMMENT = 'Analyst and BI queries (isolated from the pipeline)';

-- Hard monthly cap: notify at 50/80 %, suspend at 100 %, suspend immediately at 110 %.
CREATE RESOURCE MONITOR IF NOT EXISTS RETAIL_MONTHLY_MONITOR
    WITH CREDIT_QUOTA = {{MONTHLY_CREDIT_QUOTA}}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE RETAIL_PIPELINE_WH  SET RESOURCE_MONITOR = RETAIL_MONTHLY_MONITOR;
ALTER WAREHOUSE RETAIL_ANALYTICS_WH SET RESOURCE_MONITOR = RETAIL_MONTHLY_MONITOR;

GRANT USAGE, OPERATE ON WAREHOUSE RETAIL_PIPELINE_WH  TO ROLE RETAIL_ADMIN;
GRANT USAGE ON WAREHOUSE RETAIL_PIPELINE_WH  TO ROLE RETAIL_LOADER;
GRANT USAGE ON WAREHOUSE RETAIL_PIPELINE_WH  TO ROLE RETAIL_TRANSFORMER;
GRANT USAGE ON WAREHOUSE RETAIL_ANALYTICS_WH TO ROLE RETAIL_ANALYST;
GRANT USAGE ON WAREHOUSE RETAIL_ANALYTICS_WH TO ROLE RETAIL_ADMIN;

-- ----------------------------------------------------------------------------- database
CREATE DATABASE IF NOT EXISTS {{DATABASE}} COMMENT = 'Retail streaming platform';
GRANT OWNERSHIP ON DATABASE {{DATABASE}} TO ROLE RETAIL_ADMIN COPY CURRENT GRANTS;

USE ROLE RETAIL_ADMIN;
USE DATABASE {{DATABASE}};
CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'As-ingested events + lineage (append-only)';
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'Typed, deduplicated, conformed';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Star schema for analysts';
CREATE SCHEMA IF NOT EXISTS OPS       COMMENT = 'Pipeline metadata: migrations, DQ, reconciliation';
DROP SCHEMA IF EXISTS PUBLIC;

GRANT USAGE ON DATABASE {{DATABASE}} TO ROLE RETAIL_LOADER;
GRANT USAGE ON DATABASE {{DATABASE}} TO ROLE RETAIL_TRANSFORMER;
GRANT USAGE ON DATABASE {{DATABASE}} TO ROLE RETAIL_ANALYST;

-- Object-level grants live in snowflake/transformations/R__900_grants.sql so that they are
-- re-applied whenever new objects are added.

-- ----------------------------------------------------------------------------- loader service user
USE ROLE ACCOUNTADMIN;
-- TYPE = SERVICE: cannot log in interactively or with a password; key-pair only.
CREATE USER IF NOT EXISTS RETAIL_LOADER_SVC
    TYPE = SERVICE
    DEFAULT_ROLE = RETAIL_LOADER
    DEFAULT_WAREHOUSE = RETAIL_PIPELINE_WH
    DEFAULT_NAMESPACE = {{DATABASE}}.RAW
    COMMENT = 'Loader service (key-pair auth)';
ALTER USER RETAIL_LOADER_SVC SET RSA_PUBLIC_KEY = '{{LOADER_PUBLIC_KEY}}';
GRANT ROLE RETAIL_LOADER TO USER RETAIL_LOADER_SVC;
