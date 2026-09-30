-- =============================================================================
-- Task graph (data-model.md §5, ADR-005/010).
--
--   TASK_PIPELINE_ROOT (every 15 min, WHEN any stream has data) -> SP_STAGE_ALL
--     └─ TASK_DIMENSIONS -> SP_LOAD_DIMENSIONS
--          └─ TASK_FACTS -> SP_LOAD_FACTS
--               └─ TASK_DQ_CHECKS -> SP_RUN_DQ_CHECKS
--   TASK_INVENTORY_SNAPSHOT (daily 00:30 Asia/Kuala_Lumpur, independent; rebuilds the last 3 days)
--
-- COST: the WHEN condition is evaluated by the cloud-services layer WITHOUT a warehouse.
-- If no stream has new rows the run is skipped and costs nothing. Tasks are created
-- SUSPENDED (CREATE OR REPLACE always suspends); start them with
--     make snowflake-tasks-resume
-- Staging streams are included in WHEN so rows left behind by a failed downstream task are
-- picked up on the next schedule even if no new RAW data arrived.
-- =============================================================================

-- A graph can only be modified while its root is suspended.
ALTER TASK IF EXISTS {{DATABASE}}.OPS.TASK_PIPELINE_ROOT SUSPEND;
ALTER TASK IF EXISTS {{DATABASE}}.OPS.TASK_INVENTORY_SNAPSHOT SUSPEND;

CREATE OR REPLACE TASK {{DATABASE}}.OPS.TASK_PIPELINE_ROOT
    WAREHOUSE = RETAIL_PIPELINE_WH
    SCHEDULE = '15 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3        -- stop burning credits on a persistent bug
    USER_TASK_TIMEOUT_MS = 900000
    COMMENT = 'RAW streams -> STAGING (root of the pipeline graph)'
WHEN
       SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.RAW.POS_TRANSACTIONS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.RAW.INVENTORY_MOVEMENTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.RAW.CUSTOMER_EVENTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.RAW.PRODUCT_EVENTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.RAW.STORE_REFERENCE_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.STAGING.STG_POS_TRANSACTION_LINES_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.STAGING.STG_INVENTORY_MOVEMENTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.STAGING.STG_PRODUCT_CHANGES_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{{DATABASE}}.STAGING.STG_CUSTOMER_CHANGES_STREAM')
AS
    CALL {{DATABASE}}.STAGING.SP_STAGE_ALL();

CREATE OR REPLACE TASK {{DATABASE}}.OPS.TASK_DIMENSIONS
    WAREHOUSE = RETAIL_PIPELINE_WH
    USER_TASK_TIMEOUT_MS = 900000
    COMMENT = 'STAGING -> SCD1/SCD2 dimensions'
    AFTER {{DATABASE}}.OPS.TASK_PIPELINE_ROOT
AS
    CALL {{DATABASE}}.ANALYTICS.SP_LOAD_DIMENSIONS();

CREATE OR REPLACE TASK {{DATABASE}}.OPS.TASK_FACTS
    WAREHOUSE = RETAIL_PIPELINE_WH
    USER_TASK_TIMEOUT_MS = 900000
    COMMENT = 'STAGING -> FACT_SALES / FACT_INVENTORY'
    AFTER {{DATABASE}}.OPS.TASK_DIMENSIONS
AS
    CALL {{DATABASE}}.ANALYTICS.SP_LOAD_FACTS();

CREATE OR REPLACE TASK {{DATABASE}}.OPS.TASK_DQ_CHECKS
    WAREHOUSE = RETAIL_PIPELINE_WH
    USER_TASK_TIMEOUT_MS = 900000
    COMMENT = 'Set-level data-quality and reconciliation rules -> OPS.DQ_RESULTS'
    AFTER {{DATABASE}}.OPS.TASK_FACTS
AS
    CALL {{DATABASE}}.OPS.SP_RUN_DQ_CHECKS();

CREATE OR REPLACE TASK {{DATABASE}}.OPS.TASK_INVENTORY_SNAPSHOT
    WAREHOUSE = RETAIL_PIPELINE_WH
    SCHEDULE = 'USING CRON 30 0 * * * Asia/Kuala_Lumpur'
    USER_TASK_TIMEOUT_MS = 900000
    COMMENT = 'Rebuild closing inventory positions for the last 3 local business days (absorbs late data)'
AS
    CALL {{DATABASE}}.ANALYTICS.SP_BUILD_RECENT_INVENTORY_SNAPSHOTS(3);

-- Tasks run with their OWNER's privileges. Every task in a graph must have the same owner,
-- and transferring ownership task-by-task severs predecessor links, so the whole schema's
-- tasks are handed to the transformer in ONE statement (tasks are suspended after
-- CREATE OR REPLACE, which ownership transfer requires). Re-applied on every deploy of
-- this script, so a recreated task can never be left owned by RETAIL_ADMIN.
GRANT OWNERSHIP ON ALL TASKS IN SCHEMA {{DATABASE}}.OPS TO ROLE RETAIL_TRANSFORMER COPY CURRENT GRANTS;
