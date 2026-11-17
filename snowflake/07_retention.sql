/* =====================================================================
   Walmart Sales Pipeline - Phase 7: Snowflake retention and recovery
   (run as ACCOUNTADMIN)

   Time Travel keeps changed and dropped data for N days, so a table can be
   recovered with UNDROP or queried AT a past time. After Time Travel ends,
   Fail-safe keeps it 7 more days (recoverable by Snowflake support only).

   More than 1 day needs Enterprise edition or higher. On Standard edition these
   statements fail with "exceeds the maximum"; Time Travel then stays at 1 day.
   ===================================================================== */

USE ROLE ACCOUNTADMIN;

-- PROD: 7 days on raw data, gold tables and dashboard feedback
ALTER SCHEMA WALMART_PROD.RAW      SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER SCHEMA WALMART_PROD.GOLD     SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER SCHEMA WALMART_PROD.FEEDBACK SET DATA_RETENTION_TIME_IN_DAYS = 7;
-- DEV and STAGING keep the 1-day default (cheaper; they can be rebuilt from S3)

-- Checks: retention_time should be 7
SHOW SCHEMAS IN DATABASE WALMART_PROD;
SHOW TABLES IN SCHEMA WALMART_PROD.GOLD;

-- ---------------------------------------------------------------------
-- Recovery examples (see docs/runbook.md, "Recovering a table")
-- ---------------------------------------------------------------------
-- A dropped table:
--   UNDROP TABLE WALMART_PROD.GOLD.FCT_WEEKLY_SALES;
-- A table as it was 2 hours ago:
--   SELECT COUNT(*) FROM WALMART_PROD.GOLD.FCT_WEEKLY_SALES AT (OFFSET => -60*60*2);
-- Restore it from that point:
--   CREATE OR REPLACE TABLE WALMART_PROD.GOLD.FCT_WEEKLY_SALES_RESTORED
--     CLONE WALMART_PROD.GOLD.FCT_WEEKLY_SALES AT (OFFSET => -60*60*2);
