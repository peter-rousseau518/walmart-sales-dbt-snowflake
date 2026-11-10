/* =====================================================================
   Walmart Sales Pipeline - one-time migration: ANALYTICS -> SILVER + GOLD
   Run as ACCOUNTADMIN.
   Part A: BEFORE merging the pull request that adds the SILVER/GOLD schemas.
   Part B: AFTER the deploy has built SILVER and GOLD in STAGING and PROD
           and the dashboard reads WALMART_PROD.GOLD.
   ===================================================================== */

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------
-- Part A: let TRANSFORMER (CI/CD) create the new schemas
-- ---------------------------------------------------------------------
GRANT CREATE SCHEMA ON DATABASE WALMART_STAGING TO ROLE TRANSFORMER;
GRANT CREATE SCHEMA ON DATABASE WALMART_PROD    TO ROLE TRANSFORMER;

-- ---------------------------------------------------------------------
-- Part B: after the deploy succeeds, check the new layout, then retire ANALYTICS
-- ---------------------------------------------------------------------
-- Expected: RAW, SILVER (3 views), GOLD (4 tables)
-- SHOW SCHEMAS IN DATABASE WALMART_PROD;
-- SHOW VIEWS  IN SCHEMA WALMART_PROD.SILVER;
-- SHOW TABLES IN SCHEMA WALMART_PROD.GOLD;

-- DROP SCHEMA IF EXISTS WALMART_STAGING.ANALYTICS;
-- DROP SCHEMA IF EXISTS WALMART_PROD.ANALYTICS;
-- DROP SCHEMA IF EXISTS WALMART_DEV.ANALYTICS;
-- DROP SCHEMA IF EXISTS WALMART_DEV.DBT_PETER;   -- old dev objects; new ones live in DBT_PETER_SILVER / DBT_PETER_GOLD
