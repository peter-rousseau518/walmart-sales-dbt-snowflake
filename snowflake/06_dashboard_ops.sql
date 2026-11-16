/* =====================================================================
   Walmart Sales Pipeline - Phase 6: dashboard operations (run as ACCOUNTADMIN)

   Parts:
     Part 1  separate warehouses: TRANSFORM_WH (loads + dbt), STREAMLIT_WH (dashboard)
     Part 2  resource monitors: email at 80% of the monthly credit quota, suspend at 100%
     Part 3  point the load tasks and the dbt service role at TRANSFORM_WH
     Part 4  data-issue reporting: DATA_ISSUES table + REPORT_DATA_ISSUE procedure
             (the Streamlit form calls it; it emails data engineering through SNS)
     Part 5  checks

   COMPUTE_WH stays for worksheets and local dbt development.
   Safe to re-run.
   ===================================================================== */

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------
-- Part 1. Warehouses (X-Small, suspend after 60 seconds idle, start suspended)
-- ---------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT             = 'Loads (LOAD_ON_ARRIVAL tasks) and dbt builds';

CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT             = 'Streamlit dashboard only, so dashboard traffic never slows loads or dbt';

-- Enterprise edition and above: let the dashboard warehouse add a second cluster at peak.
-- (Standard edition rejects this line; leave it commented out there.)
-- ALTER WAREHOUSE STREAMLIT_WH SET MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

-- ---------------------------------------------------------------------
-- Part 2. Resource monitors (monthly quotas; adjust to your budget)
--   NOTIFY emails account administrators who have email notifications turned on
--   (Snowsight: your name -> Settings -> Notifications).
-- ---------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS TRANSFORM_MONITOR
  WITH CREDIT_QUOTA = 10
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

CREATE RESOURCE MONITOR IF NOT EXISTS STREAMLIT_MONITOR
  WITH CREDIT_QUOTA = 5
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = TRANSFORM_MONITOR;
ALTER WAREHOUSE STREAMLIT_WH SET RESOURCE_MONITOR = STREAMLIT_MONITOR;

-- ---------------------------------------------------------------------
-- Part 3. Loads and dbt run on TRANSFORM_WH
--   A task's warehouse can only be changed while it is suspended.
-- ---------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER;   -- dbt in GitHub Actions

ALTER TASK WALMART_DEV.RAW.LOAD_ON_ARRIVAL     SUSPEND;
ALTER TASK WALMART_DEV.RAW.LOAD_ON_ARRIVAL     SET WAREHOUSE = TRANSFORM_WH;
ALTER TASK WALMART_DEV.RAW.LOAD_ON_ARRIVAL     RESUME;

ALTER TASK WALMART_STAGING.RAW.LOAD_ON_ARRIVAL SUSPEND;
ALTER TASK WALMART_STAGING.RAW.LOAD_ON_ARRIVAL SET WAREHOUSE = TRANSFORM_WH;
ALTER TASK WALMART_STAGING.RAW.LOAD_ON_ARRIVAL RESUME;

ALTER TASK WALMART_PROD.RAW.LOAD_ON_ARRIVAL    SUSPEND;
ALTER TASK WALMART_PROD.RAW.LOAD_ON_ARRIVAL    SET WAREHOUSE = TRANSFORM_WH;
ALTER TASK WALMART_PROD.RAW.LOAD_ON_ARRIVAL    RESUME;

-- ---------------------------------------------------------------------
-- Part 4. Data-issue reporting (PROD; the dashboard reads PROD only)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS WALMART_PROD.FEEDBACK;

CREATE TABLE IF NOT EXISTS WALMART_PROD.FEEDBACK.DATA_ISSUES (
    issue_id     NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    reported_at  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    reported_by  VARCHAR,
    page         VARCHAR,
    description  VARCHAR,
    status       VARCHAR DEFAULT 'OPEN',      -- OPEN, ACKNOWLEDGED, RESOLVED
    resolution   VARCHAR,
    resolved_at  TIMESTAMP_LTZ
);

-- Called by the Streamlit form: stores the report and emails data engineering
-- (SNS topic walmart-pipeline-errors; the SQS reprocess queue filters DATA_ISSUE out).
CREATE OR REPLACE PROCEDURE WALMART_PROD.FEEDBACK.REPORT_DATA_ISSUE(
    PAGE VARCHAR, DESCRIPTION VARCHAR, REPORTED_BY VARCHAR)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
  new_id   NUMBER;
  message  VARCHAR;
BEGIN
  INSERT INTO WALMART_PROD.FEEDBACK.DATA_ISSUES (reported_by, page, description)
    VALUES (:reported_by, :page, :description);
  SELECT MAX(issue_id) INTO :new_id
    FROM WALMART_PROD.FEEDBACK.DATA_ISSUES
   WHERE reported_by IS NOT DISTINCT FROM :reported_by AND page = :page;

  message := TO_JSON(OBJECT_CONSTRUCT(
    'type',        'DATA_ISSUE',
    'issue_id',    new_id,
    'page',        page,
    'reported_by', reported_by,
    'description', description,
    'reported_at', CURRENT_TIMESTAMP()::VARCHAR,
    'sla',         'Acknowledge within 1 business day; fix or explain within 3'));
  CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
    SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
    SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  RETURN new_id;
END;
$$;

-- If the Streamlit app is owned by SYSADMIN rather than ACCOUNTADMIN, it also needs these
-- (harmless if the app is owned by ACCOUNTADMIN):
GRANT USAGE ON WAREHOUSE STREAMLIT_WH TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA WALMART_PROD.FEEDBACK TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE WALMART_PROD.FEEDBACK.REPORT_DATA_ISSUE(VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------
-- Part 5. Checks
-- ---------------------------------------------------------------------
SHOW WAREHOUSES LIKE '%_WH';          -- TRANSFORM_WH and STREAMLIT_WH, each with its resource_monitor
SHOW TASKS LIKE 'LOAD_ON_ARRIVAL' IN ACCOUNT;   -- warehouse = TRANSFORM_WH, state = started

-- Test the issue procedure (sends one email):
-- CALL WALMART_PROD.FEEDBACK.REPORT_DATA_ISSUE('Test page', 'Test report from setup', 'setup');
-- SELECT * FROM WALMART_PROD.FEEDBACK.DATA_ISSUES ORDER BY issue_id DESC;

-- Weekly cost review: credits per warehouse over the last 7 days
-- SELECT warehouse_name, ROUND(SUM(credits_used), 2) AS credits
-- FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
-- GROUP BY 1 ORDER BY 2 DESC;
