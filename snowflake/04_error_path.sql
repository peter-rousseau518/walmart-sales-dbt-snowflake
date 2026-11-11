/* =====================================================================
   Walmart Sales Pipeline - Phase 3: error path (run as ACCOUNTADMIN)
   Scheduled COPY INTO load + failed-file alerts to SNS.

   Run in order:
     Part 1  notification integration (then finish the IAM trust policy in AWS)
     Part 2  grants
     Part 3  load procedures (one per environment)
     Part 4  weekly load task (PROD)
     Part 5  test with a bad file (see the build guide)

   Replace before running:
     <SNS_TOPIC_ARN>  arn:aws:sns:<region>:<account_id>:walmart-pipeline-errors
     <SNS_ROLE_ARN>   arn:aws:iam::<account_id>:role/snowflake-sns-publisher
   ===================================================================== */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ---------------------------------------------------------------------
-- Part 1. Outbound notification integration: Snowflake -> SNS topic
-- ---------------------------------------------------------------------
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS WALMART_ERRORS_SNS
  ENABLED               = TRUE
  TYPE                  = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION             = OUTBOUND
  AWS_SNS_TOPIC_ARN     = '<SNS_TOPIC_ARN>'
  AWS_SNS_ROLE_ARN      = '<SNS_ROLE_ARN>';

-- Copy SF_AWS_IAM_USER_ARN and SF_AWS_EXTERNAL_ID into the trust policy of the
-- snowflake-sns-publisher role in AWS (aws/iam/snowflake-sns-trust-policy.json).
DESC NOTIFICATION INTEGRATION WALMART_ERRORS_SNS;

-- ---------------------------------------------------------------------
-- Part 2. Grants: dbt (TRANSFORMER) sends the PROD quarantine alert
-- ---------------------------------------------------------------------
GRANT USAGE ON INTEGRATION WALMART_ERRORS_SNS TO ROLE TRANSFORMER;

-- ---------------------------------------------------------------------
-- Part 3 (DEV). Load procedure: COPY every landing folder, collect files that
-- were skipped (ON_ERROR = SKIP_FILE does not fail the statement), and publish
-- them to SNS. SNS fans out to: email, the SQS reprocess queue, and the Lambda
-- that moves each failed file to the error bucket.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WALMART_DEV.RAW.LOAD_LANDING_FILES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  failed    ARRAY DEFAULT ARRAY_CONSTRUCT();
  batch     ARRAY;
  loaded    INTEGER DEFAULT 0;
  n         INTEGER;
  message   VARCHAR;
BEGIN
  -- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
  COPY INTO WALMART_DEV.RAW.DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                                       _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_DEV.RAW.LANDING_STAGE/department/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'DEPARTMENT', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- stores.csv: Store, Type, Size
  COPY INTO WALMART_DEV.RAW.STORES (store_id, store_type, store_size,
                                   _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_DEV.RAW.LANDING_STAGE/stores/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'STORES', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
  COPY INTO WALMART_DEV.RAW.FACT (store_id, week_date, temperature, fuel_price,
                                 markdown1, markdown2, markdown3, markdown4, markdown5,
                                 cpi, unemployment, is_holiday,
                                 _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_DEV.RAW.LANDING_STAGE/fact/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'FACT', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- Publish skipped files to SNS
  IF (ARRAY_SIZE(failed) > 0) THEN
    message := TO_JSON(OBJECT_CONSTRUCT(
      'type',           'FAILED_FILES',
      'environment',    'DEV',
      'landing_bucket', 'walmart-sales-landing-dev-pr518',
      'error_bucket',   'walmart-sales-errors-dev-pr518',
      'run_at',         CURRENT_TIMESTAMP()::VARCHAR,
      'failed_files',   failed));
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  END IF;

  RETURN OBJECT_CONSTRUCT('environment', 'DEV', 'files_loaded', loaded,
                          'files_failed', ARRAY_SIZE(failed), 'failed_files', failed);
END;
$$;

-- ---------------------------------------------------------------------
-- Part 3 (STAGING). Load procedure: COPY every landing folder, collect files that
-- were skipped (ON_ERROR = SKIP_FILE does not fail the statement), and publish
-- them to SNS. SNS fans out to: email, the SQS reprocess queue, and the Lambda
-- that moves each failed file to the error bucket.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WALMART_STAGING.RAW.LOAD_LANDING_FILES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  failed    ARRAY DEFAULT ARRAY_CONSTRUCT();
  batch     ARRAY;
  loaded    INTEGER DEFAULT 0;
  n         INTEGER;
  message   VARCHAR;
BEGIN
  -- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
  COPY INTO WALMART_STAGING.RAW.DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                                       _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_STAGING.RAW.LANDING_STAGE/department/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'DEPARTMENT', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- stores.csv: Store, Type, Size
  COPY INTO WALMART_STAGING.RAW.STORES (store_id, store_type, store_size,
                                   _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_STAGING.RAW.LANDING_STAGE/stores/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'STORES', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
  COPY INTO WALMART_STAGING.RAW.FACT (store_id, week_date, temperature, fuel_price,
                                 markdown1, markdown2, markdown3, markdown4, markdown5,
                                 cpi, unemployment, is_holiday,
                                 _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_STAGING.RAW.LANDING_STAGE/fact/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'FACT', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- Publish skipped files to SNS
  IF (ARRAY_SIZE(failed) > 0) THEN
    message := TO_JSON(OBJECT_CONSTRUCT(
      'type',           'FAILED_FILES',
      'environment',    'STAGING',
      'landing_bucket', 'walmart-sales-landing-staging-pr518',
      'error_bucket',   'walmart-sales-errors-staging-pr518',
      'run_at',         CURRENT_TIMESTAMP()::VARCHAR,
      'failed_files',   failed));
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  END IF;

  RETURN OBJECT_CONSTRUCT('environment', 'STAGING', 'files_loaded', loaded,
                          'files_failed', ARRAY_SIZE(failed), 'failed_files', failed);
END;
$$;

-- ---------------------------------------------------------------------
-- Part 3 (PROD). Load procedure: COPY every landing folder, collect files that
-- were skipped (ON_ERROR = SKIP_FILE does not fail the statement), and publish
-- them to SNS. SNS fans out to: email, the SQS reprocess queue, and the Lambda
-- that moves each failed file to the error bucket.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WALMART_PROD.RAW.LOAD_LANDING_FILES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  failed    ARRAY DEFAULT ARRAY_CONSTRUCT();
  batch     ARRAY;
  loaded    INTEGER DEFAULT 0;
  n         INTEGER;
  message   VARCHAR;
BEGIN
  -- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
  COPY INTO WALMART_PROD.RAW.DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                                       _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_PROD.RAW.LANDING_STAGE/department/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'DEPARTMENT', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- stores.csv: Store, Type, Size
  COPY INTO WALMART_PROD.RAW.STORES (store_id, store_type, store_size,
                                   _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_PROD.RAW.LANDING_STAGE/stores/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'STORES', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
  COPY INTO WALMART_PROD.RAW.FACT (store_id, week_date, temperature, fuel_price,
                                 markdown1, markdown2, markdown3, markdown4, markdown5,
                                 cpi, unemployment, is_holiday,
                                 _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_PROD.RAW.LANDING_STAGE/fact/)
  ON_ERROR = SKIP_FILE;
  -- COPY returns one row per file; ARRAY_AGG skips the NULLs, so only failed files are kept
  SELECT COALESCE(ARRAY_AGG(IFF(r:"status"::VARCHAR IN ('LOAD_FAILED', 'PARTIALLY_LOADED'),
                                OBJECT_CONSTRUCT('table', 'FACT', 'file', r:"file"::VARCHAR,
                                                 'error', r:"first_error"::VARCHAR),
                                NULL)), ARRAY_CONSTRUCT()),
         COALESCE(SUM(IFF(r:"status"::VARCHAR = 'LOADED', 1, 0)), 0)
    INTO :batch, :n
    FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  failed := ARRAY_CAT(failed, batch);
  loaded := loaded + n;

  -- Publish skipped files to SNS
  IF (ARRAY_SIZE(failed) > 0) THEN
    message := TO_JSON(OBJECT_CONSTRUCT(
      'type',           'FAILED_FILES',
      'environment',    'PROD',
      'landing_bucket', 'walmart-sales-landing-prod-pr518',
      'error_bucket',   'walmart-sales-errors-prod-pr518',
      'run_at',         CURRENT_TIMESTAMP()::VARCHAR,
      'failed_files',   failed));
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  END IF;

  RETURN OBJECT_CONSTRUCT('environment', 'PROD', 'files_loaded', loaded,
                          'files_failed', ARRAY_SIZE(failed), 'failed_files', failed);
END;
$$;

-- ---------------------------------------------------------------------
-- Part 4. Weekly load task (PROD): Mondays 6:00 AM Eastern.
-- ERROR_INTEGRATION also alerts SNS if the task itself fails (e.g. stage access).
-- The GitHub "dbt scheduled refresh" workflow rebuilds SILVER and GOLD afterwards.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TASK WALMART_PROD.RAW.WEEKLY_LOAD
  WAREHOUSE         = COMPUTE_WH
  SCHEDULE          = 'USING CRON 0 6 * * 1 America/New_York'
  ERROR_INTEGRATION = WALMART_ERRORS_SNS
  COMMENT           = 'Weekly COPY INTO of landing files; skipped files are published to SNS'
AS
  CALL WALMART_PROD.RAW.LOAD_LANDING_FILES();

ALTER TASK WALMART_PROD.RAW.WEEKLY_LOAD RESUME;      -- tasks are created suspended
SHOW TASKS IN SCHEMA WALMART_PROD.RAW;               -- state should be 'started'

-- Run it now instead of waiting for Monday (backfill or testing):
-- EXECUTE TASK WALMART_PROD.RAW.WEEKLY_LOAD;
-- Or call the procedure directly in any environment:
-- CALL WALMART_DEV.RAW.LOAD_LANDING_FILES();

-- ---------------------------------------------------------------------
-- Part 5. Test (DEV): upload aws/test-files/fact_bad_test.csv to
-- s3://walmart-sales-landing-dev-pr518/landing/fact/ then run:
-- ---------------------------------------------------------------------
-- CALL WALMART_DEV.RAW.LOAD_LANDING_FILES();
-- Expected: files_failed = 1. Within a minute: an email, a message in the SQS queue,
-- and the file moved to s3://walmart-sales-errors-dev-pr518/fact/<date>/.
