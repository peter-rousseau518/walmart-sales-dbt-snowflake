/* =====================================================================
   Walmart Sales Pipeline - Phase 4: event-driven load + load audit
   (run as ACCOUNTADMIN, after 04_error_path.sql)

   How a file flows:
     1. A file lands in s3://walmart-sales-landing-<env>-pr518/landing/<source>/
     2. S3 sends an event notification to Snowflake's SQS queue, which refreshes
        the directory table on the LANDING_EVENTS stage
     3. The stream LANDING_EVENTS_STREAM records the new file
     4. The triggered task LOAD_ON_ARRIVAL wakes up, logs the arrival and calls
        LOAD_LANDING_FILES(): COPY INTO with ON_ERROR = SKIP_FILE
     5. Every file's result is written to RAW.LOAD_AUDIT; skipped files are
        published to SNS (email + SQS reprocess queue + Lambda -> error bucket)

   Parts:
     Part 1  audit tables (all environments)
     Part 2  load procedures with audit (replaces the Part 3 procedures in 04)
     Part 3  event stage, stream and triggered task (all environments)
     Part 4  remove the old weekly schedule
     Part 5  get the SQS ARN for the S3 event notification (see the build guide)

   Safe to re-run. The COPY stage (LANDING_STAGE) is not changed, so COPY's
   load history is untouched and no file is loaded twice.
   ===================================================================== */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ---------------------------------------------------------------------
-- Part 1 (DEV). Audit tables
--   LOAD_AUDIT    one row per file per load attempt (the error table)
--   FILE_ARRIVALS one row per file event seen by the stream
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WALMART_DEV.RAW.LOAD_AUDIT (
    run_id        VARCHAR       NOT NULL,
    environment   VARCHAR       NOT NULL,
    table_name    VARCHAR       NOT NULL,
    file_name     VARCHAR       NOT NULL,
    status        VARCHAR       NOT NULL,   -- LOADED, LOAD_FAILED, PARTIALLY_LOADED
    rows_parsed   NUMBER,
    rows_loaded   NUMBER,
    errors_seen   NUMBER,
    first_error   VARCHAR,
    loaded_at     TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS WALMART_DEV.RAW.FILE_ARRIVALS (
    relative_path VARCHAR,
    size_bytes    NUMBER,
    last_modified TIMESTAMP_LTZ,
    event_action  VARCHAR,                  -- INSERT (new file) or DELETE (file removed)
    seen_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- Part 1 (STAGING). Audit tables
--   LOAD_AUDIT    one row per file per load attempt (the error table)
--   FILE_ARRIVALS one row per file event seen by the stream
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WALMART_STAGING.RAW.LOAD_AUDIT (
    run_id        VARCHAR       NOT NULL,
    environment   VARCHAR       NOT NULL,
    table_name    VARCHAR       NOT NULL,
    file_name     VARCHAR       NOT NULL,
    status        VARCHAR       NOT NULL,   -- LOADED, LOAD_FAILED, PARTIALLY_LOADED
    rows_parsed   NUMBER,
    rows_loaded   NUMBER,
    errors_seen   NUMBER,
    first_error   VARCHAR,
    loaded_at     TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS WALMART_STAGING.RAW.FILE_ARRIVALS (
    relative_path VARCHAR,
    size_bytes    NUMBER,
    last_modified TIMESTAMP_LTZ,
    event_action  VARCHAR,                  -- INSERT (new file) or DELETE (file removed)
    seen_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- Part 1 (PROD). Audit tables
--   LOAD_AUDIT    one row per file per load attempt (the error table)
--   FILE_ARRIVALS one row per file event seen by the stream
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WALMART_PROD.RAW.LOAD_AUDIT (
    run_id        VARCHAR       NOT NULL,
    environment   VARCHAR       NOT NULL,
    table_name    VARCHAR       NOT NULL,
    file_name     VARCHAR       NOT NULL,
    status        VARCHAR       NOT NULL,   -- LOADED, LOAD_FAILED, PARTIALLY_LOADED
    rows_parsed   NUMBER,
    rows_loaded   NUMBER,
    errors_seen   NUMBER,
    first_error   VARCHAR,
    loaded_at     TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS WALMART_PROD.RAW.FILE_ARRIVALS (
    relative_path VARCHAR,
    size_bytes    NUMBER,
    last_modified TIMESTAMP_LTZ,
    event_action  VARCHAR,                  -- INSERT (new file) or DELETE (file removed)
    seen_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- Part 2 (DEV). Load procedure: COPY every landing folder, log every file
-- to LOAD_AUDIT, and publish skipped files to SNS.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WALMART_DEV.RAW.LOAD_LANDING_FILES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id   VARCHAR DEFAULT UUID_STRING();
  qid      VARCHAR;
  failed   ARRAY;
  loaded   INTEGER;
  message  VARCHAR;
BEGIN

  -- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
  COPY INTO WALMART_DEV.RAW.DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_DEV.RAW.LANDING_STAGE/department/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_DEV.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'DEV', 'DEPARTMENT', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- stores.csv: Store, Type, Size
  COPY INTO WALMART_DEV.RAW.STORES (store_id, store_type, store_size,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_DEV.RAW.LANDING_STAGE/stores/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_DEV.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'DEV', 'STORES', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
  COPY INTO WALMART_DEV.RAW.FACT (store_id, week_date, temperature, fuel_price,
                    markdown1, markdown2, markdown3, markdown4, markdown5,
                    cpi, unemployment, is_holiday,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_DEV.RAW.LANDING_STAGE/fact/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_DEV.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'DEV', 'FACT', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- Summarize this run from the audit table
  SELECT COALESCE(ARRAY_AGG(IFF(status <> 'LOADED',
                                OBJECT_CONSTRUCT('table', table_name, 'file', file_name, 'error', first_error),
                                NULL)), ARRAY_CONSTRUCT()),
         COUNT_IF(status = 'LOADED')
    INTO :failed, :loaded
    FROM WALMART_DEV.RAW.LOAD_AUDIT
   WHERE run_id = :run_id;

  -- Publish skipped files to SNS (email, SQS reprocess queue, Lambda -> error bucket)
  IF (ARRAY_SIZE(failed) > 0) THEN
    message := TO_JSON(OBJECT_CONSTRUCT(
      'type',           'FAILED_FILES',
      'environment',    'DEV',
      'run_id',         run_id,
      'landing_bucket', 'walmart-sales-landing-dev-pr518',
      'error_bucket',   'walmart-sales-errors-dev-pr518',
      'run_at',         CURRENT_TIMESTAMP()::VARCHAR,
      'failed_files',   failed));
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  END IF;

  -- Retention: keep two years of load history
  DELETE FROM WALMART_DEV.RAW.LOAD_AUDIT WHERE loaded_at < DATEADD(year, -2, CURRENT_TIMESTAMP());

  RETURN OBJECT_CONSTRUCT('environment', 'DEV', 'run_id', run_id, 'files_loaded', loaded,
                          'files_failed', ARRAY_SIZE(failed), 'failed_files', failed);
END;
$$;

-- ---------------------------------------------------------------------
-- Part 2 (STAGING). Load procedure: COPY every landing folder, log every file
-- to LOAD_AUDIT, and publish skipped files to SNS.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WALMART_STAGING.RAW.LOAD_LANDING_FILES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id   VARCHAR DEFAULT UUID_STRING();
  qid      VARCHAR;
  failed   ARRAY;
  loaded   INTEGER;
  message  VARCHAR;
BEGIN

  -- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
  COPY INTO WALMART_STAGING.RAW.DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_STAGING.RAW.LANDING_STAGE/department/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_STAGING.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'STAGING', 'DEPARTMENT', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- stores.csv: Store, Type, Size
  COPY INTO WALMART_STAGING.RAW.STORES (store_id, store_type, store_size,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_STAGING.RAW.LANDING_STAGE/stores/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_STAGING.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'STAGING', 'STORES', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
  COPY INTO WALMART_STAGING.RAW.FACT (store_id, week_date, temperature, fuel_price,
                    markdown1, markdown2, markdown3, markdown4, markdown5,
                    cpi, unemployment, is_holiday,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_STAGING.RAW.LANDING_STAGE/fact/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_STAGING.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'STAGING', 'FACT', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- Summarize this run from the audit table
  SELECT COALESCE(ARRAY_AGG(IFF(status <> 'LOADED',
                                OBJECT_CONSTRUCT('table', table_name, 'file', file_name, 'error', first_error),
                                NULL)), ARRAY_CONSTRUCT()),
         COUNT_IF(status = 'LOADED')
    INTO :failed, :loaded
    FROM WALMART_STAGING.RAW.LOAD_AUDIT
   WHERE run_id = :run_id;

  -- Publish skipped files to SNS (email, SQS reprocess queue, Lambda -> error bucket)
  IF (ARRAY_SIZE(failed) > 0) THEN
    message := TO_JSON(OBJECT_CONSTRUCT(
      'type',           'FAILED_FILES',
      'environment',    'STAGING',
      'run_id',         run_id,
      'landing_bucket', 'walmart-sales-landing-staging-pr518',
      'error_bucket',   'walmart-sales-errors-staging-pr518',
      'run_at',         CURRENT_TIMESTAMP()::VARCHAR,
      'failed_files',   failed));
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  END IF;

  -- Retention: keep two years of load history
  DELETE FROM WALMART_STAGING.RAW.LOAD_AUDIT WHERE loaded_at < DATEADD(year, -2, CURRENT_TIMESTAMP());

  RETURN OBJECT_CONSTRUCT('environment', 'STAGING', 'run_id', run_id, 'files_loaded', loaded,
                          'files_failed', ARRAY_SIZE(failed), 'failed_files', failed);
END;
$$;

-- ---------------------------------------------------------------------
-- Part 2 (PROD). Load procedure: COPY every landing folder, log every file
-- to LOAD_AUDIT, and publish skipped files to SNS.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WALMART_PROD.RAW.LOAD_LANDING_FILES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id   VARCHAR DEFAULT UUID_STRING();
  qid      VARCHAR;
  failed   ARRAY;
  loaded   INTEGER;
  message  VARCHAR;
BEGIN

  -- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
  COPY INTO WALMART_PROD.RAW.DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_PROD.RAW.LANDING_STAGE/department/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_PROD.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'PROD', 'DEPARTMENT', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- stores.csv: Store, Type, Size
  COPY INTO WALMART_PROD.RAW.STORES (store_id, store_type, store_size,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_PROD.RAW.LANDING_STAGE/stores/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_PROD.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'PROD', 'STORES', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
  COPY INTO WALMART_PROD.RAW.FACT (store_id, week_date, temperature, fuel_price,
                    markdown1, markdown2, markdown3, markdown4, markdown5,
                    cpi, unemployment, is_holiday,
                    _file_name, _file_row_number, _loaded_at)
  FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
        FROM @WALMART_PROD.RAW.LANDING_STAGE/fact/)
  ON_ERROR = SKIP_FILE;
  qid := LAST_QUERY_ID();
  -- COPY returns one row per file (or one summary row when there is nothing new)
  INSERT INTO WALMART_PROD.RAW.LOAD_AUDIT
      (run_id, environment, table_name, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
  SELECT :run_id, 'PROD', 'FACT', r:"file"::VARCHAR, r:"status"::VARCHAR,
         r:"rows_parsed"::NUMBER, r:"rows_loaded"::NUMBER, r:"errors_seen"::NUMBER, r:"first_error"::VARCHAR
  FROM (SELECT OBJECT_CONSTRUCT(*) AS r FROM TABLE(RESULT_SCAN(:qid)))
  WHERE r:"file" IS NOT NULL;

  -- Summarize this run from the audit table
  SELECT COALESCE(ARRAY_AGG(IFF(status <> 'LOADED',
                                OBJECT_CONSTRUCT('table', table_name, 'file', file_name, 'error', first_error),
                                NULL)), ARRAY_CONSTRUCT()),
         COUNT_IF(status = 'LOADED')
    INTO :failed, :loaded
    FROM WALMART_PROD.RAW.LOAD_AUDIT
   WHERE run_id = :run_id;

  -- Publish skipped files to SNS (email, SQS reprocess queue, Lambda -> error bucket)
  IF (ARRAY_SIZE(failed) > 0) THEN
    message := TO_JSON(OBJECT_CONSTRUCT(
      'type',           'FAILED_FILES',
      'environment',    'PROD',
      'run_id',         run_id,
      'landing_bucket', 'walmart-sales-landing-prod-pr518',
      'error_bucket',   'walmart-sales-errors-prod-pr518',
      'run_at',         CURRENT_TIMESTAMP()::VARCHAR,
      'failed_files',   failed));
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:message),
      SNOWFLAKE.NOTIFICATION.INTEGRATION('WALMART_ERRORS_SNS'));
  END IF;

  -- Retention: keep two years of load history
  DELETE FROM WALMART_PROD.RAW.LOAD_AUDIT WHERE loaded_at < DATEADD(year, -2, CURRENT_TIMESTAMP());

  RETURN OBJECT_CONSTRUCT('environment', 'PROD', 'run_id', run_id, 'files_loaded', loaded,
                          'files_failed', ARRAY_SIZE(failed), 'failed_files', failed);
END;
$$;

-- ---------------------------------------------------------------------
-- Part 3 (DEV). Event stage, stream and triggered task
--   LANDING_EVENTS is a second stage on the same folder, used only to detect
--   new files (directory table with AUTO_REFRESH from S3 events via SQS).
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS WALMART_DEV.RAW.LANDING_EVENTS
  URL                 = 's3://walmart-sales-landing-dev-pr518/landing/'
  STORAGE_INTEGRATION = s3_walmart_int
  DIRECTORY           = (ENABLE = TRUE AUTO_REFRESH = TRUE);

-- Register the files already in the bucket (only needed once), before the stream exists,
-- so existing files do not trigger a load
ALTER STAGE WALMART_DEV.RAW.LANDING_EVENTS REFRESH;

CREATE STREAM IF NOT EXISTS WALMART_DEV.RAW.LANDING_EVENTS_STREAM ON STAGE WALMART_DEV.RAW.LANDING_EVENTS;

-- Triggered task: no SCHEDULE; it runs when the stream has new rows (at most every 30 seconds)
CREATE OR REPLACE TASK WALMART_DEV.RAW.LOAD_ON_ARRIVAL
  WAREHOUSE         = COMPUTE_WH
  ERROR_INTEGRATION = WALMART_ERRORS_SNS
  COMMENT           = 'Loads landing files when they arrive; skipped files go to SNS'
  WHEN SYSTEM$STREAM_HAS_DATA('WALMART_DEV.RAW.LANDING_EVENTS_STREAM')
AS
EXECUTE IMMEDIATE $$
BEGIN
  -- Reading the stream in a DML statement consumes it, so the task does not fire again for the same files
  INSERT INTO WALMART_DEV.RAW.FILE_ARRIVALS (relative_path, size_bytes, last_modified, event_action)
    SELECT relative_path, size, last_modified, METADATA$ACTION
    FROM WALMART_DEV.RAW.LANDING_EVENTS_STREAM;
  CALL WALMART_DEV.RAW.LOAD_LANDING_FILES();
END;
$$;

ALTER TASK WALMART_DEV.RAW.LOAD_ON_ARRIVAL RESUME;   -- tasks are created suspended

-- ---------------------------------------------------------------------
-- Part 3 (STAGING). Event stage, stream and triggered task
--   LANDING_EVENTS is a second stage on the same folder, used only to detect
--   new files (directory table with AUTO_REFRESH from S3 events via SQS).
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS WALMART_STAGING.RAW.LANDING_EVENTS
  URL                 = 's3://walmart-sales-landing-staging-pr518/landing/'
  STORAGE_INTEGRATION = s3_walmart_int
  DIRECTORY           = (ENABLE = TRUE AUTO_REFRESH = TRUE);

-- Register the files already in the bucket (only needed once), before the stream exists,
-- so existing files do not trigger a load
ALTER STAGE WALMART_STAGING.RAW.LANDING_EVENTS REFRESH;

CREATE STREAM IF NOT EXISTS WALMART_STAGING.RAW.LANDING_EVENTS_STREAM ON STAGE WALMART_STAGING.RAW.LANDING_EVENTS;

-- Triggered task: no SCHEDULE; it runs when the stream has new rows (at most every 30 seconds)
CREATE OR REPLACE TASK WALMART_STAGING.RAW.LOAD_ON_ARRIVAL
  WAREHOUSE         = COMPUTE_WH
  ERROR_INTEGRATION = WALMART_ERRORS_SNS
  COMMENT           = 'Loads landing files when they arrive; skipped files go to SNS'
  WHEN SYSTEM$STREAM_HAS_DATA('WALMART_STAGING.RAW.LANDING_EVENTS_STREAM')
AS
EXECUTE IMMEDIATE $$
BEGIN
  -- Reading the stream in a DML statement consumes it, so the task does not fire again for the same files
  INSERT INTO WALMART_STAGING.RAW.FILE_ARRIVALS (relative_path, size_bytes, last_modified, event_action)
    SELECT relative_path, size, last_modified, METADATA$ACTION
    FROM WALMART_STAGING.RAW.LANDING_EVENTS_STREAM;
  CALL WALMART_STAGING.RAW.LOAD_LANDING_FILES();
END;
$$;

ALTER TASK WALMART_STAGING.RAW.LOAD_ON_ARRIVAL RESUME;   -- tasks are created suspended

-- ---------------------------------------------------------------------
-- Part 3 (PROD). Event stage, stream and triggered task
--   LANDING_EVENTS is a second stage on the same folder, used only to detect
--   new files (directory table with AUTO_REFRESH from S3 events via SQS).
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS WALMART_PROD.RAW.LANDING_EVENTS
  URL                 = 's3://walmart-sales-landing-prod-pr518/landing/'
  STORAGE_INTEGRATION = s3_walmart_int
  DIRECTORY           = (ENABLE = TRUE AUTO_REFRESH = TRUE);

-- Register the files already in the bucket (only needed once), before the stream exists,
-- so existing files do not trigger a load
ALTER STAGE WALMART_PROD.RAW.LANDING_EVENTS REFRESH;

CREATE STREAM IF NOT EXISTS WALMART_PROD.RAW.LANDING_EVENTS_STREAM ON STAGE WALMART_PROD.RAW.LANDING_EVENTS;

-- Triggered task: no SCHEDULE; it runs when the stream has new rows (at most every 30 seconds)
CREATE OR REPLACE TASK WALMART_PROD.RAW.LOAD_ON_ARRIVAL
  WAREHOUSE         = COMPUTE_WH
  ERROR_INTEGRATION = WALMART_ERRORS_SNS
  COMMENT           = 'Loads landing files when they arrive; skipped files go to SNS'
  WHEN SYSTEM$STREAM_HAS_DATA('WALMART_PROD.RAW.LANDING_EVENTS_STREAM')
AS
EXECUTE IMMEDIATE $$
BEGIN
  -- Reading the stream in a DML statement consumes it, so the task does not fire again for the same files
  INSERT INTO WALMART_PROD.RAW.FILE_ARRIVALS (relative_path, size_bytes, last_modified, event_action)
    SELECT relative_path, size, last_modified, METADATA$ACTION
    FROM WALMART_PROD.RAW.LANDING_EVENTS_STREAM;
  CALL WALMART_PROD.RAW.LOAD_LANDING_FILES();
END;
$$;

ALTER TASK WALMART_PROD.RAW.LOAD_ON_ARRIVAL RESUME;   -- tasks are created suspended

-- ---------------------------------------------------------------------
-- Part 4. Remove the old weekly schedule (replaced by LOAD_ON_ARRIVAL)
-- ---------------------------------------------------------------------
DROP TASK IF EXISTS WALMART_PROD.RAW.WEEKLY_LOAD;

-- Checkpoint: each environment should show LOAD_ON_ARRIVAL with state 'started'
SHOW TASKS LIKE 'LOAD_ON_ARRIVAL' IN ACCOUNT;

-- ---------------------------------------------------------------------
-- Part 5. The SQS queue ARN for the S3 event notifications
--   Copy the value in the directory_notification_channel row. It is the same
--   for all three stages (one Snowflake queue per AWS region), so one ARN is
--   used for all three buckets.
-- ---------------------------------------------------------------------
DESC STAGE WALMART_DEV.RAW.LANDING_EVENTS;

-- ---------------------------------------------------------------------
-- After the S3 event notifications are set up: checks (see the build guide)
-- ---------------------------------------------------------------------
-- SELECT * FROM WALMART_DEV.RAW.FILE_ARRIVALS ORDER BY seen_at DESC;
-- SELECT * FROM WALMART_DEV.RAW.LOAD_AUDIT ORDER BY loaded_at DESC;
-- SELECT * FROM TABLE(WALMART_DEV.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'LOAD_ON_ARRIVAL'))
--   ORDER BY scheduled_time DESC;
-- Run a load by hand at any time (backfill or testing):
-- CALL WALMART_DEV.RAW.LOAD_LANDING_FILES();
