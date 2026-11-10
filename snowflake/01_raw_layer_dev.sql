/* =====================================================================
   Walmart Sales Pipeline - RAW (bronze) layer for DEV
   Database : WALMART_DEV
   Bucket   : s3://walmart-sales-landing-dev-pr518/landing/
   Run top to bottom in a Snowsight worksheet. Safe to re-run: COPY INTO
   skips files it has already loaded.
   Prerequisite: snowflake/00_account_setup.sql has been run once.
   ===================================================================== */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ---------------------------------------------------------------------
-- 1. Environment database and schemas
--    RAW       = bronze: data exactly as delivered, typed by the load contract
--    SILVER and GOLD are created by dbt the first time it builds
--    (DEV and CI use prefixed schemas, e.g. DBT_PETER_GOLD)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS WALMART_DEV;
CREATE SCHEMA   IF NOT EXISTS WALMART_DEV.RAW;
USE SCHEMA WALMART_DEV.RAW;

-- ---------------------------------------------------------------------
-- 2. Load contract, part 1: the file format
--    'NA' and empty strings become real NULLs at the warehouse gate.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT CSV_CONTRACT
  TYPE                         = CSV
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('NA', '')
  EMPTY_FIELD_AS_NULL          = TRUE
  DATE_FORMAT                  = 'AUTO';

-- ---------------------------------------------------------------------
-- 3. Stage: one folder per source file under landing/
-- ---------------------------------------------------------------------
CREATE OR REPLACE STAGE LANDING_STAGE
  URL                 = 's3://walmart-sales-landing-dev-pr518/landing/'
  STORAGE_INTEGRATION = s3_walmart_int
  FILE_FORMAT         = CSV_CONTRACT;

-- Checkpoint: must list department/, stores/ and fact/ files
LIST @LANDING_STAGE;

-- ---------------------------------------------------------------------
-- 4. Load contract, part 2: explicit table definitions (replaces INFER_SCHEMA)
--    Types and NOT NULL keys are enforced on load. A file with text in a
--    numeric column, a bad date, or a missing key fails instead of loading.
--    Every row records where it came from and when it was loaded.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEPARTMENT (
    store_id          NUMBER(4,0)   NOT NULL,
    dept_id           NUMBER(4,0)   NOT NULL,
    week_date         DATE          NOT NULL,
    weekly_sales      NUMBER(12,2),
    is_holiday        BOOLEAN,
    _file_name        VARCHAR       NOT NULL,
    _file_row_number  NUMBER        NOT NULL,
    _loaded_at        TIMESTAMP_LTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS STORES (
    store_id          NUMBER(4,0)   NOT NULL,
    store_type        VARCHAR(1)    NOT NULL,
    store_size        NUMBER(10,0),
    _file_name        VARCHAR       NOT NULL,
    _file_row_number  NUMBER        NOT NULL,
    _loaded_at        TIMESTAMP_LTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS FACT (
    store_id          NUMBER(4,0)   NOT NULL,
    week_date         DATE          NOT NULL,
    temperature       NUMBER(6,2),
    fuel_price        NUMBER(6,3),
    markdown1         NUMBER(12,2),
    markdown2         NUMBER(12,2),
    markdown3         NUMBER(12,2),
    markdown4         NUMBER(12,2),
    markdown5         NUMBER(12,2),
    cpi               NUMBER(14,7),
    unemployment      NUMBER(6,3),
    is_holiday        BOOLEAN,
    _file_name        VARCHAR       NOT NULL,
    _file_row_number  NUMBER        NOT NULL,
    _loaded_at        TIMESTAMP_LTZ NOT NULL
);

-- ---------------------------------------------------------------------
-- 5. Load. Columns are read by position ($1, $2 ...) in the source file order.
--    ON_ERROR = SKIP_FILE: a file that breaks the contract loads nothing.
-- ---------------------------------------------------------------------
-- department.csv: Store, Dept, Date, Weekly_Sales, IsHoliday
COPY INTO DEPARTMENT (store_id, dept_id, week_date, weekly_sales, is_holiday,
                      _file_name, _file_row_number, _loaded_at)
FROM (
    SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @LANDING_STAGE/department/
)
ON_ERROR = SKIP_FILE;

-- stores.csv: Store, Type, Size
COPY INTO STORES (store_id, store_type, store_size,
                  _file_name, _file_row_number, _loaded_at)
FROM (
    SELECT $1, $2, $3, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @LANDING_STAGE/stores/
)
ON_ERROR = SKIP_FILE;

-- fact.csv: Store, Date, Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment, IsHoliday
COPY INTO FACT (store_id, week_date, temperature, fuel_price,
                markdown1, markdown2, markdown3, markdown4, markdown5,
                cpi, unemployment, is_holiday,
                _file_name, _file_row_number, _loaded_at)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @LANDING_STAGE/fact/
)
ON_ERROR = SKIP_FILE;

-- ---------------------------------------------------------------------
-- 6. Checks
-- ---------------------------------------------------------------------
-- Expected: DEPARTMENT 421,570 | STORES 45 | FACT 8,190
SELECT 'DEPARTMENT' AS table_name, COUNT(*) AS row_count FROM DEPARTMENT
UNION ALL SELECT 'STORES', COUNT(*) FROM STORES
UNION ALL SELECT 'FACT',   COUNT(*) FROM FACT;

-- 'NA' now arrives as a real NULL (expected: 4,158 of 8,190)
SELECT COUNT(*) AS total_rows, COUNT(*) - COUNT(markdown1) AS null_markdown1 FROM FACT;

-- Any file skipped in the last day shows up here with its first error
SELECT file_name, status, row_count, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'WALMART_DEV.RAW.FACT',
       START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())))
UNION ALL
SELECT file_name, status, row_count, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'WALMART_DEV.RAW.DEPARTMENT',
       START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())))
UNION ALL
SELECT file_name, status, row_count, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'WALMART_DEV.RAW.STORES',
       START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())));
