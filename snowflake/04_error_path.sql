/* =====================================================================
   Walmart Sales Pipeline - error path: Snowflake -> SNS (run as ACCOUNTADMIN)

   Run in order:
     Part 1  notification integration (then finish the IAM trust policy in AWS)
     Part 2  grants
   Then run 05_event_driven_load.sql (load procedures, audit table, triggered task).

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
-- Parts 3-5 moved to 05_event_driven_load.sql: the load procedures (now with the
-- LOAD_AUDIT error table) and the event-triggered task LOAD_ON_ARRIVAL, which
-- replaced the original weekly schedule.
-- ---------------------------------------------------------------------
