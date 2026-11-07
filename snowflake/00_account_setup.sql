/* =====================================================================
   Walmart Sales Pipeline - one-time account setup (run once, as ACCOUNTADMIN)
   Points the existing storage integration at the three environment buckets.
   ===================================================================== */

USE ROLE ACCOUNTADMIN;

-- Keep compute cheap: suspend after 60 seconds idle
ALTER WAREHOUSE COMPUTE_WH SET
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE;

-- The storage integration was created in the proof of concept. ALTER keeps the
-- same external ID, so the IAM trust policy does not need to change.
-- (CREATE OR REPLACE would generate a new external ID and break the trust policy.)
ALTER STORAGE INTEGRATION s3_walmart_int SET STORAGE_ALLOWED_LOCATIONS = (
  's3://walmart-sales-landing-dev-pr518/landing/',
  's3://walmart-sales-landing-staging-pr518/landing/',
  's3://walmart-sales-landing-prod-pr518/landing/'
);

DESC INTEGRATION s3_walmart_int;   -- confirm STORAGE_ALLOWED_LOCATIONS
