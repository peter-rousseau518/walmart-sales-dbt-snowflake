# Walmart Sales Analytics: S3, Snowflake, dbt, Streamlit

[![dbt CI](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/ci.yml/badge.svg)](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/ci.yml)
[![dbt deploy](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/deploy.yml/badge.svg)](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/deploy.yml)

An event-driven analytics pipeline. Each CSV file that lands in S3 triggers a load into a Snowflake raw layer under
an explicit load contract; dbt models it into a tested star schema (bronze → silver → gold); and a multi-page
Streamlit in Snowflake app reads the gold tables. The pipeline runs in DEV, STAGING and PROD with CI/CD in GitHub
Actions. Failed files and rejected rows follow an error path (load audit table, error bucket, SNS alerts to the
source team, SQS reprocess queue with dead-letter queues) and are backfilled automatically when corrected.

## Architecture

![Production architecture](docs/images/architecture-production-design.png)

| Layer | Where | What happens |
|---|---|---|
| Landing | S3 `walmart-sales-landing-<env>` | Files arrive in `landing/department/`, `landing/stores/`, `landing/fact/`; each new file sends an S3 event to SQS |
| Bronze | Snowflake `WALMART_<ENV>.RAW` | The event triggers a task that runs `LOAD_LANDING_FILES()`: COPY INTO typed tables, `ON_ERROR = SKIP_FILE`, every file logged in `LOAD_AUDIT` |
| Silver | `WALMART_<ENV>.SILVER` | dbt views: rename, standardize, de-duplicate (latest load wins), row checks; bad rows go to `QUARANTINE_ROWS` |
| Gold | `WALMART_<ENV>.GOLD` | dbt tables: star schema with 2 dimensions and 2 facts (sales fact is incremental), gated by tests |
| Presentation | Streamlit in Snowflake | Home page + 10 pages on `WALMART_PROD.GOLD`, on its own warehouse, with a "Report a data issue" form |
| Error path | S3, SNS, SQS, Lambda | Failed files moved to `walmart-sales-errors-<env>`; source team emailed; `walmart-reprocess-queue` tracks the backfill |

## What I built

- **Three environments** (DEV, STAGING, PROD): one S3 landing bucket and one Snowflake database each, connected
  through a storage integration with a read-only IAM role (external ID) scoped to the `landing/` folders
- **Event-driven ingestion with SQS:** S3 event notifications go to an SQS queue; Snowflake refreshes the stage's
  directory table, a stream records each new file, and a triggered task runs COPY INTO. COPY's load history means a
  file never loads twice
- **An explicit load contract:** typed raw tables with NOT NULL keys and a file format that turns "NA" into NULL.
  Every row records its source file, row number and load time
- **A load audit (error) table:** `RAW.LOAD_AUDIT` records every file's status, row count and first error. If 900 of
  1,000 files are good, those 900 flow through; the 100 bad files go to the error path
- **Error handling with SNS and SQS:** failed files, rejected rows and task failures publish to the SNS topic
  `walmart-pipeline-errors`, which emails the source team, adds a message to `walmart-reprocess-queue` (with a
  dead-letter queue), and triggers a Lambda that moves the file to the error bucket with an `.error.json`
- **Automatic backfill:** a corrected file re-sent to `landing/` triggers its own load; the incremental fact merges
  only the affected weeks, and the SQS message is cleared once `LOAD_AUDIT` shows it loaded
- **Medallion schemas:** RAW (bronze), SILVER (staging views and the quarantine table), GOLD (mart tables)
- **Row-level data quality:** checks flag duplicates, unknown stores and missing sales. Good rows flow to gold; the
  rest land in `SILVER.QUARANTINE_ROWS` with a `reject_reason`, and PROD sends an alert
- **Incremental processing:** `fct_weekly_sales` reads only rows loaded since the last run and merges them on store,
  department and week, so corrected weeks replace old rows instead of duplicating them
- **CI/CD with GitHub Actions:** every pull request is built and tested in its own throwaway schema in DEV; merging to
  `main` deploys to STAGING, then to PROD after a manual approval. After each load, a Lambda starts the dbt refresh in
  PROD. CI runs as a key-pair service user with a least-privilege `TRANSFORMER` role
- **dbt tests:** unique, not_null, relationships from each fact to each dimension, grain tests on both facts, a
  holiday-rule test, a warning test on quarantined rows, and a source freshness check
- **A 10-page Streamlit app** on its own warehouse with a resource monitor; its cache clears after every dbt refresh,
  and a "Report a data issue" form sends problems straight to data engineering

## Data model

![Star schema](docs/images/star-schema.png)

| Table | Grain | Rows |
|---|---|---|
| `dim_store` | one row per store | 45 |
| `dim_date` | one row per week (calendar spine, Feb 2010 – Dec 2015) | 308 |
| `fct_weekly_sales` | one row per store, department, and week | 421,570 |
| `fct_store_weekly_conditions` | one row per store and week | 8,190 |

## Data quality decisions

- **Each fact at its own grain.** Weekly conditions (temperature, CPI, fuel price, markdowns) are recorded per store and
  week, so they live in their own fact instead of repeating on every department row.
- **Why a LEFT JOIN to combine them.** The app sums sales to store-week first, then LEFT JOINs the conditions fact on
  store and week. LEFT keeps every sales week even if its conditions row is missing (an INNER JOIN would drop those
  sales). The risks and how they're handled: fan-out (the conditions grain test guarantees one row per store-week, so
  the join can't add rows); repeated values (joining at department grain overstated markdowns ~65× in my first
  version, so the app aggregates first); and NULLs (shown as "no data", never as zero).
- **"NA" handled at the gate.** The load contract converts "NA" to NULL (`NULL_IF = ('NA', '')`). MarkDown1 is missing
  in about 51% of rows because markdown data starts in November 2011.
- **Two levels of protection:** file level (a file that breaks the contract is skipped and sent to the error path) and
  row level (a bad row in a good file is quarantined, so one bad row never blocks a load).
- `dim_date` is a calendar spine with retail holiday rules and a `has_sales` flag, so empty weeks are never averaged.

## Security, retention and cost

- **Access:** storage integration with an IAM role and external ID (no AWS keys stored); key-pair service user for CI;
  no credentials in Git
- **Encryption:** S3, SNS and SQS use KMS customer-managed keys, and the buckets deny non-TLS requests. Snowflake
  encrypts all data at rest and in transit
- **Data retention:** S3 versioning on the landing and error buckets; landing files move to Glacier after 90 days and
  are kept 7 years; error files are deleted 180 days after they are fixed; `LOAD_AUDIT` keeps 2 years
- **Backup and recovery:** Time Travel (7 days on RAW and GOLD) and Fail-safe; any layer can be rebuilt from the raw
  files in S3
- **Compute cost:** separate X-Small warehouses for transforms and the dashboard, 60-second auto-suspend, and resource
  monitors that alert at 80% and suspend at 100% of the monthly quota

## Service levels

- A new file is in the dashboard within 1 hour of landing; the weekly drop is visible by Monday 9:00 AM ET
- The source team is emailed within minutes of a failed load
- Data issues reported in the app are acknowledged within 1 business day and fixed or explained within 3

The runbook for loads, failures and reprocessing is in [docs/runbook.md](docs/runbook.md).

## CI/CD and environments

| Environment | Snowflake database | How code gets there |
|---|---|---|
| DEV | `WALMART_DEV` (`DBT_PETER_SILVER`/`_GOLD`, plus `CI_PR_<n>_SILVER`/`_GOLD` per pull request) | `dbt build` locally; CI builds every pull request |
| STAGING | `WALMART_STAGING` (`SILVER`, `GOLD`) | Automatic on merge to `main` |
| PROD | `WALMART_PROD` (`SILVER`, `GOLD`) | After STAGING passes **and** a reviewer approves the `production` environment |

- `.github/workflows/ci.yml`: builds and tests each pull request; drops its schemas when the pull request closes
- `.github/workflows/deploy.yml`: STAGING on merge, then PROD behind an approval gate
- `.github/workflows/refresh.yml`: rebuilds PROD after each load (started by Lambda) or on demand
- `ci/profiles.yml`: connection targets for CI (secrets come from GitHub, never from the repo)
- The `main` branch is protected: a pull request must pass CI before it can merge

## Repository layout

- `models/staging/`: silver models; `checks/` holds the row-level checks, `quarantine_rows.sql` the rejected rows
- `models/marts/`: gold dimensions and facts
- `tests/`: custom dbt tests (grain, holiday rule, quarantine warning)
- `macros/`: schema naming (SILVER/GOLD, prefixed in DEV and CI), CI schema cleanup, quarantine alert
- `snowflake/`: account setup, per-environment raw layer and load contract, CI service user, SILVER/GOLD schemas,
  and the error path (notification integration, load procedures, load task)
- `aws/iam/`: IAM policies (Snowflake read access, SNS publish, Lambda)
- `aws/lambda/move_failed_files.py`: moves failed files to the error bucket
- `aws/test-files/`: a bad and a corrected file for testing the error path
- `.github/workflows/`, `ci/`: CI/CD pipeline
- `streamlit_app/`: the Streamlit in Snowflake app
- `docs/runbook.md`: operating the pipeline

## Screenshots

![Screenshot 1](docs/images/shot-1.png)

![Screenshot 2](docs/images/shot-2.png)

![Screenshot 3](docs/images/shot-3.png)

![Screenshot 4](docs/images/shot-4.png)

![Screenshot 5](docs/images/shot-5.png)

## What's next

- Power BI semantic model on the gold tables
- Snowpipe if files start arriving continuously (thousands per hour)

## Data source

[Walmart Recruiting – Store Sales Forecasting (Kaggle)](https://www.kaggle.com/c/walmart-recruiting-store-sales-forecasting)

## Tools

Snowflake (tasks, streams, stored procedures, notification integrations), dbt, AWS (S3, IAM, KMS, SNS, SQS, Lambda),
GitHub Actions, Streamlit, SQL, Python
