# Walmart Sales Analytics: S3, Snowflake, dbt, Streamlit

[![dbt CI](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/ci.yml/badge.svg)](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/ci.yml)
[![dbt deploy](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/deploy.yml/badge.svg)](https://github.com/peter-rousseau518/walmart-sales-dbt-snowflake/actions/workflows/deploy.yml)

An end-to-end analytics pipeline. Weekly CSV files land in S3, a scheduled Snowflake task loads them into a
raw layer under an explicit load contract, dbt models them into a star schema (bronze → silver → gold), and a
multi-page Streamlit in Snowflake app reads the gold tables. The pipeline runs in three environments (DEV,
STAGING, PROD) with CI/CD in GitHub Actions, and failed files and rejected rows are routed to an error path
with SNS alerts, an SQS reprocess queue and an error bucket.

## Architecture

![Production design](docs/images/architecture-production-design.png)

| Layer | Where | What happens |
|---|---|---|
| Landing | S3 `walmart-sales-landing-<env>` | Weekly files arrive in `landing/department/`, `landing/stores/`, `landing/fact/` |
| Bronze | Snowflake `WALMART_<ENV>.RAW` | Task `WEEKLY_LOAD` (Mondays 6 AM ET) runs `LOAD_LANDING_FILES()`: COPY INTO typed tables, `ON_ERROR = SKIP_FILE` |
| Silver | `WALMART_<ENV>.SILVER` | dbt views: rename, cast, de-duplicate (latest load wins), row checks; bad rows go to `QUARANTINE_ROWS` |
| Gold | `WALMART_<ENV>.GOLD` | dbt tables: star schema with 2 dimensions and 2 facts, gated by tests |
| Presentation | Streamlit in Snowflake | Home page + 10 pages reading `WALMART_PROD.GOLD` |
| Error path | S3, SNS, SQS, Lambda | Failed files moved to `walmart-sales-errors-<env>`; alerts by email and to `walmart-reprocess-queue` |

## What I built

- **Three environments** (DEV, STAGING, PROD): one S3 landing bucket and one Snowflake database each, connected
  through a storage integration with a read-only IAM policy scoped to the `landing/` folders
- **An explicit load contract:** typed raw tables with NOT NULL keys and a file format that turns "NA" into NULL.
  Every row records its source file, row number and load time
- **Scheduled ingestion:** a Snowflake task calls a stored procedure that runs COPY INTO for each source. COPY
  tracks load history, so files already loaded are never loaded twice (idempotent)
- **Medallion schemas:** RAW (bronze), SILVER (staging views and the quarantine table), GOLD (mart tables)
- **Row-level data quality:** checks flag duplicates, unknown stores and missing sales. Good rows flow to gold; the
  rest land in `SILVER.QUARANTINE_ROWS` with a `reject_reason`, and a warn-level test reports them
- **Error path:** files that break the contract are skipped and published to SNS. A Lambda moves each failed file
  to the error bucket with an `.error.json` describing why; email alerts the source team; the SQS queue (with a
  dead-letter queue) tracks files waiting to be reprocessed. Rejected rows in PROD also trigger an SNS alert
- **CI/CD with GitHub Actions:** every pull request is built and tested in its own throwaway schema in DEV; merging
  to `main` deploys to STAGING, then to PROD after a manual approval. A scheduled workflow refreshes PROD after the
  weekly load. CI runs as a key-pair service user with a least-privilege `TRANSFORMER` role
- **A dbt project** with 2 dimensions (`dim_store`, and `dim_date` as a calendar spine) and 2 fact tables, each at
  its true grain: `fct_weekly_sales` (store × department × week, about 421K rows) and `fct_store_weekly_conditions`
  (store × week)
- **dbt tests:** unique, not_null, relationships from each fact to each dimension, grain tests on both facts
  (`dbt_utils.unique_combination_of_columns` plus a custom singular test), a test that the calendar's holiday rules
  match the source holiday flags, and a warning test on quarantined rows
- **A 10-page Streamlit app** covering sales by store, holiday, temperature, store size, store type, month,
  markdowns, fuel price, CPI, and department

## Data model

![Star schema](docs/images/star-schema.png)

| Table | Grain | Rows |
|---|---|---|
| `dim_store` | one row per store | 45 |
| `dim_date` | one row per week (calendar spine, Feb 2010 – Dec 2015) | 308 |
| `fct_weekly_sales` | one row per store, department, and week | 421,570 |
| `fct_store_weekly_conditions` | one row per store and week | 8,190 |

## Data quality decisions

- Weekly conditions (temperature, CPI, fuel price, markdowns) are recorded per store and week, so they live in their
  own fact table at that grain instead of repeating on every department row. To combine them with sales, aggregate
  sales to store-week first, then join (the app does this).
- The source writes missing values as the text "NA" (markdowns, CPI, unemployment). The load contract converts them
  to NULL at the warehouse gate (`NULL_IF = ('NA', '')`). MarkDown1 is missing in about 51% of rows because markdown
  data starts in November 2011.
- Two levels of protection: **file level** (a file that breaks the contract is skipped whole and sent to the error
  path) and **row level** (a bad row in a good file is quarantined, so one bad row never blocks a whole load).
- Re-sent rows are de-duplicated in silver: the latest load wins, so a corrected file can simply be re-sent.
- Grain tests check that no store/department/week (sales) or store/week (conditions) combination appears twice.
- `dim_date` is generated as a calendar spine with retail holiday rules and a `has_sales` flag, so weeks without
  sales data are never counted by mistake.

## Error handling and reprocessing

1. The weekly load skips any file that breaks the load contract (`ON_ERROR = SKIP_FILE`), so good files still load.
2. The procedure collects the skipped files and publishes a `FAILED_FILES` message to the SNS topic
   `walmart-pipeline-errors` through a Snowflake notification integration.
3. SNS fans out to three subscribers: **email** (source team and data engineering), the **SQS** queue
   `walmart-reprocess-queue` (one message per alert, with a dead-letter queue), and the **Lambda**
   `walmart-move-failed-files`, which moves the file to `walmart-sales-errors-<env>/<source>/<date>/` with an
   `.error.json` (original location, source table and Snowflake's first error).
4. The source team fixes the file and re-sends it to `landing/`. The next load picks it up, silver keeps the latest
   load, and the SQS message is deleted once the data is confirmed.
5. If the task itself fails (for example, the stage can't be reached), its `ERROR_INTEGRATION` alerts the same topic.

The step-by-step runbook is in [docs/runbook.md](docs/runbook.md). Test files are in `aws/test-files/`.

## CI/CD and environments

| Environment | Snowflake database | How code gets there |
|---|---|---|
| DEV | `WALMART_DEV` (`DBT_PETER_SILVER`/`_GOLD`, plus `CI_PR_<n>_SILVER`/`_GOLD` per pull request) | `dbt build` locally; CI builds every pull request |
| STAGING | `WALMART_STAGING` (`SILVER`, `GOLD`) | Automatic on merge to `main` |
| PROD | `WALMART_PROD` (`SILVER`, `GOLD`) | After STAGING passes **and** a reviewer approves the `production` environment |

- `.github/workflows/ci.yml`: builds and tests each pull request; drops its schemas when the pull request closes
- `.github/workflows/deploy.yml`: STAGING on merge, then PROD behind an approval gate
- `.github/workflows/refresh.yml`: rebuilds PROD every Monday after the weekly load (or on demand after a reprocess)
- `ci/profiles.yml`: connection targets for CI (secrets come from GitHub, never from the repo)
- The `main` branch is protected: a pull request must pass the CI check before it can merge

## Repository layout

- `models/staging/`: silver models; `checks/` holds the row-level checks, `quarantine_rows.sql` the rejected rows
- `models/marts/`: gold dimensions and facts
- `tests/`: custom dbt tests (grain, holiday rule, quarantine warning)
- `macros/`: schema naming (SILVER/GOLD, prefixed in DEV and CI), CI schema cleanup, quarantine alert
- `snowflake/00_account_setup.sql`: one-time account setup (warehouse, storage integration locations)
- `snowflake/01_raw_layer_<env>.sql`: per-environment database, load contract and stage
- `snowflake/02_ci_service_user.sql`: `TRANSFORMER` role and the `GITHUB_CI` key-pair service user
- `snowflake/03_separate_silver_gold.sql`: SILVER and GOLD schemas
- `snowflake/04_error_path.sql`: SNS notification integration, load procedures and the weekly load task
- `aws/iam/`: IAM policies (Snowflake read access, SNS publish, Lambda)
- `aws/lambda/move_failed_files.py`: moves failed files to the error bucket
- `aws/test-files/`: a bad and a corrected file for testing the error path
- `.github/workflows/`, `ci/`: CI/CD pipeline
- `streamlit_app/`: the Streamlit in Snowflake app
- `profiles.yml.example`: sample dbt connection profile

## Screenshots

![Screenshot 1](docs/images/shot-1.png)

![Screenshot 2](docs/images/shot-2.png)

![Screenshot 3](docs/images/shot-3.png)

![Screenshot 4](docs/images/shot-4.png)

![Screenshot 5](docs/images/shot-5.png)

## How to run it

1. Create the landing buckets (`walmart-sales-landing-<env>-<suffix>`) and upload each CSV to its folder:
   `landing/department/`, `landing/stores/`, `landing/fact/`. Attach `aws/iam/snowflake-read-policy.json` to the
   Snowflake IAM role.
2. In Snowflake, run `snowflake/00_account_setup.sql` once, then `snowflake/01_raw_layer_<env>.sql` for each
   environment, then `02_ci_service_user.sql` and `03_separate_silver_gold.sql`.
3. Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and fill in your account and credentials.
4. Run `pip install -r ci/requirements.txt`, `dbt deps`, then `dbt build` (DEV). STAGING and PROD are deployed by
   GitHub Actions; add the repository secrets `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` and `SNOWFLAKE_PRIVATE_KEY`.
5. Error path: create the error buckets, the SNS topic (email, SQS and Lambda subscribers), the SQS queue and
   dead-letter queue, and the Lambda (policies in `aws/iam/`), then run `snowflake/04_error_path.sql`. Test it with
   the files in `aws/test-files/` (see [docs/runbook.md](docs/runbook.md)).
6. Create a Streamlit in Snowflake app and paste in the files from `streamlit_app/`.

## What's next

- Power BI semantic model on the gold tables
- Snowpipe auto-ingest if files start arriving more often than weekly
- Load `fct_weekly_sales` incrementally as the data grows

## Data source

[Walmart Recruiting – Store Sales Forecasting (Kaggle)](https://www.kaggle.com/c/walmart-recruiting-store-sales-forecasting)

## Tools

Snowflake (tasks, stored procedures, notification integrations), dbt, AWS (S3, IAM, SNS, SQS, Lambda),
GitHub Actions, Streamlit, SQL, Python
