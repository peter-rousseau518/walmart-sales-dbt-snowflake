# Runbook: loads, failures and reprocessing

## How a file flows

1. A file lands in `landing/<source>/` in `walmart-sales-landing-<env>`.
2. S3 sends an event notification to the stage's SQS queue; Snowflake refreshes the stage's directory table.
3. A stream on the directory table records the new file, and the triggered task runs `RAW.LOAD_LANDING_FILES()`
   (COPY INTO with `ON_ERROR = SKIP_FILE`). Every file's result is written to `RAW.LOAD_AUDIT`.
4. When rows load, Snowflake publishes a "loaded" event to SNS, and a Lambda starts the GitHub Actions
   **dbt scheduled refresh** workflow, which rebuilds SILVER and GOLD in PROD (tests gate each layer).
5. The Streamlit cache clears on the next page view because the refresh time changed.

COPY INTO remembers which files it has loaded, so a file never loads twice.

| Check | Query or place |
|---|---|
| Recent loads | `SELECT * FROM WALMART_PROD.RAW.LOAD_AUDIT ORDER BY loaded_at DESC;` |
| Task runs | `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY scheduled_time DESC;` |
| dbt refresh | GitHub → Actions → dbt scheduled refresh |

## When a file fails

You receive an email from SNS with `"type": "FAILED_FILES"`, listing each file, its table and Snowflake's first error.

1. **Find the file.** The Lambda has moved it to `walmart-sales-errors-<env>/<source>/<YYYY-MM-DD>/`, next to a
   `<file>.error.json` with the original location and the error. `LOAD_AUDIT` shows it as skipped.
2. **Nothing bad loaded.** The file was skipped as a whole, so the raw table is unchanged; all other files loaded.
3. **The source team fixes it** (they received the same email) and re-sends it to `landing/<source>/` under a new
   file name, because COPY skips a name it has already seen.
4. **The backfill runs itself:** the event triggers the load, the incremental fact merges only the affected weeks,
   and dbt refreshes gold.
5. **Close it out:** when `LOAD_AUDIT` shows the file loaded, delete its message from `walmart-reprocess-queue`.
   Messages received three times without being deleted move to `walmart-reprocess-dlq`; check it weekly.

## When rows are rejected

You receive an email with `"type": "REJECTED_ROWS"` (PROD only). The file loaded, but some rows failed a check.

```sql
SELECT reject_reason, source_table, COUNT(*) AS row_count
FROM WALMART_PROD.SILVER.QUARANTINE_ROWS
GROUP BY 1, 2;
```

| reject_reason | What it means | Fix |
|---|---|---|
| `unknown store_id` | A store not in the stores file | Source team adds the store, or corrects the ID |
| `missing weekly_sales` | Sales value is empty | Source team re-sends the row |
| `duplicate ... (older load)` | A newer load replaced the row | Nothing; this is normal after a re-send |

Good rows from the same file are already in GOLD. When the corrected rows are re-sent, the latest load wins and
the row leaves the quarantine table on the next dbt run.

## When dbt doesn't refresh after a load

`LOAD_AUDIT` shows LOADED but no **dbt scheduled refresh** run appears in GitHub Actions:

1. Lambda → `walmart-start-dbt-refresh` → **Monitor** → **View CloudWatch logs** → latest log stream.
2. `GitHub returned 401`: the token expired or was mistyped. Create a new fine-grained token
   (Contents: Read and write, this repository only) and update `GITHUB_TOKEN` in the Lambda's
   environment variables.
3. `GitHub returned 404`: `GITHUB_REPO` is wrong or the token doesn't cover this repository.
4. No log at all: the Lambda's SNS subscription or its filter policy (`aws/sns/`) is missing.

Until it's fixed, run the refresh by hand: Actions → dbt scheduled refresh → Run workflow.

## When a user reports a data issue

Reports from the dashboard's **Report a data issue** form are stored in `WALMART_PROD.FEEDBACK.DATA_ISSUES`
and emailed to data engineering (`"type": "DATA_ISSUE"`). Acknowledge within 1 business day; fix or explain
within 3.

```sql
SELECT * FROM WALMART_PROD.FEEDBACK.DATA_ISSUES WHERE status <> 'RESOLVED' ORDER BY reported_at;

UPDATE WALMART_PROD.FEEDBACK.DATA_ISSUES SET status = 'ACKNOWLEDGED' WHERE issue_id = <id>;
UPDATE WALMART_PROD.FEEDBACK.DATA_ISSUES
   SET status = 'RESOLVED', resolution = '<what was wrong and what changed>', resolved_at = CURRENT_TIMESTAMP()
 WHERE issue_id = <id>;
```

## Weekly cost check

Loads and dbt run on `TRANSFORM_WH`; the dashboard runs on `STREAMLIT_WH`. Each has a resource monitor that
emails at 80% of its monthly credit quota and suspends the warehouse at 100%.

```sql
SELECT warehouse_name, ROUND(SUM(credits_used), 2) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 2 DESC;
```

If a monitor suspends a warehouse, raise its quota with
`ALTER RESOURCE MONITOR <name> SET CREDIT_QUOTA = <n>;` after checking which queries used the credits
(Snowsight → Monitoring → Query History, filtered by warehouse).

## When the task itself fails

The task's `ERROR_INTEGRATION` sends an email (for example, if Snowflake can't reach the stage). Check the
error in `TASK_HISTORY`, fix it (often the storage integration or IAM trust policy), then run
`EXECUTE TASK WALMART_PROD.RAW.LOAD_ON_ARRIVAL;`.

## Recovering a table

- Dropped table: `UNDROP TABLE <name>;` (Time Travel: 7 days on PROD RAW, GOLD and FEEDBACK; 1 day in DEV and STAGING)
- Bad data in gold: clone from Time Travel (`CREATE TABLE ... CLONE ... AT (OFFSET => -3600)`), or rebuild with
  `dbt build --full-refresh` from RAW
- Lost raw data: reload from the files in S3. Landing buckets are versioned, so a deleted or overwritten file can be
  restored from **Show versions** for 30 days. Files older than 90 days are in Glacier and must be restored
  (S3 → object → **Initiate restore**) before Snowflake can read them again. Examples: `snowflake/07_retention.sql`

## Encryption and access errors

All six buckets, the SNS topic and both SQS queues are encrypted with the customer-managed KMS key
`alias/walmart-pipeline`, and the buckets refuse any request that doesn't use TLS (`aws/s3/`).

An error mentioning **kms** or **AccessDenied** after a key or policy change usually means a role lost access to
the key. The roles that need `aws/iam/walmart-kms-use-policy.json` attached:

| Role | Why |
|---|---|
| Snowflake storage-integration role (from `DESC INTEGRATION s3_walmart_int`) | Reads encrypted landing files |
| `snowflake-sns-publisher` | Publishes to the encrypted SNS topic |
| `walmart-move-failed-files-role` | Reads failed files and writes them to the encrypted error buckets |

Never schedule the key for deletion: encrypted files and messages can't be read without it.

## Testing the error path

1. Upload `aws/test-files/fact_bad_test.csv` to `landing/fact/` in the landing bucket.
2. The load runs on its own → `LOAD_AUDIT` shows one skipped file.
3. Check: the email arrives, a message is in the SQS queue, the file is in the error bucket with its
   `.error.json`, and `RAW.FACT` still has 8,190 rows.
4. Upload `aws/test-files/fact_fixed_test.csv` → `RAW.FACT` has 8,191 rows.
5. Delete the SQS message.
