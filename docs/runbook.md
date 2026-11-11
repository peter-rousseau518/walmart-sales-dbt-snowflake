# Runbook: weekly load, failures and reprocessing

## Normal week

| When (ET) | What | Where to check |
|---|---|---|
| Monday 6:00 AM | Task `WALMART_PROD.RAW.WEEKLY_LOAD` runs `LOAD_LANDING_FILES()` | `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY scheduled_time DESC;` |
| Monday ~7–8 AM | GitHub Actions `dbt scheduled refresh` rebuilds PROD SILVER and GOLD | Actions tab → dbt scheduled refresh |
| After refresh | Streamlit app shows the new week | `WALMART_PROD.GOLD` |

COPY INTO remembers which files it has loaded, so re-running the task never loads the same file twice.

## When a file fails

You receive an email from SNS with `"type": "FAILED_FILES"`, listing each file, its table and Snowflake's first error.

1. **Find the file.** The Lambda has moved it to `walmart-sales-errors-<env>/<source>/<YYYY-MM-DD>/`, next to a
   `<file>.error.json` with the original location and the error.
2. **Confirm nothing bad loaded.** The file was skipped as a whole (`ON_ERROR = SKIP_FILE`), so the raw table
   row count is unchanged.
3. **Send it back to the source team** with the error, or fix it if the problem is obvious (for example, a text
   value in a numeric column).
4. **Re-send the corrected file** to the same `landing/<source>/` folder. Give it a new file name, because COPY
   skips a file name it has already seen.
5. **Load it:** wait for the next Monday run, or run it now:
   ```sql
   CALL WALMART_PROD.RAW.LOAD_LANDING_FILES();
   ```
   Then run the GitHub Actions workflow **dbt scheduled refresh** by hand (Run workflow).
6. **Close it out:** check the row count, then delete the message from `walmart-reprocess-queue` in SQS.
   Messages that are received three times without being deleted move to `walmart-reprocess-dlq`.

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

## When the task itself fails

The task's `ERROR_INTEGRATION` sends an email (for example, if Snowflake can't reach the stage). Check the
error in `TASK_HISTORY`, fix it (often the storage integration or IAM trust policy), then run
`EXECUTE TASK WALMART_PROD.RAW.WEEKLY_LOAD;`.

## Testing the error path

1. Upload `aws/test-files/fact_bad_test.csv` to `landing/fact/` in the landing bucket.
2. `CALL WALMART_<ENV>.RAW.LOAD_LANDING_FILES();` → one failed file.
3. Check: the email arrives, a message is in the SQS queue, the file is in the error bucket with its
   `.error.json`, and `RAW.FACT` still has 8,190 rows.
4. Upload `aws/test-files/fact_fixed_test.csv` and call the procedure again → `RAW.FACT` has 8,191 rows.
5. Delete the SQS message.
