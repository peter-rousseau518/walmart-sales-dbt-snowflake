# SNS subscription filter policies (topic: walmart-pipeline-errors)

Set on each subscription: SNS -> Topics -> walmart-pipeline-errors -> Subscriptions ->
select the subscription -> Edit -> Subscription filter policy -> scope **Message body**.

| Subscription | Policy file | Receives |
|---|---|---|
| Email | `filter-email-and-sqs.json` | Everything except `FILES_LOADED` (failed files, rejected rows, task failures) |
| SQS `walmart-reprocess-queue` | `filter-email-and-sqs.json` | Same as email |
| Lambda `walmart-start-dbt-refresh` | `filter-dbt-refresh-lambda.json` | Only `FILES_LOADED` |
| Lambda `walmart-move-failed-files` | none | Everything; the function acts only on `FAILED_FILES` |

The second condition in `filter-email-and-sqs.json` (`"exists": false`) keeps Snowflake's task-failure
notifications, which have no `type` field.
