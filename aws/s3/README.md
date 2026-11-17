# S3 security and retention settings

Applied to the 3 landing buckets (`walmart-sales-landing-<env>-pr518`) and the 3 error buckets
(`walmart-sales-errors-<env>-pr518`).

| Setting | Landing buckets | Error buckets |
|---|---|---|
| Default encryption | SSE-KMS, key `alias/walmart-pipeline`, Bucket Key on | Same |
| Bucket policy | `bucket-policies/<bucket>.json`: deny any request not using TLS | Same |
| Versioning | Enabled | Enabled |
| Lifecycle: current files | Glacier Flexible Retrieval after 90 days; delete after 2,555 days (7 years) | Delete after 180 days |
| Lifecycle: old versions | Delete 30 days after they are replaced or deleted | Same |
| Lifecycle: cleanup | Delete expired delete markers; abort incomplete multipart uploads after 7 days | Same |

The KMS key's own policy adds `../kms/key-policy-sns-statement.json` so SNS can deliver to the encrypted
SQS queues. Roles that read or write encrypted objects or messages get `../iam/walmart-kms-use-policy.json`:
the Snowflake storage-integration role, `snowflake-sns-publisher`, and `walmart-move-failed-files-role`.
