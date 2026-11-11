"""Move files that failed the Snowflake load contract to the error bucket.

Trigger: subscription to the SNS topic walmart-pipeline-errors.
Snowflake's load procedure publishes a JSON message like:
    {"type": "FAILED_FILES", "environment": "DEV",
     "landing_bucket": "walmart-sales-landing-dev-pr518",
     "error_bucket": "walmart-sales-errors-dev-pr518",
     "failed_files": [{"table": "FACT", "file": "s3://.../landing/fact/x.csv", "error": "..."}]}

For each failed file the function copies it to
    s3://<error_bucket>/<source>/<YYYY-MM-DD>/<file name>
writes the Snowflake error next to it (<file name>.error.json), then deletes the
original from the landing bucket so the next scheduled load does not retry it.
Other message types (REJECTED_ROWS, task errors) are ignored here; they still
reach email and the SQS queue through their own subscriptions.
"""
import json
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")


def split_s3_path(path, default_bucket):
    """Return (bucket, key) for 's3://bucket/key' or a key relative to the landing bucket."""
    if path.startswith("s3://"):
        bucket, _, key = path[len("s3://"):].partition("/")
        return bucket, unquote_plus(key)
    key = path.lstrip("/")
    if not key.startswith("landing/"):
        key = "landing/" + key
    return default_bucket, unquote_plus(key)


def move_file(landing_bucket, error_bucket, failed, day):
    bucket, key = split_s3_path(failed["file"], landing_bucket)
    parts = key.split("/")
    source = parts[1] if len(parts) > 2 and parts[0] == "landing" else "unknown"
    name = parts[-1]
    dest = f"{source}/{day}/{name}"

    s3.copy_object(Bucket=error_bucket, Key=dest, CopySource={"Bucket": bucket, "Key": key})
    s3.put_object(
        Bucket=error_bucket,
        Key=dest + ".error.json",
        Body=json.dumps({"original": f"s3://{bucket}/{key}", **failed}, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    s3.delete_object(Bucket=bucket, Key=key)
    return {"from": f"s3://{bucket}/{key}", "to": f"s3://{error_bucket}/{dest}"}


def handler(event, context):
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    moved, errors = [], []
    for record in event.get("Records", []):
        try:
            message = json.loads(record["Sns"]["Message"])
        except (KeyError, json.JSONDecodeError):
            continue
        if message.get("type") != "FAILED_FILES":
            continue
        for failed in message.get("failed_files", []):
            try:
                moved.append(move_file(message["landing_bucket"], message["error_bucket"], failed, day))
            except Exception as exc:  # keep going; report every file that could not be moved
                errors.append({"file": failed.get("file"), "error": str(exc)})
    print(json.dumps({"moved": moved, "errors": errors}))
    if errors:
        raise RuntimeError(f"{len(errors)} file(s) could not be moved: {errors}")
    return {"moved": moved}
