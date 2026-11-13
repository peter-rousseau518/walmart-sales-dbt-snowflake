"""Start the dbt refresh in GitHub Actions after Snowflake loads new files.

Trigger: subscription to the SNS topic walmart-pipeline-errors, with a filter policy
so it only receives {"type": "FILES_LOADED"} messages. Snowflake's load procedure
publishes one after every run that loaded at least one file:
    {"type": "FILES_LOADED", "environment": "PROD", "run_id": "...",
     "files_loaded": 1, "loaded_files": [{"table": "FACT", "file": "...", "rows": 1}]}

For STAGING and PROD the function sends a repository_dispatch event ("raw-loaded")
to GitHub, which starts the "dbt scheduled refresh" workflow for that environment.
DEV is skipped: DEV is built by hand with dbt build.

Environment variables (Lambda -> Configuration -> Environment variables):
    GITHUB_REPO   peter-rousseau518/walmart-sales-dbt-snowflake
    GITHUB_TOKEN  fine-grained personal access token for that one repository,
                  permission "Contents: Read and write" (needed for repository_dispatch)
Uses only the Python standard library, so there is nothing to package.
"""
import json
import os
import urllib.error
import urllib.request

REFRESH_ENVIRONMENTS = {"STAGING", "PROD"}


def dispatch(environment, message):
    repo = os.environ["GITHUB_REPO"]
    body = json.dumps({
        "event_type": "raw-loaded",
        "client_payload": {
            "environment": environment.lower(),          # matches the dbt target name
            "run_id": message.get("run_id"),
            "files_loaded": message.get("files_loaded"),
        },
    }).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/dispatches",
        data=body,
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "walmart-start-dbt-refresh",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status                       # 204 = accepted
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"GitHub returned {exc.code}: {detail}") from exc


def handler(event, context):
    started, skipped = [], []
    for record in event.get("Records", []):
        try:
            message = json.loads(record["Sns"]["Message"])
        except (KeyError, json.JSONDecodeError):
            continue
        if message.get("type") != "FILES_LOADED":
            continue
        environment = str(message.get("environment", "")).upper()
        if environment not in REFRESH_ENVIRONMENTS:
            skipped.append(environment)
            continue
        status = dispatch(environment, message)
        started.append({"environment": environment, "run_id": message.get("run_id"), "status": status})
    print(json.dumps({"started": started, "skipped": skipped}))
    return {"started": started, "skipped": skipped}
