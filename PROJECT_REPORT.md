# Project Report: YouTube Trending Data Pipeline

## 1. Objective

Build an end-to-end, serverless data pipeline on AWS that turns raw YouTube trending data (per-region video statistics + category reference data) into clean, query-ready analytics tables — following the **medallion architecture** pattern (Bronze → Silver → Gold) — with automated orchestration, data quality gating, and failure alerting.

The project served two purposes: a historical backfill using a static Kaggle YouTube-trending dataset (10 regions: CA, DE, FR, GB, IN, JP, KR, MX, RU, US), and a live extension that pulls the same shape of data continuously from the YouTube Data API v3.

## 2. Architecture overview

**Medallion layers**, all stored in S3 as region-partitioned datasets and registered in the Glue Data Catalog for Athena querying:

- **Bronze** — raw landing zone. Two ingestion paths feed it:
  - Bulk historical load: Kaggle CSV (video stats) + JSON (category reference) files copied in via CLI (`Scripts/aws_copy.sh`).
  - Live ingestion: `lambdas/yt-api-ingestion/lambda_function.py`, triggered on a schedule, calling the YouTube Data API v3 `videos` (mostPopular chart) and `videoCategories` endpoints per region and writing raw JSON with Hive-style `region=/date=/hour=` partitioning.
- **Silver** — cleansed, schema-enforced, deduplicated Parquet. Handles both the Kaggle CSV shape and the live API's nested JSON shape (auto-detects and flattens). Adds derived metrics (`like_ratio`, `engagement_rate`) and processing metadata (`_processed_at`, `_job_name`).
- **Gold** — three business-level aggregate tables optimized for Athena/QuickSight: `trending_analytics`, `channel_analytics`, `category_analytics`.

**Orchestration:** a single AWS Step Functions state machine (`step_functions/pipeline_orchestration.json`) runs the whole flow — ingest → parallel Silver transforms (reference data + statistics) → data quality gate → Gold build — with per-stage retries, `Catch` blocks, and SNS notifications on both success and each failure mode (ingestion, transform, DQ, Gold).

**Data quality gate:** `data_quality/dq_lambda.py` runs row-count, null-percentage, schema-presence, value-range (e.g. implausible view counts), and freshness checks against the Silver tables via Athena before allowing the pipeline to proceed to Gold. A failed check publishes details to SNS and the state machine short-circuits to `NotifyDQFailure` instead of continuing.

## 3. Engineering challenges and how they were solved

- **Glue Crawler / Glue Job restrictions on a free-tier AWS account.** Rather than blocking the project, the `crawler_alternate/` and `glue_alternate_lambda/` folders show a deliberate fallback strategy: hand-written Athena `CREATE EXTERNAL TABLE` DDL in place of a Crawler, and pandas/awswrangler scripts that reproduce the exact same PySpark transform logic (schema casting, cleansing, dedup, aggregation) runnable from a laptop, CloudShell, or a plain Lambda instead of a Glue Job — while still writing to the same S3 paths and registering with the same Glue Catalog API, so the rest of the pipeline is unaffected.
- **Malformed CSV parsing in Athena.** Some Kaggle `description` fields contain embedded raw newlines, which broke Athena's line-based CSV reader mid-row. `clean_youtube_csvs.py` re-parses each file with pandas (which handles quoted multi-line fields correctly) and re-uploads a newline-stripped version in place.
- **Pretty-printed JSON breaking the Hive JSON SerDe.** Athena's JSON SerDe expects one JSON object per line; the category reference files were pretty-printed across many lines. `minify_category_json.py` collapses each file to a single line before re-upload.
- **Mixed CSV vs. live-API JSON shapes reaching the same Silver transform.** `bronze_to_silver_statistics.py` (Glue) detects which shape it received (flattened `snippet.title`-style API columns vs. flat Kaggle CSV columns) and branches accordingly, so one Silver table serves both ingestion paths.
- **Dynamic nested-JSON flattening.** The pandas equivalent of the Bronze→Silver statistics job (`bronze_to_silver_statistics_local_statistics.py`) includes a general-purpose recursive flattener that detects and unpacks any nested JSON/dict columns before writing Parquet, rather than hardcoding a fixed set of nested fields.
- **Category-name enrichment resilience.** The Silver→Gold job wraps the category-lookup join in a try/except and always guarantees a `category_name` column (falling back to `"Unknown"`), so a missing or malformed reference table never breaks the Gold build.

## 4. Data quality approach

Checks are parameterized via environment variables (`DQ_MIN_ROW_COUNT`, `DQ_MAX_NULL_PERCENT`) and cover:
1. Row count against a minimum threshold
2. Null percentage on critical columns per table (e.g. `video_id`, `title`, `views`)
3. Presence of all expected columns
4. Value-range sanity checks (e.g. no negative or implausibly large view counts)
5. Freshness — latest record timestamp within a configurable window (skipped gracefully for backfill data with no timestamp column)

Results are logged, aggregated into a pass/fail summary, and only on failure pushed to SNS with the specific failed checks attached — keeping alert volume low.

## 5. Security review

A review of every non-`Data/` file for hardcoded secrets (AWS access/secret keys, API keys, passwords, tokens) found:

- **No AWS access keys or secret keys** in any script — all AWS access goes through the Lambda/Glue execution role via `boto3`/`awswrangler` with no explicit credentials.
- **No hardcoded YouTube API key** — `lambdas/yt-api-ingestion/lambda_function.py` correctly reads it from the `YOUTUBE_API_KEY` environment variable.
- **IAM policy files are appropriately scoped** where populated (`amzn-yt-data-pipeline-step-function-access.json` limits `lambda:InvokeFunction` to the project's function prefix and `sns:Publish` to a specific topic ARN); three of the four inline-policy files are currently empty.
- ⚠️ **Real AWS Account ID exposed in plaintext**: `828190346127` appears in full ARNs (Lambda functions and SNS topics) in `step_functions/pipeline_orchestration.json` and in the SNS ARN in `Scripts/information.md`. This isn't a credential that grants access on its own, but it identifies your specific AWS account and, combined with the exact resource-naming convention used throughout the repo, makes the account easy to fingerprint and target for reconnaissance (e.g. cross-account trust/resource-policy probing). **Recommendation:** replace it with a placeholder (e.g. `<AWS_ACCOUNT_ID>`) throughout before making the repository public, and consider regenerating the SNS topic if it's already been exposed.

No other secrets, tokens, or credentials were found in the reviewed files.

## 6. Possible next steps

- Populate the three currently-empty IAM inline policy files, or remove them if superseded.
- Add CI (GitHub Actions) to lint the Glue/Lambda scripts and validate the Step Functions ASL JSON on every push.
- Add a `.gitignore` covering `Data/` and any local AWS config/credentials files, if not already present.
- Consider parameterizing the AWS account ID and bucket names via environment variables or a config file rather than hardcoding them in the state machine definition.
