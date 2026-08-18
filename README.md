# YouTube Trending Data Pipeline

A serverless, medallion-architecture data pipeline on AWS that ingests YouTube trending video data, cleans and enriches it through **Bronze → Silver → Gold** layers, and produces analytics-ready tables for querying in Athena (and, downstream, BI tools like QuickSight).

The pipeline was originally built around a static Kaggle "YouTube Trending" CSV/JSON dataset (per-region video stats + category reference data), and was later extended with a **live YouTube Data API v3 ingestion Lambda**, so the same Silver/Gold logic works for both historical backfill and ongoing real-time ingestion.

![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?logo=apachespark&logoColor=white)

---

<img width="1024" height="567" alt="YouTube data pipeline arch without using AWS crawler" src="https://github.com/user-attachments/assets/985d3506-bbb1-4f91-8485-e626f1fc4ac7" />



---

## Architecture

```
YouTube Data API v3 ──┐
                       ▼
              ┌─────────────────┐
              │   BRONZE (raw)   │  S3 + Glue Catalog (Athena tables)
              │  CSV / JSON      │  Partitioned by region
              └────────┬─────────┘
                       │  Glue Job / Lambda (PySpark or pandas)
                       ▼
              ┌─────────────────┐
              │  SILVER (clean)  │  Parquet, schema-enforced,
              │  deduplicated    │  deduped, partitioned by region
              └────────┬─────────┘
                       │  Data Quality Lambda (gate)
                       ▼
              ┌─────────────────┐
              │  GOLD (analytics)│  trending_analytics
              │  aggregated      │  channel_analytics
              │                  │  category_analytics
              └─────────────────┘
```

Orchestrated end-to-end by an **AWS Step Functions** state machine, with **SNS alerts** on failure at every stage and a **data quality gate** between Silver and Gold.

### Data flow (as encoded in `step_functions/pipeline_orchestration.json`)
1. **IngestFromYouTubeAPI** — Lambda pulls trending videos + category metadata per region, writes raw JSON to Bronze.
2. **WaitForS3Consistency** — short buffer for S3 eventual consistency.
3. **ProcessInParallel** — two branches run concurrently:
   - Reference/category JSON → Silver (Lambda, `json_to_parquet`)
   - Bronze statistics → Silver (Glue Job or Lambda equivalent)
4. **RunDataQualityChecks** — row count, null %, schema, value-range, and freshness checks against the Silver tables; publishes to SNS and halts the pipeline on failure.
5. **SilverToGoldJob** — builds the three Gold aggregate tables.
6. **NotifySuccess / NotifyXFailure** — SNS notifications at each stage.

---

## Repository structure

| Path | Purpose |
|---|---|
| `Assets/` | Architecture diagrams and console screenshots (S3 buckets, Glue databases, Athena queries, Step Functions executions, SNS alerts, IAM roles, Lambda functions). |
| `crawler_alternate/` | Utility scripts used in place of an AWS Glue Crawler (which was restricted on the free-tier account): CSV/JSON cleanup scripts + hand-written Athena `CREATE EXTERNAL TABLE` DDL for the Bronze layer. |
| `data_quality/` | `dq_lambda.py` — the data quality gate Lambda invoked by Step Functions between Silver and Gold. |
| `glue_jobs/` | Production **AWS Glue (PySpark)** ETL jobs: `bronze_to_silver_statistics.py`, `silver_to_gold_analytics.py`. |
| `glue_alternate_lambda/` | **pandas/awswrangler** equivalents of the Glue jobs above, for running the same transforms outside Glue (local machine, Lambda, CloudShell) when Glue Job resources were restricted. |
| `iam_permissions_inline_policies/` | Inline IAM policy JSON for the pipeline's Step Functions execution role (Lambda invoke, Glue job control, SNS publish). |
| `lambdas/yt-api-ingestion/` | Live ingestion Lambda — calls the YouTube Data API v3 and writes raw JSON to Bronze on a schedule. |
| `lambdas/json_to_parquet/` | Converts Bronze category/reference JSON into validated, deduplicated Parquet in Silver. |
| `Scripts/` | `aws_copy.sh` (bulk-load the original Kaggle CSV/JSON dataset into Bronze via CLI) and `information.md` (bucket/database naming reference). |
| `step_functions/` | `pipeline_orchestration.json` — the full Step Functions state machine definition. |
| `Data/` | *(excluded from this report/README — local raw dataset, not part of the deployed pipeline)* |

---

## Tech stack

- **Ingestion:** AWS Lambda (Python), YouTube Data API v3
- **Storage:** Amazon S3 (Bronze / Silver / Gold buckets, region-partitioned)
- **Cataloging & Query:** AWS Glue Data Catalog, Amazon Athena
- **Transformation:** AWS Glue (PySpark) jobs, with pandas/awswrangler Lambda equivalents
- **Orchestration:** AWS Step Functions
- **Data Quality:** Custom Lambda-based checks (row count, nulls, schema, value ranges, freshness)
- **Alerting:** Amazon SNS
- **IAM:** Scoped inline policies per execution role

## Data layers

- **Bronze** — raw ingested data (CSV from Kaggle backfill, or JSON from the live API), partitioned by `region`.
- **Silver** — schema-enforced, deduplicated, cleansed Parquet (`clean_statistics`, `clean_reference_data`), with derived fields like `like_ratio` and `engagement_rate`.
- **Gold** — three analytics tables:
  - `trending_analytics` — daily trending summary per region
  - `channel_analytics` — channel performance & ranking per region
  - `category_analytics` — category-level trend and view-share over time

## Setup notes

1. Create the Bronze/Silver/Gold S3 buckets and Glue databases (see `Scripts/information.md` for naming convention).
2. Load historical data with `Scripts/aws_copy.sh`, then run `crawler_alternate/sql_scipts_athena/*.sql` in Athena to register the Bronze tables (used in place of a Glue Crawler).
3. Run `crawler_alternate/clean_youtube_csvs.py` and `minify_category_json.py` once against the raw Kaggle files if Athena throws parsing errors on embedded newlines / pretty-printed JSON.
4. Deploy `glue_jobs/*.py` as Glue Jobs (or use `glue_alternate_lambda/*.py` as Lambda/local equivalents if Glue Job resources are unavailable on your account tier).
5. Deploy `lambdas/yt-api-ingestion` and `lambdas/json_to_parquet` with the environment variables documented in each file's docstring.
6. Deploy `data_quality/dq_lambda.py` and attach the SNS topic for alerts.
7. Import `step_functions/pipeline_orchestration.json` as a Step Functions state machine, attaching the IAM policy in `iam_permissions_inline_policies/`.

## Screenshots

See `Assets/` for console screenshots covering S3 bucket structure, Bronze/Silver/Gold Glue databases, Athena query results at each layer, Step Functions success/failure executions, SNS alert emails, IAM roles, and Lambda functions.
