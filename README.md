# YouTube Trending Data Pipeline — Serverless AWS Medallion Architecture

**A serverless, event-driven data pipeline on AWS that ingests YouTube trending video data, processes it through Bronze → Silver → Gold layers, and serves analytics-ready tables to Athena — orchestrated by Step Functions with a data-quality gate and SNS failure alerting at every stage.**

![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazonaws&logoColor=white)
![AWS Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=flat&logo=awslambda&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-527FFF?style=flat)
![Step Functions](https://img.shields.io/badge/Step%20Functions-CD2264?style=flat&logo=amazonaws&logoColor=white)
![Athena](https://img.shields.io/badge/Amazon%20Athena-232F3E?style=flat&logo=amazonaws&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)

---

## 📌 Overview

This pipeline turns raw YouTube trending data — a historical Kaggle CSV/JSON dataset (10 regions) plus a **live YouTube Data API v3 feed** — into clean, query-ready analytics tables using the **medallion architecture** (Bronze → Silver → Gold), fully orchestrated by **AWS Step Functions** with a **data-quality gate** between Silver and Gold and **SNS email alerts** on every failure path.

The project also tells an honest infrastructure story: it was originally designed around an **AWS Glue Crawler**, but free-tier account restrictions blocked that path mid-build. Rather than stalling, the project pivoted to hand-written Athena DDL and pandas/awswrangler Lambda equivalents that reproduce the exact same transform logic — documented below as a deliberate engineering decision, not a workaround hidden from view.

---

## 🏗️ Architecture

<img width="1024" height="567" alt="YouTube data pipeline arch without using AWS crawler v0" src="https://github.com/user-attachments/assets/13c412b8-8d1f-4499-ba0d-607cf22b7306" />


| Stage | Service | What happens |
|---|---|---|
| **Data Sources** | YouTube Data API v3 + Kaggle dataset | Live trending-video pulls (via EventBridge-scheduled Lambda) and a one-time historical CSV/JSON backfill (10 regions: CA, DE, FR, GB, IN, JP, KR, MX, RU, US) |
| **Bronze (raw)** | **S3 + Glue Data Catalog** | Raw CSV/JSON landed as-is, partitioned by `region` (and `date`/`hour` for live API data), registered as Athena-queryable external tables |
| **Silver (cleansed)** | **AWS Glue (PySpark) / Lambda (pandas)** | Schema enforcement, type casting, deduplication, derived metrics (`like_ratio`, `engagement_rate`), written as partitioned Parquet |
| **Data Quality Gate** | **Lambda** | Row count, null %, schema presence, value-range, and freshness checks against Silver — pipeline halts to Gold if checks fail |
| **Gold (business aggregations)** | **AWS Glue (PySpark)** | Three analytics tables: `trending_analytics`, `channel_analytics`, `category_analytics` |
| **Orchestration** | **AWS Step Functions** | Single state machine sequencing ingest → parallel Silver transforms → DQ gate → Gold build, with per-stage retries and failure branching |
| **Alerting** | **Amazon SNS** | Email notification on success, and on each of 4 distinct failure modes (ingestion, transform, DQ, Gold) |
| **Consumption** | **Amazon Athena** (+ QuickSight downstream) | SQL queries directly against Gold Parquet via the Glue Catalog |

**Data flow:**
`YouTube API / Kaggle CSV → S3 Bronze (partitioned) → Glue/Lambda transform → S3 Silver (Parquet) → DQ Gate Lambda → Glue Gold aggregation → S3 Gold (Parquet) → Athena / QuickSight`

---

## 🔧 Pipeline Walkthrough

### 1. Ingestion — Two Paths Into Bronze
- **Historical backfill:** the original Kaggle "YouTube Trending" CSV (per-video stats) + JSON (category reference) files, bulk-loaded via `Scripts/aws_copy.sh`.
- **Live ingestion:** `lambdas/yt-api-ingestion/lambda_function.py`, triggered on an EventBridge schedule, calls the YouTube Data API v3 `videos` (`chart=mostPopular`) and `videoCategories` endpoints per region, writing raw JSON to Bronze with Hive-style `region=/date=/hour=` partitioning — so the same downstream Silver/Gold logic serves both a one-time backfill and an ongoing live feed.

![S3 Buckets](assets/01-s3-buckets.png)

### 2. Bronze — Raw Landing
Both ingestion paths land in region-partitioned S3 prefixes, registered in the Glue Data Catalog. Because a **Glue Crawler was restricted on the free-tier account**, table registration was done with hand-written Athena `CREATE EXTERNAL TABLE` DDL instead (`crawler_alternate/`) — functionally equivalent to what a Crawler would have inferred, just written explicitly.

![Bronze Glue Database](assets/05-bronze-glue-database.png)

### 3. Silver — Cleansed & Enforced
`glue_jobs/bronze_to_silver_statistics.py` (PySpark) reads Bronze via the Glue Catalog with predicate pushdown on region, then:
- **Detects the input shape** (flattened YouTube API JSON columns like `snippet.title` vs. flat Kaggle CSV columns) and branches its column-selection logic accordingly — one Silver table serves both ingestion paths
- Casts types, drops rows with a null `video_id`, standardizes region codes
- Parses `trending_date` from the Kaggle `YY.DD.MM` format into a proper date
- Adds derived metrics: `like_ratio`, `engagement_rate`
- **Deduplicates** with a `ROW_NUMBER()` window over `(video_id, region, trending_date)`, keeping the latest-processed record
- Writes partitioned, Snappy-compressed Parquet with catalog auto-update

A pandas/awswrangler equivalent (`glue_alternate_lambda/`) reproduces the same logic runnable as a Lambda or from a laptop/CloudShell — used when Glue Job resources were also constrained — including a **general-purpose recursive JSON flattener** for nested API fields, rather than hardcoding a fixed set of nested paths.

![Silver Glue Database](assets/06-silver-glue-database.png)
![Silver Parquet Data](assets/14-silver-parquet-data.png)

### 4. Data Quality Gate
`data_quality/dq_lambda.py`, invoked by Step Functions after the parallel Silver transforms, runs 5 checks against Athena-queried samples of the Silver tables: row count, null percentage on critical columns (`video_id`, `title`, `channel_title`, `views`, `region`), schema presence, value-range sanity (no negative/implausible view counts), and freshness (skipped gracefully for backfill data with no timestamp). A failure publishes the specific failed checks to SNS and the state machine **branches away from Gold entirely** rather than building on top of bad data.

![DQ Gate Caught a Real Failure](assets/12-dq-gate-caught-failure.png)
![SNS Email — DQ Failure](assets/13-sns-email-alert-dq-failure.png)

### 5. Gold — Business Aggregations
`glue_jobs/silver_to_gold_analytics.py` joins Silver statistics to the category reference lookup (wrapped in a `try/except` that **always guarantees a `category_name` column**, defaulting to `"Unknown"` if the reference join fails — so a broken lookup never breaks the Gold build) and produces three tables:
- **`trending_analytics`** — daily per-region summary: total videos/views/likes/comments, averages, unique channel/category counts
- **`channel_analytics`** — per-channel performance, ranked by total views within each region (`ROW_NUMBER()` window)
- **`category_analytics`** — category-level trend over time, including each category's **view share %** per region per day

![Gold Glue Database](assets/07-gold-glue-database.png)
![Gold Data in S3](assets/04-gold-trending-analytics-s3.png)

### 6. Orchestration — Step Functions
A single state machine (`step_functions/pipeline_orchestration.json`) sequences the whole flow: `IngestFromYouTubeAPI` → `WaitForS3Consistency` (10s buffer for S3 eventual consistency) → `ProcessInParallel` (reference-data transform and statistics ETL run concurrently) → `RunDataQualityChecks` → `EvaluateDataQuality` (Choice state) → `SilverToGoldJob` → `NotifySuccess`. Every task has `Retry` blocks with exponential backoff, and `Catch` blocks routing to one of four distinct SNS failure notifications depending on which stage failed.

![Step Functions — Full Success](assets/02-step-functions-success-graph.png)
![Step Functions — Failure Path](assets/11-step-functions-failure-graph.png)
![SNS Email — Success](assets/03-sns-email-alert-success.png)

### 7. Consumption
Gold tables are queried directly via **Amazon Athena** against the Glue Catalog, ready to plug into QuickSight or any other BI tool downstream.

![Athena Query](assets/08-athena-query.png)

---

## 🔒 Security Review

A full review of every non-`Data/` file (SQL, Python, JSON configs) for hardcoded secrets found:

- **No AWS access/secret keys anywhere** — all AWS access goes through the Lambda/Glue execution role via `boto3`/`awswrangler`, never explicit credentials.
- **No hardcoded YouTube API key** — `lambda_function.py` reads it from the `YOUTUBE_API_KEY` environment variable.
- **IAM policies appropriately scoped** where populated — `lambda:InvokeFunction` limited to the project's function-name prefix, `sns:Publish` limited to a specific topic ARN. Three of the four inline policy files are currently empty (not a risk, just unused).
- **The one real finding — a hardcoded real AWS Account ID (`828190346127`) in `step_functions/pipeline_orchestration.json` and `Scripts/information.md` — has already been fixed** in the current version of both files (verified directly: every ARN now reads `<AWS_ACCOUNT_ID>` / `<SN- ARN>`). This is called out here rather than silently assumed, since account-ID exposure doesn't grant access on its own but does make an account easier to fingerprint for reconnaissance if published alongside a consistent resource-naming convention.
- **Minor residual note:** the account ID is still visible in one auto-generated S3 bucket name inside an Assets screenshot (`aws-athena-query-results-<account-id>-ap-south-1`, in `01-s3-buckets.png`). Not a code leak, but worth cropping or redacting that specific screenshot before publishing the repo, since screenshots get committed too.

No other secrets, tokens, or credentials were found.

---

## 🗂️ Repository Structure
```
Assets/                        # Architecture diagrams + console screenshots
crawler_alternate/              # Athena DDL + CSV/JSON cleanup scripts (Glue Crawler substitute)
data_quality/dq_lambda.py       # DQ gate Lambda, invoked between Silver and Gold
glue_jobs/                      # Production PySpark ETL: bronze_to_silver_statistics.py, silver_to_gold_analytics.py
glue_alternate_lambda/          # pandas/awswrangler equivalents (Glue Job substitute)
iam_permissions_inline_policies/# Scoped inline IAM policies for the pipeline's execution role
lambdas/
├── yt-api-ingestion/           # Live YouTube Data API v3 ingestion Lambda
└── json_to_parquet/            # Bronze category/reference JSON -> validated Silver Parquet
Scripts/
├── aws_copy.sh                 # Bulk-load the Kaggle dataset into Bronze
└── information.md              # Bucket/database naming reference
step_functions/pipeline_orchestration.json   # Full Step Functions state machine
Data/                           # (excluded from this report — local raw dataset only)
```

---

## ⚙️ Tech Stack

**Ingestion:** AWS Lambda (Python), YouTube Data API v3, EventBridge scheduling
**Storage:** Amazon S3 (Bronze/Silver/Gold, region-partitioned)
**Cataloging & Query:** AWS Glue Data Catalog, Amazon Athena
**Transformation:** AWS Glue (PySpark), with pandas/awswrangler Lambda equivalents
**Orchestration:** AWS Step Functions
**Data Quality:** Custom Lambda-based validation gate
**Alerting:** Amazon SNS
**IAM:** Scoped inline policies per execution role

---

## 🚀 Reproducing This Project

1. Create the Bronze/Silver/Gold S3 buckets and Glue databases (see `Scripts/information.md` for naming convention).
2. Load historical data with `Scripts/aws_copy.sh`, then run `crawler_alternate/sql_scipts_athena/*.sql` in Athena to register the Bronze tables.
3. Run `crawler_alternate/clean_youtube_csvs.py` and `minify_category_json.py` once against the raw Kaggle files if Athena throws parsing errors on embedded newlines / pretty-printed JSON.
4. Deploy `glue_jobs/*.py` as Glue Jobs (or use `glue_alternate_lambda/*.py` as Lambda/local equivalents if Glue Job resources are unavailable on your account tier).
5. Deploy `lambdas/yt-api-ingestion` and `lambdas/json_to_parquet` with the environment variables documented in each file's docstring.
6. Deploy `data_quality/dq_lambda.py` and attach the SNS topic for alerts.
7. Import `step_functions/pipeline_orchestration.json` as a Step Functions state machine, attaching the IAM policy in `iam_permissions_inline_policies/`.

---

## 🔭 Future Work
- Populate the three currently-empty IAM inline policy files, or remove them if superseded
- Add CI (GitHub Actions) to lint the Glue/Lambda scripts and validate the Step Functions ASL JSON on every push
- Parameterize bucket names and account ID via a config file rather than per-file hardcoding, now that the direct account-ID leak has been fixed
- Redact the AWS account ID visible in `Assets/buckets.png` before publishing
- Wire up QuickSight against the Gold tables as the final consumption layer referenced in the architecture diagram but not yet built

---

## 📬 Contact
Built by **Sanjay** — [GitHub](https://github.com/Sanjayvk98) · open to AI/ML Engineer and Data Engineering roles.
