"""
Bronze -> Silver (Statistics Data) — LOCAL equivalent of the AWS Glue ETL job.

Why this exists: AWS Glue Crawler AND Glue Jobs both appear to be
restricted on this free-tier account. This script reproduces the same
logic as bronze_to_silver_statistics.py, but runs anywhere with Python
(your laptop, Cloud9, CloudShell) instead of inside the Glue service.

How each Glue-specific piece is replaced:
  - glueContext.create_dynamic_frame.from_catalog(...)
      -> awswrangler.athena.read_sql_query(...)  (reads via Athena,
         same Bronze table you already created manually)
  - PySpark DataFrame transforms (F.col, F.when, Window, etc.)
      -> equivalent pandas operations
  - glueContext.getSink(..., enableUpdateCatalog=True, ...)
      -> awswrangler.s3.to_parquet(..., database=..., table=...)
         This writes partitioned Parquet to S3 AND registers/updates
         the table in the Glue Data Catalog in one call — using the
         same Glue Catalog API (glue:CreateTable / BatchCreatePartition)
         that Athena's CREATE EXTERNAL TABLE already uses successfully
         in your account. No Glue Crawler or Glue Job resource involved.

Prerequisites:
    pip install awswrangler pandas
    aws configure   (same credentials your aws s3 cp commands use)

The Silver database must exist first — run once in Athena:
    CREATE DATABASE IF NOT EXISTS `amzn-yt-data-pipeline-silver-dev`;

Usage:
    python bronze_to_silver_statistics_local.py
"""

import awswrangler as wr
import pandas as pd
import numpy as np
import json

# ── Config (mirrors the original Glue job parameters) ───────────────────────
BRONZE_DATABASE = "amzn-yt-data-pipeline-bronze-dev"
BRONZE_TABLE = "youtube_videos"
SILVER_BUCKET = "amzn-yt-data-pipeline-silver-ap-south-1-dev"
SILVER_DATABASE = "amzn-yt-data-pipeline-silver-dev"
SILVER_TABLE = "clean_statistics"
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"

JOB_NAME = "bronze_to_silver_statistics_local"


def main():
    # ── Step 1: Read from Bronze via Athena ─────────────────────────────────
    print(f"Bronze: {BRONZE_DATABASE}.{BRONZE_TABLE}")
    print(f"Silver: {SILVER_DATABASE}.{SILVER_TABLE} -> {SILVER_PATH}")
    print("Reading from Bronze via Athena...")

    df = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{BRONZE_TABLE}"',
        database=BRONZE_DATABASE,
    )
    initial_count = len(df)
    print(f"Bronze records read: {initial_count}")

    if initial_count == 0:
        print("No records to process. Exiting.")
        return

    # ── Step 2: Schema enforcement ───────────────────────────────────────────
    print("Enforcing schema and casting types...")

    df["video_id"] = df["video_id"].astype("string")
    df["trending_date"] = df["trending_date"].astype("string")
    df["title"] = df["title"].astype("string")
    df["channel_title"] = df["channel_title"].astype("string")
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    df["publish_time"] = df["publish_time"].astype("string")
    df["tags"] = df["tags"].astype("string")
    df["thumbnail_link"] = df["thumbnail_link"].astype("string")
    df["description"] = df["description"].astype("string")
    df["region"] = df["region"].astype("string")

    for col in ["views", "likes", "dislikes", "comment_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # comments_disabled / ratings_disabled / video_error_or_removed arrive as
    # the strings "true"/"false" (lowercased during bronze cleaning) — map to
    # real booleans, same as PySpark's .cast(BooleanType()).
    for col in ["comments_disabled", "ratings_disabled", "video_error_or_removed"]:
        df[col] = df[col].astype("string").str.lower().map({"true": True, "false": False})

    # ── Step 3: Data cleansing ───────────────────────────────────────────────
    print("Cleansing data...")

    # Remove records where video_id is null (corrupt rows)
    df = df[df["video_id"].notna()].copy()

    # Standardize region codes to lower
    df["region"] = df["region"].str.lower().str.strip()

    # Parse trending_date from Kaggle format (YY.DD.MM) to a proper date
    def parse_trending_date(val):
        if pd.isna(val):
            return pd.NaT
        try:
            return pd.to_datetime(val, format="%y.%d.%m").date()
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(val).date()
            except (ValueError, TypeError):
                return pd.NaT

    df["trending_date_parsed"] = df["trending_date"].apply(parse_trending_date)

    # Fill nulls for numeric columns with 0
    for col in ["views", "likes", "dislikes", "comment_count"]:
        df[col] = df[col].fillna(0).astype("int64")

    # Derived columns
    df["like_ratio"] = np.where(
        df["views"] > 0,
        (df["likes"] / df["views"] * 100).round(4),
        0.0,
    )
    df["engagement_rate"] = np.where(
        df["views"] > 0,
        ((df["likes"] + df["dislikes"] + df["comment_count"]) / df["views"] * 100).round(4),
        0.0,
    )

    # Processing metadata
    df["_processed_at"] = pd.Timestamp.utcnow()
    df["_job_name"] = JOB_NAME

    # ── Step 4: Deduplication ────────────────────────────────────────────────
    print("Deduplicating...")

    # Keep the latest record per video_id + region + trending_date
    df = df.sort_values("_processed_at", ascending=False)
    df = df.drop_duplicates(
        subset=["video_id", "region", "trending_date_parsed"], keep="first"
    )

    clean_count = len(df)
    print(f"After cleansing & dedup: {clean_count} records "
          f"(removed {initial_count - clean_count})")

    # ── Step 5: Data quality checks ──────────────────────────────────────────
    print("Running data quality checks...")

    null_counts = {}
    for col in ["video_id", "title", "channel_title", "views"]:
        n = int(df[col].isna().sum())
        null_counts[col] = n
        if n > 0:
            print(f"  DQ WARNING: {col} has {n} null values")

    negative_views = int((df["views"] < 0).sum())
    if negative_views > 0:
        print(f"  DQ WARNING: {negative_views} records with negative views")

    print(f"  DQ check complete. Null counts: {null_counts}")

    # ── Step 6: Write to Silver layer ────────────────────────────────────────
        # ── Step 6: Write to Silver layer ────────────────────────────────────────
        # ── Step 6: Write to Silver layer ────────────────────────────────────────
        # ── Step 5.5: Automatically Flatten ALL Nested JSON Columns ──────────────
    print("Scanning and dynamically flattening all nested JSON columns...")

    def safe_json_load(val):
        """Safely parses JSON strings into dictionaries."""
        if isinstance(val, str):
            val_stripped = val.strip()
            if val_stripped.startswith('{') or val_stripped.startswith('['):
                try:
                    return json.loads(val)
                except:
                    return val
        return val

    # 1. Convert any JSON string columns into actual Python dictionaries/lists
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if any row looks like a JSON object
            sample = df[col].dropna().astype(str).head(10)
            if sample.str.contains(r'^[\{\[]').any():
                df[col] = df[col].apply(safe_json_load)

    # 2. Dynamically unpack nested dictionaries into flat columns
    while True:
        nested_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]
        if not nested_cols:
            break  # Stop when there are no more nested dictionary columns left
        
        for col in nested_cols:
            print(f"-> Flattening column: {col}")
            # Normalize the nested column
            normalized = pd.json_normalize(df[col].fillna({}))
            # Add the original column name as a prefix to avoid column name collisions
            normalized.columns = [f"{col}_{subcol}" if not subcol.startswith(col) else subcol for subcol in normalized.columns]
            
            # Drop the original nested column and join the flattened columns
            df = df.drop(columns=[col]).reset_index(drop=True)
            df = pd.concat([df, normalized], axis=1)

    print("All columns flattened. New DataFrame columns:", list(df.columns))

        # ── Step 6: Write to Silver layer ────────────────────────────────────────
    print(f"Writing to Silver (Dynamic Flat Parquet): {SILVER_PATH}")

    # Remove the pandas index entirely to prevent nested row-metadata tracking
    df_flat = df.reset_index(drop=True)

    wr.s3.to_parquet(
        df=df_flat,
        path=SILVER_PATH,
        dataset=True,
        partition_cols=["region"],
        database=SILVER_DATABASE,
        table=SILVER_TABLE,
        mode="overwrite",
        compression="snappy",
        index=False,                  # Drops Pandas structural index
        schema_evolution=True         # Allow true flat columns to build the schema dynamically
    )

    print(f"Silver write complete. {len(df_flat)} flat records written.")

if __name__ == "__main__":
    main()