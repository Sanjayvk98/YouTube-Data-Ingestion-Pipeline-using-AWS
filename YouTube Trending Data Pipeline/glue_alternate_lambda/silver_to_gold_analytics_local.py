"""
Silver -> Gold (Analytics Aggregations) — LOCAL equivalent of the AWS Glue ETL job.

Produces the same 3 Gold tables as the original PySpark job:
  1. trending_analytics — daily trending summaries per region
  2. channel_analytics  — channel performance metrics
  3. category_analytics — category-level trends over time

How each Glue-specific piece is replaced:
  - glueContext.create_dynamic_frame.from_catalog(...)
      -> awswrangler.athena.read_sql_query(...)
  - PySpark groupBy/agg/Window
      -> pandas groupby/agg + groupby().rank() / groupby().transform()
  - glueContext.getSink(..., enableUpdateCatalog=True, ...)
      -> awswrangler.s3.to_parquet(..., database=..., table=...)
         writes partitioned Snappy Parquet AND registers/updates the
         Glue Catalog table in one call — same mechanism your manual
         CREATE EXTERNAL TABLE calls already use successfully.

Category names come from the Silver reference table `clean_reference_data`
(see promote_reference_data_to_silver.py for the one-time step that
populates it) — this script touches Silver only, never Bronze.

Prerequisites:
    pip install awswrangler pandas
    aws configure

The Gold database must exist first — run once in Athena:
    CREATE DATABASE IF NOT EXISTS `amzn-yt-data-pipeline-gold-dev`;

Usage:
    python silver_to_gold_analytics_local.py
"""

import awswrangler as wr
import pandas as pd

# ── Config (mirrors the original Glue job parameters) ───────────────────────
SILVER_DATABASE = "amzn-yt-data-pipeline-silver-dev"
SILVER_STATS_TABLE = "clean_statistics"
SILVER_REFERENCE_TABLE = "clean_reference_data"

GOLD_BUCKET = "amzn-yt-data-pipeline-gold-ap-south-1-dev"
GOLD_DATABASE = "amzn-yt-data-pipeline-gold-dev"

TRENDING_PATH = f"s3://{GOLD_BUCKET}/youtube/trending_analytics/"
CHANNEL_PATH = f"s3://{GOLD_BUCKET}/youtube/channel_analytics/"
CATEGORY_PATH = f"s3://{GOLD_BUCKET}/youtube/category_analytics/"


def load_stats():
    print(f"Reading Silver: {SILVER_DATABASE}.{SILVER_STATS_TABLE} ...")
    df = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{SILVER_STATS_TABLE}"',
        database=SILVER_DATABASE,
    )
    print(f"  Statistics records: {len(df)}")
    return df


def attach_category_names(stats_df: pd.DataFrame) -> pd.DataFrame:
    """Left-joins category_name onto stats_df using the Silver
    clean_reference_data table (already flat — no JSON parsing needed
    here). Falls back to 'Unknown' for anything that fails to load or
    doesn't match, mirroring the original job's try/except + fillna
    behavior."""
    try:
        print(f"Reading category lookup: {SILVER_DATABASE}.{SILVER_REFERENCE_TABLE} ...")
        cat_df = wr.athena.read_sql_query(
            sql=f'SELECT category_id, category_name FROM "{SILVER_REFERENCE_TABLE}"',
            database=SILVER_DATABASE,
        )
        cat_df["category_id"] = pd.to_numeric(cat_df["category_id"], errors="coerce").astype("Int64")
        # Original job dedupes by category_id only (ignores region) — same here.
        cat_df = cat_df.drop_duplicates(subset=["category_id"])

        print(f"  Category lookup entries: {len(cat_df)}")

        stats_df["category_id"] = pd.to_numeric(stats_df["category_id"], errors="coerce").astype("Int64")
        stats_df = stats_df.merge(cat_df, on="category_id", how="left")

    except Exception as e:
        print(f"  WARNING: could not load category lookup ({e}). "
              f"Proceeding without category names.")

    if "category_name" not in stats_df.columns:
        stats_df["category_name"] = "Unknown"
    else:
        stats_df["category_name"] = stats_df["category_name"].fillna("Unknown")

    return stats_df


def build_trending_analytics(stats_df: pd.DataFrame) -> pd.DataFrame:
    print("Building Gold: trending_analytics...")

    trending = stats_df.groupby(["region", "trending_date_parsed"]).agg(
        total_videos=("video_id", "count"),
        total_views=("views", "sum"),
        total_likes=("likes", "sum"),
        total_dislikes=("dislikes", "sum"),
        total_comments=("comment_count", "sum"),
        avg_views_per_video=("views", "mean"),
        avg_like_ratio=("like_ratio", "mean"),
        avg_engagement_rate=("engagement_rate", "mean"),
        max_views=("views", "max"),
        unique_channels=("channel_title", "nunique"),
        unique_categories=("category_id", "nunique"),
    ).reset_index()

    trending["_aggregated_at"] = pd.Timestamp.utcnow()

    print(f"  Rows: {len(trending)} -> {TRENDING_PATH}")
    wr.s3.to_parquet(
        df=trending,
        path=TRENDING_PATH,
        dataset=True,
        partition_cols=["region"],
        database=GOLD_DATABASE,
        table="trending_analytics",
        mode="overwrite",
        compression="snappy",
    )


def build_channel_analytics(stats_df: pd.DataFrame) -> pd.DataFrame:
    print("Building Gold: channel_analytics...")

    channel = stats_df.groupby(["channel_title", "region"]).agg(
        total_videos=("video_id", "nunique"),
        total_views=("views", "sum"),
        total_likes=("likes", "sum"),
        total_comments=("comment_count", "sum"),
        avg_views_per_video=("views", "mean"),
        avg_engagement_rate=("engagement_rate", "mean"),
        peak_views=("views", "max"),
        times_trending=("trending_date_parsed", "count"),
        first_trending=("trending_date_parsed", "min"),
        last_trending=("trending_date_parsed", "max"),
        categories=("category_name", lambda s: sorted(set(s.dropna()))),
    ).reset_index()

    # Rank channels by total views within each region (equivalent to the
    # PySpark Window.partitionBy("region").orderBy(desc("total_views"))).
    channel["rank_in_region"] = (
        channel.groupby("region")["total_views"]
        .rank(method="first", ascending=False)
        .astype("int64")
    )
    channel["_aggregated_at"] = pd.Timestamp.utcnow()

    print(f"  Rows: {len(channel)} -> {CHANNEL_PATH}")
    wr.s3.to_parquet(
        df=channel,
        path=CHANNEL_PATH,
        dataset=True,
        partition_cols=["region"],
        database=GOLD_DATABASE,
        table="channel_analytics",
        mode="overwrite",
        compression="snappy",
    )


def build_category_analytics(stats_df: pd.DataFrame) -> pd.DataFrame:
    print("Building Gold: category_analytics...")

    category = stats_df.groupby(
        ["category_name", "category_id", "region", "trending_date_parsed"]
    ).agg(
        video_count=("video_id", "count"),
        total_views=("views", "sum"),
        total_likes=("likes", "sum"),
        total_comments=("comment_count", "sum"),
        avg_engagement_rate=("engagement_rate", "mean"),
        unique_channels=("channel_title", "nunique"),
    ).reset_index()

    # Category share of views per region per day (equivalent to the PySpark
    # Window.partitionBy("region", "trending_date_parsed") sum-ratio).
    region_day_total = category.groupby(["region", "trending_date_parsed"])["total_views"].transform("sum")
    category["view_share_pct"] = (category["total_views"] / region_day_total * 100).round(2)
    category["_aggregated_at"] = pd.Timestamp.utcnow()

    print(f"  Rows: {len(category)} -> {CATEGORY_PATH}")
    wr.s3.to_parquet(
        df=category,
        path=CATEGORY_PATH,
        dataset=True,
        partition_cols=["region"],
        database=GOLD_DATABASE,
        table="category_analytics",
        mode="overwrite",
        compression="snappy",
    )


def main():
    stats_df = load_stats()
    if len(stats_df) == 0:
        print("No Silver records found. Exiting.")
        return

    stats_df = attach_category_names(stats_df)

    build_trending_analytics(stats_df)
    build_channel_analytics(stats_df)
    build_category_analytics(stats_df)

    print("Gold layer build complete.")


if __name__ == "__main__":
    main()