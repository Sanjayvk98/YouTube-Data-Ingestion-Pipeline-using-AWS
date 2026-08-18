"""
Cleans embedded newlines (and normalizes boolean strings) in the YouTube
trending CSV files, then re-uploads the cleaned version to the SAME S3 key,
overwriting the original.

Why: Athena's CSV reader breaks a row wherever it sees a raw newline,
even inside a quoted field. Several `description` fields in this dataset
contain literal line breaks, which shifts columns for that row and any
row after it until the quote count re-balances. pandas' CSV parser
handles quoted multi-line fields correctly, so we use it to re-serialize
each file with newlines stripped from field values.

Usage:
    python clean_youtube_csvs.py

Requires AWS credentials with read+write access to the bucket
(same credentials the aws s3 cp commands used).
"""

import boto3
import pandas as pd
import io
import warnings

# Collect pandas' "Skipping line N" ParserWarnings instead of letting them
# print to stderr mid-run — we surface the count in the per-region summary
# so it's obvious how many rows the CSV parser itself discarded (on top of
# whatever the category_id numeric check drops afterward).
_skipped_line_count = 0


def _warning_handler(message, category, filename, lineno, file=None, line=None):
    global _skipped_line_count
    if issubclass(category, pd.errors.ParserWarning):
        _skipped_line_count += 1
    else:
        print(f"  WARNING: {message}")


warnings.showwarning = _warning_handler

BUCKET = "amzn-yt-data-pipeline-bronze-ap-south-1-dev"
PREFIX = "youtube/raw_statistics"

REGIONS = ["ca", "de", "fr", "gb", "in", "jp", "kr", "mx", "ru", "us"]

# Column names — this dataset ships with no header issues, but we set
# them explicitly in case any regional file has a variant header.
COLUMNS = [
    "video_id", "trending_date", "title", "channel_title", "category_id",
    "publish_time", "tags", "views", "likes", "dislikes", "comment_count",
    "thumbnail_link", "comments_disabled", "ratings_disabled",
    "video_error_or_removed", "description"
]

s3 = boto3.client("s3")


def clean_region(region_code: str):
    global _skipped_line_count
    _skipped_line_count = 0

    key = f"{PREFIX}/region={region_code}/{region_code.upper()}videos.csv"
    print(f"Processing s3://{BUCKET}/{key} ...")

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        print(f"  SKIP: key not found")
        return

    raw_bytes = obj["Body"].read()

    # Most of these Kaggle YouTube CSVs are latin-1 encoded, not utf-8.
    # Try utf-8 first, fall back to latin-1.
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    # engine="python" + on_bad_lines lets us see/skip truly malformed
    # rows instead of crashing the whole load.
    # dtype=str is critical: without it, pandas auto-infers columns like
    # comments_disabled ("True"/"False") as native bool dtype, which then
    # has no .str accessor and breaks the lowercasing step below. Forcing
    # everything to string keeps every column text until we explicitly
    # convert category_id to numeric further down.
    df = pd.read_csv(
        io.StringIO(text),
        engine="python",
        quotechar='"',
        on_bad_lines="warn",
        dtype=str,
        keep_default_na=False,
    )

    before_rows = len(df)

    # Strip embedded newlines / carriage returns from every text column
    # so no field can ever contain a literal line break again.
    text_cols = df.columns
    for col in text_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[\r\n]+", " ", regex=True)
            .str.strip()
        )

    # Normalize the boolean-ish string columns to lowercase true/false
    # so downstream CAST(... AS BOOLEAN) works cleanly if you want it.
    for col in ["comments_disabled", "ratings_disabled", "video_error_or_removed"]:
        if col in df.columns:
            df[col] = df[col].str.lower()

    # Drop rows where category_id isn't actually numeric — these are the
    # leftover casualties of any row-splitting that already happened
    # upstream in the original file, before this cleaning pass.
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce")
    bad_rows = df["category_id"].isna().sum()
    df = df.dropna(subset=["category_id"])
    df["category_id"] = df["category_id"].astype(int)

    print(f"  rows before: {before_rows}, "
          f"parser-skipped unrecoverable lines: {_skipped_line_count}, "
          f"dropped (bad category_id): {bad_rows}, "
          f"rows after: {len(df)}")

    # Re-serialize as clean CSV and overwrite the same S3 key.
    out_buffer = io.StringIO()
    df.to_csv(out_buffer, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=out_buffer.getvalue().encode("utf-8"),
    )
    print(f"  uploaded cleaned file back to s3://{BUCKET}/{key}")


if __name__ == "__main__":
    for region in REGIONS:
        clean_region(region)
    print("Done. Re-run your Athena query — category_id errors should be gone.")