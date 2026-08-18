"""
Minifies the pretty-printed YouTube category JSON files (one file per
region) into single-line JSON, then re-uploads to the SAME S3 key,
overwriting the original.

Why: Athena's JSON SerDe (via Hive's TextInputFormat) reads a file
line-by-line and tries to parse EACH LINE as one complete JSON record.
Since your *_category_id.json files are pretty-printed across many
lines but represent a single JSON object, Athena chokes on line 2
onward. Collapsing each file to one line fixes this because the whole
object then becomes exactly one "row" for the reader.

Usage:
    python minify_category_json.py

Requires AWS credentials with read+write access to the bucket.
"""

import boto3
import json

BUCKET = "amzn-yt-data-pipeline-bronze-ap-south-1-dev"
PREFIX = "youtube/raw_statistics_reference_data"

REGIONS = ["ca", "de", "fr", "gb", "in", "jp", "kr", "mx", "ru", "us"]

s3 = boto3.client("s3")


def minify_region(region_code: str):
    key = f"{PREFIX}/region={region_code}/{region_code.upper()}_category_id.json"
    print(f"Processing s3://{BUCKET}/{key} ...")

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        print(f"  SKIP: key not found")
        return

    raw_bytes = obj["Body"].read()

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  ERROR: could not parse JSON — {e}")
        return

    # separators=(',', ':') strips extra whitespace; ensure_ascii=False
    # keeps any non-ASCII channel/category titles intact rather than
    # escaping them.
    minified = json.dumps(data, separators=(",", ":"), ensure_ascii=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=minified.encode("utf-8"),
    )
    print(f"  uploaded minified file back to s3://{BUCKET}/{key} "
          f"({len(minified)} chars, 1 line)")


if __name__ == "__main__":
    for region in REGIONS:
        minify_region(region)
    print("Done. Re-run your Athena SELECT — the JSON parse error should be gone.")