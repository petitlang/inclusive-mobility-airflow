from __future__ import annotations

import json
from datetime import date
from typing import Any

import boto3

S3_ENDPOINT = "http://localstack:4566"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "dummy"
S3_SECRET_KEY = "dummy"

RAW_BUCKET = "raw-data-mobility"
FORMATTED_BUCKET = "formatted-data-mobility"
USAGE_BUCKET = "usage-data-mobility"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def create_buckets() -> None:
    """Create the three data lake S3 buckets if they don't exist."""
    s3 = _s3_client()
    for bucket in (RAW_BUCKET, FORMATTED_BUCKET, USAGE_BUCKET):
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"[S3] Created bucket: {bucket}")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"[S3] Bucket already exists: {bucket}")


def upload_json(
    bucket: str,
    key: str,
    data: dict[str, Any] | list[dict[str, Any]],
) -> None:
    """Upload a JSON-serializable object to S3."""
    s3 = _s3_client()
    body = json.dumps(data, indent=2, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    print(f"[S3] Uploaded s3://{bucket}/{key}")


def s3_key(layer: str, group: str, entity: str, filename: str, partition_day: str | None = None) -> str:
    """Build S3 object key following data lake convention."""
    day = partition_day or date.today().strftime("%Y%m%d")
    return f"{group}/{entity}/{day}/{filename}"


def layer_bucket(layer: str) -> str:
    """Map layer name to S3 bucket name."""
    return {"raw": RAW_BUCKET, "formatted": FORMATTED_BUCKET, "usage": USAGE_BUCKET}[layer]
