from __future__ import annotations

from io import BytesIO
from typing import Any

import boto3
import pandas as pd
from elasticsearch import Elasticsearch, helpers

from utils.paths import current_day
from utils.s3_utils import S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY, USAGE_BUCKET

ES_HOST = "http://elasticsearch:9200"

INDEX_CONFIGS = {
    "inclusive_mobility_scores": {
        "entity": "mobility_scores",
        "doc_id_field": "establishment_id",
    },
    "inclusive_mobility_risky_areas": {
        "entity": "risky_areas",
        "doc_id_field": "establishment_id",
    },
    "inclusive_mobility_improvement_priorities": {
        "entity": "improvement_priorities",
        "doc_id_field": "establishment_id",
    },
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def _read_parquet_from_s3(bucket: str, prefix: str) -> pd.DataFrame:
    """Read all parquet files under an S3 prefix into a DataFrame."""
    s3 = _s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError(f"No objects found at s3://{bucket}/{prefix}")

    dfs = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".parquet"):
            buf = BytesIO()
            s3.download_fileobj(bucket, key, buf)
            buf.seek(0)
            dfs.append(pd.read_parquet(buf))

    if not dfs:
        raise FileNotFoundError(f"No parquet files found at s3://{bucket}/{prefix}")
    return pd.concat(dfs, ignore_index=True)


def _build_actions(df: pd.DataFrame, index_name: str, doc_id_field: str) -> list[dict[str, Any]]:
    """Build ES bulk indexing actions from a DataFrame."""
    actions = []
    for _, row in df.iterrows():
        doc = row.where(row.notna(), None).to_dict()
        doc_id = str(doc.get(doc_id_field, ""))
        actions.append({"_index": index_name, "_id": doc_id, "_source": doc})
    return actions


def index_usage_outputs(**kwargs) -> str:
    """Index all usage parquet outputs from S3 into Elasticsearch.

    Returns:
        Summary string with document counts per index.
    """
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Elasticsearch not reachable at {ES_HOST}")

    day = current_day()
    summary_parts = []
    for index_name, cfg in INDEX_CONFIGS.items():
        entity = cfg["entity"]
        prefix = f"inclusive_mobility/{entity}/{day}/"
        print(f"Indexing from s3://{USAGE_BUCKET}/{prefix} -> {index_name}")

        df = _read_parquet_from_s3(USAGE_BUCKET, prefix)
        actions = _build_actions(df, index_name, cfg["doc_id_field"])

        success, errors = helpers.bulk(es, actions, raise_on_error=False, stats_only=True)
        print(f"  Indexed {success} documents into {index_name}")
        if errors:
            print(f"  WARNING: {len(errors)} errors during indexing")
        summary_parts.append(f"{index_name}: {success} docs")

    es.close()
    summary = "; ".join(summary_parts)
    print(f"Indexing complete: {summary}")
    return summary
