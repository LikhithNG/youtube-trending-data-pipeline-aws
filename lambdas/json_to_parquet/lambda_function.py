"""Normalize YouTube category JSON and write partitioned Silver Parquet."""

import json
import logging
import os
from datetime import UTC, datetime
from urllib.parse import unquote_plus

import awswrangler as wr
import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def config() -> dict:
    bucket = os.environ["S3_BUCKET_SILVER"]
    return {
        "path": f"s3://{bucket}/youtube/reference_data/",
        "database": os.getenv("GLUE_DB_SILVER", "yt_pipeline_silver_dev"),
        "table": os.getenv("GLUE_TABLE_REFERENCE", "clean_reference_data"),
        "sns_topic": os.getenv("SNS_ALERT_TOPIC_ARN", ""),
    }


def extract_objects(event: dict) -> list[dict]:
    """Accept native S3 notifications or the explicit object list from Step Functions."""
    objects = []
    for item in event.get("objects", []):
        if item.get("bucket") and item.get("key"):
            objects.append({"bucket": item["bucket"], "key": unquote_plus(item["key"])})

    records = event.get("Records", [])
    if not records and "s3" in event:
        records = [event]
    for record in records:
        s3_info = record["s3"]
        objects.append(
            {
                "bucket": s3_info["bucket"]["name"],
                "key": unquote_plus(s3_info["object"]["key"]),
            }
        )
    return objects


def read_json_from_s3(bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def region_from_key(key: str) -> str:
    for part in key.split("/"):
        if part.startswith("region="):
            return part.partition("=")[2].lower()
    raise ValueError(f"S3 key does not contain a region partition: {key}")


def normalize_categories(raw_data: dict, region: str, source_key: str) -> pd.DataFrame:
    items = raw_data.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("Category payload must contain a non-empty items array")
    frame = pd.json_normalize(items)
    required = {"id", "snippet.title"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Category payload is missing columns: {sorted(missing)}")

    frame = frame.drop_duplicates(subset=["id"], keep="last")
    frame["id"] = pd.to_numeric(frame["id"], errors="raise").astype("int64")
    frame["region"] = region
    frame["_ingestion_timestamp"] = datetime.now(UTC)
    frame["_source_file"] = source_key
    return frame


def publish_alert(topic_arn: str, errors: list[dict]) -> None:
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject="[YT Pipeline] Silver reference transform failed",
            Message=json.dumps(errors, indent=2),
        )


def lambda_handler(event, context):
    settings = config()
    objects = extract_objects(event)
    if not objects:
        raise ValueError("No S3 reference objects were supplied")

    processed = []
    errors = []
    for item in objects:
        try:
            region = region_from_key(item["key"])
            frame = normalize_categories(
                read_json_from_s3(item["bucket"], item["key"]), region, item["key"]
            )
            wr.s3.to_parquet(
                df=frame,
                path=settings["path"],
                dataset=True,
                database=settings["database"],
                table=settings["table"],
                partition_cols=["region"],
                mode="overwrite_partitions",
                schema_evolution=True,
            )
            processed.append({"key": item["key"], "region": region, "rows": len(frame)})
        except Exception as exc:
            logger.exception("Reference transform failed for key=%s", item.get("key"))
            errors.append({"key": item.get("key", "unknown"), "error": str(exc)})

    if errors:
        publish_alert(settings["sns_topic"], errors)
        raise RuntimeError(f"Reference transform failed for {len(errors)}/{len(objects)} objects")
    return {"processed": processed, "processed_count": len(processed)}
