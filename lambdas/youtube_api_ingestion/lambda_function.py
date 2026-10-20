"""Ingest YouTube trending data into the Bronze S3 layer."""

import json
import logging
import os
import re
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_REGIONS = "US,GB,CA,DE,FR,IN,JP,KR,MX,RU"
MAX_RESULTS = 50


def get_config() -> dict:
    """Load and validate runtime configuration."""
    regions = [
        value.strip().upper() for value in os.getenv("YOUTUBE_REGIONS", DEFAULT_REGIONS).split(",")
    ]
    invalid = [region for region in regions if not re.fullmatch(r"[A-Z]{2}", region)]
    if invalid:
        raise ValueError(f"Invalid ISO-3166 alpha-2 region codes: {invalid}")
    if not regions:
        raise ValueError("YOUTUBE_REGIONS must contain at least one region")
    return {
        "api_key": os.environ["YOUTUBE_API_KEY"],
        "bucket": os.environ["S3_BUCKET_BRONZE"],
        "regions": list(dict.fromkeys(regions)),
        "sns_topic": os.getenv("SNS_ALERT_TOPIC_ARN", ""),
        "fail_on_partial": os.getenv("FAIL_ON_PARTIAL_ERROR", "true").lower() == "true",
    }


def fetch_json(resource: str, params: dict, api_key: str) -> dict:
    query = urlencode({**params, "key": api_key})
    request = Request(f"{API_BASE}/{resource}?{query}", headers={"Accept": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_trending_videos(region_code: str, api_key: str) -> dict:
    return fetch_json(
        "videos",
        {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": MAX_RESULTS,
        },
        api_key,
    )


def fetch_video_categories(region_code: str, api_key: str) -> dict:
    return fetch_json("videoCategories", {"part": "snippet", "regionCode": region_code}, api_key)


def put_json(bucket: str, key: str, data: dict, now: datetime) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        Metadata={"ingestion_timestamp": now.isoformat(), "source": "youtube_data_api_v3"},
    )


def put_json_lines(bucket: str, key: str, records: list[dict], now: datetime) -> None:
    """Write one object per line so Glue returns one video per row."""
    body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={"ingestion_timestamp": now.isoformat(), "source": "youtube_data_api_v3"},
    )


def publish_alert(topic_arn: str, subject: str, message: str) -> None:
    if topic_arn:
        sns_client.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)


def lambda_handler(event, context):
    config = get_config()
    now = datetime.now(UTC)
    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingestion_id = now.strftime("%Y%m%d_%H%M%S_%f")
    result = {"success": [], "failed": []}
    statistics_objects = []
    reference_objects = []

    for region in config["regions"]:
        logger.info("Processing region=%s", region)
        try:
            trending_data = fetch_trending_videos(region, config["api_key"])
            videos = trending_data.get("items", [])
            if not videos:
                raise ValueError("YouTube API returned no trending videos")
            records = [
                {
                    **video,
                    "region": region.lower(),
                    "trending_date": date_partition,
                    "ingestion_id": ingestion_id,
                    "ingestion_timestamp": now.isoformat(),
                }
                for video in videos
            ]
            stats_key = (
                "youtube/raw_statistics/"
                f"region={region.lower()}/date={date_partition}/hour={hour_partition}/"
                f"{ingestion_id}.jsonl"
            )
            put_json_lines(config["bucket"], stats_key, records, now)
            statistics_objects.append({"bucket": config["bucket"], "key": stats_key})

            categories = fetch_video_categories(region, config["api_key"])
            if not categories.get("items"):
                raise ValueError("YouTube API returned no video categories")
            categories["_pipeline_metadata"] = {
                "ingestion_id": ingestion_id,
                "region": region.lower(),
                "ingestion_timestamp": now.isoformat(),
                "source": "youtube_data_api_v3",
            }
            reference_key = (
                "youtube/raw_statistics_reference_data/"
                f"region={region.lower()}/date={date_partition}/{region.lower()}_category_id.json"
            )
            put_json(config["bucket"], reference_key, categories, now)
            reference_objects.append({"bucket": config["bucket"], "key": reference_key})
            result["success"].append(region)
        except (HTTPError, URLError, ValueError, KeyError) as exc:
            logger.exception("Ingestion failed for region=%s", region)
            result["failed"].append({"region": region, "error": str(exc)})
        except Exception as exc:
            logger.exception("Unexpected ingestion failure for region=%s", region)
            result["failed"].append({"region": region, "error": str(exc)})

    response = {
        "ingestion_id": ingestion_id,
        "statistics_objects": statistics_objects,
        "reference_objects": reference_objects,
        "results": result,
    }
    if result["failed"]:
        publish_alert(
            config["sns_topic"],
            f"[YT Pipeline] Ingestion failure — {ingestion_id}",
            json.dumps(response, indent=2),
        )
        if config["fail_on_partial"]:
            raise RuntimeError(
                f"Ingestion failed for {len(result['failed'])}/{len(config['regions'])} regions"
            )

    logger.info("Ingestion completed: %s", json.dumps(response))
    return response
