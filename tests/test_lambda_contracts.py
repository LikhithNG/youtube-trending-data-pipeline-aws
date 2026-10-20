import importlib.util
from pathlib import Path
from urllib.error import URLError

import boto3
import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_module(monkeypatch, name, relative_path):
    monkeypatch.setattr(boto3, "client", lambda service: object())
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ingestion_normalizes_and_deduplicates_regions(monkeypatch):
    module = load_module(
        monkeypatch,
        "ingestion_config_test",
        "lambdas/youtube_api_ingestion/lambda_function.py",
    )
    monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
    monkeypatch.setenv("S3_BUCKET_BRONZE", "bronze")
    monkeypatch.setenv("YOUTUBE_REGIONS", "us, GB,us")

    assert module.get_config()["regions"] == ["US", "GB"]


def test_ingestion_raises_so_step_functions_can_retry(monkeypatch):
    module = load_module(
        monkeypatch,
        "ingestion_failure_test",
        "lambdas/youtube_api_ingestion/lambda_function.py",
    )
    monkeypatch.setattr(
        module,
        "get_config",
        lambda: {
            "api_key": "test-key",
            "bucket": "bronze",
            "regions": ["US"],
            "sns_topic": "",
            "fail_on_partial": True,
        },
    )
    monkeypatch.setattr(
        module,
        "fetch_trending_videos",
        lambda region, api_key: (_ for _ in ()).throw(URLError("offline")),
    )

    with pytest.raises(RuntimeError, match="1/1 regions"):
        module.lambda_handler({}, None)


def test_reference_lambda_accepts_step_functions_and_s3_contracts(monkeypatch):
    module = load_module(
        monkeypatch,
        "reference_contract_test",
        "lambdas/json_to_parquet/lambda_function.py",
    )
    event = {
        "objects": [{"bucket": "bronze", "key": "region=us/a.json"}],
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "bronze"},
                    "object": {"key": "region%3Dgb/b.json"},
                }
            }
        ],
    }

    assert module.extract_objects(event) == [
        {"bucket": "bronze", "key": "region=us/a.json"},
        {"bucket": "bronze", "key": "region=gb/b.json"},
    ]


def test_reference_validation_is_strict(monkeypatch):
    module = load_module(
        monkeypatch,
        "reference_validation_test",
        "lambdas/json_to_parquet/lambda_function.py",
    )
    with pytest.raises(ValueError, match="non-empty items"):
        module.normalize_categories({"items": []}, "us", "empty.json")
