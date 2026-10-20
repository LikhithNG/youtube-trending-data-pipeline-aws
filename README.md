# YouTube Trending Data Pipeline on AWS

An end-to-end data engineering project that ingests live YouTube trending data for ten regions, processes it through Bronze, Silver, and Gold layers, applies a blocking data-quality gate, and exposes analytics tables through the AWS Glue Data Catalog and Athena.

![Architecture](./YouTube%20Trending%20Data%20Pipeline.png)

## What the pipeline produces

- `trending_analytics`: daily views, likes, engagement, channel count, and category count by region.
- `channel_analytics`: channel performance, trending-day count, regional rank, and categories.
- `category_analytics`: category performance and share of regional daily views.

The default regions are `US`, `GB`, `CA`, `DE`, `FR`, `IN`, `JP`, `KR`, `MX`, and `RU`.

## Architecture

```text
EventBridge
    │
    ▼
Step Functions
    ├── Ingestion Lambda ───────────────► S3 Bronze (JSON Lines + category JSON)
    ├── Parallel
    │   ├── Glue Bronze → Silver ──────► S3 Silver statistics (Parquet)
    │   └── Reference Lambda ──────────► S3 Silver categories (Parquet)
    ├── Data-quality Lambda / Athena
    ├── Glue Silver → Gold ────────────► three S3 Gold tables (Parquet)
    └── SNS success or failure notification
```

The ingestion Lambda writes one video per JSON line. This is intentional: a standard Glue JSON classifier can then expose one video per row while retaining the nested `snippet` and `statistics` structures.

## Reliability and portability

- API and transformation failures raise exceptions, allowing Step Functions retries and `Catch` paths to work.
- The reference-data Lambda accepts both native S3 notifications and the explicit object list returned by ingestion.
- All configured regions are processed; there is no hard-coded four-region predicate.
- Category lookup joins on both `region` and `category_id`.
- Gold tables are rebuilt as idempotent snapshots instead of appending duplicate aggregates.
- Freshness checks fail closed when timestamps are missing or invalid.
- The state machine and IAM policies contain portable template variables instead of personal account IDs.
- CI validates Python syntax, style, tests, and state-machine rendering.

## Repository layout

```text
data/                           Historical Kaggle data and category fixtures
data_quality/                   Data-quality Lambda
glue_jobs/                      Bronze → Silver and Silver → Gold PySpark jobs
iam_permission/                 Portable IAM policy templates
lambdas/
  json_to_parquet/              Category reference transformation
  youtube_api_ingestion/        Live YouTube ingestion
scripts/
  aws_copy.sh                   Optional historical backfill upload
  package_lambdas.sh            Reproducible Lambda ZIP packaging
  render_state_machine.py       State-machine/IAM template renderer
step_functions/
  config.example.json           Safe example configuration
  pipeline_orchestration.template.json
tests/                          Unit tests
```

## Prerequisites

- An AWS account and AWS CLI credentials
- Python 3.11 for Lambda packaging and local tests
- AWS Glue 4.0 or newer
- A YouTube Data API v3 key
- Permission to manage S3, Lambda, Glue, Athena, Step Functions, SNS, EventBridge, IAM, and CloudWatch

Glue jobs and scheduled executions are billable. Use a separate development account, AWS Budgets, short log retention, and an EventBridge schedule appropriate for your budget.

## AWS resources

Create four uniquely named S3 buckets:

```bash
aws s3 mb s3://YOUR-BRONZE-BUCKET
aws s3 mb s3://YOUR-SILVER-BUCKET
aws s3 mb s3://YOUR-GOLD-BUCKET
aws s3 mb s3://YOUR-SCRIPT-BUCKET
```

Create the catalog databases:

```bash
aws glue create-database --database-input '{"Name":"yt_pipeline_bronze_dev"}'
aws glue create-database --database-input '{"Name":"yt_pipeline_silver_dev"}'
aws glue create-database --database-input '{"Name":"yt_pipeline_gold_dev"}'
```

Create a Glue crawler for `s3://YOUR-BRONZE-BUCKET/youtube/raw_statistics/` using the standard JSON classifier. Configure its resulting table name as `raw_statistics`. If you load historical CSV files, crawl `raw_historical_statistics/` into a separate table and run the Bronze-to-Silver job against that table; do not combine JSON and CSV in one crawler table.

The Silver and Gold jobs update the Glue Catalog themselves. The execution roles need the permissions represented by the templates under `iam_permission/`.

## Package and deploy Lambdas

`json_to_parquet` and `data_quality` use AWS SDK for pandas and Pandas, so packaging only `lambda_function.py` is insufficient.

```bash
./scripts/package_lambdas.sh
```

This creates:

```text
dist/youtube-api-ingestion.zip
dist/json-to-parquet.zip
dist/data-quality.zip
```

Deploy the functions with Python 3.11. Recommended settings are 300 seconds for ingestion, 512–1024 MB for the two Pandas functions, encryption for environment variables, and CloudWatch log retention.

### Ingestion environment

| Variable | Required | Purpose |
| --- | --- | --- |
| `YOUTUBE_API_KEY` | Yes | YouTube Data API v3 credential; inject it from Secrets Manager in production. |
| `S3_BUCKET_BRONZE` | Yes | Bronze bucket name. |
| `YOUTUBE_REGIONS` | No | Comma-separated region codes. |
| `SNS_ALERT_TOPIC_ARN` | No | SNS **topic** ARN, not a subscription ARN. |
| `FAIL_ON_PARTIAL_ERROR` | No | Defaults to `true`. |

### Reference transformation environment

| Variable | Required | Purpose |
| --- | --- | --- |
| `S3_BUCKET_SILVER` | Yes | Silver bucket name. |
| `GLUE_DB_SILVER` | No | Defaults to `yt_pipeline_silver_dev`. |
| `GLUE_TABLE_REFERENCE` | No | Defaults to `clean_reference_data`. |
| `SNS_ALERT_TOPIC_ARN` | No | Failure-notification topic. |

### Data-quality environment

| Variable | Required | Purpose |
| --- | --- | --- |
| `SNS_ALERT_TOPIC_ARN` | No | Failure-notification topic. |
| `ATHENA_WORKGROUP` | No | Defaults to `primary`; the workgroup must define a query-results location. |
| `DQ_MIN_ROW_COUNT` | No | Defaults to `10`. |
| `DQ_MAX_NULL_PERCENT` | No | Defaults to `5.0`. |

## Deploy Glue jobs

```bash
aws s3 cp glue_jobs/bronze_to_silver_statistics.py \
  s3://YOUR-SCRIPT-BUCKET/glue_jobs/bronze_to_silver_statistics.py
aws s3 cp glue_jobs/silver_to_gold_analytics.py \
  s3://YOUR-SCRIPT-BUCKET/glue_jobs/silver_to_gold_analytics.py
```

Create two Glue 4.0 jobs with job bookmarks enabled. The Bronze-to-Silver job requires these arguments:

```text
--bronze_database  yt_pipeline_bronze_dev
--bronze_table     raw_statistics
--silver_bucket    YOUR-SILVER-BUCKET
--silver_database  yt_pipeline_silver_dev
--silver_table     clean_statistics
```

The Silver-to-Gold job requires:

```text
--silver_database  yt_pipeline_silver_dev
--gold_bucket      YOUR-GOLD-BUCKET
--gold_database    yt_pipeline_gold_dev
```

The Glue role needs `s3:DeleteObject` on the Gold bucket because each Gold table is rebuilt as a full snapshot.

## Render and deploy Step Functions

Copy the example configuration and replace every example value:

```bash
cp step_functions/config.example.json step_functions/config.dev.json
python scripts/render_state_machine.py \
  --template step_functions/pipeline_orchestration.template.json \
  --config step_functions/config.dev.json \
  --output step_functions/pipeline-orchestration.json
```

Create or update the state machine using the rendered definition. Never commit `config.dev.json` if it contains internal account identifiers.

## Optional historical backfill

From the `data/` directory:

```bash
../scripts/aws_copy.sh YOUR-BRONZE-BUCKET
```

Historical CSV data is stored under `raw_historical_statistics/`, separate from live JSON Lines. Create a separate catalog table and run the same normalization job with that table name.

## Local validation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m compileall lambdas data_quality glue_jobs scripts
ruff check lambdas data_quality scripts tests
pytest -q
```

AWS Glue modules are supplied by the managed Glue runtime, so the local test suite focuses on portable logic and configuration. Validate Spark changes in an AWS Glue development endpoint or the official Glue container before production deployment.

## Example Athena query

```sql
SELECT channel_title, total_views, times_trending, rank_in_region
FROM yt_pipeline_gold_dev.channel_analytics
WHERE region = 'us'
ORDER BY rank_in_region
LIMIT 10;
```

## Security notes

- Do not commit API keys, account-specific ARNs, generated state-machine definitions, or AWS credentials.
- Prefer Secrets Manager or encrypted configuration for the YouTube API key.
- Apply least-privilege IAM and restrict the template bucket wildcard before deployment.
- Enable S3 encryption, versioning, public-access blocks, and lifecycle policies.
- Send CloudTrail and CloudWatch logs to retained, access-controlled destinations.

## Current limitations

- The project supplies portable deployment templates and commands, not a one-command Terraform/CDK stack.
- The data-quality Lambda samples up to 10,000 Athena rows; large production datasets should use SQL aggregate checks or a dedicated data-quality service.
- Gold refreshes are full snapshots. For larger datasets, replace them with transactional incremental tables such as Apache Iceberg.
