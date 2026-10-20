#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <bronze-bucket-name>" >&2
  exit 2
fi

bronze_bucket="$1"
regions=(CA DE FR GB IN JP KR MX RU US)

for region in "${regions[@]}"; do
  normalized_region="${region,,}"
  if [[ -f "${region}videos.csv" ]]; then
    aws s3 cp "${region}videos.csv" \
      "s3://${bronze_bucket}/youtube/raw_historical_statistics/region=${normalized_region}/${region}videos.csv"
  fi
  aws s3 cp "${region}_category_id.json" \
    "s3://${bronze_bucket}/youtube/raw_statistics_reference_data/region=${normalized_region}/${region}_category_id.json"
done
