#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dist_dir="${repo_root}/dist"
mkdir -p "${dist_dir}"

package_function() {
  local source_dir="$1"
  local output_name="$2"
  local build_dir
  build_dir="$(mktemp -d)"
  trap 'rm -rf "${build_dir}"' RETURN

  if [[ -f "${source_dir}/requirements.txt" ]]; then
    python3 -m pip install \
      --requirement "${source_dir}/requirements.txt" \
      --target "${build_dir}" \
      --platform manylinux2014_x86_64 \
      --implementation cp \
      --python-version 3.11 \
      --only-binary=:all:
  fi
  cp "${source_dir}/lambda_function.py" "${build_dir}/lambda_function.py"
  (cd "${build_dir}" && zip -qr "${dist_dir}/${output_name}.zip" .)
}

package_function "${repo_root}/lambdas/youtube_api_ingestion" "youtube-api-ingestion"
package_function "${repo_root}/lambdas/json_to_parquet" "json-to-parquet"
package_function "${repo_root}/data_quality" "data-quality"

echo "Lambda deployment packages written to ${dist_dir}"
