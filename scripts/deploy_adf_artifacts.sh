#!/usr/bin/env bash
set -euo pipefail

RESOURCE_GROUP="${RESOURCE_GROUP:-rg-ealh-dev-canadacentral}"
DATA_FACTORY_NAME="${DATA_FACTORY_NAME:-adf-ealh-dev-canadacentral}"

TMP_DIR=".adf_deploy_tmp"
mkdir -p "$TMP_DIR"

extract_properties() {
  local input_file="$1"
  local output_file="$2"

  python -c "import json, sys; data=json.load(open(sys.argv[1], encoding='utf-8')); json.dump(data['properties'], open(sys.argv[2], 'w', encoding='utf-8'), indent=2)" "$input_file" "$output_file"
}
deploy_with_properties() {
  local artifact_type="$1"
  local cli_type="$2"
  local source_dir="adf/${artifact_type}"

  if [ ! -d "$source_dir" ]; then
    echo "Skipping ${artifact_type}: directory not found"
    return
  fi

  for file in "$source_dir"/*.json; do
    [ -e "$file" ] || continue

    local name
    name="$(basename "$file" .json)"
    local properties_file="${TMP_DIR}/${artifact_type}_${name}.json"

    extract_properties "$file" "$properties_file"

    echo "Deploying ${artifact_type}: ${name}"

    az datafactory "$cli_type" create \
      --resource-group "$RESOURCE_GROUP" \
      --factory-name "$DATA_FACTORY_NAME" \
      --name "$name" \
      --properties @"$properties_file"
  done
}

deploy_pipelines() {
  local source_dir="adf/pipelines"

  if [ ! -d "$source_dir" ]; then
    echo "Skipping pipelines: directory not found"
    return
  fi

  for file in "$source_dir"/*.json; do
    [ -e "$file" ] || continue

    local name
    name="$(basename "$file" .json)"
    local pipeline_file="${TMP_DIR}/pipeline_${name}.json"

    extract_properties "$file" "$pipeline_file"

    echo "Deploying pipelines: ${name}"

    az datafactory pipeline create \
      --resource-group "$RESOURCE_GROUP" \
      --factory-name "$DATA_FACTORY_NAME" \
      --name "$name" \
      --pipeline @"$pipeline_file"
  done
}

deploy_with_properties "linkedServices" "linked-service"
deploy_with_properties "datasets" "dataset"
deploy_pipelines

rm -rf "$TMP_DIR"

echo "ADF artifact deployment complete."