#!/usr/bin/env bash
set -euo pipefail

RESOURCE_GROUP="rg-ealh-dev-canadacentral"
DATA_FACTORY="adf-ealh-dev-canadacentral"
TMP_DIR=".adf_deploy_tmp"

mkdir -p "$TMP_DIR"

extract_properties() {
  local input_file="$1"
  local output_file="$2"

  python -c "import json,sys; data=json.load(open(sys.argv[1])); json.dump(data['properties'], open(sys.argv[2], 'w'), indent=2)" "$input_file" "$output_file"
}

extract_properties adf/linkedServices/ls_keyvault.json "$TMP_DIR/ls_keyvault.properties.json"
extract_properties adf/linkedServices/ls_adls_lake.json "$TMP_DIR/ls_adls_lake.properties.json"
extract_properties adf/linkedServices/ls_azuresql_sales.json "$TMP_DIR/ls_azuresql_sales.properties.json"

extract_properties adf/datasets/ds_azuresql_table.json "$TMP_DIR/ds_azuresql_table.properties.json"
extract_properties adf/datasets/ds_adls_parquet_raw.json "$TMP_DIR/ds_adls_parquet_raw.properties.json"

extract_properties adf/pipelines/pl_ingest_azuresql_to_raw.json "$TMP_DIR/pl_ingest_azuresql_to_raw.properties.json"
extract_properties adf/pipelines/pl_ingest_azuresql_incremental_to_raw.json "$TMP_DIR/pl_ingest_azuresql_incremental_to_raw.properties.json"
echo "Deploying ADF linked services..."
az datafactory linked-service create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --linked-service-name "ls_keyvault" \
  --properties @"$TMP_DIR/ls_keyvault.properties.json"

az datafactory linked-service create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --linked-service-name "ls_adls_lake" \
  --properties @"$TMP_DIR/ls_adls_lake.properties.json"

az datafactory linked-service create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --linked-service-name "ls_azuresql_sales" \
  --properties @"$TMP_DIR/ls_azuresql_sales.properties.json"

echo "Deploying ADF datasets..."
az datafactory dataset create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --dataset-name "ds_azuresql_table" \
  --properties @"$TMP_DIR/ds_azuresql_table.properties.json"

az datafactory dataset create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --dataset-name "ds_adls_parquet_raw" \
  --properties @"$TMP_DIR/ds_adls_parquet_raw.properties.json"

echo "Deploying ADF pipeline..."
az datafactory pipeline create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --pipeline-name "pl_ingest_azuresql_to_raw" \
  --pipeline @"$TMP_DIR/pl_ingest_azuresql_to_raw.properties.json"

az datafactory pipeline create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY" \
  --pipeline-name "pl_ingest_azuresql_incremental_to_raw" \
  --pipeline @"$TMP_DIR/pl_ingest_azuresql_incremental_to_raw.properties.json"

echo "ADF artifact deployment complete."
