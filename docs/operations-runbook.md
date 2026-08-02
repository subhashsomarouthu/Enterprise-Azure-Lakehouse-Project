# Enterprise Azure Lakehouse Operations Runbook

## Pipeline Flow

```text
Azure SQL sales schema
  -> ADF incremental pipeline
  -> ADLS raw container
  -> Databricks Bronze ingestion
  -> Bronze DQ checks and quarantine
  -> Databricks Silver merge
  -> Databricks Gold star schema and aggregations
  -> BI / analytics consumers
```

## Main Azure Resources

```text
Resource group: rg-ealh-dev-canadacentral
ADF: adf-ealh-dev-canadacentral
ADLS: stsubhashealhdev001
Key Vault: kv-subhash-ealh-dev
Databricks workspace: dbw-ealh-dev-canadacentral
Azure SQL DB: sqldb-ealh-dev
```

## Main Data Objects

```text
Raw files:
abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/<entity>/load_date=<date>/batch_id=<batch_id>/

Bronze tables:
ealh_dev.bronze.customers
ealh_dev.bronze.products
ealh_dev.bronze.orders
ealh_dev.bronze.order_items
ealh_dev.bronze.payments

Validated Bronze tables:
ealh_dev.bronze.customers_validated
ealh_dev.bronze.products_validated
ealh_dev.bronze.orders_validated
ealh_dev.bronze.order_items_validated
ealh_dev.bronze.payments_validated

Silver tables:
ealh_dev.silver.customers
ealh_dev.silver.products
ealh_dev.silver.orders
ealh_dev.silver.order_items
ealh_dev.silver.payments

Gold tables:
ealh_dev.gold.dim_customer
ealh_dev.gold.dim_product
ealh_dev.gold.dim_date
ealh_dev.gold.fact_orders
ealh_dev.gold.fact_order_items
ealh_dev.gold.fact_payments
ealh_dev.gold.daily_sales_summary
ealh_dev.gold.product_sales_summary
ealh_dev.gold.customer_order_summary
```

## Trigger Full Parent Pipeline

Use a new batch id for every normal run.

```bash
CURRENT_WATERMARK=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

az datafactory pipeline create-run \
  --resource-group rg-ealh-dev-canadacentral \
  --factory-name adf-ealh-dev-canadacentral \
  --name pl_ingest_raw_then_run_lakehouse_job \
  --parameters "{
    \"batchId\": \"incremental_YYYYMMDD_001\",
    \"loadDate\": \"YYYY-MM-DD\",
    \"currentWatermark\": \"$CURRENT_WATERMARK\"
  }"
```

## Check ADF Run Status

```bash
az datafactory pipeline-run show \
  --resource-group rg-ealh-dev-canadacentral \
  --factory-name adf-ealh-dev-canadacentral \
  --run-id "PASTE_RUN_ID" \
  --query "{status:status, runStart:runStart, runEnd:runEnd}" -o table
```

## Check Databricks Job

In Databricks:

```text
Jobs & Pipelines -> ealh-dev-lakehouse-job -> Runs
```

All tasks should be green:

```text
bronze_ingestion
bronze_dq_checks
silver_transform
gold_aggregations
```

## Validate Raw Batch Counts

```python
storage_account = "stsubhashealhdev001"
source_system = "azuresql_sales"
load_date = "YYYY-MM-DD"
batch_id = "incremental_YYYYMMDD_001"

for entity in ["customers", "products", "orders", "order_items", "payments"]:
    path = f"abfss://raw@{storage_account}.dfs.core.windows.net/{source_system}/{entity}/load_date={load_date}/batch_id={batch_id}/"
    try:
        print(entity, spark.read.parquet(path).count())
    except Exception as e:
        print(entity, "missing or unreadable", str(e))
```

## Validate Bronze Batch Counts

```sql
SELECT _source_entity, _batch_id, COUNT(*) AS records
FROM ealh_dev.bronze.orders
WHERE _batch_id = 'incremental_YYYYMMDD_001'
GROUP BY _source_entity, _batch_id;
```

## Validate Silver Duplicates

```sql
SELECT order_id, COUNT(*) AS records
FROM ealh_dev.silver.orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Expected result: no rows.

## Check Gold Tables

```sql
SELECT *
FROM ealh_dev.gold.daily_sales_summary
ORDER BY order_date DESC
LIMIT 10;

SELECT *
FROM ealh_dev.gold.product_sales_summary
ORDER BY sales_amount DESC
LIMIT 10;

SELECT *
FROM ealh_dev.gold.customer_order_summary
ORDER BY lifetime_order_amount DESC
LIMIT 10;
```

## Check Data Quality Results

```sql
SELECT *
FROM ealh_dev.audit.data_quality_results
ORDER BY check_timestamp DESC
LIMIT 50;
```

## Check Quarantine Records

```sql
SELECT *
FROM ealh_dev.quarantine.rejected_records
ORDER BY rejected_at DESC
LIMIT 50;
```

## Check Schema Drift

```sql
SELECT *
FROM ealh_dev.audit.schema_drift_log
ORDER BY detected_at DESC
LIMIT 50;
```

## Check Source Watermarks

Run in Azure SQL:

```sql
SELECT source_table, last_watermark_value
FROM metadata.adf_watermark_tracking
ORDER BY source_table;
```

## Watermark Rule

Watermarks advance only after:

```text
ADF raw ingestion succeeds
Databricks Bronze/DQ/Silver/Gold job succeeds
Update_Watermarks activity succeeds
```

This prevents data loss when downstream processing fails.

## Common Failures

### Pipeline Not Found

Check deployed ADF pipelines:

```bash
az datafactory pipeline list \
  --resource-group rg-ealh-dev-canadacentral \
  --factory-name adf-ealh-dev-canadacentral \
  --query "[].name" -o table
```

Redeploy artifacts:

```bash
./scripts/deploy_adf_artifacts.sh
```

### Key Vault Forbidden

ADF managed identity needs:

```text
Key Vault Secrets User
```

Your user needs:

```text
Key Vault Secrets Officer
```

### ADLS Access Failed From ADF

ADF managed identity needs:

```text
Storage Blob Data Contributor
```

### ADLS Access Failed From Databricks

Databricks Unity Catalog access connector needs:

```text
Storage Blob Data Contributor
```

### Bronze Finds No Files

Check that `load_date` and `batch_id` match the ADF raw path exactly.

### Silver Finds No Validated Records

Run DQ checks for the same `batch_id` before Silver.

### Duplicate Bronze Batch

Bronze is batch-idempotent. Rerunning the same batch deletes that `_batch_id` first, then appends again.

### Duplicate Silver Business Keys

Silver should merge by primary key. Check the merge condition and source deduplication by `updated_at`.