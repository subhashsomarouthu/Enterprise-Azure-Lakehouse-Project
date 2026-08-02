# ADF Ingestion Design

## Purpose

Azure Data Factory is the top-level orchestrator for the dev lakehouse pipeline. It extracts incremental source data from Azure SQL Database into ADLS Gen2 raw storage, triggers the Databricks lakehouse job, and updates source watermarks only after downstream processing succeeds.

## Source

Azure SQL Database source tables:

```text
sales.customers
sales.products
sales.orders
sales.order_items
sales.payments
```

ADF reads ingestion metadata from Azure SQL:

```text
metadata.adf_ingestion_tables
metadata.adf_watermark_tracking
```

## Target Raw Layout

```text
abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/<entity>/load_date=<yyyy-mm-dd>/batch_id=<batch_id>/
```

Example:

```text
abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/orders/load_date=2026-08-02/batch_id=incremental_20260802_005/
```

## Ingestion Pattern

Initial load copies full source tables to raw.

Incremental load uses the source `updated_at` watermark:

```sql
updated_at > last_watermark_value
AND updated_at <= currentWatermark
```

The parent pipeline receives:

```text
batchId
loadDate
currentWatermark
```

## Pipelines

```text
pl_ingest_azuresql_to_raw
```

Manual/full-style raw ingestion pipeline.

```text
pl_ingest_azuresql_incremental_to_raw
```

Metadata-driven incremental child pipeline. It looks up active source tables, loops through them, and copies changed rows to raw.

```text
pl_ingest_raw_then_run_lakehouse_job
```

Parent pipeline. It runs incremental raw ingestion, retrieves the Databricks token from Key Vault, triggers the Databricks bundle-managed job, and updates Azure SQL watermarks after success.

## Schedule Trigger

```text
trg_daily_lakehouse_dev
```

This trigger is deployed as `Stopped` to prevent automatic dev costs. If started, it runs daily at 02:00 UTC and passes dynamic parameters to the parent pipeline:

```text
batchId = scheduled_<scheduled timestamp>
loadDate = scheduled date
currentWatermark = scheduled timestamp
```

## Watermark Rule

Watermarks advance only after:

```text
ADF raw ingestion succeeds
Databricks Bronze/DQ/Silver/Gold/DQ job succeeds
Update_Watermarks activity succeeds
```

This avoids skipping source changes when downstream processing fails.

## Deployment

ADF artifacts are stored in Git:

```text
adf/linkedServices
adf/datasets
adf/pipelines
adf/triggers
```

Deploy locally:

```bash
bash scripts/deploy_adf_artifacts.sh
```

Deploy from GitHub:

```text
Actions -> Deploy Dev -> Run workflow
```

## Real Company Notes

ADF linked services store connection definitions, with secrets backed by Key Vault. ADF datasets define source and sink shapes. ADF pipelines use metadata-driven ForEach patterns for scalable ingestion. Watermarks usually live in control tables and are advanced only after the full downstream job succeeds.