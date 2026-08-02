# Enterprise Azure Lakehouse Architecture

## Project Goal

Build a production-style Azure lakehouse using Azure SQL Database, Azure Data Factory, ADLS Gen2, Azure Databricks, Unity Catalog, Delta Lake, Terraform, and GitHub Actions.

## Implemented Flow

```text
Azure SQL OLTP source
  -> ADF metadata-driven incremental ingestion
  -> ADLS Gen2 raw Parquet files
  -> Databricks Bronze Delta tables
  -> Bronze DQ and quarantine
  -> Databricks Silver current-state tables
  -> Silver DQ gate
  -> Gold dimensional model and aggregates
  -> Gold DQ checks
```

ADF is the top-level orchestrator. Databricks Workflow handles lakehouse task dependencies after ADF lands raw data.

## Azure Resources

| Resource | Name |
|---|---|
| Resource Group | rg-ealh-dev-canadacentral |
| Storage Account | stsubhashealhdev001 |
| Data Factory | adf-ealh-dev-canadacentral |
| Databricks Workspace | dbw-ealh-dev-canadacentral |
| Key Vault | kv-subhash-ealh-dev |
| SQL Server | sql-subhash-ealh-dev |
| SQL Database | sqldb-ealh-dev |

## ADLS Containers

```text
raw
bronze
silver
gold
checkpoint
landing
archive
quarantine
```

Raw stores non-Delta landed files. Bronze, Silver, and Gold store Delta tables at external ADLS locations governed through Unity Catalog.

## Raw Path Standard

```text
abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/<entity>/load_date=<yyyy-mm-dd>/batch_id=<batch_id>/
```

Example:

```text
abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/orders/load_date=2026-08-02/batch_id=incremental_20260802_005/
```

## Unity Catalog Layout

Catalog:

```text
ealh_dev
```

Schemas:

```text
config
control
audit
quarantine
bronze
silver
gold
```

Unity Catalog external locations map ADLS containers to governed storage locations for raw, bronze, silver, gold, checkpoint, and quarantine.

## Medallion Layers

### Raw

Raw is plain file storage in ADLS. It is not ACID. ADF lands source extracts as Parquet under source/entity/load_date/batch_id paths.

### Bronze

Bronze is the first Delta/ACID layer. It stores source-shaped records with technical metadata, schema drift logging, and batch-idempotent reload behavior.

### Silver

Silver stores cleaned current-state business entities. It reads Bronze validated tables and uses Delta MERGE by primary key.

### Gold

Gold contains both dimensional and aggregate serving tables:

```text
dim_customer       SCD Type 2 customer dimension
dim_product        Type 1 product dimension
dim_date           Date dimension
fact_orders        Order fact
fact_order_items   Order item fact
fact_payments      Payment fact
```

Gold also includes aggregate tables:

```text
daily_sales_summary
product_sales_summary
customer_order_summary
```

## Orchestration

```text
ADF parent pipeline
  -> ADF incremental raw ingestion child pipeline
  -> Databricks bundle-managed job
  -> Azure SQL watermark update after success
```

The Databricks job runs:

```text
bronze_ingestion
  -> bronze_dq_checks
    -> silver_transform
      -> silver_dq_checks
        -> gold_star_schema
          -> gold_aggregations
            -> gold_dq_checks
```

## Schedule Trigger

ADF trigger:

```text
trg_daily_lakehouse_dev
```

It is deployed in a stopped state for cost control. If enabled, it runs daily at 02:00 UTC and passes dynamic `batchId`, `loadDate`, and `currentWatermark` parameters to the parent pipeline.

## Databricks Compute

The bundle-managed Databricks job uses serverless job compute. No classic all-purpose cluster or fixed node type is defined in this project.

## CI/CD

GitHub Actions CI validates Terraform and Databricks bundle configuration on PRs.

Manual dev deployment deploys:

```text
ADF artifacts
Databricks Asset Bundle
```

Terraform is validated in CI but not automatically applied from GitHub Actions.