# Enterprise Azure Lakehouse Project

Production-style Azure data engineering project that ingests transactional data from Azure SQL into ADLS Gen2, processes it through a Databricks Medallion architecture, validates data quality, handles schema drift, and deploys ADF plus Databricks artifacts through GitHub Actions.

## Architecture

```text
Azure SQL OLTP source
  -> Azure Data Factory incremental extraction
  -> ADLS Gen2 raw zone
  -> Databricks Bronze Delta tables
  -> Bronze DQ checks and quarantine
  -> Databricks Silver Delta tables
  -> Silver DQ checks
  -> Databricks Gold aggregate tables
  -> Gold DQ checks
```

ADF is the top-level orchestrator. It extracts source data to raw ADLS and then triggers the Databricks Asset Bundle managed job. Databricks handles Bronze, DQ, Silver, Gold, and final validation.

## What This Project Demonstrates

- Terraform-based Azure infrastructure
- Azure SQL OLTP source schema and generated source data
- Metadata-driven ADF ingestion
- Incremental extraction using watermarks
- ADLS raw landing path design
- Unity Catalog storage credentials, external locations, schemas, and tables
- Bronze, Silver, and Gold Delta Lake tables
- Batch-idempotent Bronze ingestion
- Schema drift detection and logging in Bronze
- Bronze DQ with validated tables and quarantine support
- Silver MERGE/upsert processing by business key
- Silver DQ gates before Gold processing
- Gold star schema, SCD Type 2 customer dimension, business aggregates, and Gold DQ checks
- Databricks Asset Bundles for job deployment
- GitHub Actions CI and manual dev deployment`n- Disabled ADF schedule trigger for controlled scheduled orchestration

## Main Azure Resources

```text
Resource group: rg-ealh-dev-canadacentral
Azure Data Factory: adf-ealh-dev-canadacentral
Storage account: stsubhashealhdev001
Key Vault: kv-subhash-ealh-dev
Databricks workspace: dbw-ealh-dev-canadacentral
Azure SQL Database: sqldb-ealh-dev
```

## Repository Structure

```text
adf/                         ADF linked services, datasets, pipelines, triggers, config
config/                      Environment config files
databricks/notebooks/        Bronze, Silver, Gold, setup, and quality notebooks
databricks/workflows/        Databricks job definition for Asset Bundles
docs/                        Architecture, security, ingestion design, runbook
infra/terraform/             Azure infrastructure as code
scripts/                     Deployment helper scripts
sql/source/                  Azure SQL source schema, seed, metadata, procs
src/ealh/source_generators/  Python source data generator
.github/workflows/           CI and manual dev deployment workflows
```

## Data Flow

ADF reads active source table metadata from Azure SQL:

```text
metadata.adf_ingestion_tables
metadata.adf_watermark_tracking
```

It extracts records using this watermark pattern:

```sql
updated_at > last_watermark_value
AND updated_at <= currentWatermark
```

Raw files are written to ADLS as Parquet:

```text
raw/azuresql_sales/<entity>/load_date=<date>/batch_id=<batch_id>/
```

After raw ingestion succeeds, ADF triggers the Databricks lakehouse job:

```text
[dev subhashsomarouthu2000] ealh-dev-lakehouse-job
```

The Databricks job runs these tasks:

```text
bronze_ingestion
  -> bronze_dq_checks
    -> silver_transform
      -> silver_dq_checks
        -> gold_star_schema
          -> gold_aggregations
            -> gold_dq_checks
```

Watermarks are updated only after the Databricks job succeeds.

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

Main table patterns:

```text
ealh_dev.bronze.<entity>
ealh_dev.bronze.<entity>_validated
ealh_dev.silver.<entity>
ealh_dev.gold.<dimension_or_fact_or_aggregate_table>
ealh_dev.audit.data_quality_results
ealh_dev.audit.schema_drift_log
ealh_dev.quarantine.rejected_records
```

## Medallion Layers

Bronze stores raw source records in Delta format with technical metadata:

```text
_source_system_id
_source_entity
_load_date
_batch_id
_source_file_path
_bronze_loaded_at
_record_hash
_primary_key
```

Bronze is batch-idempotent: rerunning the same batch deletes existing rows for that `_batch_id` and reloads the batch.

Silver stores clean current-state business records. It reads Bronze validated tables, deduplicates by primary key, and uses Delta MERGE to upsert records.

Gold stores both dimensional tables and business-ready aggregate tables:

```text
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

## Data Quality

Bronze DQ validates raw ingested data and writes valid rows into `<entity>_validated` tables. Failed records are written to quarantine.

Silver DQ checks current-state table quality, including uniqueness, not-null rules, and numeric validity. Silver DQ gates Gold processing.

Gold DQ checks dimensional and reporting-table sanity, including unique keys, SCD2 current-version rules, non-null keys/dates, and non-negative metrics.

DQ results are written to:

```text
ealh_dev.audit.data_quality_results
```

## Schema Drift

Bronze detects schema drift by comparing incoming raw batch columns with the existing Bronze Delta table schema.

Current policy:

```text
Added columns: allow and log
Missing columns in older batches: allow and log
Type changes: fail
```

Schema drift events are written to:

```text
ealh_dev.audit.schema_drift_log
```

Silver and Gold do not blindly expose every new Bronze column. New business columns should be reviewed before being propagated to curated/reporting layers.

## Infrastructure

Terraform lives under:

```text
infra/terraform/environments/dev
```

Common local commands:

```bash
terraform fmt -recursive
terraform init
terraform validate
terraform plan -out=tfplan
terraform apply "tfplan"
```

Terraform creates and manages Azure infrastructure. It is validated in CI, but this project does not auto-apply Terraform from GitHub Actions.

## ADF Deployment

ADF artifacts are stored as JSON under:

```text
adf/linkedServices
adf/datasets
adf/pipelines`nadf/triggers
```

Deploy locally:

```bash
bash scripts/deploy_adf_artifacts.sh
```

The deploy script deploys all linked services, datasets, pipelines, and triggers from the repo folders.

## Databricks Compute

The Databricks job uses serverless job compute. There is no classic cluster or node-size configuration in this project. Databricks manages compute provisioning, scaling, runtime selection, and termination for the job tasks.

## Databricks Deployment

Databricks Asset Bundle config:

```text
databricks.yml
databricks/workflows/ealh_dev_lakehouse_job.yml
```

Validate locally:

```bash
databricks bundle validate -t dev
```

Deploy locally:

```bash
databricks bundle deploy -t dev
```

The bundle deploys the Databricks lakehouse job and notebook files to the dev workspace.

## CI/CD

CI workflow:

```text
.github/workflows/ci.yml
```

Runs on pull requests to `main`:

```text
terraform fmt
terraform validate
databricks bundle validate
```

Manual dev deployment workflow:

```text
.github/workflows/deploy-dev.yml
```

Runs from GitHub Actions with manual trigger:

```text
Actions -> Deploy Dev -> Run workflow
```

It deploys:

```text
ADF artifacts
Databricks Asset Bundle
```

Required GitHub secrets:

```text
AZURE_CREDENTIALS
DATABRICKS_HOST
DATABRICKS_TOKEN
```

## Run End-To-End Pipeline

Create a source change in Azure SQL:

```sql
UPDATE TOP (1) sales.orders
SET
    total_amount = total_amount + 1,
    updated_at = SYSUTCDATETIME()
WHERE order_id >= 530;
```

Trigger the ADF parent pipeline:

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

## Scheduled Runs

The repo includes a daily ADF schedule trigger:

```text
adf/triggers/trg_daily_lakehouse_dev.json
```

The trigger is deployed in a stopped state for cost control:

```text
trg_daily_lakehouse_dev
runtimeState: Stopped
schedule: daily at 02:00 UTC
```

If started, it runs the parent pipeline with dynamic parameters:

```text
batchId = scheduled_<trigger scheduled timestamp>
loadDate = trigger scheduled date
currentWatermark = trigger scheduled timestamp
```

Keep it stopped in dev unless a scheduled run is intentionally needed.

## Monitoring

ADF run monitoring:

```text
Azure Data Factory -> Monitor -> pl_ingest_raw_then_run_lakehouse_job
```

Databricks job monitoring:

```text
Databricks -> Jobs & Pipelines -> [dev subhashsomarouthu2000] ealh-dev-lakehouse-job
```

Useful audit queries:

```sql
SELECT *
FROM ealh_dev.audit.data_quality_results
ORDER BY check_timestamp DESC
LIMIT 50;

SELECT *
FROM ealh_dev.audit.schema_drift_log
ORDER BY detected_at DESC
LIMIT 50;
```

Gold validation query:

```sql
SELECT *
FROM ealh_dev.gold.daily_sales_summary
ORDER BY order_date DESC
LIMIT 10;
```

## Real Company Ownership Model

Platform or cloud engineers usually own base infrastructure, networking, security policies, shared Terraform modules, CI/CD service connections, and permission guardrails.

Data engineers usually own source ingestion logic, ADF pipelines, Databricks notebooks/jobs, DQ rules, schema drift handling, Silver transformations, Gold data products, audit logging, and operational runbooks.

Analytics engineers or BI engineers usually consume Gold tables and own semantic models, dashboards, KPI definitions, and reporting contracts.

## Current Limitations

This is a dev learning project, not a hardened production deployment. Known follow-ups:

```text
Add raw retry cleanup or temp-folder promotion
Add alerting for ADF and Databricks failures
Add freshness checks and SLA monitoring
Add automated smoke tests after deployment
Move production auth from PAT to service principal or workload identity
Add Terraform remote state and controlled apply workflow
Add environment-specific deployment promotion for qa/prod
```
