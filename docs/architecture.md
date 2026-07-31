# Enterprise Azure Lakehouse Architecture

## Project Goal

Build a real-company-style Azure data engineering platform using Azure SQL DB, Azure Data Factory, ADLS Gen2, Azure Databricks, Unity Catalog, Delta Lake, Delta Live Tables, Airflow, and CI/CD.

## Target Flow

Azure SQL DB -> Azure Data Factory -> ADLS Gen2 Raw -> Databricks Bronze -> Silver -> Gold -> BI/Dashboard

## Azure Resources

| Resource | Name |
|---|---|
| Resource Group | rg-ealh-dev-canadacentral |
| Storage Account | stealhdev001 |
| Data Factory | adf-ealh-dev-canadacentral |
| Databricks Workspace | dbw-ealh-dev-canadacentral |
| Key Vault | kv-ealh-dev-001 |
| SQL Server | sql-ealh-dev-001 |
| SQL Database | sqldb-ealh-dev |

## ADLS Containers

- raw
- bronze
- silver
- gold
- checkpoint
- landing
- archive
- quarantine

## Raw Path Standard

/raw/source_system/entity/load_date=yyyy-mm-dd/batch_id=yyyyMMdd_HHmmss/

Example:

/raw/azure_sql/orders/load_date=2026-07-31/batch_id=20260731_010000/

## Unity Catalog Standard

Catalog:

ealh_dev

Schemas:

- bronze
- silver
- gold
- audit
- config

## Pipeline Layers

### Raw

Stores source-extracted files as landed from source systems.

### Bronze

Source-shaped Delta tables with ingestion metadata.

### Silver

Cleaned, deduplicated, conformed business entities.

### Gold

Dimensional model, facts, dimensions, KPIs, and reporting aggregates.

## Orchestration

ADF handles source-to-raw ingestion.
Databricks handles raw-to-Bronze-to-Silver-to-Gold processing.
Airflow can orchestrate ADF and Databricks end-to-end.

## CI/CD

Azure DevOps validates code, tests notebooks, deploys Databricks Asset Bundles, and promotes changes across dev, qa, and prod.
