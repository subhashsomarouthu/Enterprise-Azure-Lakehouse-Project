# ADF Ingestion Design

## Purpose

Azure Data Factory ingests source data from Azure SQL DB into ADLS Gen2 raw storage.

## Source

Azure SQL Database:

- sales.customers
- sales.products
- sales.orders
- sales.order_items
- sales.payments

## Target Raw Layout

abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/{entity}/load_date=yyyy-mm-dd/batch_id=yyyyMMddHHmmss/

Example:

abfss://raw@stsubhashealhdev001.dfs.core.windows.net/azuresql_sales/orders/load_date=2026-07-31/batch_id=20260731100000/

## Ingestion Pattern

Initial load:

- Copy all rows from source table to raw.

Incremental load:

- Use updated_at watermark.
- Copy records where updated_at > last_watermark and updated_at <= current_watermark.
- Update control watermark after successful Bronze ingestion.

## Why ADF

ADF handles source connectivity and movement into raw storage.
Databricks handles raw-to-Bronze/Silver/Gold processing.

## Real Company Notes

ADF linked services store connection details.
ADF datasets represent source/sink structures.
ADF pipelines use ForEach to loop over metadata-driven table configs.
Watermarks are stored in control tables.
