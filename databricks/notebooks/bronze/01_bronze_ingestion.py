# Databricks notebook source
# Enterprise Azure Lakehouse - Bronze Ingestion
# Reads ADF-landed Parquet files from ADLS raw and writes Bronze Delta tables.

from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, sha2, concat_ws

dbutils.widgets.text("catalog_name", "ealh_dev")
dbutils.widgets.text("source_system_id", "azuresql_sales")
dbutils.widgets.text("load_date", "2026-08-01")
dbutils.widgets.text("batch_id", "manual_20260801_001")
dbutils.widgets.text("storage_account", "stsubhashealhdev001")

CATALOG = dbutils.widgets.get("catalog_name")
SOURCE_SYSTEM_ID = dbutils.widgets.get("source_system_id")
LOAD_DATE = dbutils.widgets.get("load_date")
BATCH_ID = dbutils.widgets.get("batch_id")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

RAW_BASE = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"

TABLES = [
    {"entity": "customers", "primary_key": "customer_id"},
    {"entity": "products", "primary_key": "product_id"},
    {"entity": "orders", "primary_key": "order_id"},
    {"entity": "order_items", "primary_key": "order_item_id"},
    {"entity": "payments", "primary_key": "payment_id"},
]

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.audit")


def add_bronze_metadata(df, entity, primary_key):
    source_cols = df.columns
    return (
        df.withColumn("_source_system_id", lit(SOURCE_SYSTEM_ID))
        .withColumn("_source_entity", lit(entity))
        .withColumn("_load_date", lit(LOAD_DATE))
        .withColumn("_batch_id", lit(BATCH_ID))
        .withColumn("_source_file_path", input_file_name())
        .withColumn("_bronze_loaded_at", current_timestamp())
        .withColumn("_record_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in source_cols]), 256))
        .withColumn("_primary_key", col(primary_key).cast("string"))
    )


def ingest_entity(entity, primary_key):
    source_path = f"{RAW_BASE}/{SOURCE_SYSTEM_ID}/{entity}/load_date={LOAD_DATE}/batch_id={BATCH_ID}"
    target_table = f"{CATALOG}.bronze.{entity}"
    target_path = f"{BRONZE_BASE}/{SOURCE_SYSTEM_ID}/{entity}"

    raw_df = spark.read.format("parquet").load(source_path)
    bronze_df = add_bronze_metadata(raw_df, entity, primary_key)

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", target_path)
        .saveAsTable(target_table)
    )

    count = bronze_df.count()
    print(f"Ingested {count} records into {target_table}")
    return {
        "entity": entity,
        "target_table": target_table,
        "source_path": source_path,
        "target_path": target_path,
        "records_written": count,
    }


results = []
for table in TABLES:
    results.append(ingest_entity(table["entity"], table["primary_key"]))

audit_rows = [
    (
        f"{BATCH_ID}_bronze_{r['entity']}",
        "bronze_ingestion",
        "bronze",
        "dev",
        "SUCCESS",
        datetime.now(timezone.utc),
        datetime.now(timezone.utc),
        None,
        r["records_written"],
        r["records_written"],
        None,
    )
    for r in results
]

audit_schema = """
run_id STRING,
pipeline_name STRING,
layer STRING,
environment STRING,
status STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
duration_seconds DOUBLE,
records_read BIGINT,
records_written BIGINT,
error_message STRING
"""

spark.createDataFrame(audit_rows, audit_schema).write.format("delta").mode("append").saveAsTable(
    f"{CATALOG}.audit.pipeline_run_log"
)

display(spark.createDataFrame(results))
