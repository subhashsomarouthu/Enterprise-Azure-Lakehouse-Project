# Databricks notebook source
# Enterprise Azure Lakehouse - Bronze Ingestion
# Reads ADF-landed Parquet files from ADLS raw and writes Bronze Delta tables.
# Includes schema drift detection and audit logging.

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2

dbutils.widgets.text("catalog_name", "ealh_dev")
dbutils.widgets.text("source_system_id", "azuresql_sales")
dbutils.widgets.text("load_date", "2026-08-01")
dbutils.widgets.text("batch_id", "manual_20260801_001")
dbutils.widgets.text("storage_account", "stsubhashealhdev001")
dbutils.widgets.dropdown("fail_on_type_change", "true", ["true", "false"])
dbutils.widgets.dropdown("fail_on_removed_column", "true", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog_name")
SOURCE_SYSTEM_ID = dbutils.widgets.get("source_system_id")
LOAD_DATE = dbutils.widgets.get("load_date")
BATCH_ID = dbutils.widgets.get("batch_id")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
FAIL_ON_TYPE_CHANGE = dbutils.widgets.get("fail_on_type_change").lower() == "true"
FAIL_ON_REMOVED_COLUMN = dbutils.widgets.get("fail_on_removed_column").lower() == "false"

RAW_BASE = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"

TABLES = [
    {"entity": "customers", "primary_key": "customer_id"},
    {"entity": "products", "primary_key": "product_id"},
    {"entity": "orders", "primary_key": "order_id"},
    {"entity": "order_items", "primary_key": "order_item_id"},
    {"entity": "payments", "primary_key": "payment_id"},
]

TECHNICAL_COLUMNS = {
    "_source_system_id",
    "_source_entity",
    "_load_date",
    "_batch_id",
    "_source_file_path",
    "_bronze_loaded_at",
    "_record_hash",
    "_primary_key",
}

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.audit")


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def business_schema(schema):
    return {
        field.name: field.dataType.simpleString()
        for field in schema.fields
        if field.name not in TECHNICAL_COLUMNS
    }


def detect_schema_drift(entity, incoming_df, target_table, primary_key):
    incoming = business_schema(incoming_df.schema)

    if not table_exists(target_table):
        print(f"{target_table} does not exist. Treating this as initial schema registration.")
        return []

    existing_df = spark.table(target_table)
    existing = business_schema(existing_df.schema)

    added_columns = sorted(set(incoming) - set(existing))
    removed_columns = sorted(set(existing) - set(incoming))
    common_columns = sorted(set(incoming).intersection(set(existing)))

    drift_rows = []

    for column_name in added_columns:
        drift_rows.append(
            (
                str(uuid4()),
                SOURCE_SYSTEM_ID,
                entity,
                "added_column",
                column_name,
                None,
                incoming[column_name],
                "info",
                "auto_accepted",
                datetime.now(timezone.utc),
                None,
                None,
                "New source column detected and allowed in Bronze.",
            )
        )

    for column_name in removed_columns:
        severity = "critical" if column_name == primary_key else "warning"
        drift_rows.append(
            (
                str(uuid4()),
                SOURCE_SYSTEM_ID,
                entity,
                "removed_column",
                column_name,
                existing[column_name],
                None,
                severity,
                "pending_review",
                datetime.now(timezone.utc),
                None,
                None,
                "Column exists in Bronze but is missing from incoming raw batch.",
            )
        )

    for column_name in common_columns:
        if incoming[column_name] != existing[column_name]:
            drift_rows.append(
                (
                    str(uuid4()),
                    SOURCE_SYSTEM_ID,
                    entity,
                    "type_change",
                    column_name,
                    existing[column_name],
                    incoming[column_name],
                    "critical",
                    "pending_review",
                    datetime.now(timezone.utc),
                    None,
                    None,
                    "Incoming data type differs from existing Bronze table type.",
                )
            )

    return drift_rows


def write_schema_drift(drift_rows):
    if not drift_rows:
        return

    schema = """
    drift_id STRING,
    source_system_id STRING,
    table_name STRING,
    drift_type STRING,
    column_name STRING,
    old_data_type STRING,
    new_data_type STRING,
    severity STRING,
    status STRING,
    detected_at TIMESTAMP,
    reviewed_by STRING,
    reviewed_at TIMESTAMP,
    notes STRING
    """

    (
        spark.createDataFrame(drift_rows, schema)
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.audit.schema_drift_log")
    )


def enforce_schema_policy(drift_rows):
    type_changes = [row for row in drift_rows if row[3] == "type_change"]
    removed_columns = [row for row in drift_rows if row[3] == "removed_column"]

    if FAIL_ON_TYPE_CHANGE and type_changes:
        details = [(row[4], row[5], row[6]) for row in type_changes]
        raise ValueError(f"Schema drift policy violation: type changes detected: {details}")

    if FAIL_ON_REMOVED_COLUMN and removed_columns:
        details = [(row[4], row[5], row[6]) for row in removed_columns]
        raise ValueError(f"Schema drift policy violation: removed columns detected: {details}")


def add_bronze_metadata(df, entity, primary_key):
    source_cols = df.columns

    return (
        df.withColumn("_source_system_id", lit(SOURCE_SYSTEM_ID))
        .withColumn("_source_entity", lit(entity))
        .withColumn("_load_date", lit(LOAD_DATE))
        .withColumn("_batch_id", lit(BATCH_ID))
        .withColumn("_source_file_path", col("_metadata.file_path"))
        .withColumn("_bronze_loaded_at", current_timestamp())
        .withColumn(
            "_record_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in source_cols]), 256),
        )
        .withColumn("_primary_key", col(primary_key).cast("string"))
    )


def ingest_entity(entity, primary_key):
    source_path = f"{RAW_BASE}/{SOURCE_SYSTEM_ID}/{entity}/load_date={LOAD_DATE}/batch_id={BATCH_ID}"
    target_table = f"{CATALOG}.bronze.{entity}"
    target_path = f"{BRONZE_BASE}/{SOURCE_SYSTEM_ID}/{entity}"

    raw_df = spark.read.format("parquet").load(source_path)

    if primary_key not in raw_df.columns:
        raise ValueError(f"Primary key {primary_key} is missing from incoming raw data for {entity}")

    drift_rows = detect_schema_drift(entity, raw_df, target_table, primary_key)
    write_schema_drift(drift_rows)
    enforce_schema_policy(drift_rows)

    bronze_df = add_bronze_metadata(raw_df, entity, primary_key)

    if table_exists(target_table):
        spark.sql(f"DELETE FROM {target_table} WHERE _batch_id = '{BATCH_ID}'")

        (
            bronze_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
    else:
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
        "schema_drift_count": len(drift_rows),
    }


results = []
for table in TABLES:
    results.append(ingest_entity(table["entity"], table["primary_key"]))

audit_rows = [
    (
        f"{BATCH_ID}_bronze_{r['entity']}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
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

(
    spark.createDataFrame(audit_rows, audit_schema)
    .write.format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.audit.pipeline_run_log")
)

display(spark.createDataFrame(results))