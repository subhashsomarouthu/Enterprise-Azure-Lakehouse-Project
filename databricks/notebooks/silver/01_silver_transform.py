# Databricks notebook source
# Enterprise Azure Lakehouse - Silver Transform
# Bronze validated -> Silver current-state Delta tables

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "ealh_dev")
dbutils.widgets.text("storage_account", "stsubhashealhdev001")
dbutils.widgets.text("source_system", "azuresql_sales")
dbutils.widgets.text("batch_id", "")

CATALOG = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
SOURCE_SYSTEM = dbutils.widgets.get("source_system")
BATCH_ID = dbutils.widgets.get("batch_id").strip()

if not BATCH_ID:
    raise ValueError("batch_id is required")

#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

tables = [
    {"entity": "customers", "pk": "customer_id"},
    {"entity": "products", "pk": "product_id"},
    {"entity": "orders", "pk": "order_id"},
    {"entity": "order_items", "pk": "order_item_id"},
    {"entity": "payments", "pk": "payment_id"},
]

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def silver_path(entity: str) -> str:
    return f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SOURCE_SYSTEM}/{entity}"

def transform_for_silver(df, entity: str, pk: str):
    business_cols = [c for c in df.columns if not c.startswith("_")]

    cleaned = (
        df
        .filter(F.col(pk).isNotNull())
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_batch_id", F.lit(BATCH_ID))
        .withColumn("_silver_source_entity", F.lit(entity))
    )

    window_spec = Window.partitionBy(pk).orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("_bronze_loaded_at").desc_nulls_last()
    )

    deduped = (
        cleaned
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return deduped

for table in tables:
    entity = table["entity"]
    pk = table["pk"]

    source_table = f"{CATALOG}.bronze.{entity}_validated"
    target_table = f"{CATALOG}.silver.{entity}"
    target_path = silver_path(entity)

    if not table_exists(source_table):
        raise ValueError(f"Missing validated Bronze table: {source_table}. Run Bronze DQ first.")

    batch_df = (
        spark.table(source_table)
        .filter(F.col("_batch_id") == BATCH_ID)
    )

    source_count = batch_df.count()

    if source_count == 0:
        print(f"No records found for {source_table}, batch_id={BATCH_ID}")
        continue

    silver_df = transform_for_silver(batch_df, entity, pk)

    if not table_exists(target_table):
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("path", target_path)
            .saveAsTable(target_table)
        )

        print(f"Created {target_table} with {silver_df.count()} records")
    else:
        delta_target = DeltaTable.forName(spark, target_table)

        merge_condition = f"target.{pk} = source.{pk}"

        (
            delta_target.alias("target")
            .merge(silver_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll(
                condition="source.updated_at >= target.updated_at"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Merged {silver_df.count()} records into {target_table}")

print("Silver transform complete.")