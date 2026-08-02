# Databricks notebook source
# Enterprise Azure Lakehouse - Silver Data Quality Checks

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ealh_dev")
dbutils.widgets.text("batch_id", "")

dbutils.widgets.text("catalog", "ealh_dev")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("run_id", "")

CATALOG = dbutils.widgets.get("catalog")
BATCH_ID = dbutils.widgets.get("batch_id").strip()
RUN_ID = dbutils.widgets.get("run_id").strip()

if not BATCH_ID:
    raise ValueError("batch_id is required")

if not RUN_ID:
    RUN_ID = f"manual_{BATCH_ID}"

dbutils.widgets.text("run_id", "")

RUN_ID = dbutils.widgets.get("run_id").strip()

if not RUN_ID:
    RUN_ID = f"manual_{BATCH_ID}"

CATALOG = dbutils.widgets.get("catalog")
BATCH_ID = dbutils.widgets.get("batch_id").strip()

if not BATCH_ID:
    raise ValueError("batch_id is required")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.audit")

results = []

def add_result(table_name, rule_name, failed_count, total_count):
    passed_count = total_count - failed_count
    pass_percentage = 100.0 if total_count == 0 else round((passed_count / total_count) * 100, 2)
    status = "PASS" if failed_count == 0 else "FAIL"

    results.append((
        RUN_ID,
        "silver",
        table_name,
        rule_name,
        int(total_count),
        int(failed_count),
        float(pass_percentage),
        status,
        BATCH_ID
    ))

def check_unique_pk(table_name, pk):
    df = spark.table(f"{CATALOG}.silver.{table_name}")
    total_count = df.count()

    failed_count = (
        df.groupBy(pk)
        .count()
        .filter((F.col(pk).isNull()) | (F.col("count") > 1))
        .count()
    )

    add_result(table_name, f"SLV_{table_name.upper()}_{pk.upper()}_UNIQUE", failed_count, total_count)

def check_not_null(table_name, column_name):
    df = spark.table(f"{CATALOG}.silver.{table_name}")
    total_count = df.count()
    failed_count = df.filter(F.col(column_name).isNull()).count()

    add_result(table_name, f"SLV_{table_name.upper()}_{column_name.upper()}_NOT_NULL", failed_count, total_count)

def check_non_negative(table_name, column_name):
    df = spark.table(f"{CATALOG}.silver.{table_name}")
    total_count = df.count()
    failed_count = df.filter(F.col(column_name) < 0).count()

    add_result(table_name, f"SLV_{table_name.upper()}_{column_name.upper()}_NON_NEGATIVE", failed_count, total_count)

def check_positive(table_name, column_name):
    df = spark.table(f"{CATALOG}.silver.{table_name}")
    total_count = df.count()
    failed_count = df.filter(F.col(column_name) <= 0).count()

    add_result(table_name, f"SLV_{table_name.upper()}_{column_name.upper()}_POSITIVE", failed_count, total_count)

check_unique_pk("customers", "customer_id")
check_not_null("customers", "email")

check_unique_pk("products", "product_id")
check_non_negative("products", "unit_price")

check_unique_pk("orders", "order_id")
check_not_null("orders", "customer_id")
check_non_negative("orders", "total_amount")

check_unique_pk("order_items", "order_item_id")
check_not_null("order_items", "order_id")
check_not_null("order_items", "product_id")
check_positive("order_items", "quantity")

check_unique_pk("payments", "payment_id")
check_not_null("payments", "order_id")
check_non_negative("payments", "amount")

results_df = spark.createDataFrame(
    results,
    [
        "run_id",
        "layer",
        "table_name",
        "rule_name",
        "total_records",
        "failed_records",
        "pass_percentage",
        "status",
        "batch_id"
    ]
).withColumn("check_timestamp", F.current_timestamp())

(
    results_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.audit.data_quality_results")
)

display(results_df.orderBy("table_name", "rule_name"))

failed_rules = results_df.filter(F.col("status") == "FAIL").count()

if failed_rules > 0:
    raise ValueError(f"Silver DQ failed: {failed_rules} rules failed")

print("Silver DQ checks complete.")
