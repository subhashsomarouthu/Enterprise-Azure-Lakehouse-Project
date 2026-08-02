# Databricks notebook source
# Enterprise Azure Lakehouse - Gold Data Quality Checks

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "ealh_dev")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("run_id", "")

CATALOG = dbutils.widgets.get("catalog")
BATCH_ID = dbutils.widgets.get("batch_id").strip()
RUN_ID = dbutils.widgets.get("run_id").strip()

if not BATCH_ID:
    BATCH_ID = "not_applicable"

if not RUN_ID:
    RUN_ID = "manual_gold_dq_run"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.audit")

results = []


def add_result(table_name, rule_name, failed_count, total_count):
    passed_count = total_count - failed_count
    pass_percentage = 100.0 if total_count == 0 else round((passed_count / total_count) * 100, 2)
    status = "PASS" if failed_count == 0 else "FAIL"

    results.append((
        RUN_ID,
        "gold",
        table_name,
        rule_name,
        int(total_count),
        int(failed_count),
        float(pass_percentage),
        status,
        BATCH_ID
    ))


def read_gold(table_name):
    return spark.table(f"{CATALOG}.gold.{table_name}")


def check_not_null(table_name, column_name):
    df = read_gold(table_name)
    total_count = df.count()
    failed_count = df.filter(F.col(column_name).isNull()).count()
    add_result(table_name, f"GLD_{table_name.upper()}_{column_name.upper()}_NOT_NULL", failed_count, total_count)


def check_non_negative(table_name, column_name):
    df = read_gold(table_name)
    total_count = df.count()
    failed_count = df.filter(F.col(column_name).isNull() | (F.col(column_name) < 0)).count()
    add_result(table_name, f"GLD_{table_name.upper()}_{column_name.upper()}_NON_NEGATIVE", failed_count, total_count)


def check_unique(table_name, column_name):
    df = read_gold(table_name)
    total_count = df.count()
    failed_count = (
        df.groupBy(column_name)
        .count()
        .filter(F.col(column_name).isNull() | (F.col("count") > 1))
        .count()
    )
    add_result(table_name, f"GLD_{table_name.upper()}_{column_name.upper()}_UNIQUE", failed_count, total_count)


def check_one_current_customer_version():
    table_name = "dim_customer"
    df = read_gold(table_name)
    total_count = df.count()
    failed_count = (
        df.groupBy("customer_id")
        .agg(F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("current_versions"))
        .filter(F.col("current_versions") > 1)
        .count()
    )
    add_result(table_name, "GLD_DIM_CUSTOMER_ONE_CURRENT_VERSION", failed_count, total_count)


# Existing aggregate table checks.
check_not_null("daily_sales_summary", "order_date")
check_non_negative("daily_sales_summary", "total_orders")
check_non_negative("daily_sales_summary", "gross_order_amount")
check_non_negative("daily_sales_summary", "avg_order_value")

check_not_null("product_sales_summary", "product_id")
check_non_negative("product_sales_summary", "units_sold")
check_non_negative("product_sales_summary", "sales_amount")

check_not_null("customer_order_summary", "customer_id")
check_non_negative("customer_order_summary", "total_orders")
check_non_negative("customer_order_summary", "lifetime_order_amount")

# Dimensional model checks.
check_unique("dim_customer", "customer_sk")
check_not_null("dim_customer", "customer_id")
check_not_null("dim_customer", "effective_start_at")
check_one_current_customer_version()

check_unique("dim_product", "product_key")
check_not_null("dim_product", "product_id")
check_not_null("dim_product", "product_name")
check_non_negative("dim_product", "unit_price")

check_unique("dim_date", "date_key")
check_not_null("dim_date", "calendar_date")

check_unique("fact_orders", "order_id")
check_not_null("fact_orders", "customer_id")
check_not_null("fact_orders", "order_date_key")
check_non_negative("fact_orders", "total_amount")

check_unique("fact_order_items", "order_item_id")
check_not_null("fact_order_items", "order_id")
check_not_null("fact_order_items", "product_id")
check_non_negative("fact_order_items", "quantity")
check_non_negative("fact_order_items", "line_total")

check_unique("fact_payments", "payment_id")
check_not_null("fact_payments", "order_id")
check_not_null("fact_payments", "payment_date_key")
check_non_negative("fact_payments", "amount")

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
    raise ValueError(f"Gold DQ failed: {failed_rules} rules failed")

print("Gold DQ checks complete.")