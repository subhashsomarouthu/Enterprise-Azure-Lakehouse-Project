# Databricks notebook source
# Enterprise Azure Lakehouse - Gold Aggregations

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ealh_dev")
dbutils.widgets.text("storage_account", "stsubhashealhdev001")
dbutils.widgets.text("source_system", "azuresql_sales")

CATALOG = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
SOURCE_SYSTEM = dbutils.widgets.get("source_system")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

def gold_path(table_name: str) -> str:
    return f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SOURCE_SYSTEM}/{table_name}"

customers = spark.table(f"{CATALOG}.silver.customers")
products = spark.table(f"{CATALOG}.silver.products")
orders = spark.table(f"{CATALOG}.silver.orders")
order_items = spark.table(f"{CATALOG}.silver.order_items")
payments = spark.table(f"{CATALOG}.silver.payments")

daily_sales_summary = (
    orders.alias("o")
    .join(payments.alias("p"), F.col("o.order_id") == F.col("p.order_id"), "left")
    .groupBy(F.to_date("o.order_date").alias("order_date"))
    .agg(
        F.countDistinct("o.order_id").alias("total_orders"),
        F.countDistinct("o.customer_id").alias("active_customers"),
        F.sum("o.total_amount").alias("gross_order_amount"),
        F.sum(F.when(F.col("p.payment_status") == "completed", F.col("p.amount")).otherwise(F.lit(0))).alias("completed_payment_amount"),
        F.avg("o.total_amount").alias("avg_order_value")
    )
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

product_sales_summary = (
    order_items.alias("oi")
    .join(products.alias("p"), F.col("oi.product_id") == F.col("p.product_id"), "left")
    .groupBy(
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("p.category")
    )
    .agg(
        F.sum("oi.quantity").alias("units_sold"),
        F.sum("oi.line_total").alias("sales_amount"),
        F.countDistinct("oi.order_id").alias("order_count")
    )
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

customer_order_summary = (
    orders.alias("o")
    .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .groupBy(
        F.col("o.customer_id"),
        F.col("c.customer_name"),
        F.col("c.email")
    )
    .agg(
        F.countDistinct("o.order_id").alias("total_orders"),
        F.sum("o.total_amount").alias("lifetime_order_amount"),
        F.avg("o.total_amount").alias("avg_order_value"),
        F.max("o.order_date").alias("last_order_date")
    )
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

gold_tables = {
    "daily_sales_summary": daily_sales_summary,
    "product_sales_summary": product_sales_summary,
    "customer_order_summary": customer_order_summary,
}

for table_name, df in gold_tables.items():
    target_table = f"{CATALOG}.gold.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", gold_path(table_name))
        .saveAsTable(target_table)
    )

    print(f"Refreshed {target_table} with {df.count()} records")

print("Gold aggregations complete.")