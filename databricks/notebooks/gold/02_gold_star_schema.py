# Databricks notebook source
# Enterprise Azure Lakehouse - Gold Star Schema
# Builds dimensional Gold tables and SCD Type 2 customer dimension from Silver current-state tables.

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


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

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def gold_path(table_name: str) -> str:
    return f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SOURCE_SYSTEM}/{table_name}"


def write_overwrite(df, table_name: str):
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


customers = spark.table(f"{CATALOG}.silver.customers")
products = spark.table(f"{CATALOG}.silver.products")
orders = spark.table(f"{CATALOG}.silver.orders")
order_items = spark.table(f"{CATALOG}.silver.order_items")
payments = spark.table(f"{CATALOG}.silver.payments")

# COMMAND ----------

# SCD Type 2 customer dimension. This tracks customer attribute changes from this point forward.
customer_attributes = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "state_province",
    "country",
    "customer_segment",
    "is_active",
]

incoming_customers = (
    customers
    .select(
        "customer_id",
        *customer_attributes,
        F.col("created_at").alias("source_created_at"),
        F.col("updated_at").alias("source_updated_at")
    )
    .withColumn("customer_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn(
        "attribute_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in customer_attributes]
            ),
            256
        )
    )
    .withColumn("effective_start_at", F.coalesce(F.col("source_updated_at"), F.current_timestamp()))
    .withColumn("effective_end_at", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    .withColumn("_gold_loaded_at", F.current_timestamp())
    .withColumn("_gold_updated_at", F.current_timestamp())
)

incoming_customer_dim = incoming_customers.withColumn(
    "customer_sk",
    F.sha2(F.concat_ws("||", F.col("customer_id").cast("string"), F.col("effective_start_at").cast("string")), 256)
).select(
    "customer_sk",
    "customer_id",
    "customer_name",
    *customer_attributes,
    "attribute_hash",
    "effective_start_at",
    "effective_end_at",
    "is_current",
    "source_created_at",
    "source_updated_at",
    "_gold_batch_id",
    "_gold_loaded_at",
    "_gold_updated_at"
)

customer_dim_table = f"{CATALOG}.gold.dim_customer"

if not table_exists(customer_dim_table):
    (
        incoming_customer_dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", gold_path("dim_customer"))
        .saveAsTable(customer_dim_table)
    )
    print(f"Created {customer_dim_table} with {incoming_customer_dim.count()} current customer versions")
else:
    current_customer_dim = (
        spark.table(customer_dim_table)
        .filter(F.col("is_current") == True)
        .select(
            F.col("customer_id").alias("target_customer_id"),
            F.col("attribute_hash").alias("target_attribute_hash")
        )
    )

    changed_customers = (
        incoming_customer_dim.alias("source")
        .join(
            current_customer_dim.alias("target"),
            F.col("source.customer_id") == F.col("target.target_customer_id"),
            "left"
        )
        .filter(
            F.col("target.target_customer_id").isNull()
            | (F.col("source.attribute_hash") != F.col("target.target_attribute_hash"))
        )
        .select("source.*")
    )

    changed_count = changed_customers.count()

    if changed_count > 0:
        delta_customer_dim = DeltaTable.forName(spark, customer_dim_table)
        close_source = changed_customers.select("customer_id", "effective_start_at")

        (
            delta_customer_dim.alias("target")
            .merge(
                close_source.alias("source"),
                "target.customer_id = source.customer_id AND target.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current": "false",
                "effective_end_at": "source.effective_start_at",
                "_gold_updated_at": "current_timestamp()"
            })
            .execute()
        )

        (
            changed_customers.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(customer_dim_table)
        )

    print(f"Applied {changed_count} SCD2 customer changes to {customer_dim_table}")

# COMMAND ----------

# Type 1 product dimension.
dim_product = (
    products
    .select(
        F.col("product_id").alias("product_key"),
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_price",
        "cost_price",
        "is_active",
        F.col("created_at").alias("source_created_at"),
        F.col("updated_at").alias("source_updated_at")
    )
    .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

write_overwrite(dim_product, "dim_product")

# Date dimension from order and payment dates.
order_dates = orders.select(F.to_date("order_date").alias("calendar_date"))
payment_dates = payments.select(F.to_date("payment_date").alias("calendar_date"))

dim_date = (
    order_dates
    .unionByName(payment_dates)
    .filter(F.col("calendar_date").isNotNull())
    .distinct()
    .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("calendar_date"))
    .withColumn("quarter", F.quarter("calendar_date"))
    .withColumn("month", F.month("calendar_date"))
    .withColumn("day", F.dayofmonth("calendar_date"))
    .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("calendar_date").isin([1, 7]))
    .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    .withColumn("_gold_loaded_at", F.current_timestamp())
    .select(
        "date_key",
        "calendar_date",
        "year",
        "quarter",
        "month",
        "day",
        "day_of_week",
        "is_weekend",
        "_gold_batch_id",
        "_gold_loaded_at"
    )
)

write_overwrite(dim_date, "dim_date")

# COMMAND ----------

current_customers = (
    spark.table(customer_dim_table)
    .filter(F.col("is_current") == True)
    .select("customer_id", "customer_sk")
)

if "is_priority_order" not in orders.columns:
    orders_for_fact = orders.withColumn("is_priority_order", F.lit(None).cast(BooleanType()))
else:
    orders_for_fact = orders

fact_orders = (
    orders_for_fact.alias("o")
    .join(current_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .select(
        F.col("o.order_id"),
        F.col("c.customer_sk"),
        F.col("o.customer_id"),
        F.date_format(F.to_date("o.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.col("o.order_date").alias("order_timestamp"),
        F.col("o.order_status"),
        F.col("o.sales_channel"),
        F.col("o.payment_method"),
        F.col("o.shipping_city"),
        F.col("o.shipping_country"),
        F.col("o.subtotal_amount"),
        F.col("o.discount_amount"),
        F.col("o.tax_amount"),
        F.col("o.shipping_amount"),
        F.col("o.total_amount"),
        F.col("o.is_priority_order")
    )
    .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

write_overwrite(fact_orders, "fact_orders")

fact_order_items = (
    order_items.alias("oi")
    .join(orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "left")
    .select(
        F.col("oi.order_item_id"),
        F.col("oi.order_id"),
        F.col("oi.product_id").alias("product_key"),
        F.col("oi.product_id"),
        F.date_format(F.to_date("o.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.discount_amount"),
        F.col("oi.line_total")
    )
    .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

write_overwrite(fact_order_items, "fact_order_items")

fact_payments = (
    payments
    .select(
        "payment_id",
        "order_id",
        F.date_format(F.to_date("payment_date"), "yyyyMMdd").cast("int").alias("payment_date_key"),
        F.col("payment_date").alias("payment_timestamp"),
        "payment_method",
        "payment_status",
        "amount",
        "transaction_reference"
    )
    .withColumn("_gold_batch_id", F.lit(BATCH_ID))
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

write_overwrite(fact_payments, "fact_payments")

print("Gold star schema build complete.")