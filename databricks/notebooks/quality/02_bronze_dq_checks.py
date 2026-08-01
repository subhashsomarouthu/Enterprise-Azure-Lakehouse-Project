# Databricks notebook source
# Enterprise Azure Lakehouse - Bronze Data Quality Checks

import json
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql.functions import col, current_timestamp, lit, struct, to_json, when

dbutils.widgets.text("catalog_name", "ealh_dev")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("run_id", "")

CATALOG = dbutils.widgets.get("catalog_name")
ENVIRONMENT = dbutils.widgets.get("environment")
RUN_ID = dbutils.widgets.get("run_id") or f"bronze_dq_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

BRONZE = f"{CATALOG}.bronze"
CONFIG = f"{CATALOG}.config"
AUDIT = f"{CATALOG}.audit"
QUARANTINE = f"{CATALOG}.quarantine"

TABLES = ["customers", "products", "orders", "order_items", "payments"]

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUARANTINE}")


def get_rules(table_name):
    return (
        spark.table(f"{CONFIG}.dq_rules")
        .filter(col("target_layer") == "bronze")
        .filter(col("target_table") == table_name)
        .filter(col("is_active") == True)
        .collect()
    )


def run_table_dq(table_name):
    source_table = f"{BRONZE}.{table_name}"
    validated_table = f"{BRONZE}.{table_name}_validated"

    df = spark.table(source_table)
    total_count = df.count()
    rules = get_rules(table_name)

    if not rules:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(validated_table)
        )
        return []

    failed_condition = None
    rule_results = []

    for rule in rules:
        rule_id = rule["rule_id"]
        rule_expression = rule["rule_expression"]
        severity = rule["severity"]
        rule_type = rule["rule_type"]

        failed_df = df.filter(f"NOT ({rule_expression})")
        failed_count = failed_df.count()
        passed_count = total_count - failed_count
        pass_rate = 100.0 if total_count == 0 else round((passed_count / total_count) * 100, 4)
        status = "PASS" if failed_count == 0 else ("FAIL" if severity == "critical" else "WARN")

        rule_results.append(
            (
                RUN_ID,
                table_name,
                rule_id,
                rule_type,
                severity,
                passed_count,
                failed_count,
                pass_rate,
                status,
                datetime.now(timezone.utc),
            )
        )

        condition = f"NOT ({rule_expression})"
        failed_condition = condition if failed_condition is None else f"({failed_condition}) OR ({condition})"

        if failed_count > 0:
            quarantine_df = (
                failed_df.withColumn("rejection_id", lit(str(uuid4())))
                .withColumn("run_id", lit(RUN_ID))
                .withColumn("batch_id", col("_batch_id") if "_batch_id" in failed_df.columns else lit(None))
                .withColumn("source_system_id", col("_source_system_id") if "_source_system_id" in failed_df.columns else lit(None))
                .withColumn("source_table", lit(table_name))
                .withColumn("target_layer", lit("bronze"))
                .withColumn("target_table", lit(table_name))
                .withColumn("rejection_reason", lit(rule_expression))
                .withColumn("rule_id", lit(rule_id))
                .withColumn("severity", lit(severity))
                .withColumn("raw_record", to_json(struct(*[col(c) for c in failed_df.columns])))
                .withColumn("source_file_path", col("_source_file_path") if "_source_file_path" in failed_df.columns else lit(None))
                .withColumn("rejected_at", current_timestamp())
                .withColumn("reviewed_status", lit("pending_review"))
                .withColumn("reviewed_by", lit(None))
                .withColumn("reviewed_at", lit(None).cast("timestamp"))
                .withColumn("notes", lit(None))
                .select(
                    "rejection_id",
                    "run_id",
                    "batch_id",
                    "source_system_id",
                    "source_table",
                    "target_layer",
                    "target_table",
                    "rejection_reason",
                    "rule_id",
                    "severity",
                    "raw_record",
                    "source_file_path",
                    "rejected_at",
                    "reviewed_status",
                    "reviewed_by",
                    "reviewed_at",
                    "notes",
                )
            )

            quarantine_df.write.format("delta").mode("append").saveAsTable(f"{QUARANTINE}.rejected_records")

    if failed_condition:
        valid_df = df.filter(f"NOT ({failed_condition})")
    else:
        valid_df = df

    (
        valid_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(validated_table)
    )

    print(f"{table_name}: total={total_count}, valid={valid_df.count()}, rules={len(rules)}")
    return rule_results


all_results = []
for table_name in TABLES:
    all_results.extend(run_table_dq(table_name))

results_schema = """
run_id STRING,
table_name STRING,
rule_id STRING,
rule_type STRING,
severity STRING,
passed_records BIGINT,
failed_records BIGINT,
pass_rate DOUBLE,
status STRING,
checked_at TIMESTAMP
"""

if all_results:
    spark.createDataFrame(all_results, results_schema).write.format("delta").mode("append").saveAsTable(
        f"{AUDIT}.data_quality_results"
    )

display(spark.table(f"{AUDIT}.data_quality_results").filter(col("run_id") == RUN_ID))
