-- Enterprise Azure Lakehouse - Unity Catalog Setup

CREATE CATALOG IF NOT EXISTS ealh_dev
COMMENT 'Enterprise Azure Lakehouse dev catalog';

CREATE SCHEMA IF NOT EXISTS ealh_dev.config
COMMENT 'Configuration tables that drive ingestion and transformation pipelines';

CREATE SCHEMA IF NOT EXISTS ealh_dev.control
COMMENT 'Control tables for watermarks, backfills, locks, and operational state';

CREATE SCHEMA IF NOT EXISTS ealh_dev.audit
COMMENT 'Audit tables for pipeline runs, data quality, schema drift, and errors';

CREATE SCHEMA IF NOT EXISTS ealh_dev.quarantine
COMMENT 'Rejected records and files requiring investigation or replay';

CREATE SCHEMA IF NOT EXISTS ealh_dev.bronze
COMMENT 'Bronze layer: source-shaped Delta tables with ingestion metadata';

CREATE SCHEMA IF NOT EXISTS ealh_dev.silver
COMMENT 'Silver layer: cleaned, conformed, deduplicated business entities';

CREATE SCHEMA IF NOT EXISTS ealh_dev.gold
COMMENT 'Gold layer: dimensional models, facts, aggregates, and marts';

CREATE TABLE IF NOT EXISTS ealh_dev.config.source_systems (
  source_system_id STRING NOT NULL,
  source_system_name STRING NOT NULL,
  source_type STRING NOT NULL,
  owner_team STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Registered source systems such as Azure SQL, APIs, files, or event streams';

CREATE TABLE IF NOT EXISTS ealh_dev.config.ingestion_tables (
  source_system_id STRING NOT NULL,
  source_schema STRING,
  source_table STRING NOT NULL,
  target_entity STRING NOT NULL,
  raw_path STRING NOT NULL,
  bronze_table STRING NOT NULL,
  load_type STRING NOT NULL,
  incremental_column STRING,
  primary_key STRING,
  file_format STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Metadata-driven ingestion configuration for source-to-raw and raw-to-bronze processing';

CREATE TABLE IF NOT EXISTS ealh_dev.config.dq_rules (
  rule_id STRING NOT NULL,
  target_layer STRING NOT NULL,
  target_table STRING NOT NULL,
  column_name STRING,
  rule_type STRING NOT NULL,
  rule_expression STRING NOT NULL,
  severity STRING NOT NULL,
  is_active BOOLEAN,
  created_at TIMESTAMP
)
USING DELTA
COMMENT 'Data quality rules used by validation and quarantine logic';

CREATE TABLE IF NOT EXISTS ealh_dev.control.watermark_tracking (
  source_system_id STRING NOT NULL,
  source_table STRING NOT NULL,
  target_table STRING NOT NULL,
  watermark_column STRING NOT NULL,
  last_watermark_value STRING,
  last_successful_batch_id STRING,
  last_successful_run_id STRING,
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Stores last successful incremental watermark per source table';

CREATE TABLE IF NOT EXISTS ealh_dev.control.backfill_requests (
  request_id STRING NOT NULL,
  source_system_id STRING NOT NULL,
  source_table STRING NOT NULL,
  start_value STRING,
  end_value STRING,
  status STRING NOT NULL,
  requested_by STRING,
  requested_at TIMESTAMP,
  completed_at TIMESTAMP
)
USING DELTA
COMMENT 'Tracks controlled historical backfill requests';

CREATE TABLE IF NOT EXISTS ealh_dev.audit.pipeline_run_log (
  run_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  layer STRING NOT NULL,
  environment STRING NOT NULL,
  status STRING NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  records_read BIGINT,
  records_written BIGINT,
  error_message STRING
)
USING DELTA
COMMENT 'Pipeline execution audit log';

CREATE TABLE IF NOT EXISTS ealh_dev.audit.ingestion_batch_log (
  batch_id STRING NOT NULL,
  source_system_id STRING NOT NULL,
  source_table STRING NOT NULL,
  raw_path STRING,
  bronze_table STRING,
  load_type STRING,
  batch_start_time TIMESTAMP,
  batch_end_time TIMESTAMP,
  status STRING,
  records_copied BIGINT,
  records_ingested BIGINT,
  error_message STRING
)
USING DELTA
COMMENT 'Batch-level ingestion audit log';

CREATE TABLE IF NOT EXISTS ealh_dev.audit.data_quality_results (
  run_id STRING NOT NULL,
  table_name STRING NOT NULL,
  rule_id STRING,
  rule_type STRING,
  severity STRING,
  passed_records BIGINT,
  failed_records BIGINT,
  pass_rate DOUBLE,
  status STRING,
  checked_at TIMESTAMP
)
USING DELTA
COMMENT 'Data quality result history';

CREATE TABLE IF NOT EXISTS ealh_dev.audit.schema_drift_log (
  drift_id STRING NOT NULL,
  source_system_id STRING,
  table_name STRING NOT NULL,
  drift_type STRING NOT NULL,
  column_name STRING,
  old_data_type STRING,
  new_data_type STRING,
  severity STRING,
  status STRING,
  detected_at TIMESTAMP,
  reviewed_by STRING,
  reviewed_at TIMESTAMP,
  notes STRING
)
USING DELTA
COMMENT 'Schema drift detection and review log';

CREATE TABLE IF NOT EXISTS ealh_dev.audit.error_log (
  error_id STRING NOT NULL,
  run_id STRING,
  pipeline_name STRING,
  layer STRING,
  table_name STRING,
  error_type STRING,
  error_message STRING,
  error_context STRING,
  created_at TIMESTAMP
)
USING DELTA
COMMENT 'Centralized pipeline error log';

CREATE TABLE IF NOT EXISTS ealh_dev.quarantine.rejected_records (
  rejection_id STRING NOT NULL,
  run_id STRING,
  batch_id STRING,
  source_system_id STRING,
  source_table STRING,
  target_layer STRING,
  target_table STRING,
  rejection_reason STRING NOT NULL,
  rule_id STRING,
  severity STRING,
  raw_record STRING,
  source_file_path STRING,
  rejected_at TIMESTAMP,
  reviewed_status STRING,
  reviewed_by STRING,
  reviewed_at TIMESTAMP,
  notes STRING
)
USING DELTA
COMMENT 'Generic quarantine table for rejected records across ingestion and transformation layers';

SHOW SCHEMAS IN ealh_dev;
