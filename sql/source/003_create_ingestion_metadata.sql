CREATE SCHEMA metadata;
GO

CREATE TABLE metadata.adf_ingestion_tables (
    source_system_id NVARCHAR(100) NOT NULL,
    source_schema NVARCHAR(100) NOT NULL,
    source_table NVARCHAR(100) NOT NULL,
    target_entity NVARCHAR(100) NOT NULL,
    primary_key_column NVARCHAR(100) NOT NULL,
    incremental_column NVARCHAR(100) NOT NULL,
    load_type NVARCHAR(50) NOT NULL,
    file_format NVARCHAR(50) NOT NULL,
    raw_base_path NVARCHAR(500) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_adf_ingestion_tables
        PRIMARY KEY (source_system_id, source_schema, source_table)
);

CREATE TABLE metadata.adf_watermark_tracking (
    source_system_id NVARCHAR(100) NOT NULL,
    source_schema NVARCHAR(100) NOT NULL,
    source_table NVARCHAR(100) NOT NULL,
    watermark_column NVARCHAR(100) NOT NULL,
    last_watermark_value DATETIME2 NOT NULL,
    last_successful_batch_id NVARCHAR(100),
    last_successful_run_id NVARCHAR(100),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_adf_watermark_tracking
        PRIMARY KEY (source_system_id, source_schema, source_table)
);

INSERT INTO metadata.adf_ingestion_tables
(source_system_id, source_schema, source_table, target_entity, primary_key_column, incremental_column, load_type, file_format, raw_base_path)
VALUES
('azuresql_sales', 'sales', 'customers', 'customers', 'customer_id', 'updated_at', 'incremental', 'parquet', 'azuresql_sales/customers'),
('azuresql_sales', 'sales', 'products', 'products', 'product_id', 'updated_at', 'incremental', 'parquet', 'azuresql_sales/products'),
('azuresql_sales', 'sales', 'orders', 'orders', 'order_id', 'updated_at', 'incremental', 'parquet', 'azuresql_sales/orders'),
('azuresql_sales', 'sales', 'order_items', 'order_items', 'order_item_id', 'updated_at', 'incremental', 'parquet', 'azuresql_sales/order_items'),
('azuresql_sales', 'sales', 'payments', 'payments', 'payment_id', 'updated_at', 'incremental', 'parquet', 'azuresql_sales/payments');

INSERT INTO metadata.adf_watermark_tracking
(source_system_id, source_schema, source_table, watermark_column, last_watermark_value, last_successful_batch_id, last_successful_run_id)
VALUES
('azuresql_sales', 'sales', 'customers', 'updated_at', '1900-01-01', NULL, NULL),
('azuresql_sales', 'sales', 'products', 'updated_at', '1900-01-01', NULL, NULL),
('azuresql_sales', 'sales', 'orders', 'updated_at', '1900-01-01', NULL, NULL),
('azuresql_sales', 'sales', 'order_items', 'updated_at', '1900-01-01', NULL, NULL),
('azuresql_sales', 'sales', 'payments', 'updated_at', '1900-01-01', NULL, NULL);
GO
