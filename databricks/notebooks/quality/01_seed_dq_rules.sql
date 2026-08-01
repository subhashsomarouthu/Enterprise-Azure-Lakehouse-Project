-- Enterprise Azure Lakehouse - Seed Data Quality Rules

INSERT INTO ealh_dev.config.dq_rules
(rule_id, target_layer, target_table, column_name, rule_type, rule_expression, severity, is_active, created_at)
VALUES
('BRZ_CUSTOMERS_PK_NOT_NULL', 'bronze', 'customers', 'customer_id', 'not_null', 'customer_id IS NOT NULL', 'critical', true, current_timestamp()),
('BRZ_CUSTOMERS_EMAIL_NOT_NULL', 'bronze', 'customers', 'email', 'not_null', 'email IS NOT NULL', 'warning', true, current_timestamp()),
('BRZ_PRODUCTS_PK_NOT_NULL', 'bronze', 'products', 'product_id', 'not_null', 'product_id IS NOT NULL', 'critical', true, current_timestamp()),
('BRZ_PRODUCTS_PRICE_VALID', 'bronze', 'products', 'unit_price', 'range_check', 'unit_price >= 0', 'critical', true, current_timestamp()),
('BRZ_ORDERS_PK_NOT_NULL', 'bronze', 'orders', 'order_id', 'not_null', 'order_id IS NOT NULL', 'critical', true, current_timestamp()),
('BRZ_ORDERS_CUSTOMER_NOT_NULL', 'bronze', 'orders', 'customer_id', 'not_null', 'customer_id IS NOT NULL', 'critical', true, current_timestamp()),
('BRZ_ORDERS_TOTAL_VALID', 'bronze', 'orders', 'total_amount', 'range_check', 'total_amount >= 0', 'critical', true, current_timestamp()),
('BRZ_ORDER_ITEMS_PK_NOT_NULL', 'bronze', 'order_items', 'order_item_id', 'not_null', 'order_item_id IS NOT NULL', 'critical', true, current_timestamp()),
('BRZ_ORDER_ITEMS_QUANTITY_VALID', 'bronze', 'order_items', 'quantity', 'range_check', 'quantity > 0', 'critical', true, current_timestamp()),
('BRZ_PAYMENTS_PK_NOT_NULL', 'bronze', 'payments', 'payment_id', 'not_null', 'payment_id IS NOT NULL', 'critical', true, current_timestamp()),
('BRZ_PAYMENTS_AMOUNT_VALID', 'bronze', 'payments', 'amount', 'range_check', 'amount >= 0', 'critical', true, current_timestamp());
