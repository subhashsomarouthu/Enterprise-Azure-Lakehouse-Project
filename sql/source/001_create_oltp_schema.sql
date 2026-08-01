CREATE SCHEMA sales;
GO

CREATE TABLE sales.customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(100) NOT NULL,
    last_name NVARCHAR(100) NOT NULL,
    email NVARCHAR(255) NOT NULL,
    phone NVARCHAR(50),
    city NVARCHAR(100),
    state_province NVARCHAR(100),
    country NVARCHAR(100),
    customer_segment NVARCHAR(50),
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    is_active BIT NOT NULL DEFAULT 1
);

CREATE TABLE sales.products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    product_name NVARCHAR(255) NOT NULL,
    category NVARCHAR(100) NOT NULL,
    subcategory NVARCHAR(100),
    brand NVARCHAR(100),
    unit_price DECIMAL(18,2) NOT NULL,
    cost_price DECIMAL(18,2) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    is_active BIT NOT NULL DEFAULT 1
);

CREATE TABLE sales.orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME2 NOT NULL,
    order_status NVARCHAR(50) NOT NULL,
    sales_channel NVARCHAR(50) NOT NULL,
    payment_method NVARCHAR(50),
    shipping_city NVARCHAR(100),
    shipping_country NVARCHAR(100),
    subtotal_amount DECIMAL(18,2) NOT NULL,
    discount_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    tax_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    shipping_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    total_amount DECIMAL(18,2) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id)
        REFERENCES sales.customers(customer_id)
);

CREATE TABLE sales.order_items (
    order_item_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    discount_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    line_total DECIMAL(18,2) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id)
        REFERENCES sales.orders(order_id),
    CONSTRAINT fk_order_items_products FOREIGN KEY (product_id)
        REFERENCES sales.products(product_id)
);

CREATE TABLE sales.payments (
    payment_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    payment_date DATETIME2 NOT NULL,
    payment_method NVARCHAR(50) NOT NULL,
    payment_status NVARCHAR(50) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    transaction_reference NVARCHAR(100),
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_payments_orders FOREIGN KEY (order_id)
        REFERENCES sales.orders(order_id)
);

CREATE INDEX ix_customers_updated_at ON sales.customers(updated_at);
CREATE INDEX ix_products_updated_at ON sales.products(updated_at);
CREATE INDEX ix_orders_updated_at ON sales.orders(updated_at);
CREATE INDEX ix_order_items_updated_at ON sales.order_items(updated_at);
CREATE INDEX ix_payments_updated_at ON sales.payments(updated_at);
GO
