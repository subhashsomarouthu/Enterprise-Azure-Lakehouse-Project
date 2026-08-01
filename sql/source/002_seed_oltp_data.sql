SET NOCOUNT ON;

INSERT INTO sales.customers
(first_name, last_name, email, phone, city, state_province, country, customer_segment)
VALUES
('Aarav', 'Sharma', 'aarav.sharma@example.com', '+1-416-555-0101', 'Toronto', 'Ontario', 'Canada', 'Retail'),
('Maya', 'Patel', 'maya.patel@example.com', '+1-647-555-0102', 'Mississauga', 'Ontario', 'Canada', 'Premium'),
('Noah', 'Singh', 'noah.singh@example.com', '+1-905-555-0103', 'Brampton', 'Ontario', 'Canada', 'Retail'),
('Olivia', 'Brown', 'olivia.brown@example.com', '+1-416-555-0104', 'Toronto', 'Ontario', 'Canada', 'Wholesale'),
('Liam', 'Wilson', 'liam.wilson@example.com', '+1-613-555-0105', 'Ottawa', 'Ontario', 'Canada', 'Retail'),
('Sophia', 'Khan', 'sophia.khan@example.com', '+1-514-555-0106', 'Montreal', 'Quebec', 'Canada', 'Premium'),
('Ethan', 'Chen', 'ethan.chen@example.com', '+1-604-555-0107', 'Vancouver', 'British Columbia', 'Canada', 'Retail'),
('Emma', 'Garcia', 'emma.garcia@example.com', '+1-403-555-0108', 'Calgary', 'Alberta', 'Canada', 'Retail'),
('Lucas', 'Martin', 'lucas.martin@example.com', '+1-780-555-0109', 'Edmonton', 'Alberta', 'Canada', 'Wholesale'),
('Ava', 'Thomas', 'ava.thomas@example.com', '+1-204-555-0110', 'Winnipeg', 'Manitoba', 'Canada', 'Premium');

INSERT INTO sales.products
(product_name, category, subcategory, brand, unit_price, cost_price)
VALUES
('Wireless Mouse', 'Electronics', 'Accessories', 'NorthTech', 29.99, 12.50),
('Mechanical Keyboard', 'Electronics', 'Accessories', 'NorthTech', 119.99, 62.00),
('USB-C Hub', 'Electronics', 'Accessories', 'MapleWare', 49.99, 24.00),
('Noise Cancelling Headphones', 'Electronics', 'Audio', 'SoundPeak', 199.99, 110.00),
('Desk Lamp', 'Home Office', 'Lighting', 'BrightHome', 39.99, 18.00),
('Ergonomic Chair', 'Home Office', 'Furniture', 'WorkWell', 349.99, 210.00),
('Standing Desk', 'Home Office', 'Furniture', 'WorkWell', 599.99, 370.00),
('Water Bottle', 'Lifestyle', 'Fitness', 'HydroLeaf', 24.99, 8.00),
('Yoga Mat', 'Lifestyle', 'Fitness', 'FlexForm', 44.99, 16.00),
('Backpack', 'Lifestyle', 'Bags', 'UrbanTrail', 79.99, 35.00);

INSERT INTO sales.orders
(customer_id, order_date, order_status, sales_channel, payment_method, shipping_city, shipping_country,
 subtotal_amount, discount_amount, tax_amount, shipping_amount, total_amount)
VALUES
(1, DATEADD(day, -20, SYSUTCDATETIME()), 'Delivered', 'Web', 'Credit Card', 'Toronto', 'Canada', 149.98, 10.00, 18.20, 5.99, 164.17),
(2, DATEADD(day, -18, SYSUTCDATETIME()), 'Delivered', 'Mobile App', 'PayPal', 'Mississauga', 'Canada', 199.99, 0.00, 26.00, 0.00, 225.99),
(3, DATEADD(day, -16, SYSUTCDATETIME()), 'Shipped', 'Web', 'Credit Card', 'Brampton', 'Canada', 49.99, 0.00, 6.50, 4.99, 61.48),
(4, DATEADD(day, -15, SYSUTCDATETIME()), 'Delivered', 'Store', 'Debit Card', 'Toronto', 'Canada', 349.99, 25.00, 42.25, 0.00, 367.24),
(5, DATEADD(day, -14, SYSUTCDATETIME()), 'Cancelled', 'Web', 'Credit Card', 'Ottawa', 'Canada', 599.99, 50.00, 0.00, 0.00, 0.00),
(6, DATEADD(day, -12, SYSUTCDATETIME()), 'Delivered', 'Mobile App', 'Credit Card', 'Montreal', 'Canada', 224.98, 15.00, 27.30, 5.99, 243.27),
(7, DATEADD(day, -10, SYSUTCDATETIME()), 'Delivered', 'Web', 'PayPal', 'Vancouver', 'Canada', 79.99, 0.00, 9.60, 6.99, 96.58),
(8, DATEADD(day, -8, SYSUTCDATETIME()), 'Processing', 'Web', 'Credit Card', 'Calgary', 'Canada', 44.99, 0.00, 5.40, 4.99, 55.38),
(9, DATEADD(day, -6, SYSUTCDATETIME()), 'Delivered', 'Store', 'Debit Card', 'Edmonton', 'Canada', 629.98, 30.00, 72.00, 0.00, 671.98),
(10, DATEADD(day, -3, SYSUTCDATETIME()), 'Delivered', 'Mobile App', 'Credit Card', 'Winnipeg', 'Canada', 24.99, 0.00, 3.00, 4.99, 32.98);

INSERT INTO sales.order_items
(order_id, product_id, quantity, unit_price, discount_amount, line_total)
VALUES
(1, 1, 1, 29.99, 0.00, 29.99),
(1, 2, 1, 119.99, 10.00, 109.99),
(2, 4, 1, 199.99, 0.00, 199.99),
(3, 3, 1, 49.99, 0.00, 49.99),
(4, 6, 1, 349.99, 25.00, 324.99),
(5, 7, 1, 599.99, 50.00, 549.99),
(6, 4, 1, 199.99, 15.00, 184.99),
(6, 8, 1, 24.99, 0.00, 24.99),
(7, 10, 1, 79.99, 0.00, 79.99),
(8, 9, 1, 44.99, 0.00, 44.99),
(9, 7, 1, 599.99, 30.00, 569.99),
(9, 1, 1, 29.99, 0.00, 29.99),
(10, 8, 1, 24.99, 0.00, 24.99);

INSERT INTO sales.payments
(order_id, payment_date, payment_method, payment_status, amount, transaction_reference)
VALUES
(1, DATEADD(day, -20, SYSUTCDATETIME()), 'Credit Card', 'Captured', 164.17, 'TXN-10001'),
(2, DATEADD(day, -18, SYSUTCDATETIME()), 'PayPal', 'Captured', 225.99, 'TXN-10002'),
(3, DATEADD(day, -16, SYSUTCDATETIME()), 'Credit Card', 'Authorized', 61.48, 'TXN-10003'),
(4, DATEADD(day, -15, SYSUTCDATETIME()), 'Debit Card', 'Captured', 367.24, 'TXN-10004'),
(5, DATEADD(day, -14, SYSUTCDATETIME()), 'Credit Card', 'Voided', 0.00, 'TXN-10005'),
(6, DATEADD(day, -12, SYSUTCDATETIME()), 'Credit Card', 'Captured', 243.27, 'TXN-10006'),
(7, DATEADD(day, -10, SYSUTCDATETIME()), 'PayPal', 'Captured', 96.58, 'TXN-10007'),
(8, DATEADD(day, -8, SYSUTCDATETIME()), 'Credit Card', 'Authorized', 55.38, 'TXN-10008'),
(9, DATEADD(day, -6, SYSUTCDATETIME()), 'Debit Card', 'Captured', 671.98, 'TXN-10009'),
(10, DATEADD(day, -3, SYSUTCDATETIME()), 'Credit Card', 'Captured', 32.98, 'TXN-10010');
