import argparse
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import pyodbc
from dotenv import load_dotenv
from faker import Faker

fake = Faker("en_CA")
Faker.seed(42)
random.seed(42)

CATEGORIES = {
    "Electronics": ["Accessories", "Audio", "Computers", "Mobile"],
    "Home Office": ["Furniture", "Lighting", "Storage"],
    "Lifestyle": ["Fitness", "Bags", "Travel"],
    "Apparel": ["Men", "Women", "Kids"],
    "Grocery": ["Pantry", "Beverages", "Snacks"],
}

BRANDS = ["NorthTech", "MapleWare", "SoundPeak", "BrightHome", "WorkWell", "HydroLeaf", "UrbanTrail"]
CHANNELS = ["Web", "Mobile App", "Store", "Marketplace"]
ORDER_STATUSES = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Gift Card"]


def connect():
    load_dotenv()

    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USER")
    password = os.getenv("AZURE_SQL_PASSWORD")

    if not all([server, database, username, password]):
        raise ValueError("Missing one or more Azure SQL environment variables.")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server}.database.windows.net,1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def truncate_source_tables(cursor):
    cursor.execute("DELETE FROM sales.payments")
    cursor.execute("DELETE FROM sales.order_items")
    cursor.execute("DELETE FROM sales.orders")
    cursor.execute("DELETE FROM sales.products")
    cursor.execute("DELETE FROM sales.customers")


def insert_customers(cursor, count):
    rows = []
    for _ in range(count):
        rows.append(
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number()[:50],
                fake.city(),
                fake.province(),
                "Canada",
                random.choice(["Retail", "Premium", "Wholesale", "At Risk"]),
            )
        )

    cursor.executemany(
        """
        INSERT INTO sales.customers
        (first_name, last_name, email, phone, city, state_province, country, customer_segment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def insert_products(cursor, count):
    rows = []
    category_keys = list(CATEGORIES.keys())

    for _ in range(count):
        category = random.choice(category_keys)
        subcategory = random.choice(CATEGORIES[category])
        cost = round(random.uniform(5, 500), 2)
        price = round(cost * random.uniform(1.25, 2.8), 2)

        rows.append(
            (
                fake.catch_phrase()[:255],
                category,
                subcategory,
                random.choice(BRANDS),
                price,
                cost,
            )
        )

    cursor.executemany(
        """
        INSERT INTO sales.products
        (product_name, category, subcategory, brand, unit_price, cost_price)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def fetch_ids(cursor, table, id_col):
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return [row[0] for row in cursor.fetchall()]


def insert_orders(cursor, count, batch_size):
    customer_ids = fetch_ids(cursor, "sales.customers", "customer_id")
    product_rows = []
    cursor.execute("SELECT product_id, unit_price FROM sales.products")
    for row in cursor.fetchall():
        product_rows.append((row[0], float(row[1])))

    if not customer_ids or not product_rows:
        raise ValueError("Customers and products must exist before generating orders.")

    start_date = datetime.now(timezone.utc) - timedelta(days=365)
    orders_created = 0

    while orders_created < count:
        batch_count = min(batch_size, count - orders_created)
        order_rows = []
        order_item_payloads = []
        payment_payloads = []

        for _ in range(batch_count):
            customer_id = random.choice(customer_ids)
            order_date = start_date + timedelta(minutes=random.randint(0, 365 * 24 * 60))
            status = random.choice(ORDER_STATUSES)
            channel = random.choice(CHANNELS)
            payment_method = random.choice(PAYMENT_METHODS)

            selected_items = random.choices(product_rows, k=random.randint(1, 5))
            subtotal = 0.0
            item_details = []

            for product_id, unit_price in selected_items:
                quantity = random.randint(1, 3)
                discount = round(unit_price * quantity * random.choice([0, 0, 0.05, 0.10]), 2)
                line_total = round((unit_price * quantity) - discount, 2)
                subtotal += line_total
                item_details.append((product_id, quantity, unit_price, discount, line_total))

            discount_amount = round(subtotal * random.choice([0, 0, 0.05, 0.10, 0.15]), 2)
            tax_amount = round((subtotal - discount_amount) * 0.13, 2)
            shipping_amount = 0 if subtotal > 100 else round(random.uniform(4.99, 15.99), 2)
            total_amount = 0 if status == "Cancelled" else round(subtotal - discount_amount + tax_amount + shipping_amount, 2)

            order_rows.append(
                (
                    customer_id,
                    order_date,
                    status,
                    channel,
                    payment_method,
                    fake.city(),
                    "Canada",
                    round(subtotal, 2),
                    discount_amount,
                    tax_amount,
                    shipping_amount,
                    total_amount,
                )
            )
            order_item_payloads.append(item_details)
            payment_payloads.append((order_date, payment_method, status, total_amount))

        cursor.executemany(
            """
            INSERT INTO sales.orders
            (customer_id, order_date, order_status, sales_channel, payment_method, shipping_city, shipping_country,
             subtotal_amount, discount_amount, tax_amount, shipping_amount, total_amount)
            OUTPUT INSERTED.order_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            order_rows,
        )

        # pyodbc executemany with OUTPUT can be driver-sensitive, so fetch recent ids separately.
        cursor.execute(f"SELECT TOP ({batch_count}) order_id FROM sales.orders ORDER BY order_id DESC")
        order_ids = [row[0] for row in cursor.fetchall()]
        order_ids.reverse()

        item_rows = []
        payment_rows = []

        for order_id, item_details, payment_detail in zip(order_ids, order_item_payloads, payment_payloads):
            for product_id, quantity, unit_price, discount, line_total in item_details:
                item_rows.append((order_id, product_id, quantity, unit_price, discount, line_total))

            order_date, payment_method, status, total_amount = payment_detail
            payment_status = "Voided" if status == "Cancelled" else ("Refunded" if status == "Returned" else "Captured")
            payment_rows.append(
                (
                    order_id,
                    order_date + timedelta(minutes=random.randint(0, 60)),
                    payment_method,
                    payment_status,
                    total_amount,
                    f"TXN-{uuid.uuid4().hex[:12].upper()}",
                )
            )

        cursor.executemany(
            """
            INSERT INTO sales.order_items
            (order_id, product_id, quantity, unit_price, discount_amount, line_total)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            item_rows,
        )

        cursor.executemany(
            """
            INSERT INTO sales.payments
            (order_id, payment_date, payment_method, payment_status, amount, transaction_reference)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            payment_rows,
        )

        orders_created += batch_count
        print(f"Inserted {orders_created:,}/{count:,} orders")


def main():
    parser = argparse.ArgumentParser(description="Generate Azure SQL OLTP retail source data.")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--orders", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--truncate", action="store_true")
    args = parser.parse_args()

    conn = connect()
    conn.autocommit = False
    cursor = conn.cursor()
    cursor.fast_executemany = True

    try:
        if args.truncate:
            print("Truncating source tables...")
            truncate_source_tables(cursor)
            conn.commit()

        print("Inserting customers...")
        insert_customers(cursor, args.customers)
        conn.commit()

        print("Inserting products...")
        insert_products(cursor, args.products)
        conn.commit()

        print("Inserting orders, order items, and payments...")
        insert_orders(cursor, args.orders, args.batch_size)
        conn.commit()

        print("Source data generation complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()