"""OLTP workload simulator for the `shop` database.

Continuously produces INSERT / UPDATE / DELETE traffic (captured by Debezium) and,
with probability ``--bad-ratio``, writes records that violate the data-quality
rules so the quarantine path can be observed end to end.

  python generator.py --rate 2 --bad-ratio 0.05
  python generator.py --once 200        # run 200 actions then exit
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import time
from decimal import Decimal

import psycopg2
from faker import Faker

log = logging.getLogger("datagen")
fake = Faker()

CATEGORIES = ["electronics", "computers", "audio", "accessories", "home", "books"]
PAYMENTS = ["card", "cod", "bank_transfer", "e_wallet"]
CITIES = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue", "Nha Trang"]
NEXT_STATUS = {"pending": "paid", "paid": "shipped", "shipped": "delivered"}


def connect():
    while True:
        try:
            conn = psycopg2.connect(
                host=os.environ.get("PGHOST", "localhost"),
                port=int(os.environ.get("PGPORT", "5432")),
                dbname=os.environ.get("PGDATABASE", "shop"),
                user=os.environ.get("PGUSER", "shop"),
                password=os.environ.get("PGPASSWORD", "shop"),
            )
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Postgres not ready (%s), retrying...", exc)
            time.sleep(3)


def column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (table, column)
    )
    return cur.fetchone() is not None


# --------------------------------------------------------------------------- good actions
def new_customer(cur):
    cols = {"full_name": fake.name(), "email": fake.unique.email(), "phone": fake.msisdn()[:15],
            "city": random.choice(CITIES), "status": "active"}
    if column_exists(cur, "customers", "loyalty_tier"):  # appears after the schema-evolution demo
        cols["loyalty_tier"] = random.choice(["bronze", "silver", "gold"])
    keys = ", ".join(cols)
    cur.execute(f"INSERT INTO customers ({keys}) VALUES ({', '.join(['%s'] * len(cols))}) RETURNING customer_id",
                list(cols.values()))
    return f"insert customer {cur.fetchone()[0]}"


def new_product(cur):
    cur.execute(
        "INSERT INTO products (sku, name, category, price) VALUES (%s, %s, %s, %s) RETURNING product_id",
        (f"SKU-{fake.unique.random_int(10000, 99999)}", fake.catch_phrase()[:60],
         random.choice(CATEGORIES), Decimal(random.randint(500, 200000)) / 100),
    )
    return f"insert product {cur.fetchone()[0]}"


def new_order(cur):
    cur.execute("SELECT customer_id FROM customers WHERE status = 'active' ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return new_customer(cur)
    cur.execute(
        "INSERT INTO orders (customer_id, status, payment_method) VALUES (%s, 'pending', %s) RETURNING order_id",
        (row[0], random.choice(PAYMENTS)),
    )
    order_id = cur.fetchone()[0]
    cur.execute("SELECT product_id, price FROM products WHERE is_active AND price > 0 ORDER BY random() LIMIT %s",
                (random.randint(1, 4),))
    for product_id, price in cur.fetchall():
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, random.randint(1, 3), price),
        )
    cur.execute(
        "UPDATE orders SET total_amount = COALESCE((SELECT SUM(quantity * unit_price) FROM order_items "
        "WHERE order_id = %s), 0) WHERE order_id = %s",
        (order_id, order_id),
    )
    return f"insert order {order_id}"


def advance_order(cur):
    cur.execute("SELECT order_id, status FROM orders WHERE status IN ('pending','paid','shipped') "
                "ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return new_order(cur)
    order_id, status = row
    new_status = "cancelled" if status == "pending" and random.random() < 0.15 else NEXT_STATUS[status]
    cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (new_status, order_id))
    return f"update order {order_id}: {status} -> {new_status}"


def update_customer(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return new_customer(cur)
    cur.execute("UPDATE customers SET city = %s, phone = %s WHERE customer_id = %s",
                (random.choice(CITIES), fake.msisdn()[:15], row[0]))
    return f"update customer {row[0]}"


def update_price(cur):
    cur.execute("SELECT product_id, price FROM products WHERE price > 0 ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return new_product(cur)
    factor = Decimal(str(round(random.uniform(0.85, 1.15), 2)))
    cur.execute("UPDATE products SET price = ROUND(price * %s, 2) WHERE product_id = %s", (factor, row[0]))
    return f"update product {row[0]} price x{factor}"


def delete_cancelled_order(cur):
    cur.execute("SELECT order_id FROM orders WHERE status = 'cancelled' ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return advance_order(cur)
    cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))  # cascades to order_items
    return f"delete order {row[0]} (+items)"


# --------------------------------------------------------------------------- bad actions (-> quarantine)
def bad_record(cur):
    # weighted so that no single table gets a disproportionate share of bad rows
    kind = random.choices(
        ["null_email", "bad_email", "dup_email", "neg_price", "bad_status", "zero_qty"],
        weights=[1, 1, 1, 2, 3, 3],
    )[0]
    if kind == "null_email":
        cur.execute("INSERT INTO customers (full_name, email, city) VALUES (%s, NULL, %s) RETURNING customer_id",
                    (fake.name(), random.choice(CITIES)))
    elif kind == "bad_email":
        cur.execute("INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s) RETURNING customer_id",
                    (fake.name(), "not-an-email", random.choice(CITIES)))
    elif kind == "dup_email":
        cur.execute("SELECT email FROM customers WHERE email IS NOT NULL ORDER BY random() LIMIT 1")
        email = (cur.fetchone() or ["customer1@example.com"])[0]
        cur.execute("INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s) RETURNING customer_id",
                    (fake.name(), email, random.choice(CITIES)))
    elif kind == "neg_price":
        cur.execute("INSERT INTO products (sku, name, category, price) VALUES (%s, %s, %s, %s) RETURNING product_id",
                    (f"SKU-BAD-{random.randint(1, 10**6)}", "Broken product", "electronics", Decimal("-9.99")))
    elif kind == "bad_status":
        cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
        cid = (cur.fetchone() or [None])[0]
        cur.execute("INSERT INTO orders (customer_id, status, payment_method) VALUES (%s, 'unknown', 'card') "
                    "RETURNING order_id", (cid,))
    else:  # zero_qty
        cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1")
        oid = (cur.fetchone() or [None])[0]
        cur.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, 1, 0, 10) "
                    "RETURNING order_item_id", (oid,))
    return f"BAD {kind} -> id {cur.fetchone()[0]}"


ACTIONS = [
    (new_order, 35),
    (advance_order, 30),
    (new_customer, 10),
    (update_customer, 8),
    (update_price, 7),
    (new_product, 3),
    (delete_cancelled_order, 7),
]


def step(conn, bad_ratio: float) -> str:
    with conn.cursor() as cur:
        if random.random() < bad_ratio:
            msg = bad_record(cur)
        else:
            fn = random.choices([a for a, _ in ACTIONS], weights=[w for _, w in ACTIONS])[0]
            msg = fn(cur)
    conn.commit()
    return msg


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=2.0, help="actions per second")
    p.add_argument("--bad-ratio", type=float, default=0.05, help="probability of a DQ-violating record")
    p.add_argument("--once", type=int, default=0, help="run N actions then exit (0 = forever)")
    args = p.parse_args()

    conn = connect()
    n = 0
    log.info("Generating workload: %.1f actions/s, bad ratio %.0f%%", args.rate, args.bad_ratio * 100)
    while args.once == 0 or n < args.once:
        try:
            msg = step(conn, args.bad_ratio)
            n += 1
            if n % 20 == 0 or msg.startswith("BAD"):
                log.info("#%d %s", n, msg)
        except psycopg2.InterfaceError:
            conn = connect()
        except psycopg2.Error as exc:
            conn.rollback()
            log.warning("Action failed: %s", exc)
        time.sleep(1.0 / args.rate if args.rate > 0 else 0)


if __name__ == "__main__":
    main()
