"""Central configuration: storage paths, Kafka settings, table metadata and data-quality rules."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# --------------------------------------------------------------------------- infrastructure
KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
CDC_TOPIC_PREFIX = _env("CDC_TOPIC_PREFIX", "cdc")
CDC_TOPIC_PATTERN = rf"{CDC_TOPIC_PREFIX}\.public\..*"
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(_env("KAFKA_MAX_OFFSETS_PER_TRIGGER", "20000"))
STREAM_TRIGGER_SECONDS = int(_env("STREAM_TRIGGER_SECONDS", "30"))

S3_ENDPOINT = _env("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "minioadmin")
LAKEHOUSE_BUCKET = _env("LAKEHOUSE_BUCKET", "lakehouse")

# Override with e.g. LAKEHOUSE_ROOT=/tmp/lakehouse to run everything on the local filesystem.
LAKEHOUSE_ROOT = _env("LAKEHOUSE_ROOT", f"s3a://{LAKEHOUSE_BUCKET}")

DQ_GE_ENABLED = _env("DQ_GE_ENABLED", "true").lower() in ("1", "true", "yes")


class Paths:
    """Physical layout of the lakehouse."""

    root = LAKEHOUSE_ROOT
    bronze_events = f"{root}/bronze/cdc_events"
    quarantine = f"{root}/quarantine/cdc_rejects"
    dq_results = f"{root}/audit/dq_results"
    checkpoints = f"{root}/_checkpoints"

    @classmethod
    def silver(cls, table: str) -> str:
        return f"{cls.root}/silver/{table}"

    @classmethod
    def gold(cls, table: str) -> str:
        return f"{cls.root}/gold/{table}"

    @classmethod
    def checkpoint(cls, name: str) -> str:
        return f"{cls.checkpoints}/{name}"


# --------------------------------------------------------------------------- data quality rules
@dataclass(frozen=True)
class Rule:
    """A declarative DQ rule. Translated both into a Great Expectations expectation
    (batch-level validation report) and a Spark predicate (row-level quarantine)."""

    kind: str  # not_null | unique | in_set | between | regex
    column: str
    values: tuple = ()
    min_value: float | None = None
    max_value: float | None = None
    pattern: str | None = None

    @property
    def name(self) -> str:
        return f"{self.kind}:{self.column}"


def not_null(*cols: str) -> list[Rule]:
    return [Rule("not_null", c) for c in cols]


def unique(*cols: str) -> list[Rule]:
    return [Rule("unique", c) for c in cols]


def in_set(col: str, values: list[str]) -> Rule:
    return Rule("in_set", col, values=tuple(values))


def between(col: str, min_value: float | None = None, max_value: float | None = None) -> Rule:
    return Rule("between", col, min_value=min_value, max_value=max_value)


def regex(col: str, pattern: str) -> Rule:
    return Rule("regex", col, pattern=pattern)


# --------------------------------------------------------------------------- table metadata
@dataclass(frozen=True)
class TableConfig:
    name: str
    primary_key: str
    # Explicit Spark types for columns whose JSON representation is ambiguous
    # (Debezium sends NUMERIC as string and TIMESTAMPTZ as ISO-8601 string).
    column_types: dict = field(default_factory=dict)
    rules: list = field(default_factory=list)


ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]
REVENUE_STATUSES = ["paid", "shipped", "delivered"]
CUSTOMER_STATUSES = ["active", "inactive", "banned"]
PRODUCT_CATEGORIES = ["electronics", "computers", "audio", "accessories", "home", "books"]
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

TABLES: dict[str, TableConfig] = {
    "customers": TableConfig(
        name="customers",
        primary_key="customer_id",
        column_types={
            "customer_id": "bigint",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
        rules=[
            *not_null("customer_id", "full_name", "email"),
            *unique("customer_id", "email"),
            regex("email", EMAIL_REGEX),
            in_set("status", CUSTOMER_STATUSES),
        ],
    ),
    "products": TableConfig(
        name="products",
        primary_key="product_id",
        column_types={
            "product_id": "bigint",
            "price": "decimal(12,2)",
            "is_active": "boolean",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
        rules=[
            *not_null("product_id", "sku", "name", "price"),
            *unique("product_id", "sku"),
            between("price", 0, 1_000_000),
            in_set("category", PRODUCT_CATEGORIES),
        ],
    ),
    "orders": TableConfig(
        name="orders",
        primary_key="order_id",
        column_types={
            "order_id": "bigint",
            "customer_id": "bigint",
            "total_amount": "decimal(14,2)",
            "order_ts": "timestamp",
            "updated_at": "timestamp",
        },
        rules=[
            *not_null("order_id", "customer_id", "status", "order_ts"),
            *unique("order_id"),
            in_set("status", ORDER_STATUSES),
            between("total_amount", 0, 100_000_000),
        ],
    ),
    "order_items": TableConfig(
        name="order_items",
        primary_key="order_item_id",
        column_types={
            "order_item_id": "bigint",
            "order_id": "bigint",
            "product_id": "bigint",
            "quantity": "int",
            "unit_price": "decimal(12,2)",
            "created_at": "timestamp",
        },
        rules=[
            *not_null("order_item_id", "order_id", "product_id", "quantity", "unit_price"),
            *unique("order_item_id"),
            between("quantity", 1, 1000),
            between("unit_price", 0, 1_000_000),
        ],
    ),
}

GOLD_TABLES = ["daily_sales", "customer_ltv", "product_performance"]
