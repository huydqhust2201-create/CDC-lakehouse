from pyspark.sql import functions as F

from lakehouse.config import TABLES, between, in_set, not_null, regex, unique, EMAIL_REGEX
from lakehouse.quality.rules import DQ_FAILURES_COL, split_valid_invalid


def test_row_level_rules_split_valid_and_invalid(spark):
    df = spark.createDataFrame(
        [
            (1, "Alice", "alice@example.com", "active"),
            (2, "Bob", None, "active"),                 # not_null:email
            (3, "Carol", "not-an-email", "active"),     # regex:email
            (4, "Dan", "dan@example.com", "weird"),     # in_set:status
            (5, "Eve", "dup@example.com", "active"),    # first owner of the email -> valid
            (6, "Frank", "dup@example.com", "active"),  # unique:email (in batch duplicate)
        ],
        "customer_id long, full_name string, email string, status string",
    )
    rules = [*not_null("customer_id", "email"), *unique("email"), regex("email", EMAIL_REGEX),
             in_set("status", ["active", "inactive"])]

    valid, invalid = split_valid_invalid(df, rules, "customer_id")

    assert sorted(r.customer_id for r in valid.collect()) == [1, 5]
    failures = {r.customer_id: set(r[DQ_FAILURES_COL]) for r in invalid.collect()}
    assert failures[2] == {"not_null:email"}
    assert failures[3] == {"regex:email"}
    assert failures[4] == {"in_set:status"}
    assert failures[6] == {"unique:email"}
    assert 5 not in failures


def test_unique_checked_against_existing_silver_rows(spark):
    existing = spark.createDataFrame([(1, "a@x.com"), (2, "b@x.com")], "customer_id long, email string")
    batch = spark.createDataFrame(
        [(1, "a@x.com"),   # same entity updated -> not a duplicate
         (3, "b@x.com")],  # new customer reusing customer 2's email -> duplicate
        "customer_id long, email string",
    )
    valid, invalid = split_valid_invalid(batch, unique("email"), "customer_id", existing=existing)
    assert [r.customer_id for r in valid.collect()] == [1]
    assert [r.customer_id for r in invalid.collect()] == [3]


def test_between_ignores_nulls_like_great_expectations(spark):
    df = spark.createDataFrame([(1, 5), (2, None), (3, 0)], "id long, quantity int")
    valid, invalid = split_valid_invalid(df, [between("quantity", 1, 1000)], "id")
    assert sorted(r.id for r in valid.collect()) == [1, 2]
    assert [r.id for r in invalid.collect()] == [3]


def test_every_configured_table_has_rules():
    for cfg in TABLES.values():
        assert any(r.kind == "not_null" and r.column == cfg.primary_key for r in cfg.rules)


def test_great_expectations_report_on_micro_batch(spark):
    from lakehouse.quality import validate_with_ge

    df = spark.createDataFrame(
        [(1, "a@x.com", "10.50"), (2, None, "-1.00"), (3, "c@x.com", "5.00")],
        "id long, email string, price string",
    ).withColumn("price", F.col("price").cast("decimal(12,2)"))
    rules = [*not_null("id", "email"), *unique("id"), between("price", 0, 100)]

    result = validate_with_ge(df, rules, asset_name="test_products")

    assert result.engine == "pandas"
    assert result.success is False
    failed = {(d["expectation"], d["column"]): d["unexpected_count"] for d in result.details if not d["success"]}
    assert failed == {
        ("expect_column_values_to_not_be_null", "email"): 1,
        ("expect_column_values_to_be_between", "price"): 1,
    }
