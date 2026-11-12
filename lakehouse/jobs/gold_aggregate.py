"""Gold: batch aggregates built from Silver (orchestrated by Airflow).

Usage:
  python -m lakehouse.jobs.gold_aggregate --table daily_sales --run-date 2026-11-12
  python -m lakehouse.jobs.gold_aggregate --table all --full-refresh

daily_sales / product_performance are rebuilt incrementally for the window
[run_date - lookback_days, run_date] using Delta ``replaceWhere`` (late CDC
updates for recent days are picked up). customer_ltv is always fully rebuilt.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import GOLD_TABLES, REVENUE_STATUSES, Paths
from lakehouse.spark_session import get_spark

log = logging.getLogger("gold_aggregate")


def silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("delta").load(Paths.silver(table))


def is_revenue() -> F.Column:
    return F.col("status").isin(*REVENUE_STATUSES)


# --------------------------------------------------------------------------- builders
def build_daily_sales(spark: SparkSession) -> DataFrame:
    orders = silver(spark, "orders").withColumn("order_date", F.to_date("order_ts"))
    return orders.groupBy("order_date").agg(
        F.count("*").alias("total_orders"),
        F.sum(F.when(is_revenue(), 1).otherwise(0)).alias("completed_orders"),
        F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).alias("pending_orders"),
        F.coalesce(F.sum(F.when(is_revenue(), F.col("total_amount"))), F.lit(0)).cast("decimal(18,2)").alias("gross_revenue"),
        F.avg(F.when(is_revenue(), F.col("total_amount"))).cast("decimal(18,2)").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
    ).withColumn(
        "cancellation_rate", F.round(F.col("cancelled_orders") / F.col("total_orders"), 4)
    ).withColumn("_gold_updated_at", F.current_timestamp())


def build_product_performance(spark: SparkSession) -> DataFrame:
    orders = silver(spark, "orders").where(is_revenue()).select(
        "order_id", F.to_date("order_ts").alias("order_date")
    )
    items = silver(spark, "order_items")
    products = silver(spark, "products").select("product_id", "sku", "name", "category")
    return (
        items.join(orders, "order_id")
        .join(products, "product_id", "left")
        .groupBy("order_date", "product_id", "sku", "name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("orders_count"),
            F.sum(F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)").alias("revenue"),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def build_customer_ltv(spark: SparkSession) -> DataFrame:
    customers = silver(spark, "customers").select("customer_id", "full_name", "email", "city", "status")
    orders = silver(spark, "orders").where(is_revenue())
    agg = orders.groupBy("customer_id").agg(
        F.count("*").alias("total_orders"),
        F.sum("total_amount").cast("decimal(18,2)").alias("lifetime_value"),
        F.avg("total_amount").cast("decimal(18,2)").alias("avg_order_value"),
        F.min("order_ts").alias("first_order_ts"),
        F.max("order_ts").alias("last_order_ts"),
    )
    ltv = F.coalesce(F.col("lifetime_value"), F.lit(0))
    return (
        customers.join(agg, "customer_id", "left")
        .fillna({"total_orders": 0})
        .withColumn("lifetime_value", ltv.cast("decimal(18,2)"))
        .withColumn("days_since_last_order", F.datediff(F.current_date(), F.to_date("last_order_ts")))
        .withColumn(
            "segment",
            F.when(F.col("total_orders") == 0, "prospect")
            .when(ltv >= 20000, "vip")
            .when(ltv >= 5000, "loyal")
            .when(F.col("days_since_last_order") > 30, "at_risk")
            .otherwise("regular"),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


BUILDERS = {
    "daily_sales": (build_daily_sales, "order_date"),
    "product_performance": (build_product_performance, "order_date"),
    "customer_ltv": (build_customer_ltv, None),
}


# --------------------------------------------------------------------------- writer
def write_gold(spark: SparkSession, table: str, run_date: dt.date, lookback_days: int, full_refresh: bool) -> int:
    builder, date_col = BUILDERS[table]
    path = Paths.gold(table)
    df = builder(spark)

    incremental = date_col is not None and not full_refresh and DeltaTable.isDeltaTable(spark, path)
    if incremental:
        start = run_date - dt.timedelta(days=lookback_days)
        predicate = f"{date_col} >= '{start}' AND {date_col} <= '{run_date}'"
        df = df.where(predicate)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", predicate)
            .option("mergeSchema", "true")
            .save(path)
        )
        log.info("Gold %s: replaced window %s", table, predicate)
    else:
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if date_col:
            writer = writer.partitionBy(date_col)
        writer.save(path)
        log.info("Gold %s: full refresh", table)

    rows = spark.read.format("delta").load(path).count()
    log.info("Gold %s now has %d rows", table, rows)
    return rows


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--table", default="all", choices=["all", *GOLD_TABLES])
    p.add_argument("--run-date", default=dt.date.today().isoformat(), help="logical date (Airflow {{ ds }})")
    p.add_argument("--lookback-days", type=int, default=3)
    p.add_argument("--full-refresh", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date)
    spark = get_spark(f"gold_{args.table}")
    tables = GOLD_TABLES if args.table == "all" else [args.table]
    for t in tables:
        write_gold(spark, t, run_date, args.lookback_days, args.full_refresh)
    spark.stop()


if __name__ == "__main__":
    main()
