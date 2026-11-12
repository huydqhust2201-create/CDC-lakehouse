"""Print a quick overview of the lakehouse (row counts, schemas, samples).

  docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse
  docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse --table silver/customers
"""
from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse.config import GOLD_TABLES, TABLES, Paths
from lakehouse.spark_session import get_spark


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--table", help="relative path e.g. silver/orders, gold/daily_sales, quarantine/cdc_rejects")
    p.add_argument("--limit", type=int, default=10)
    args = p.parse_args(argv)

    spark = get_spark("inspect", master="local[1]")
    if args.table:
        df = spark.read.format("delta").load(f"{Paths.root}/{args.table}")
        df.printSchema()
        print(f"rows = {df.count()}")
        df.show(args.limit, truncate=60)
        DeltaTable.forPath(spark, f"{Paths.root}/{args.table}").history(5).select(
            "version", "timestamp", "operation", "operationMetrics"
        ).show(truncate=80)
        return

    paths = (
        [("bronze/cdc_events", Paths.bronze_events)]
        + [(f"silver/{t}", Paths.silver(t)) for t in TABLES]
        + [(f"gold/{t}", Paths.gold(t)) for t in GOLD_TABLES]
        + [("quarantine/cdc_rejects", Paths.quarantine), ("audit/dq_results", Paths.dq_results)]
    )
    print(f"{'table':<30}{'rows':>10}{'version':>10}")
    for name, path in paths:
        if DeltaTable.isDeltaTable(spark, path):
            dt = DeltaTable.forPath(spark, path)
            print(f"{name:<30}{dt.toDF().count():>10}{dt.history(1).first().version:>10}")
        else:
            print(f"{name:<30}{'-':>10}{'-':>10}")

    if DeltaTable.isDeltaTable(spark, Paths.bronze_events):
        print("\nBronze events by table/op:")
        spark.read.format("delta").load(Paths.bronze_events).groupBy("source_table").pivot(
            "op", ["r", "c", "u", "d"]
        ).count().orderBy("source_table").show()
    if DeltaTable.isDeltaTable(spark, Paths.quarantine):
        print("Quarantine by rule:")
        spark.read.format("delta").load(Paths.quarantine).select(
            "source_table", F.explode("failed_rules").alias("rule")
        ).groupBy("source_table", "rule").count().orderBy(F.desc("count")).show(truncate=False)


if __name__ == "__main__":
    main()
