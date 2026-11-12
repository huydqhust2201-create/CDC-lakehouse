"""Pre-flight check for the daily DAG: every Silver table exists and is non-empty.
Exits non-zero so that Airflow retries the task (Silver may still be bootstrapping)."""
from __future__ import annotations

import logging
import sys

from delta.tables import DeltaTable

from lakehouse.config import TABLES, Paths
from lakehouse.spark_session import get_spark

log = logging.getLogger("check_silver")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = get_spark("check_silver")
    missing = []
    for name in TABLES:
        path = Paths.silver(name)
        if not DeltaTable.isDeltaTable(spark, path):
            missing.append(name)
            continue
        dt = DeltaTable.forPath(spark, path)
        last = dt.history(1).select("version", "timestamp").first()
        rows = dt.toDF().count()
        log.info("silver.%s: %d rows, last commit v%s at %s", name, rows, last.version, last.timestamp)
        if rows == 0:
            missing.append(name)
    spark.stop()
    if missing:
        log.error("Silver tables not ready: %s", missing)
        sys.exit(1)


if __name__ == "__main__":
    main()
