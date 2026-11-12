"""Delta maintenance for Gold tables: OPTIMIZE (compaction) + VACUUM.

Bronze/Silver are written continuously by the streaming job; compacting them
concurrently would conflict with the streaming MERGE, so only Gold (written
exclusively by Airflow) is maintained here.
"""
from __future__ import annotations

import argparse
import logging

from delta.tables import DeltaTable

from lakehouse.config import GOLD_TABLES, Paths
from lakehouse.spark_session import get_spark

log = logging.getLogger("maintenance")


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--retain-hours", type=int, default=168)
    args = p.parse_args(argv)

    spark = get_spark("gold_maintenance")
    for t in GOLD_TABLES:
        path = Paths.gold(t)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        table = DeltaTable.forPath(spark, path)
        metrics = table.optimize().executeCompaction().select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        table.vacuum(args.retain_hours)
        log.info("gold.%s optimized (added=%s removed=%s), vacuumed (retain %sh)",
                 t, metrics[0], metrics[1], args.retain_hours)
    spark.stop()


if __name__ == "__main__":
    main()
