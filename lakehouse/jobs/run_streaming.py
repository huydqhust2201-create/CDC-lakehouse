"""Run Bronze and Silver streaming queries in a single Spark application
(saves memory on a laptop; each job can also be run on its own)."""
from __future__ import annotations

import logging

from lakehouse.jobs.bronze_ingest import start_bronze_stream
from lakehouse.jobs.silver_merge import start_silver_stream
from lakehouse.spark_session import get_spark

log = logging.getLogger("run_streaming")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = get_spark("cdc_lakehouse_streaming")

    bronze = start_bronze_stream(spark)  # also creates the Bronze table if missing
    silver = start_silver_stream(spark)
    log.info("Streaming queries running: %s, %s", bronze.name, silver.name)

    # Fail fast: if any query dies the container exits and Docker restarts it,
    # resuming from the checkpoints (exactly-once into Delta).
    spark.streams.awaitAnyTermination()
    for q in spark.streams.active:
        q.stop()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
