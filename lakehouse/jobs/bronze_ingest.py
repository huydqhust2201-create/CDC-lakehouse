"""Bronze: Kafka (Debezium topics) -> Delta Lake, append-only raw CDC log.

Run standalone:  python -m lakehouse.jobs.bronze_ingest
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from lakehouse import config
from lakehouse.cdc import BRONZE_SCHEMA, ensure_delta_table, parse_kafka_to_bronze
from lakehouse.config import Paths
from lakehouse.spark_session import get_spark

log = logging.getLogger("bronze_ingest")


def start_bronze_stream(spark: SparkSession) -> StreamingQuery:
    ensure_delta_table(spark, Paths.bronze_events, BRONZE_SCHEMA, partition_by=["source_table", "ingest_date"])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribePattern", config.CDC_TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", config.KAFKA_MAX_OFFSETS_PER_TRIGGER)
        # pick up topics Debezium creates after startup quickly
        .option("kafka.metadata.max.age.ms", "10000")
        .load()
    )

    bronze = parse_kafka_to_bronze(raw)

    query = (
        bronze.writeStream.format("delta")
        .queryName("bronze_cdc_events")
        .outputMode("append")
        .option("checkpointLocation", Paths.checkpoint("bronze_cdc_events"))
        .option("mergeSchema", "true")
        .partitionBy("source_table", "ingest_date")
        .trigger(processingTime=f"{config.STREAM_TRIGGER_SECONDS} seconds")
        .start(Paths.bronze_events)
    )
    log.info("Bronze stream started: %s -> %s", config.CDC_TOPIC_PATTERN, Paths.bronze_events)
    return query


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = get_spark("bronze_ingest")
    start_bronze_stream(spark).awaitTermination()


if __name__ == "__main__":
    main()
