"""Silver: Bronze CDC log -> clean, current-state Delta tables.

For every micro-batch and every source table:
  1. parse Debezium payloads (schema inferred per batch -> schema evolution)
  2. keep the latest change per primary key
  3. cast to the configured types / align with the existing Silver schema
  4. Data Quality: Great Expectations report + row-level rules
     -> invalid rows are written to the quarantine table, never to Silver
  5. MERGE INTO Silver (insert / update / delete)

Quarantine rows and DQ audit rows of all tables are written once per micro-batch
(one Delta commit each) to keep the per-batch overhead low.

Run standalone:  python -m lakehouse.jobs.silver_merge
"""
from __future__ import annotations

import logging
import time
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from lakehouse import config
from lakehouse.audit import dq_result_row, write_dq_results
from lakehouse.cdc import (
    META_COLUMNS,
    apply_column_types,
    events_to_records,
    latest_per_key,
    merge_into_silver,
)
from lakehouse.config import TABLES, Paths, TableConfig
from lakehouse.quality import split_valid_invalid, validate_with_ge
from lakehouse.quality.rules import DQ_FAILURES_COL
from lakehouse.spark_session import get_spark

log = logging.getLogger("silver_merge")


def to_quarantine_rows(invalid: DataFrame, cfg: TableConfig, batch_id: int) -> DataFrame:
    """Normalise rejected rows of any table to the common quarantine schema."""
    data_cols = [c for c in invalid.columns if c not in META_COLUMNS and c != DQ_FAILURES_COL]
    return invalid.select(
        F.lit(cfg.name).alias("source_table"),
        F.col(cfg.primary_key).cast("string").alias("record_key"),
        F.to_json(F.struct(*data_cols)).alias("record"),
        F.col(DQ_FAILURES_COL).alias("failed_rules"),
        F.col("_cdc_op").alias("cdc_op"),
        F.col("_cdc_lsn").alias("cdc_lsn"),
        F.lit(str(batch_id)).alias("batch_id"),
        F.current_timestamp().alias("quarantined_at"),
    ).withColumn("quarantine_date", F.to_date("quarantined_at"))


def write_quarantine(frames: list[DataFrame]) -> None:
    if not frames:
        return
    (
        reduce(lambda a, b: a.unionByName(b), frames)
        .coalesce(1)
        .write.format("delta")
        .mode("append")
        .partitionBy("source_table", "quarantine_date")
        .save(Paths.quarantine)
    )


def process_table(
    spark: SparkSession, events: DataFrame, cfg: TableConfig, batch_id: int
) -> tuple[DataFrame | None, tuple | None]:
    """Returns (quarantine rows already materialised or None, audit row)."""
    records = events_to_records(spark, events)
    if records is None:
        return None, None

    silver_path = Paths.silver(cfg.name)
    target_df = spark.read.format("delta").load(silver_path) if DeltaTable.isDeltaTable(spark, silver_path) else None

    latest = latest_per_key(records, cfg.primary_key)
    latest = apply_column_types(latest, cfg, target_df.schema if target_df is not None else None)
    latest = latest.localCheckpoint(eager=True)  # cut the lineage: JSON parsing runs once

    deletes = latest.where(F.col("_cdc_op") == "d")
    upserts = latest.where(F.col("_cdc_op") != "d")
    n_upserts = upserts.count()

    ge_result = None
    if n_upserts > 0 and config.DQ_GE_ENABLED:
        try:
            ge_result = validate_with_ge(
                upserts.drop(*META_COLUMNS), cfg.rules, asset_name=f"silver_{cfg.name}", row_count=n_upserts
            )
            if not ge_result.success:
                failed = [f"{d['expectation']}({d['column']})={d['unexpected_count']}"
                          for d in ge_result.details if not d["success"]]
                log.warning("[GE] %s batch %s failed: %s", cfg.name, batch_id, ", ".join(failed))
        except Exception:  # GE must never stop the pipeline; row rules still apply
            log.exception("[GE] validation error for %s batch %s", cfg.name, batch_id)

    valid, invalid = split_valid_invalid(upserts, cfg.rules, cfg.primary_key, existing=target_df)
    invalid = invalid.localCheckpoint(eager=True)
    n_invalid = invalid.count()

    merge_source = valid.unionByName(deletes, allowMissingColumns=True)
    merge_into_silver(spark, merge_source, silver_path, cfg.primary_key)

    audit = dq_result_row(
        layer="silver_stream", table=cfg.name, batch_id=str(batch_id),
        rows_checked=n_upserts, rows_valid=n_upserts - n_invalid, rows_quarantined=n_invalid, ge=ge_result,
    )
    quarantine = to_quarantine_rows(invalid, cfg, batch_id) if n_invalid > 0 else None
    return quarantine, audit


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    started = time.time()
    batch_df = batch_df.persist()
    try:
        if batch_df.isEmpty():
            return
        tables = sorted(r.source_table for r in batch_df.select("source_table").distinct().collect() if r.source_table)
        quarantine_frames, audit_rows = [], []
        for table in tables:
            cfg = TABLES.get(table)
            if cfg is None:
                log.info("Skipping unknown table %s", table)
                continue
            q, a = process_table(spark, batch_df.where(F.col("source_table") == table), cfg, batch_id)
            if q is not None:
                quarantine_frames.append(q)
            if a is not None:
                audit_rows.append(a)
        write_quarantine(quarantine_frames)
        write_dq_results(spark, audit_rows)
        log.info("Silver batch %s done in %.1fs (tables=%s, quarantined=%d)", batch_id, time.time() - started,
                 tables, sum(a[6] for a in audit_rows))
    finally:
        batch_df.unpersist()


def start_silver_stream(spark: SparkSession) -> StreamingQuery:
    query = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 500)
        .load(Paths.bronze_events)
        .writeStream.queryName("silver_merge")
        .foreachBatch(process_batch)
        .option("checkpointLocation", Paths.checkpoint("silver_merge"))
        .trigger(processingTime=f"{config.STREAM_TRIGGER_SECONDS} seconds")
        .start()
    )
    log.info("Silver stream started: %s -> %s/silver/*", Paths.bronze_events, Paths.root)
    return query


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = get_spark("silver_merge")
    while not DeltaTable.isDeltaTable(spark, Paths.bronze_events):
        log.info("Waiting for Bronze table %s ...", Paths.bronze_events)
        time.sleep(10)
    start_silver_stream(spark).awaitTermination()


if __name__ == "__main__":
    main()
