"""Helpers for turning Debezium change events into Delta Lake tables.

Debezium envelope (JSON converter, schemas disabled)::

    {"before": {...} | null, "after": {...} | null,
     "source": {"table": "orders", "lsn": 123, "ts_ms": 1700000000000, ...},
     "op": "c" | "u" | "d" | "r", "ts_ms": 1700000000123}
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import TableConfig

CDC_OPS = ("c", "r", "u", "d")
META_COLUMNS = ["_cdc_op", "_cdc_lsn", "_cdc_ts_ms"]

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("topic", T.StringType()),
        T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("kafka_offset", T.LongType()),
        T.StructField("kafka_ts", T.TimestampType()),
        T.StructField("source_table", T.StringType()),
        T.StructField("op", T.StringType()),
        T.StructField("source_lsn", T.LongType()),
        T.StructField("source_ts_ms", T.LongType()),
        T.StructField("event_ts_ms", T.LongType()),
        T.StructField("key_json", T.StringType()),
        T.StructField("before_json", T.StringType()),
        T.StructField("after_json", T.StringType()),
        T.StructField("value_json", T.StringType()),
        T.StructField("ingested_at", T.TimestampType()),
        T.StructField("ingest_date", T.DateType()),
    ]
)


# --------------------------------------------------------------------------- Bronze
def parse_kafka_to_bronze(raw: DataFrame) -> DataFrame:
    """Kafka records -> Bronze rows. Payloads are kept as raw JSON strings so that
    Bronze never breaks when the source schema changes."""
    value = F.col("value").cast("string")
    df = raw.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key_json"),
        value.alias("value_json"),
    ).where(F.col("value_json").isNotNull())  # drop tombstones

    j = F.col("value_json")
    return df.select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        F.get_json_object(j, "$.source.table").alias("source_table"),
        F.get_json_object(j, "$.op").alias("op"),
        F.get_json_object(j, "$.source.lsn").cast("long").alias("source_lsn"),
        F.get_json_object(j, "$.source.ts_ms").cast("long").alias("source_ts_ms"),
        F.get_json_object(j, "$.ts_ms").cast("long").alias("event_ts_ms"),
        "key_json",
        F.get_json_object(j, "$.before").alias("before_json"),
        F.get_json_object(j, "$.after").alias("after_json"),
        "value_json",
        F.current_timestamp().alias("ingested_at"),
        F.current_date().alias("ingest_date"),
    )


def ensure_delta_table(spark: SparkSession, path: str, schema: T.StructType, partition_by: list[str] | None = None) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return
    writer = spark.createDataFrame([], schema).write.format("delta").mode("ignore")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


# --------------------------------------------------------------------------- Silver
def events_to_records(spark: SparkSession, events: DataFrame) -> DataFrame | None:
    """Bronze rows of ONE table -> typed-by-inference records with CDC metadata.

    The record schema is inferred from the JSON payloads of the micro-batch, so a
    column added at the source automatically appears here (schema evolution).
    For deletes the ``before`` image is used (REPLICA IDENTITY FULL)."""
    record = F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json"))
    wrapped = (
        events.where(F.col("op").isin(*CDC_OPS))
        .withColumn("record_json", record)
        .where(F.col("record_json").isNotNull())
        .select(
            F.concat(
                F.lit('{"_cdc_op":"'), F.col("op"),
                F.lit('","_cdc_lsn":'), F.coalesce(F.col("source_lsn").cast("string"), F.lit("0")),
                F.lit(',"_cdc_ts_ms":'), F.coalesce(F.col("source_ts_ms").cast("string"), F.lit("0")),
                F.lit(',"_kafka_partition":'), F.col("kafka_partition").cast("string"),
                F.lit(',"_kafka_offset":'), F.col("kafka_offset").cast("string"),
                F.lit(',"data":'), F.col("record_json"),
                F.lit("}"),
            ).alias("j")
        )
    )
    if wrapped.isEmpty():
        return None

    parsed = spark.read.json(wrapped.rdd.map(lambda r: r.j))
    if "data" not in parsed.columns:
        return None
    return parsed.select(
        "_cdc_op", "_cdc_lsn", "_cdc_ts_ms", "_kafka_partition", "_kafka_offset", "data.*"
    )


def latest_per_key(records: DataFrame, primary_key: str) -> DataFrame:
    """Keep only the last change per primary key inside a micro-batch (by LSN, then offset)."""
    w = Window.partitionBy(primary_key).orderBy(
        F.col("_cdc_lsn").desc(), F.col("_kafka_partition").desc(), F.col("_kafka_offset").desc()
    )
    return (
        records.where(F.col(primary_key).isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "_kafka_partition", "_kafka_offset")
    )


def apply_column_types(df: DataFrame, cfg: TableConfig, target_schema: T.StructType | None = None) -> DataFrame:
    """Cast columns to the configured types, then align any remaining column with
    the existing Silver schema so type inference drift never breaks the MERGE."""
    for col_name, spark_type in cfg.column_types.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
    df = df.withColumn("_cdc_lsn", F.col("_cdc_lsn").cast("long")).withColumn(
        "_cdc_ts_ms", F.col("_cdc_ts_ms").cast("long")
    )
    if target_schema is not None:
        target_types = {f.name: f.dataType for f in target_schema.fields}
        for f in df.schema.fields:
            tgt = target_types.get(f.name)
            if tgt is not None and tgt != f.dataType:
                df = df.withColumn(f.name, F.col(f.name).cast(tgt))
    return df


def merge_into_silver(spark: SparkSession, source: DataFrame, path: str, primary_key: str) -> None:
    """Upsert/delete a micro-batch into a Silver Delta table with MERGE INTO.

    * op 'd'        -> DELETE matched row
    * op c/r/u      -> UPDATE matched row / INSERT new row
    * ``_cdc_lsn`` guard makes the merge idempotent and safe against replays.
    * New source columns are added automatically (schema.autoMerge.enabled).
    """
    source = source.withColumn("_silver_updated_at", F.current_timestamp())
    if not DeltaTable.isDeltaTable(spark, path):
        source.limit(0).write.format("delta").save(path)

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), f"t.{primary_key} = s.{primary_key}")
        .whenMatchedDelete(condition="s._cdc_op = 'd' AND s._cdc_lsn >= t._cdc_lsn")
        .whenMatchedUpdateAll(condition="s._cdc_op <> 'd' AND s._cdc_lsn >= t._cdc_lsn")
        .whenNotMatchedInsertAll(condition="s._cdc_op <> 'd'")
        .execute()
    )
