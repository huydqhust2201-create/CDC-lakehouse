"""Persist data-quality results to the ``audit/dq_results`` Delta table."""
from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from lakehouse.config import Paths
from lakehouse.quality.ge_validator import GEResult

log = logging.getLogger(__name__)

DQ_RESULTS_SCHEMA = T.StructType(
    [
        T.StructField("run_ts", T.TimestampType()),
        T.StructField("layer", T.StringType()),
        T.StructField("source_table", T.StringType()),
        T.StructField("batch_id", T.StringType()),
        T.StructField("rows_checked", T.LongType()),
        T.StructField("rows_valid", T.LongType()),
        T.StructField("rows_quarantined", T.LongType()),
        T.StructField("ge_success", T.BooleanType()),
        T.StructField("ge_evaluated", T.IntegerType()),
        T.StructField("ge_unsuccessful", T.IntegerType()),
        T.StructField("ge_details", T.StringType()),
    ]
)


def dq_result_row(
    *,
    layer: str,
    table: str,
    batch_id: str,
    rows_checked: int,
    rows_valid: int,
    rows_quarantined: int,
    ge: GEResult | None,
) -> tuple:
    log.info(
        "[DQ] layer=%s table=%s batch=%s checked=%d valid=%d quarantined=%d ge_success=%s ge_engine=%s",
        layer, table, batch_id, rows_checked, rows_valid, rows_quarantined,
        None if ge is None else ge.success, None if ge is None else ge.engine,
    )
    return (
        dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
        layer,
        table,
        str(batch_id),
        int(rows_checked),
        int(rows_valid),
        int(rows_quarantined),
        None if ge is None else ge.success,
        None if ge is None else ge.evaluated,
        None if ge is None else ge.unsuccessful,
        None if ge is None else ge.to_json(),
    )


def write_dq_results(spark: SparkSession, rows: list[tuple]) -> None:
    """Append several DQ results in ONE Delta commit."""
    if not rows:
        return
    (
        spark.createDataFrame(rows, DQ_RESULTS_SCHEMA)
        .coalesce(1)
        .write.format("delta")
        .mode("append")
        .partitionBy("layer")
        .save(Paths.dq_results)
    )


def write_dq_result(spark: SparkSession, **kwargs) -> None:
    write_dq_results(spark, [dq_result_row(**kwargs)])
