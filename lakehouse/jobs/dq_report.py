"""Daily data-quality report (Airflow task).

* ``--check silver``     : run the Great Expectations suites on the full Silver tables.
                           Fails (exit 1) if any expectation fails -> visible in Airflow UI.
* ``--check quarantine`` : summarise rows quarantined on ``--run-date`` per table/rule.
                           Fails if the reject ratio exceeds ``--max-reject-ratio``.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse.audit import write_dq_result
from lakehouse.config import TABLES, Paths
from lakehouse.quality import validate_with_ge
from lakehouse.spark_session import get_spark

log = logging.getLogger("dq_report")


def check_silver(spark, run_date: dt.date) -> bool:
    ok = True
    for name, cfg in TABLES.items():
        path = Paths.silver(name)
        if not DeltaTable.isDeltaTable(spark, path):
            log.warning("silver.%s does not exist yet", name)
            continue
        df = spark.read.format("delta").load(path)
        data = df.drop(*[c for c in df.columns if c.startswith("_")])
        rows = data.count()
        result = validate_with_ge(data, cfg.rules, asset_name=f"daily_{name}")
        write_dq_result(
            spark, layer="silver_daily", table=name, batch_id=run_date.isoformat(),
            rows_checked=rows, rows_valid=rows if result.success else -1, rows_quarantined=0, ge=result,
        )
        status = "PASSED" if result.success else "FAILED"
        log.info("[GE] silver.%s %s (%d/%d expectations, %d rows)",
                 name, status, result.successful, result.evaluated, rows)
        for d in result.details:
            if not d["success"]:
                log.error("   x %s(%s): unexpected=%s sample=%s",
                          d["expectation"], d["column"], d["unexpected_count"], d["partial_unexpected_list"])
        ok = ok and result.success
    return ok


def check_quarantine(spark, run_date: dt.date, max_reject_ratio: float) -> bool:
    if not DeltaTable.isDeltaTable(spark, Paths.quarantine):
        log.info("No quarantine table yet - nothing rejected so far.")
        return True

    q = spark.read.format("delta").load(Paths.quarantine).where(F.col("quarantine_date") == F.lit(run_date))
    by_rule = (
        q.select("source_table", F.explode("failed_rules").alias("rule"))
        .groupBy("source_table", "rule").count().orderBy(F.desc("count"))
    )
    for r in by_rule.collect():
        log.warning("[QUARANTINE] %s  %-30s %d", r.source_table, r.rule, r["count"])

    if not DeltaTable.isDeltaTable(spark, Paths.dq_results):
        return True
    stats = (
        spark.read.format("delta").load(Paths.dq_results)
        .where((F.col("layer") == "silver_stream") & (F.to_date("run_ts") == F.lit(run_date)))
        .groupBy("source_table")
        .agg(F.sum("rows_checked").alias("checked"), F.sum("rows_quarantined").alias("rejected"))
        .collect()
    )
    ok = True
    for s in stats:
        ratio = (s.rejected or 0) / s.checked if s.checked else 0.0
        log.info("[QUARANTINE] %s: %s/%s rejected (%.2f%%)", s.source_table, s.rejected, s.checked, ratio * 100)
        if ratio > max_reject_ratio:
            log.error("Reject ratio for %s above threshold %.2f%%", s.source_table, max_reject_ratio * 100)
            ok = False
    return ok


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--check", choices=["silver", "quarantine"], required=True)
    p.add_argument("--run-date", default=dt.date.today().isoformat())
    p.add_argument("--max-reject-ratio", type=float, default=0.2)
    args = p.parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date)

    spark = get_spark(f"dq_report_{args.check}")
    if args.check == "silver":
        ok = check_silver(spark, run_date)
    else:
        ok = check_quarantine(spark, run_date, args.max_reject_ratio)
    spark.stop()
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
