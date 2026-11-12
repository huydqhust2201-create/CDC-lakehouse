"""Daily batch orchestration for the CDC Lakehouse.

check_silver ─┬─> dq_silver_report ─┬─> gold_daily_sales ──────────┐
              │                     ├─> gold_product_performance ──┼─> gold_maintenance
              │                     └─> gold_customer_ltv ─────────┘
              └─> dq_quarantine_report

* Bronze/Silver are fed continuously by the Spark streaming container.
* Each task runs a PySpark job (local mode) inside the Airflow worker.
* Retries with exponential back-off; failures are logged by ``on_failure_callback``
  and visible in the Airflow UI (Grid / Graph / task logs).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

PY = "cd /opt/lakehouse && python -m"


def notify_failure(context) -> None:
    ti = context["task_instance"]
    log.error(
        "Task failed: dag=%s task=%s run=%s try=%s log=%s exception=%s",
        ti.dag_id, ti.task_id, context.get("run_id"), ti.try_number, ti.log_url, context.get("exception"),
    )


def notify_retry(context) -> None:
    ti = context["task_instance"]
    log.warning("Retrying task %s (try %s): %s", ti.task_id, ti.try_number, context.get("exception"))


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
}

with DAG(
    dag_id="lakehouse_daily",
    description="Silver DQ checks + Gold aggregates for the CDC lakehouse",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="0 1 * * *",  # every day at 01:00 UTC
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # each task is a Spark driver JVM - keep memory bounded (8 GB laptop)
    tags=["lakehouse", "gold", "data-quality"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    check_silver = BashOperator(
        task_id="check_silver",
        bash_command=f"{PY} lakehouse.jobs.check_silver",
        retries=5,
        retry_delay=timedelta(minutes=1),
    )

    dq_silver = BashOperator(
        task_id="dq_silver_report",
        bash_command=f"{PY} lakehouse.jobs.dq_report --check silver --run-date {{{{ ds }}}}",
    )

    dq_quarantine = BashOperator(
        task_id="dq_quarantine_report",
        bash_command=f"{PY} lakehouse.jobs.dq_report --check quarantine --run-date {{{{ ds }}}} --max-reject-ratio 0.2",
        retries=0,
    )

    gold_tasks = [
        BashOperator(
            task_id=f"gold_{table}",
            bash_command=(
                f"{PY} lakehouse.jobs.gold_aggregate --table {table} "
                "--run-date {{ ds }} --lookback-days {{ params.lookback_days }}"
            ),
            params={"lookback_days": 3},
        )
        for table in ("daily_sales", "product_performance", "customer_ltv")
    ]

    maintenance = BashOperator(
        task_id="gold_maintenance",
        bash_command=f"{PY} lakehouse.jobs.maintenance --retain-hours 168",
        trigger_rule="all_success",
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    start >> check_silver >> [dq_silver, dq_quarantine]
    dq_silver >> gold_tasks >> maintenance >> end
    dq_quarantine >> end


with DAG(
    dag_id="lakehouse_gold_full_refresh",
    description="Manual full rebuild of every Gold table",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "gold", "manual"],
) as full_refresh_dag:
    BashOperator(
        task_id="gold_full_refresh",
        bash_command=f"{PY} lakehouse.jobs.gold_aggregate --table all --full-refresh",
    )
