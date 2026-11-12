"""Great Expectations (GX Core 1.x) validation of Spark DataFrames.

The same declarative rules used for row-level quarantine are translated into GE
expectations. GE produces the batch-level validation report (success flag,
per-expectation statistics, unexpected counts) that is persisted to the
``audit/dq_results`` Delta table and surfaced in logs / Airflow.

Execution engine:
* micro-batches / small tables (<= ``GE_PANDAS_MAX_ROWS``) are converted to pandas
  and validated with GE's pandas engine - one in-memory pass instead of dozens of
  small Spark jobs per expectation (orders of magnitude faster on a laptop);
* larger DataFrames are validated with GE's Spark engine.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import Rule

log = logging.getLogger(__name__)

GE_PANDAS_MAX_ROWS = int(os.environ.get("GE_PANDAS_MAX_ROWS", "200000"))

_CONTEXT = None
_DATASOURCES: dict[str, object] = {}
_BATCH_DEFS: dict[tuple, object] = {}
_SUITES: dict[tuple, object] = {}


@dataclass
class GEResult:
    success: bool
    evaluated: int
    successful: int
    unsuccessful: int
    details: list[dict]
    engine: str = "spark"

    def to_json(self) -> str:
        return json.dumps(self.details, default=str)


def _rule_to_expectation(gx, rule: Rule):
    e = gx.expectations
    if rule.kind == "not_null":
        return e.ExpectColumnValuesToNotBeNull(column=rule.column)
    if rule.kind == "unique":
        return e.ExpectColumnValuesToBeUnique(column=rule.column)
    if rule.kind == "in_set":
        return e.ExpectColumnValuesToBeInSet(column=rule.column, value_set=list(rule.values))
    if rule.kind == "between":
        return e.ExpectColumnValuesToBeBetween(
            column=rule.column, min_value=rule.min_value, max_value=rule.max_value
        )
    if rule.kind == "regex":
        return e.ExpectColumnValuesToMatchRegex(column=rule.column, regex=rule.pattern)
    raise ValueError(f"Unsupported rule kind: {rule.kind}")


def _get_context():
    global _CONTEXT
    if _CONTEXT is None:
        logging.getLogger("great_expectations").setLevel(logging.WARNING)
        import great_expectations as gx

        _CONTEXT = gx.get_context(mode="ephemeral")
        try:  # no tqdm progress bars in streaming / Airflow logs
            from great_expectations.data_context.types.base import ProgressBarsConfig

            _CONTEXT.variables.progress_bars = ProgressBarsConfig(globally=False)
        except Exception:  # pragma: no cover - older/newer GX layouts
            log.debug("Could not disable GE progress bars", exc_info=True)
    return _CONTEXT


def _get_datasource(engine: str):
    if engine not in _DATASOURCES:
        context = _get_context()
        if engine == "pandas":
            _DATASOURCES[engine] = context.data_sources.add_pandas(name="lakehouse_pandas")
        else:
            _DATASOURCES[engine] = context.data_sources.add_spark(name="lakehouse_spark")
    return _DATASOURCES[engine]


def _get_batch_definition(asset_name: str, engine: str):
    key = (asset_name, engine)
    if key not in _BATCH_DEFS:
        asset = _get_datasource(engine).add_dataframe_asset(name=f"{asset_name}_{engine}")
        _BATCH_DEFS[key] = asset.add_batch_definition_whole_dataframe(f"{asset_name}_{engine}_batch")
    return _BATCH_DEFS[key]


def _get_suite(suite_name: str, rules: list[Rule], columns: tuple[str, ...]):
    """Suites are cached per (name, column set) so a schema change rebuilds the suite."""
    key = (suite_name, columns)
    if key not in _SUITES:
        import great_expectations as gx

        context = _get_context()
        suite = context.suites.add(gx.ExpectationSuite(name=f"{suite_name}_{len(_SUITES)}"))
        for rule in rules:
            if rule.column in columns:
                suite.add_expectation(_rule_to_expectation(gx, rule))
        _SUITES[key] = suite
    return _SUITES[key]


def _to_pandas(df: DataFrame):
    """Only the rule columns are needed; decimals become doubles so numeric
    comparisons are vectorised in pandas."""
    casts = [
        F.col(f.name).cast("double").alias(f.name) if isinstance(f.dataType, T.DecimalType) else F.col(f.name)
        for f in df.schema.fields
    ]
    return df.select(*casts).toPandas()


def validate_with_ge(
    df: DataFrame, rules: list[Rule], asset_name: str, row_count: int | None = None, engine: str = "auto"
) -> GEResult:
    """Validate a Spark DataFrame against ``rules`` using Great Expectations."""
    rule_cols = sorted({r.column for r in rules if r.column in df.columns})
    df = df.select(*rule_cols)
    if engine == "auto":
        n = row_count if row_count is not None else df.count()
        engine = "pandas" if n <= GE_PANDAS_MAX_ROWS else "spark"

    data = _to_pandas(df) if engine == "pandas" else df
    batch_def = _get_batch_definition(asset_name, engine)
    suite = _get_suite(asset_name, rules, tuple(rule_cols))
    batch = batch_def.get_batch(batch_parameters={"dataframe": data})
    result = batch.validate(suite)

    details = []
    for r in result.results:
        cfg = r.expectation_config
        res = r.result or {}
        details.append(
            {
                "expectation": getattr(cfg, "type", None) or getattr(cfg, "expectation_type", None),
                "column": (cfg.kwargs or {}).get("column"),
                "success": bool(r.success),
                "element_count": res.get("element_count"),
                "unexpected_count": res.get("unexpected_count"),
                "unexpected_percent": res.get("unexpected_percent"),
                "partial_unexpected_list": (res.get("partial_unexpected_list") or [])[:5],
            }
        )
    stats = result.statistics or {}
    return GEResult(
        success=bool(result.success),
        evaluated=int(stats.get("evaluated_expectations", len(details))),
        successful=int(stats.get("successful_expectations", sum(d["success"] for d in details))),
        unsuccessful=int(stats.get("unsuccessful_expectations", sum(not d["success"] for d in details))),
        details=details,
        engine=engine,
    )
