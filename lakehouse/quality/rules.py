"""Row-level data quality: evaluate declarative rules as Spark predicates and split a
DataFrame into valid rows (go to Silver) and invalid rows (go to quarantine)."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.config import Rule

DQ_FAILURES_COL = "_dq_failures"


def _failure_condition(rule: Rule, df: DataFrame) -> Column | None:
    """Return a boolean Column that is TRUE when the row violates ``rule``.

    Mirrors Great Expectations semantics: except for ``not_null``, NULL values
    are not considered violations (GE ignores nulls for these expectations)."""
    if rule.column not in df.columns:
        # Missing column: only not_null can be violated by it.
        return F.lit(True) if rule.kind == "not_null" else None

    c = F.col(rule.column)
    if rule.kind == "not_null":
        cond = c.isNull()
        if dict(df.dtypes).get(rule.column) == "string":
            cond = cond | (F.trim(c) == "")
        return cond
    if rule.kind == "in_set":
        return c.isNotNull() & ~c.isin(*rule.values)
    if rule.kind == "between":
        cond = F.lit(False)
        if rule.min_value is not None:
            cond = cond | (c < F.lit(rule.min_value))
        if rule.max_value is not None:
            cond = cond | (c > F.lit(rule.max_value))
        return c.isNotNull() & cond
    if rule.kind == "regex":
        return c.isNotNull() & ~c.cast("string").rlike(rule.pattern)
    if rule.kind == "unique":
        return None  # handled separately (needs window / join)
    raise ValueError(f"Unsupported rule kind: {rule.kind}")


def split_valid_invalid(
    df: DataFrame,
    rules: list[Rule],
    primary_key: str,
    existing: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Evaluate ``rules`` on every row.

    ``unique`` rules are checked inside the batch (the row with the smallest primary
    key keeps the value) *and* against the rows already in Silver (``existing``),
    ignoring the row with the same primary key (an update of the same entity is not
    a duplicate).

    Returns ``(valid_df, invalid_df)``; ``invalid_df`` carries an array column
    ``_dq_failures`` with the names of the violated rules.
    """
    flags: list[Column] = []
    tmp_cols: list[str] = []

    for i, rule in enumerate(rules):
        if rule.kind == "unique":
            if rule.column not in df.columns:
                continue
            dup_col = f"_dq_dup_{i}"
            # In-batch duplicates: the first claimant (smallest primary key) keeps the
            # value, later ones are rejected - the legitimate owner is never quarantined.
            w = Window.partitionBy(rule.column).orderBy(F.col(primary_key).asc())
            df = df.withColumn(
                dup_col,
                F.col(rule.column).isNotNull() & (F.row_number().over(w) > 1),
            )
            if existing is not None and rule.column != primary_key and rule.column in existing.columns:
                ext_col = f"_dq_ext_{i}"
                taken = (
                    existing.where(F.col(rule.column).isNotNull())
                    .select(F.col(rule.column).alias("_u_val"), F.col(primary_key).alias("_u_pk"))
                )
                conflicts = (
                    df.select(primary_key, rule.column)
                    .join(taken, (F.col(rule.column) == F.col("_u_val")) & (F.col(primary_key) != F.col("_u_pk")))
                    .select(primary_key)
                    .distinct()
                    .withColumn(ext_col, F.lit(True))
                )
                df = df.join(conflicts, on=primary_key, how="left").withColumn(
                    dup_col, F.col(dup_col) | F.coalesce(F.col(ext_col), F.lit(False))
                ).drop(ext_col)
            tmp_cols.append(dup_col)
            flags.append(F.when(F.col(dup_col), F.lit(rule.name)))
        else:
            cond = _failure_condition(rule, df)
            if cond is not None:
                flags.append(F.when(F.coalesce(cond, F.lit(False)), F.lit(rule.name)))

    if not flags:
        return df, df.limit(0).withColumn(DQ_FAILURES_COL, F.array().cast("array<string>"))

    df = df.withColumn(DQ_FAILURES_COL, F.array_compact(F.array(*flags))).drop(*tmp_cols)
    has_failure = F.size(F.col(DQ_FAILURES_COL)) > 0
    valid = df.where(~has_failure).drop(DQ_FAILURES_COL)
    invalid = df.where(has_failure)
    return valid, invalid
