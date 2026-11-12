import json

from lakehouse.cdc import apply_column_types, events_to_records, latest_per_key, merge_into_silver
from lakehouse.config import TABLES

BRONZE_COLS = "op string, source_lsn long, source_ts_ms long, kafka_partition int, kafka_offset long, " \
              "before_json string, after_json string"


def ev(op, lsn, offset, before=None, after=None):
    return (op, lsn, lsn, 0, offset,
            json.dumps(before) if before else None,
            json.dumps(after) if after else None)


def product(pid, price, **extra):
    return {"product_id": pid, "sku": f"SKU-{pid}", "name": f"P{pid}", "category": "audio",
            "price": price, "is_active": True, "created_at": "2026-11-12T01:00:00Z",
            "updated_at": "2026-11-12T01:00:00Z", **extra}


def to_silver(spark, rows, path):
    cfg = TABLES["products"]
    events = spark.createDataFrame(rows, BRONZE_COLS)
    records = events_to_records(spark, events)
    latest = apply_column_types(latest_per_key(records, cfg.primary_key), cfg)
    merge_into_silver(spark, latest, path, cfg.primary_key)
    return spark.read.format("delta").load(path)


def test_latest_change_wins_within_batch(spark):
    events = spark.createDataFrame(
        [ev("c", 10, 1, after=product(1, "10.00")),
         ev("u", 20, 2, before=product(1, "10.00"), after=product(1, "12.50"))],
        BRONZE_COLS,
    )
    latest = latest_per_key(events_to_records(spark, events), "product_id").collect()
    assert len(latest) == 1
    assert latest[0]["price"] == "12.50" and latest[0]["_cdc_op"] == "u"


def test_merge_upsert_delete_and_schema_evolution(spark, tmp_path):
    path = str(tmp_path / "silver_products")

    # batch 1: snapshot + insert
    df = to_silver(spark, [ev("r", 1, 1, after=product(1, "10.00")),
                           ev("c", 2, 2, after=product(2, "20.00"))], path)
    assert sorted(r.product_id for r in df.collect()) == [1, 2]
    assert dict(df.dtypes)["price"] == "decimal(12,2)"
    assert dict(df.dtypes)["updated_at"] == "timestamp"

    # batch 2: update 1, delete 2, insert 3 with a NEW column -> schema evolution
    df = to_silver(spark, [
        ev("u", 3, 3, before=product(1, "10.00"), after=product(1, "11.00")),
        ev("d", 4, 4, before=product(2, "20.00")),
        ev("c", 5, 5, after=product(3, "30.00", warranty_months=12)),
    ], path)
    rows = {r.product_id: r for r in df.collect()}
    assert set(rows) == {1, 3}
    assert str(rows[1].price) == "11.00"
    assert "warranty_months" in df.columns
    assert rows[3].warranty_months == 12 and rows[1].warranty_months is None

    # batch 3: replayed old event must not overwrite newer state (LSN guard)
    df = to_silver(spark, [ev("u", 2, 6, after=product(1, "1.00"))], path)
    assert str({r.product_id: r for r in df.collect()}[1].price) == "11.00"
