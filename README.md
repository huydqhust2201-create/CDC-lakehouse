# Realtime CDC Lakehouse

Pipeline đồng bộ dữ liệu giao dịch từ **PostgreSQL** (OLTP) vào **Delta Lake** trên **MinIO** theo kiến trúc
**Medallion (Bronze – Silver – Gold)**, gần thời gian thực.

```
 ┌────────────┐  WAL/pgoutput  ┌──────────┐   cdc.public.*   ┌─────────────────────────────────────────────┐
 │ PostgreSQL │ ─────────────▶ │ Debezium │ ───────────────▶ │                    Kafka                    │
 │  (shop)    │                │ (Connect)│                  └───────────────────────┬─────────────────────┘
 └─────▲──────┘                └──────────┘                                          │ Spark Structured Streaming
       │ INSERT/UPDATE/DELETE                                                        ▼
 ┌─────┴──────┐        ┌───────────────────────────── MinIO (s3a://lakehouse) ──────────────────────────────┐
 │  datagen   │        │  bronze/cdc_events      append-only, raw JSON, partition (source_table, ingest_date) │
 └────────────┘        │        │ foreachBatch: parse → dedupe → cast → Great Expectations → split          │
                       │        ├──────────────▶ quarantine/cdc_rejects   (bản ghi lỗi + failed_rules)         │
                       │        ▼ MERGE INTO (upsert/delete, schema evolution)                              │
                       │  silver/{customers,products,orders,order_items}  trạng thái hiện tại               │
                       │        │ Airflow DAG hằng ngày (PySpark batch)                                      │
                       │        ▼                                                                           │
                       │  gold/{daily_sales,product_performance,customer_ltv}                               │
                       │  audit/dq_results   kết quả kiểm tra chất lượng theo batch/ngày                    │
                       └────────────────────────────────────────────────────────────────────────────────────┘
```

## Công nghệ

| Thành phần | Phiên bản | Vai trò |
|---|---|---|
| PostgreSQL | 16 | Nguồn OLTP, `wal_level=logical`, `REPLICA IDENTITY FULL` |
| Debezium | 2.7 (Kafka Connect) | Bắt thay đổi (c/r/u/d) qua `pgoutput`, user replication riêng |
| Kafka | 3.8 (KRaft) | Topic `cdc.public.<table>` |
| PySpark | 3.5.3 | Structured Streaming (Bronze, Silver), batch (Gold) |
| Delta Lake | 3.2.1 | ACID, `MERGE INTO`, schema evolution, time travel |
| MinIO | `pgsty/minio` | Object storage S3-compatible (S3A). MinIO đã gỡ image Docker chính thức, dùng bản build cộng đồng |
| Apache Airflow | 2.10 | Điều phối batch job Gold hằng ngày |
| Great Expectations | GX Core 1.x | Kiểm tra chất lượng trước khi ghi Silver |
| Docker Compose | – | Toàn bộ hạ tầng chạy local |

## Cấu trúc thư mục

```
├── docker-compose.yml
├── postgres/init/            # schema OLTP, publication, user debezium, dữ liệu seed
├── debezium/                 # cấu hình connector PostgreSQL
├── docker/                   # Dockerfile Spark & Airflow (+ tải jar Delta/Kafka/S3A)
├── lakehouse/                # package Python dùng chung cho streaming và Airflow
│   ├── config.py             # đường dẫn, Kafka, metadata bảng, luật DQ
│   ├── spark_session.py      # SparkSession + Delta + MinIO
│   ├── cdc.py                # parse Debezium, dedupe, ép kiểu, MERGE INTO
│   ├── audit.py              # ghi kết quả DQ vào audit/dq_results
│   ├── quality/
│   │   ├── rules.py          # luật → biểu thức Spark, tách valid / quarantine
│   │   └── ge_validator.py   # luật → Great Expectations suite, validate DataFrame
│   └── jobs/
│       ├── bronze_ingest.py      # Kafka → Bronze (streaming)
│       ├── silver_merge.py       # Bronze → Silver (streaming, foreachBatch)
│       ├── run_streaming.py      # chạy Bronze + Silver trong 1 Spark app
│       ├── gold_aggregate.py     # Silver → Gold (batch)
│       ├── check_silver.py       # kiểm tra Silver sẵn sàng (task đầu DAG)
│       ├── dq_report.py          # GE trên toàn bộ Silver + báo cáo quarantine
│       ├── maintenance.py        # OPTIMIZE + VACUUM Gold
│       └── inspect_lakehouse.py  # xem nhanh số dòng/schema các bảng
├── airflow/dags/lakehouse_daily.py
├── datagen/                  # giả lập workload OLTP + script schema evolution
└── tests/                    # unit test (Spark local, không cần Kafka/MinIO)
```

## Chạy dự án

Yêu cầu: Docker Desktop (WSL2), cổng 5432, 8080, 8083, 9000, 9001, 29092, 4040 trống.

Cấu hình mặc định đã được tinh chỉnh để chạy trên **laptop 8 GB RAM** (máy ảo Docker 5 GB + swap, xem
`%USERPROFILE%\.wslconfig`): Spark streaming 1.2 GB / 2 core, heap Kafka 512 MB, Connect 768 MB, Airflow chạy
tuần tự từng task Spark. Kafka UI không bật mặc định: `docker compose --profile ui up -d kafka-ui`
(http://localhost:8085). Với máy nhiều RAM hơn có thể tăng `SPARK_DRIVER_MEMORY`, `SPARK_MASTER`, `max_active_tasks`.

```bash
cp .env.example .env          # tùy chọn
docker compose up -d --build  # lần đầu build image mất vài phút (tải Spark, jar)
docker compose ps
```

Thứ tự khởi động được Compose tự xử lý: Postgres → Kafka → Connect → `connect-init` đăng ký connector →
`minio-init` tạo bucket → `spark-streaming` chạy Bronze + Silver → `datagen` bắt đầu sinh giao dịch → Airflow.

| Giao diện | URL | Tài khoản |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8085 | – |
| Spark UI (streaming) | http://localhost:4040 | – |
| Kafka Connect REST | http://localhost:8083/connectors/pg-shop-connector/status | – |

Không có `make` trên Windows? Mỗi lệnh trong [Makefile](Makefile) chỉ là một lệnh `docker compose` – có thể copy chạy trực tiếp.

### Kiểm tra từng tầng

```bash
make inspect      # = docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse
```

```
table                               rows   version
bronze/cdc_events                   1840        25
silver/customers                      62        24
silver/orders                        171        25
...
quarantine/cdc_rejects                 9         6
```

Xem chi tiết một bảng (schema, mẫu dữ liệu, lịch sử commit Delta):

```bash
docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse --table silver/orders
docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse --table quarantine/cdc_rejects
```

## Chi tiết từng tầng

### 1. CDC: PostgreSQL → Debezium → Kafka
- `wal_level=logical`, publication `dbz_publication`, user `debezium` chỉ có quyền `REPLICATION` + `SELECT`.
- `REPLICA IDENTITY FULL` để sự kiện `UPDATE/DELETE` có đầy đủ ảnh `before`.
- `snapshot.mode=initial`: dữ liệu có sẵn được đẩy với `op=r`, sau đó stream `c/u/d` liên tục.
- `decimal.handling.mode=string` để không mất độ chính xác `NUMERIC`.

### 2. Bronze – log thay đổi thô
`bronze/cdc_events` (append-only) lưu nguyên payload JSON (`before_json`, `after_json`, `value_json`) kèm
metadata `op`, `source_lsn`, `source_ts_ms`, topic/partition/offset. Vì payload là chuỗi, Bronze **không bao giờ
vỡ khi schema nguồn thay đổi**, và có thể replay để dựng lại Silver bất cứ lúc nào.

### 3. Silver – trạng thái hiện tại, đã kiểm tra chất lượng
Mỗi micro-batch (`foreachBatch`) với từng bảng:
1. **Parse** payload; schema được suy ra từ chính batch → cột mới ở nguồn tự xuất hiện.
2. **Dedupe**: giữ thay đổi cuối cùng theo khóa chính (`source_lsn`, rồi offset).
3. **Ép kiểu** theo cấu hình (`NUMERIC → decimal`, `TIMESTAMPTZ → timestamp`) và căn theo schema Silver hiện có.
4. **Data Quality**
   - Great Expectations chạy suite trên batch (not null, unique, in set, between, regex) → kết quả lưu vào
     `audit/dq_results`. Micro-batch nhỏ (≤ `GE_PANDAS_MAX_ROWS`, mặc định 200k dòng) được validate bằng
     engine **pandas** của GE (một lượt trong bộ nhớ, nhanh hơn hàng chục lần so với chạy từng expectation
     thành nhiều Spark job); bảng lớn tự chuyển sang engine Spark.
   - Cùng bộ luật được dịch sang biểu thức Spark để đánh giá **từng dòng**; dòng vi phạm được ghi vào
     `quarantine/cdc_rejects` với `failed_rules`, `record` (JSON), `cdc_lsn` … và **không** vào Silver.
   - Luật `unique` kiểm tra cả trong batch lẫn với dữ liệu đã có trong Silver (vd. email trùng khách hàng khác).
     Trong cùng batch, bản ghi có khóa chính nhỏ nhất giữ giá trị, các bản ghi đến sau mới bị cách ly – chủ sở
     hữu hợp lệ không bao giờ bị loại oan.
   - Quarantine và audit của mọi bảng được ghi **một commit Delta mỗi micro-batch** để giảm overhead.
5. **MERGE INTO**
   ```
   WHEN MATCHED AND s._cdc_op = 'd'  AND s._cdc_lsn >= t._cdc_lsn THEN DELETE
   WHEN MATCHED AND s._cdc_op <> 'd' AND s._cdc_lsn >= t._cdc_lsn THEN UPDATE SET *
   WHEN NOT MATCHED AND s._cdc_op <> 'd'                         THEN INSERT *
   ```
   Điều kiện LSN giúp idempotent khi replay; `spark.databricks.delta.schema.autoMerge.enabled=true` cho phép
   **schema evolution** (cột mới được thêm vào bảng Delta).

Luật DQ khai báo tập trung ở [lakehouse/config.py](lakehouse/config.py), ví dụ `orders`:
`not_null(order_id, customer_id, status, order_ts)`, `unique(order_id)`,
`status ∈ {pending, paid, shipped, delivered, cancelled}`, `0 ≤ total_amount ≤ 1e8`.

### 4. Gold – batch tổng hợp (Airflow)
| Bảng | Nội dung | Cách ghi |
|---|---|---|
| `gold/daily_sales` | số đơn, đơn hoàn tất/hủy, doanh thu, AOV, số khách theo ngày | `replaceWhere` cửa sổ `ds-3 … ds` |
| `gold/product_performance` | số lượng bán, doanh thu, số đơn theo sản phẩm/ngày | `replaceWhere` cửa sổ |
| `gold/customer_ltv` | LTV, số đơn, đơn đầu/cuối, phân khúc (vip/loyal/at_risk/…) | overwrite toàn bộ |

Cửa sổ lookback giúp hấp thụ các cập nhật CDC đến muộn (vd. đơn hôm qua chuyển sang `delivered` hôm nay).

### 5. Airflow DAG `lakehouse_daily` (01:00 UTC hằng ngày)

```
start → check_silver ─┬─> dq_silver_report ─┬─> gold_daily_sales ─────────┐
                      │                     ├─> gold_product_performance ─┼─> gold_maintenance → end
                      │                     └─> gold_customer_ltv ────────┘
                      └─> dq_quarantine_report ───────────────────────────────────────────────> end
```
- `retries=2`, exponential back-off, `execution_timeout`, `on_failure_callback`/`on_retry_callback` ghi log lỗi.
- `check_silver` retry tối đa 5 lần (chờ streaming khởi tạo Silver).
- `dq_silver_report` chạy GE trên toàn bộ Silver; fail nếu có expectation không đạt.
- `dq_quarantine_report` thống kê bản ghi bị cách ly theo luật; fail nếu tỷ lệ lỗi > 20%.
- `max_active_tasks=2` để giới hạn số JVM Spark chạy song song.
- DAG `lakehouse_gold_full_refresh` (chạy tay) dựng lại toàn bộ Gold.

Chạy thử ngay: bật DAG trên UI và bấm ▶ hoặc `make dag-trigger`.

## Demo các tình huống

**Upsert / delete** – `datagen` liên tục tạo đơn, chuyển trạng thái `pending → paid → shipped → delivered`,
hủy đơn, xóa đơn đã hủy (cascade `order_items`). Kiểm tra trực tiếp:

```bash
docker compose exec postgres psql -U shop -d shop -c "UPDATE products SET price = 999 WHERE product_id = 1;"
docker compose exec postgres psql -U shop -d shop -c "DELETE FROM orders WHERE order_id = 5;"
# ~15-30s sau
docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse --table silver/products --limit 3
```

**Schema evolution** – thêm cột ở nguồn:

```bash
make schema-change   # ALTER TABLE customers ADD COLUMN loyalty_tier ...
docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse --table silver/customers
# cột loyalty_tier xuất hiện trong schema Silver, lịch sử Delta có commit MERGE với schema mới
```

**Quarantine** – `datagen` với `--bad-ratio 0.05` cố ý sinh: email null / sai định dạng / trùng, giá âm,
trạng thái đơn không hợp lệ, số lượng = 0.

```bash
docker compose exec spark-streaming python -m lakehouse.jobs.inspect_lakehouse --table quarantine/cdc_rejects
docker compose logs spark-streaming | grep -E "\[DQ\]|\[GE\]"
```

## Test

```bash
make test   # = docker compose run --rm --no-deps spark-streaming python -m pytest -q tests
```
Test bao phủ: tách valid/invalid theo luật, unique so với Silver hiện có, dedupe theo LSN, MERGE
insert/update/delete, schema evolution và chống ghi đè khi replay sự kiện cũ.

## Replay Silver từ Bronze

Bronze là log thay đổi đầy đủ, nên khi sửa logic Silver (luật DQ, ép kiểu…) có thể dựng lại Silver mà không
cần đọc lại PostgreSQL/Kafka:

```bash
docker compose stop spark-streaming
docker compose run --rm --no-deps --entrypoint sh minio-init -c '
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
  for p in silver quarantine audit gold _checkpoints/silver_merge; do mc rm -r --force local/lakehouse/$p; done'
docker compose up -d spark-streaming        # Silver đọc lại Bronze từ đầu
docker compose exec airflow-scheduler airflow dags trigger lakehouse_daily   # dựng lại Gold
```
Nhờ dedupe theo LSN và điều kiện `s._cdc_lsn >= t._cdc_lsn` trong MERGE, kết quả replay giống hệt xử lý gốc.

## Ghi chú vận hành
- Bronze/Silver chạy trong **một** Spark app (`run_streaming.py`) để tiết kiệm RAM; mỗi job vẫn chạy riêng được
  (`python -m lakehouse.jobs.bronze_ingest` / `silver_merge`). Checkpoint trên MinIO → container restart sẽ
  tiếp tục đúng offset (exactly-once vào Delta).
- `OPTIMIZE`/`VACUUM` chỉ chạy trên Gold; Bronze/Silver được ghi liên tục bởi streaming, compaction đồng thời sẽ
  xung đột với MERGE (S3 không có cơ chế lock nhiều writer).
- Đặt lại toàn bộ dữ liệu: `make clean` (xóa volume Postgres, Kafka, MinIO, Airflow DB).
- Độ trễ PostgreSQL → Silver trên laptop 4 core / 8 GB khoảng 30–90 giây (trigger 30 s, mỗi bảng ~10–15 s/batch);
  log `Current batch is falling behind` là bình thường khi Airflow đang chạy job Spark song song.
- Mật khẩu trong repo chỉ dành cho môi trường demo local.
