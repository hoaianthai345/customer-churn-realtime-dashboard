# Team Coding Guide - Realtime BI Multi-Node

Tai lieu nay la chuan chung de cac thanh vien code cung mot kieu cho du an BI dashboard realtime.
Phan nay ap dung cho flow hien tai: `Batch preload (members + transactions) -> User-log Replay -> Kafka -> Spark Structured Streaming -> ClickHouse -> Dashboard`.

## 1. Tra loi nhanh: "Khong dung API sao?"

Co, nhung khong dung API HTTP cho **data plane realtime**.

- Du lieu realtime bat buoc di qua Kafka event (`user_log_events`).
- Du lieu nen (`members`, `transactions`) duoc load san vao ClickHouse truoc khi replay.
- Spark job doc topic, tinh KPI, ghi ClickHouse.
- Dashboard doc du lieu tu FastAPI/ClickHouse.

Ly do:

- Giam coupling giua producer/consumer.
- Scale ngang nhieu node de hon (them partition + them consumer).
- Tranh nghen do call API dong bo tung event.

API sync chi nen dung cho control plane (health check, trigger job, admin), khong dung de van chuyen event KPI realtime.

## 2. Nguyen tac kien truc bat buoc

1. Event-driven first: moi thay doi du lieu realtime (log stream) phai co event contract ro rang.
2. Key theo `msno`: producer luon gui Kafka key = `msno` de giu ordering theo user.
3. Exactly-once practical: sink phai idempotent (KPI dang dung `ReplacingMergeTree(processed_at)`).
4. Stateless app code: khong luu state business vao local file cua 1 node.
5. Config qua `.env`: khong hardcode host/port/topic trong code.

## 3. Quy uoc code theo folder

- `apps/producers/`: chi replay/publish event. Khong tinh KPI o day.
- `apps/streaming/common/`: schema, transform, writer, checkpoint helper dung lai.
- `apps/streaming/jobs/`: 1 file = 1 streaming job ro nghia vu.
- `infra/clickhouse/init/`: DDL phai dong bo voi schema output cua Spark.
- `docs/`: cap nhat architecture + event contract + KPI khi thay doi logic.
- `tests/`: moi thay doi logic parse/transform/schema phai co test.

## 4. Chuan viet producer

1. Parse va cast du lieu trong `apps/producers/common/serializers.py`.
2. Bo qua row loi (`msno` rong, field critical null).
3. Publish theo batch ngay, `flush()` sau moi ngay replay.
4. Khong call API ben ngoai trong loop tung event.
5. Log it nhat: `batch index`, `event_date`, `rows`, `total_events`.

## 5. Chuan viet Spark streaming job

Skeleton bat buoc:

1. `create_spark_session(<job-name>)`
2. `readStream.format("kafka")`
3. `parse_kafka_json(..., <schema>)`
4. `prepare_fact_*` (neu co fact table)
5. `foreachBatch(write_*)` -> `write_batch_to_clickhouse`
6. checkpoint rieng cho tung stream (`checkpoint_path("<job>_<purpose>")`)

Rule:

- Khong `collect()` du lieu lon ve driver.
- Ham transform dat o `apps/streaming/common/transforms.py` de test duoc.
- `batch_df.rdd.isEmpty()` duoc dung de bo qua micro-batch rong truoc khi ghi sink.
- Dat ten app/job/checkpoint ro nghia vu (`transaction_kpi`, `activity_fact`, ...).

## 6. Rule quan trong cho moi truong nhieu node

1. **Checkpoint phai la shared storage** (S3/HDFS/NFS).  
   Local path chi dung local demo.
2. **Khong phu thuoc local disk cua node** cho business state.
3. **Topic partition** phai du de scale consumer (`>=` tong so consumer task muc tieu).
4. **Offset strategy**:
   - Local/dev: co the `startingOffsets=earliest`.
   - Prod: uu tien resume tu checkpoint, han che reset offset.
5. **Schema evolution co kiem soat**:
   - Them field moi: cho phep null/default.
   - Khong xoa/doi ten field cu neu chua migrate consumer.

## 7. Checklist khi them KPI/job moi

1. Them/doi schema o `apps/streaming/common/schemas.py`.
2. Them transform o `apps/streaming/common/transforms.py`.
3. Tao job moi trong `apps/streaming/jobs/`.
4. Tao script run trong `apps/streaming/run/`.
5. Tao/doi DDL ClickHouse trong `infra/clickhouse/init/`.
6. Cap nhat `docs/architecture_diagrams/event_contracts.md` + `docs/kpi_definitions.md`.
7. Them test cho schema/transform.
8. Chay `pytest` truoc khi merge.

## 8. Quy uoc test va review

Bat buoc co test khi:

- Them field vao event payload/schema.
- Sua cong thuc KPI.
- Sua ham serializer/transform.

Toi thieu:

- 1 test "happy path"
- 1 test "invalid/null input"

PR review checklist:

1. Co pha vo event contract khong?
2. Co dam bao idempotent khi ghi ClickHouse khong?
3. Co checkpoint rieng, khong trung job khac khong?
4. Co hardcode host/topic/path khong?
5. Da cap nhat docs lien quan chua?

## 9. Anti-pattern can tranh

- Dung HTTP API sync de day tung event realtime sang service khac.
- Ghi ClickHouse theo tung row (thay vi batch).
- Dat business logic trong shell script run.
- Dung chung checkpoint location cho 2 query/job.
- Merge code thay doi schema nhung khong cap nhat docs + tests.

## 10. Lenh check nhanh truoc khi push

```bash
pytest -q
bash scripts/validate_stack.sh
```

Neu sua producer/streaming logic lon, nen chay them:

```bash
bash scripts/run_pipeline.sh
```

## 11. Mau commit message khuyen nghi

- `feat(streaming): add weekly retention kpi job`
- `fix(producer): skip invalid msno in transaction replay`
- `docs: update event contract for user_log_events v2`
