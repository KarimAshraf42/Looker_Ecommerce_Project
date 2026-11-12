# Looker E-commerce: End-to-End Data Engineering Pipeline

An automated data platform built on the **Looker E-commerce BigQuery dataset**. It uses a Medallion architecture (bronze → silver → gold) with a **batch path** and a **streaming path** that both land in the same Hive star schema, orchestrated by **Airflow** and consumed by a six-page **Power BI** report.

The design priority is correctness first (no duplicates, no lost events, no overwritten history), then automation, then performance. Every load step is idempotent, so the pipeline can be re-run safely.

## Architecture

```
BATCH      CSV files → HDFS /Data/Bronze → Spark → HDFS /Data/Silver (Parquet)
                                                         │
                                                         ▼
                                        Hive: looker_ecommerce_dwh  ──►  Power BI
                                                         ▲
STREAMING  Producer → Kafka (events) → Spark Structured Streaming
                          │                      │
                          │                      └→ late_streaming_events (scheduled recovery)
                          └→ Kafka (events_dlq)   ← invalid events
```

**Stack:** HDFS, Apache Spark (PySpark + Structured Streaming), Hive, Apache Kafka (3 brokers), Apache Airflow, Jupyter notebooks (run headless with `nbconvert`), Docker, Power BI.

## Repository layout

| Path | Purpose |
|---|---|
| `Notebooks/bronze_layer.ipynb` | Uploads the raw CSVs to HDFS and profiles data quality |
| `Notebooks/silver_layer.ipynb` | Explicit schemas, cleaning, quarantine of invalid rows |
| `Notebooks/gold_layer.ipynb` | Loads the Hive star schema |
| `Notebooks/utils.ipynb` | Reusable loaders: `scd2_load`, `upsert_fact`, `simple_load`, `attach_scd2_key` |
| `Notebooks/streaming_layer.ipynb` | Kafka → Hive main stream and the dead-letter stream |
| `Notebooks/late_streaming_events.ipynb` | Scheduled recovery of events older than the watermark |
| `Notebooks/Inspection/` | Read-only notebooks that explain quarantined rows, DLQ messages and late events |
| `Streaming/kafka_producer.py` | Clickstream simulator with injected faults |
| `Airflow/looker_ecommerce_pipeline_dag.py` | Daily DAG |
| `SQL/looker_ecommerce_dwh.sql` | Hive DDL, plus the ER diagram image |
| `Power BI/Looker_Ecommerce_Power_BI.pdf` | Dashboard export |
| `Docs/` | Final report (PDF) and proof screenshots |

## Batch path

**Bronze.** The five raw files (`users`, `products`, `distribution_centers`, `order_items`, `events`) are copied to `/Data/Bronze` unchanged, and the run fails if any is missing. Each dataset is then profiled: nulls, duplicate keys, categorical values, out-of-range values, future timestamps, order-lifecycle date order, and orphan keys. These checks are informational; the rules are enforced in silver.

**Silver.**
- Each file is read with an explicit schema (no `inferSchema`).
- Text is trimmed and standardized, emails lower-cased, money cast to `DECIMAL(10,2)`, order status mapped to IDs (Processing 0, Shipped 1, Complete 2, Returned 3, Cancelled 4, Unknown -1), and columns with no analytical value dropped.
- Nulls are handled by column role: descriptive attributes get `Unknown`; missing keys, timestamps and prices make a row invalid rather than being invented.
- `split_invalid()` writes rows that fail the validity rule to `/Data/Silver/Invalid_…` instead of deleting them. Clean rows go to `/Data/Silver/Cleaned_…`.
- **Known source issue:** 35,872 order items have `shipped_at` earlier than `created_at`. They are quarantined, not repaired, because there is no way to know which timestamp is wrong. Dashboard revenue and order counts therefore cover validated records only.

**Gold.** A star schema in `looker_ecommerce_dwh`, stored as Parquet with Snappy:

| Table | Type | Physical design |
|---|---|---|
| `fact_order_items` | Fact, one row per order item, four date keys + status | Partitioned by `order_year`, 4 buckets on `user_key` |
| `fact_events` | Fact, one row per web event | Partitioned by `event_year`, `event_month` |
| `dim_users` | SCD Type 2 | Partitioned by `traffic_source`, 4 buckets on `user_key` |
| `dim_products` | SCD Type 2, distribution center denormalized | 4 buckets on `product_key` |
| `dim_order_status` | Dimension | Small table |
| `dim_date` | Dimension, one row per calendar day | Small table |

Key design choices:
- **Surrogate keys** are `max(existing) + row_number()`, since Hive has no auto-increment.
- **Unknown members** (`user_key = 0`, `product_key = 0`, `status_id = -1`) mean facts never have a null or broken foreign key.
- **Role-playing date dimension:** `fact_order_items` links to `dim_date` four times (created, shipped, delivered, returned).
- **Point-in-time joins:** each fact gets the dimension version valid on its date, falling back to the current version, then to the unknown member.
- Primary and foreign keys are declared as informational constraints (`DISABLE NOVALIDATE RELY`).

### Loaders (`utils.ipynb`)

| Loader | Behavior | Used for |
|---|---|---|
| `scd2_load` | Detects new and changed records, expires old versions, inserts new ones; does nothing if nothing changed | `dim_users`, `dim_products` |
| `upsert_fact` | Replaces only the partitions the batch touches, merging new rows with untouched old ones | `fact_order_items` |
| `simple_load` | Inserts only keys not already in the table; never overwrites | `fact_events`, `dim_date`, `dim_order_status` |

Hive tables can't be updated in place, so SCD2 and upsert rebuild the affected data in a staging table, overwrite the target, and always drop the staging table, even on failure.

## Streaming path

**Producer (`kafka_producer.py`).** Simulates user sessions through the funnel Home → Department → Product → Cart → Purchase (20% of completed funnels end in Cancel) and sends them to topic `events` with `acks=all`. It injects faults at configurable rates:

| Fault | Default | How |
|---|---|---|
| Duplicates | 5% | Resends an earlier event with the same `event_id` |
| Late | 5% | `created_at` 1–5 minutes in the past |
| Very late | 2% | `created_at` 61–180 minutes in the past |
| Invalid | 2% | Missing ID, sequence ≤ 0, future date, bad date, or non-JSON |

Events are synthetic rather than replayed from `events.csv`, because replayed historical timestamps would all be "very late" and wouldn't exercise the real-time path.

**Main stream (`streaming_layer`).** Parses JSON with an explicit schema, applies the same cleaning as silver, validates with one shared rule (`event_id` present, parseable `created_at` not in the future, `sequence_number ≥ 1`), drops events more than 1 hour older than their arrival, de-duplicates on `event_id` + `created_at` inside a 1-hour watermark, attaches the point-in-time user key, and loads into `fact_events` every 5 minutes through `simple_load`.

**Dead-letter queue.** A second stream keeps events that fail validation and writes their raw payload to `events_dlq`, so nothing invalid reaches the warehouse and nothing invalid is lost.

**Late-event recovery (`late_streaming_events`).** Re-reads the last 3 days of the topic, applies the same rules, keeps events older than 2 hours (so it never competes with the live stream), de-duplicates, and inserts only what is missing. Because it shares the insert-only loader with the stream, both can write `fact_events` concurrently without overwriting each other.

### Measured results (one test run)

| Measure | Result |
|---|---|
| Messages in `events` | 6,222 (5,962 unique payloads) |
| Valid on time | 5,354 (92.6%) |
| Late, accepted by the stream | 260 (4.5%), average delay 3.1 min |
| Very late, dropped by the stream | 170 (2.9%), delay 63–180 min |
| Of those, already recovered | 15 of 170 |
| Unique invalid events in the DLQ | 178 (seq ≤ 0: 39, future date: 38, bad date: 36, not JSON: 33, no ID: 32) |

## Orchestration (Airflow)

DAG `looker_ecommerce_pipeline` runs daily. Each notebook runs inside the Hadoop container through the Docker API using `jupyter nbconvert --execute`, and the executed copy is saved per day as an audit trail.

```
Start → Check_Hadoop_Container ─┬→ Bronze → Silver → Gold ─┐
                                └──────────────────────────┴→ Late_Streaming_Events → Power_BI → End
```

- **Change detection:** the branch task fingerprints the source files (name, size, modified time) and compares it with the last successful run, stored in an Airflow Variable. If nothing changed, bronze/silver/gold are skipped; late-event recovery still runs.
- **Safe fingerprint:** it is saved only after gold succeeds, so a failed run is retried in full next time.
- **Reliability:** 1 retry with a 5-minute delay, 45-minute task timeout, 4-hour DAG timeout, one active run at a time, e-mail on failure.

## Idempotency

| Component | Why a re-run is safe |
|---|---|
| Bronze | `put -f` overwrites the same files with the same content |
| Silver | Output and quarantine folders are overwritten with the same result |
| `dim_users`, `dim_products` | Only new or changed business keys produce rows |
| `fact_order_items` | Upsert by `order_item_id` replaces only touched partitions |
| `dim_date`, `dim_order_status`, `fact_events` | Insert-only for keys that don't exist yet |
| DAG | Unchanged sources skip the batch layers |

Proof screenshots are in `Docs/`: an SCD2 city change (user 457 gets a new version with `user_key` 100,001) and a fact upsert (order item 36,717 moves from Shipped to Complete in place), plus first-run and second-run DAG views.

## Power BI report

Six pages: **Executive, Products, Customers, Geography, Operations, Web.** See `Power BI/Looker_Ecommerce_Power_BI.pdf`.

## Documentation

The full design report, covering decisions, challenges and results, is in `Docs/Looker Ecommerce Project Report.pdf`.
