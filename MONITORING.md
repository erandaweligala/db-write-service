# Monitoring — Kafka Lag & DLQ Observability

This service exposes Prometheus metrics at **`/q/metrics`** (see `quarkus.micrometer.export.prometheus` in `application.yml`). This document covers the metrics and dashboards for **Kafka consumer lag** and **dead-letter / dropped message events** — the operational signals that tell you when the pipeline is falling behind or losing writes.

A ready-to-import Grafana dashboard lives at [`dashboards/db-write-service-observability.json`](dashboards/db-write-service-observability.json).

---

## 1. Kafka consumer lag

Lag metrics come from the **Micrometer Kafka client binder**, which is already enabled:

```yaml
quarkus:
  micrometer:
    binder:
      kafka:
        enabled: true   # application.yml
```

The binder registers the underlying Kafka consumer client metrics into the Prometheus registry. The ones that matter for lag:

| Metric | Type | Meaning |
|---|---|---|
| `kafka_consumer_fetch_manager_records_lag_max` | gauge | Max records-lag across the partitions a consumer client is assigned. Primary lag signal. |
| `kafka_consumer_fetch_manager_records_lag` | gauge | Per-topic/partition records-lag (present when per-partition metrics are exposed). |
| `kafka_consumer_fetch_manager_records_lag_avg` | gauge | Average lag across assigned partitions. |

Useful labels: `client_id`, and (for the per-partition metric) `topic`, `partition`.

> **Note on semantics.** This is *client-side fetch lag* — the difference between the consumer's current position and the log-end offset of its assigned partitions. It reflects real-time consumption pressure. It is not the same as consumer-*group* lag (committed offset vs end offset) that `kafka-consumer-groups --describe` reports, though the two track closely under steady load.

### PromQL

```promql
# Worst-case lag across every consumer client
max(kafka_consumer_fetch_manager_records_lag_max)

# Lag per consumer client
max by (client_id) (kafka_consumer_fetch_manager_records_lag_max)

# Lag per topic/partition
sum by (topic, partition) (kafka_consumer_fetch_manager_records_lag)
```

---

## 2. Dead-letter & dropped events

There was no counter for "how many messages did we fail to persist". `ExceptionMetricsService` counts exceptions by root-cause *type*, which is a different question. `DlqMetrics` fills the gap with two counters that separate the two dispositions a terminal failure can have:

| Metric | Type | Disposition | Payload |
|---|---|---|---|
| `kafka_dlq_events_total` | counter | Routed to a dead-letter topic (`failure-strategy: dead-letter-queue`) | **preserved** — replayable |
| `kafka_dropped_events_total` | counter | Acknowledged and discarded (`failure-strategy: ignore`) | **lost** — higher severity |

Both are tagged with:

- **`channel`** — the logical consumer channel (e.g. `DC-DR`, `DR-DC`, `scheduler`, `provisioning`, `dr-provisioning`, `ums-mysql-dc`, `ums-mysql-dr`).
- **`reason`** — coarse cause, bounded cardinality:
  - `deserialization` — poison payload that could not be parsed.
  - `db_permanent` — DB rejected the write permanently (constraint, bad column, syntax).
  - `db_transient_exhausted` — a transient DB failure that did not recover before the retry budget was spent.
  - `missing_headers` — required Kafka headers (correlationId / replyTopic) were absent.
  - `unknown` — cause could not be classified.

The counters are incremented at each consumer's terminal-failure boundary (after `ResilientDbWriteExecutor` has exhausted its retries). The same numbers are available as JSON at **`GET /api/monitoring/dlq`** and summarized under `dlq` in **`GET /api/monitoring/summary`**.

### PromQL

```promql
# DLQ event rate by channel
sum by (channel) (rate(kafka_dlq_events_total[5m]))

# DLQ event rate by reason (a db_transient_exhausted spike == DB was down past the retry budget)
sum by (reason) (rate(kafka_dlq_events_total[5m]))

# Dropped (lost) messages — these did NOT make it to a DLT
sum by (channel) (rate(kafka_dropped_events_total[5m]))

# 24h totals
sum(increase(kafka_dlq_events_total[24h]))
sum(increase(kafka_dropped_events_total[24h]))
```

---

## 3. Supporting throughput / failure metrics

These already existed (`PerformanceMetrics`, incremented in `DBWriteRepository`) and provide context on the same dashboard:

| Metric | Meaning |
|---|---|
| `db_updates_total` | Successful single-row DB writes. |
| `db_updates_failures_total` | Failed DB write attempts. |
| `db_update_duration_seconds` | DB write latency (timer; use `_bucket` for quantiles). |
| `db_batch_updates_total` | Batch write rows. |

```promql
# Throughput
sum(rate(db_updates_total[1m]))

# Failure ratio
sum(rate(db_updates_failures_total[5m]))
  / clamp_min(sum(rate(db_updates_total[5m])) + sum(rate(db_updates_failures_total[5m])), 1)
```

---

## 4. Suggested alerts

Thresholds are starting points — tune to your traffic. They mirror the targets in `QUICK_START_GUIDE.md`.

```yaml
groups:
  - name: db-write-service
    rules:
      - alert: KafkaConsumerLagHigh
        expr: max(kafka_consumer_fetch_manager_records_lag_max) > 5000
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Kafka consumer lag > 5000 for 5m"

      - alert: MessagesDropped
        expr: sum(increase(kafka_dropped_events_total[10m])) > 0
        for: 0m
        labels: { severity: critical }
        annotations:
          summary: "Messages discarded without dead-lettering (payload lost)"

      - alert: DlqEventsSpiking
        expr: sum(rate(kafka_dlq_events_total[5m])) > 1
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Messages being dead-lettered at > 1/s for 5m"

      - alert: DbTransientRetriesExhausted
        expr: sum(rate(kafka_dlq_events_total{reason="db_transient_exhausted"}[5m])) > 0
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "DB down long enough to exhaust the write-retry budget"

      - alert: DbWriteFailureRateHigh
        expr: |
          sum(rate(db_updates_failures_total[5m]))
            / clamp_min(sum(rate(db_updates_total[5m])) + sum(rate(db_updates_failures_total[5m])), 1) > 0.01
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "DB write failure ratio > 1% for 5m"
```

---

## 5. Importing the dashboard

1. Grafana → **Dashboards → New → Import**.
2. Upload `dashboards/db-write-service-observability.json`.
3. Select your Prometheus data source when prompted (`DS_PROMETHEUS`).

The dashboard has a `channel` template variable (populated from `kafka_dlq_events_total`) so you can scope the DLQ panels to a single channel.
