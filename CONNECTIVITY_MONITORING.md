# Dependency Connectivity Monitoring (Oracle / Redis / Kafka)

## Overview

The DB write service tracks whether it can reach the infrastructure it cannot run without —
**Oracle**, **Redis**, and **Kafka** — and publishes that state to Prometheus so Grafana can chart and alert on
it. Every connectivity issue or exception against one of those dependencies lands on the
same set of metrics.

Two signals feed one state machine:

| Signal | What it catches | Where it comes from |
| --- | --- | --- |
| **Live traffic failures** | Real request failures as they happen | Every exception recorded with `Source.ORACLE` / `Source.MYSQL` / `Source.REDIS` / `Source.KAFKA` is forwarded from `ExceptionMetricsService` to `ConnectivityMonitoringService` |
| **Active probes** | Outages during idle periods, and recovery | A scheduler pings each probeable dependency every `connectivity.probe-interval` (default 15s) |

A dependency flips to **DOWN** after `connectivity.failure-threshold` (default 3) consecutive
connectivity failures, and back to **UP** on the first success from either signal. Both
transitions are logged (`Connectivity DOWN for …` / `Connectivity RESTORED for …`).

Dependencies and their probes:

| `dependency` | What it is | Probe |
| --- | --- | --- |
| `database` | Oracle | `SELECT 1 FROM DUAL` |
| `redis` | Redis | `PING` |
| `kafka` | Kafka | AdminClient `describeCluster` |

## What counts as a connectivity issue

`ConnectivityFailureClassifier` walks the cause chain of each failure and tags it with a
reason. Only the transport reasons drive the up/down state; everything else is counted as
`application_error` and never takes a dependency down.

| `reason` tag | Typical cause |
| --- | --- |
| `connection_refused` | Nothing listening (`ConnectException`, `ORA-12541`) |
| `connection_timeout` | Connect/read/command timed out, Kafka batch expiry, Elasticsearch listener timeout |
| `connection_closed` | Reset, broken pipe, `ORA-03113`, HTTP connection pool shut down |
| `host_unreachable` | DNS failure, no route to host |
| `pool_exhausted` | No connection could be borrowed; wait queue full |
| `authentication_failed` | `ORA-01017`, Redis `NOAUTH`/`WRONGPASS`, Kafka SASL |
| `tls_failure` | Handshake or certificate validation failure |
| `broker_unavailable` | Kafka broker/coordinator missing, topic not in metadata |
| `service_unavailable` | Reached but unusable — listener down, cluster down, circuit open, Elasticsearch has no living connections |
| `application_error` | Not a connectivity problem; the dependency answered with an error |

## Metrics

Scraped from `/q/metrics` (Prometheus format). Counter names carry the `_total` suffix
Prometheus adds; gauges appear exactly as named.

| Metric | Type | Tags | Meaning |
| --- | --- | --- | --- |
| `dependency_up` | gauge | `service`, `dependency` | 1 = reachable, 0 = down |
| `dependency_connectivity_failure_count_total` | counter | `service`, `dependency`, `reason` | Transport failures |
| `dependency_error_count_total` | counter | `service`, `dependency`, `reason` | All failures, connectivity or not |
| `dependency_connectivity_failure_daily_count` | gauge | `service`, `dependency` | Failures in the current 24h window (resets 00:00) |
| `dependency_consecutive_failure_count` | gauge | `service`, `dependency` | Current failure streak |
| `dependency_outage_count_total` | counter | `service`, `dependency` | UP → DOWN transitions |
| `dependency_downtime_seconds` | gauge | `service`, `dependency` | Length of the outage in progress (0 when up) |
| `dependency_last_failure_timestamp_seconds` | gauge | `service`, `dependency` | Epoch seconds of the last failure |
| `dependency_last_success_timestamp_seconds` | gauge | `service`, `dependency` | Epoch seconds of the last success |
| `dependency_outage_duration_seconds_*` | timer | `service`, `dependency` | One observation per completed outage |
| `dependency_probe_latency_seconds_*` | timer | `service`, `dependency`, `outcome` | Probe round-trip, `outcome` = success/failure |

`dependency` is one of `database`, `redis` or `kafka`.

### The `service` tag

Every AAA service exports these metric names, so **each series carries a `service` tag** —
`db-write-service` here — set from `connectivity.service-name`. Always scope queries by it;
without it `dependency_up{dependency="redis"}` matches every service in the estate at
once. The shipped dashboard and alert rules are already scoped.

Useful queries:

```promql
# Anything down right now in this service
dependency_up{service="db-write-service"} == 0

# Connectivity failure rate, split by dependency and reason
sum by (dependency, reason) (rate(dependency_connectivity_failure_count_total{service="db-write-service"}[5m]))

# Probe latency (average of successful probes)
  rate(dependency_probe_latency_seconds_sum{service="db-write-service",outcome="success"}[5m])
/ rate(dependency_probe_latency_seconds_count{service="db-write-service",outcome="success"}[5m])

# Outages in the last hour
increase(dependency_outage_count_total{service="db-write-service"}[1h])

# Every AAA service at a glance
min by (service, dependency) (dependency_up)
```

## Grafana

Import `grafana/dashboards/dependency-connectivity-dashboard.json`
(**Dashboards → New → Import → Upload JSON file**) and pick your Prometheus datasource when
prompted. UID: `aaa-db-write-connectivity`.

The dashboard is organised as:

1. **Connectivity status** — one UP/DOWN stat per dependency plus a count of what is down.
2. **Availability over time** — state timeline of every outage, and the outage in progress.
3. **Connectivity failures** — failure rate by dependency and reason, all errors including
   application errors, failures so far today, outage starts, and a current-state table.
4. **Health probes** — probe latency and probe failure ratio, the earliest warning that a
   dependency is degrading.
5. **Exceptions behind the failures** — the `application_exception_count_total` breakdown by
   root exception type, source, and layer, for the window in view. Note that
   `application_exception_count` carries no `service` tag; if one Prometheus scrapes several
   AAA services, add a `job="…"` matcher to those two panels.

Alerting rules live in `grafana/alerts/dependency-connectivity-alerts.yml` — load them into
Prometheus via `rule_files`, or import them into Grafana unified alerting. They are named
`AaaDbWrite*` and scoped to `service="db-write-service"`, so they sit alongside the other AAA
services' rules without colliding. They cover: a dependency down, all dependencies down on
one pod, sustained connectivity errors, pool exhaustion, authentication failures, flapping,
slow probes, and missing metrics.

## REST endpoint

For a quick human check without scraping metrics:

```
GET /monitoring/connectivity
```

Returns `200` when everything is reachable and `503` when at least one dependency is down:

```json
{
  "status": "DOWN",
  "dependencies": {
    "database": {
      "dependency": "database",
      "up": true,
      "consecutiveFailures": 0,
      "connectivityFailureCount": 0,
      "dailyConnectivityFailureCount": 0,
      "outageCount": 0,
      "downtimeSeconds": 0,
      "lastFailureEpochSeconds": 0,
      "lastSuccessEpochSeconds": 1755791234,
      "lastFailureReason": null
    },
    "redis": { "up": false, "lastFailureReason": "connection_refused", "...": "..." },
    "kafka": { "up": true, "...": "..." }
  }
}
```

## Configuration

```yaml
connectivity:
  service-name: db-write-service   # value of the `service` tag on every dependency_* metric
  enabled: true             # master switch; false disables the probes (failures are still counted)
  failure-threshold: 3      # consecutive connectivity failures before a dependency is marked DOWN
  probe-interval: 15s       # how often each dependency is pinged
  probe-timeout-ms: 2000    # per-probe budget; a slower answer counts as a failure
  probe-database: true      # SELECT 1 FROM DUAL
  probe-redis: true         # PING
  probe-kafka: true         # AdminClient describeCluster
```

Tuning notes:

- **Flapping** (`AaaDbWriteDependencyFlapping` firing): raise `failure-threshold`, or lengthen
  `probe-timeout-ms` if the dependency is simply slow rather than absent.
- **Slow detection**: lower `probe-interval`. Each probe is one round trip per pod, so 15s is
  cheap; below 5s the value drops off.
- **Probe cost**: the Kafka probe holds a single long-lived `AdminClient` per pod, created on
  first use and closed at shutdown. Set the matching `probe-*` flag to `false` to opt out of
  any individual probe.

## Where the wiring lives

| File | Role |
| --- | --- |
| `domain/service/ConnectivityMonitoringService.java` | State machine, metrics, probes |
| `domain/service/ConnectivityFailureClassifier.java` | Decides connectivity vs application error |
| `domain/service/ConnectivityFailureReason.java` | The `reason` tag values |
| `domain/service/ExceptionMetricsService.java` | Forwards dependency exceptions into the monitor |
| `application/config/ConnectivityMonitoringConfig.java` | `connectivity.*` configuration |
| `application/controller/ConnectivityResource.java` | `GET /monitoring/connectivity` |

Exceptions reach the monitor from `DBWriteRepository` and `ResilientDbWriteExecutor` (Oracle), `DBWriteConsumer` and `UMSWriteConsumer` (Kafka). Any future call site gets the same treatment
for free by recording its exception with the matching `ExceptionMetricsService.Source`.


## Notes

- `Source.ORACLE` and `Source.MYSQL` both map to the `database` dependency: UMS writes and accounting writes share the single reactive `Pool` this service injects, so there is one database connection to be up or down, not two.
