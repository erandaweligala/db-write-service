# Error Monitoring

How to find out **what is breaking in db-write-service, why, and how often** — without reading a log.

Metrics are exposed to Prometheus at `/q/metrics`; the same data is readable as JSON at
`GET /api/monitoring/errors`. A ready-to-import Grafana dashboard lives at
[`grafana/dashboards/application-errors-dashboard.json`](grafana/dashboards/application-errors-dashboard.json)
and alert rules at
[`grafana/alerts/application-errors-alerts.yml`](grafana/alerts/application-errors-alerts.yml).

---

## 1. The problem this solves

The service already counted exceptions by root-cause **class name**
(`ExceptionMetricsService` → `application_exception_root_count`). In practice that is not
enough to act on. A dashboard row reading:

```
SQLException ......... 4,812
```

tells an operator that something is wrong with the database, and nothing more. It does not
distinguish 4,812 duplicate-key rejections (harmless — a misbehaving producer) from 4,812
"TNS:no listener" failures (the database is gone). Both are `SQLException`. One is a
Tuesday; the other is an outage.

The catalog groups by the **full identity** of a fault instead:

```
Error          Code        Reason                                          Occurrences
SQLException   ORA-12541   TNS:no listener                                       4,802
SQLException   ORA-00001   unique constraint (AAA.PK_SESSION) violated on id #       10
```

Same total. Completely different conversation.

---

## 2. The four fields

Every row carries exactly what is needed to recognise a fault at a glance.

| Field | Example | Where it comes from |
|---|---|---|
| **Error** | `SQLException` | simple class name of the **root cause** (the cause chain is walked to the bottom) |
| **Code** | `ORA-12541` | the application's own response code if it set one, else the SQL vendor code / SQLState, else the HTTP status, else a `LETTERS-DIGITS` code at the head of the message. `none` when the error genuinely has no code |
| **Reason** | `TNS:no listener` | the exception message, with its variable parts masked (see §4) |
| **Occurrences** | `4,802` | how many times this exact fault has happened — **always present** |

Two extra fields are carried for when the row alone is not enough:

- **`sampleMessage`** — one real, unredacted message for this signature, so the masked
  `reason` can always be traced back to something concrete.
- **`origin`** — the `Class.method:line` that threw it.

Both are on the JSON endpoint. They are captured **once per distinct fault**, not per
occurrence.

---

## 3. Where to look

### Grafana

Import `grafana/dashboards/application-errors-dashboard.json` (Dashboards → New → Import)
and pick your Prometheus data source. The dashboard is one table, sorted by occurrences
descending — **the top row is the loudest problem you have** — over a rate-over-time panel
and four headline stats.

### Straight off the pod

```bash
curl -s http://localhost:8080/api/monitoring/errors | jq '.errors[0]'
```

```json
{
  "error": "SQLException",
  "code": "ORA-12541",
  "reason": "TNS:no listener",
  "occurrences": 4802,
  "layer": "repository",
  "source": "oracle",
  "sampleMessage": "ORA-12541: TNS:no listener",
  "origin": "SessionRepository.insert:214",
  "firstSeen": 1756108800000,
  "lastSeen": 1756112400000
}
```

### PromQL

```promql
# The catalog, worst first
sort_desc(sum by (error, code, reason) (increase(application_error_occurrences_total{service="db-write-service"}[1h])))

# Just one error class, split by code
sum by (code, reason) (increase(application_error_occurrences_total{service="db-write-service", error="SQLException"}[1h]))

# Current error rate, per minute
sum(rate(application_error_occurrences_total{service="db-write-service"}[5m])) * 60

# Errors that are new in the last 10 minutes (nothing like them in the hour before)
sum by (error, code, reason) (increase(application_error_occurrences_total{service="db-write-service"}[10m])) > 0
  unless
sum by (error, code, reason) (increase(application_error_occurrences_total{service="db-write-service"}[1h] offset 10m)) > 0
```

---

## 4. Why this does not blow up Prometheus

Raw exception messages are unique per occurrence:

```
ORA-00001: unique constraint (AAA.PK_SESSION) violated on id 88213
ORA-00001: unique constraint (AAA.PK_SESSION) violated on id 88214
```

Used directly as a label, each would create a new time series — the classic way to take
down a Prometheus. Two defences:

**1. The reason is normalised.** A single bounded pass over the message masks the parts
that change:

| Rule | Before | After |
|---|---|---|
| Wrapper prefixes dropped | `io.vertx.core.VertxException: Connection refused` | `Connection refused` |
| Digit-led tokens masked | `timed out after 30000ms` | `timed out after #` |
| Addresses masked | `refused: 10.200.140.151:1521` | `refused: #:#` |
| Long mixed identifiers masked | `session a3f1e2b4c5d67890` | `session #` |
| Quoted literals masked | `Unknown column 'subscriber_id'` | `Unknown column ?` |
| Truncated | anything over 96 chars | clipped with `...` |

Names that are *not* variable are deliberately kept, because they are the useful part and
are naturally low cardinality: `DC-DR`, `utf8mb4`, `PK_SESSION`, and anything in
parentheses — `unique constraint (AAA.PK_SESSION) violated` stays intact.

**2. There is a hard ceiling.** At 200 distinct signatures, further *new* signatures are
folded into a single `(other)` row instead of registering new meters. Already-known
signatures keep counting normally. An overflow costs you detail, never accuracy: the
totals stay exact, and the `AaaDbWriteErrorCatalogSaturated` alert tells you it is happening.

---

## 5. Performance

Nothing in the catalog runs on a success path. It runs only after an exception has already
been constructed — which is itself the expensive part.

| | Cost |
|---|---|
| Constructing the exception (20 frames deep), before we see it | ~944 ns |
| **Recording it** — normalise, look up, count | **~440 ns** |
| Success path | **0** — not on it |

Measured single-threaded on the target JDK 21. So the whole of error monitoring costs
about half again what the JVM already spent creating the exception. At a sustained
1,000 errors/sec that is 0.04% of one core; at 10,000/sec, 0.4%.

That number holds because the expensive work happens **once per distinct fault**, not once
per occurrence:

| | Repeat occurrence (the common case) | First sight of a signature (once, ever) |
|---|---|---|
| Message scan | yes, bounded to 96 chars of output | yes |
| Hash lookup + two counter increments | yes | yes |
| Stack-trace access (`getStackTrace()`) | **no** | yes — to record `origin` |
| Meter registration | **no** | yes |
| Locking, periodic sweeps, GC pressure in steady state | **none** | — |

All state is bounded and fixed after warm-up, so there is no periodic work to do and
nothing to collect.

---

## 6. How it is wired

`ExceptionMetricsService.recordException(...)` — already called from every catch site in
the service — now also feeds the catalog. **No call site changed.** The code, the reason
and the origin are all derived from the throwable that was already being passed.

The two views stay in step by construction: the catalog is fed from the same point, after
the same de-duplication, so an observation suppressed as a retry duplicate is absent from
both.

```
catch (Throwable t)
  └─ exceptionMetrics.recordException(t, Layer.X, Source.Y)
       ├─ application_exception_root_count   "which exception classes"
       ├─ ErrorCatalog.record(...)           "which specific faults, and why"  <-- new
       └─ ConnectivityMonitoringService      "is the dependency up"
```
