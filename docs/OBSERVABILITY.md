# Observability

Metrics, structured events, dashboards and alerts.

- [Metric reference](#metric-reference)
- [Why two families per verdict](#why-two-families-per-verdict)
- [Label cardinality](#label-cardinality)
- [Structured events](#structured-events)
- [Dashboard](#dashboard)
- [Configuration reporting](#configuration-reporting)
- [Alerts](#alerts)

---

## Metric reference

All at `GET /metrics`, Prometheus text format. Every series carries
`service` and `instance`.

### Worker verdicts

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `worker_health_ready` | gauge | — | 1 when `/ready` would return 200 |
| `worker_health_live` | gauge | — | 1 when `/live` would return 200 |
| `worker_health_status` | gauge | — | aggregate severity: 0 ok, 1 starting, 2 degraded, 3 unknown, 4 failing |
| `worker_health_readiness_state` | gauge | `state` | one-hot: ready / degraded / unready / starting |
| `worker_health_uptime_seconds` | gauge | — | monitor uptime |
| `worker_health_boot_complete` | gauge | — | 0 while `starting` |

### Per check

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `worker_health_check_status` | gauge | `check`, `critical`, `evidence` | 1 healthy, 0 not |
| `worker_health_check_severity` | gauge | `check`, `critical`, `evidence` | 0–4, as above |
| `worker_health_check_latency_ms` | gauge | `check`, `critical`, `evidence` | last evaluation latency |
| `worker_health_check_evidence_age_ms` | gauge | `check`, `critical`, `evidence` | age of the signal behind the verdict |
| `worker_health_check_transitions_total` | counter | `check`, `critical`, `evidence` | status changes — flap detector |
| `worker_health_check_interval_seconds` | gauge | `check` | current interval, backoff included |
| `worker_health_check_error` | gauge | `check`, `category` | 1 for the category currently reported |

### Processing

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `worker_health_message_received_total` | counter | `queue` | messages entering the handler |
| `worker_health_message_success_total` | counter | `queue` | handled without raising |
| `worker_health_message_failure_total` | counter | `queue` | handler raised |
| `worker_health_messages_in_flight` | gauge | `queue` | currently being handled |
| `worker_health_queue_lag` | gauge | `queue` | messages waiting in the broker |
| `worker_health_last_message_age_seconds` | gauge | `queue` | silence since the last delivery |
| `worker_health_last_success_age_seconds` | gauge | `queue` | silence since the last success |
| `worker_health_handler_duration_ms` | gauge | `queue`, `quantile` | p50 / p95 / p99 / max |

### Worker internals

| Metric | Meaning |
|---|---|
| `worker_health_loop_lag_ms` | how far the health loop slipped from its cadence |
| `worker_health_runner_tick_delay_ms` | lateness of the last runner tick |
| `worker_health_snapshot_build_ms` | time to assemble a snapshot |
| `worker_health_worker_to_health_delta_ms` | **how far behind the worker the health signal is** |
| `worker_health_last_activity_age_ms` | since the worker last did real work |
| `worker_health_eval_age_ms` | age of the newest check evaluation |
| `worker_health_oldest_eval_age_ms` | age of the stalest check evaluation |

Plus rolling windows for everything timed: `worker_health_window_ms{metric,
stat}` and `worker_health_window_count{metric}`, covering per-dependency
call latency (`dependency.<name>.duration_ms`), per-stage handler timing
(`worker.<queue>.stage.<name>_ms`), and check durations.

`worker_health_worker_to_health_delta_ms` is the one with no equivalent in
other health libraries, and the one to put on a dashboard. A check that runs
in 2ms but is standing on a 90-second-old observation is not a 2ms-fresh
signal — this is the number that says so.

---

## Why two families per verdict

Each verdict is exported twice, on purpose:

```promql
worker_health_check_status{check="postgres"} == 0      # binary: alert on this
worker_health_check_severity{check="postgres"}         # 0..4: chart this
```

Alerts read the binary family because `== 0` cannot be misread and keeps
working if a status value is added later. Dashboards read the severity
family because they want to colour `degraded` differently from `failing`.
Making one series do both jobs is how an alert ends up saying `< 2` and
nobody remembers why.

In the binary family, `disabled` counts as **up**: a check switched off
deliberately must not page anyone.

---

## Label cardinality

Every label value comes from a registered name or a closed enum:

| Label | Source |
|---|---|
| `service`, `instance` | configuration |
| `check` | registered check names |
| `queue` | queue names passed to `@handler` |
| `critical` | `"true"` / `"false"` |
| `evidence` | `observed` / `introspected` / `probed` / `none` |
| `category` | the `ErrorCategory` enum |
| `state` | the `Readiness` enum |
| `quantile`, `stat`, `metric` | fixed sets |

No error string, exception message, host, or message ID ever becomes a
label. That is enforced structurally: what gets reported is a closed enum,
not the exception text — see [OPERATIONS.md](OPERATIONS.md#security).

---

## Structured events

One JSON object per line on stdout, and the last 50 at `GET /events`.

```json
{
  "timestamp": "2026-06-15T10:20:30Z",
  "level": "error",
  "event": "health_transition",
  "service": "billing-worker",
  "instance": "billing-1",
  "check": "postgres",
  "previous_status": "ok",
  "current_status": "failing",
  "critical": true,
  "category": "timeout",
  "evidence": "probed",
  "latency_ms": 2001.4,
  "evidence_age_ms": 0.0,
  "detail": "check exceeded its timeout"
}
```

### Catalogue

| Event | When | Level |
|---|---|---|
| `worker_started` | monitor started | info |
| `worker_health_configured` | wiring summary: probes installed, clients instrumented | info |
| `worker_stopped` | monitor stopped | info |
| `boot_grace_started` / `boot_grace_completed` | boot window opens / every critical check has been healthy once | info |
| `check_registered` | a check was registered | info |
| `health_transition` | a check's effective status changed | info / warning / error |
| `dependency_recovered` | a check returned to OK from failing or degraded | info |
| `readiness_changed` | `/ready`'s answer changed, with `reasons` | info / warning / error |
| `liveness_changed` | `/live`'s answer changed, with `loop_lag_ms` | info / error |
| `processing_stale_detected` | `stalled` or `not_consuming` | error |
| `queue_lag_threshold_crossed` | backlog above threshold | error |
| `probe_timeout` / `probe_error` | a check timed out or raised | warning |
| `local_fault_detected` | a fault a restart could plausibly repair | error |

### What is deliberately not logged

Successful probes, health endpoint requests, every scheduler tick,
credentials, connection strings, and driver exception text. A worker that
logs every successful probe produces ~5,760 lines a day per check and
teaches its team to filter the whole stream out.

### Consuming events yourself

```python
health.monitor.on_event(lambda event: my_queue.put(event))
```

A sink that raises is swallowed — a broken exporter must never break health.

---

## Dashboard

Import [`deploy/grafana/worker-health-overview.json`](../deploy/grafana/worker-health-overview.json).
Variables: `datasource`, `logs`, `service`, `instance`, `queue`, `check`.

**One question per row**, and only the three you need during an incident are
open by default. Every panel carries a `description`, which Grafana shows as
an ⓘ in its header — so the explanation travels with the panel instead of
living in a document nobody opens at 3am.

| Row | The question it answers | Open by default |
|---|---|---|
| Fleet status | Can my workers do their job right now? | yes |
| Dependencies | If not, which dependency is at fault? | yes |
| Message flow | Is work actually moving through them? | yes |
| Latency detail | Why is it slow? | collapsed |
| Evidence freshness | Can I trust what this dashboard says? | collapsed |
| State changes | What changed, and when? | collapsed |

The first panel of the first row is a text panel repeating that table plus the
status vocabulary, so a reader who has never seen the dashboard can orient
without leaving it.

Two panels are worth knowing by name:

- **Backlog vs silence** plots queue depth against time-since-last-message on
  one chart. Neither is alarming alone — a deep queue with a busy consumer is
  fine, and a silent consumer on an empty queue is fine. Both rising together
  is the stuck consumer, and it is what `SilentConsumerWithBacklog` fires on.
- **How far behind the worker is the health signal** is the freshness check on
  the dashboard itself. If it climbs, every other panel is describing a worker
  nobody has heard from recently.

The state-change log needs a Loki (or equivalent) datasource selected in the
`logs` variable. Without a log pipeline, the same events are available at each
worker's `/events`.

The repo also ships a zero-dependency live dashboard (`docker compose up -d`,
then <http://localhost:9000>) that polls `/health` and streams to browsers
over SSE. It leads with one plain sentence — *"All 3 workers are processing
normally"*, or *"reconcile cannot process work: postgres is failing — it
accepted the connection but never answered"* — and carries a built-in "How to
read this page" guide covering the four states, the three evidence levels and
what each graph shows.

---

## Configuration reporting

`GET /config` answers the question metrics cannot: **what settings is this
worker making its decisions with?**

```json
{
  "service": "billing-worker",
  "instance": "billing-1",
  "runner": "thread",
  "boot_grace_s": 25,
  "checks": {
    "postgres": {
      "critical": true, "enabled": true,
      "interval_s": 15.0, "timeout_s": 2.0, "ttl_s": 32.0,
      "failure_threshold": 3, "success_threshold": 2,
      "max_silence_s": 60.0,
      "backoff_initial_s": 5.0, "backoff_max_s": 60.0,
      "check_class": "PostgresCheck", "dependency": "postgres"
    }
  },
  "queues": ["billing.in"],
  "instrumented": {"db_engine": "postgres", "redis_client": "redis"},
  "source": "/app/workers/worker-health.yaml",
  "probes": [ ... the declared specs, params redacted ... ]
}
```

Three properties worth knowing:

- **It reports what is in force, not what was written.** Every value is read
  off the running state machine, so a check disabled with
  `monitor.set_enabled(...)` shows as disabled here. Two sources of truth for
  a threshold is how a dashboard ends up disagreeing with the behaviour it
  describes.
- **`instrumented` is the answer to the most common confusion.** If a check
  reports `probed` when you expected `observed`, this map says whether its
  client was ever instrumented.
- **It is redacted.** Probe params can hold a DSN; the password is masked and
  the host survives, because knowing *which* database is the point.

The same per-check block appears in `/health` under `checks.<name>.config`.
It is deliberately absent from `/ready`, which a supervisor polls every few
seconds and which does not need thresholds to decide whether to route
traffic.

The bundled dashboard renders this as a **Probe configuration** panel:
one row per check, every column captioned with what the setting does, and
workers sharing a configuration grouped into one block — so a worker whose
settings have drifted from the rest of the fleet appears as its own group.

Like every other endpoint on this port, `/config` is unauthenticated. It
exposes no credentials, but it does describe your topology — bind the health
port to loopback or a private interface, as
[OPERATIONS.md](OPERATIONS.md#security) says.

---

## Alerts

The full set is in [`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml).
The five that matter most:

```yaml
# Cannot process work.
- alert: WorkerNotReady
  expr: worker_health_ready == 0
  for: 2m
  labels: {severity: critical}

# The loop is wedged. This is the one a restart actually fixes.
- alert: WorkerNotAlive
  expr: worker_health_live == 0
  for: 1m
  labels: {severity: critical}

# A dependency the worker cannot work without.
- alert: CriticalDependencyFailing
  expr: worker_health_check_status{critical="true"} == 0
  for: 2m
  labels: {severity: critical}

# The signature this project exists to detect: work waiting, nobody taking it.
- alert: SilentConsumerWithBacklog
  expr: |
    worker_health_queue_lag > 100
    and on (service, instance, queue)
    worker_health_last_message_age_seconds > 120
  for: 5m
  labels: {severity: critical}

# A green light standing on stale evidence.
- alert: HealthEvidenceStale
  expr: worker_health_oldest_eval_age_ms > 120000
  for: 5m
  labels: {severity: warning}
```

`SilentConsumerWithBacklog` is the one to keep. Neither half alerts alone:
a deep queue with a busy consumer is fine, and a silent consumer on an empty
queue is fine. The conjunction is the outage — and it is invisible to every
process-level check.

Note also `WorkerHealthEndpointDown` (`up == 0`): a worker that is
unreachable is not the same as a worker that is unready, and without its own
rule it looks healthy by absence.
