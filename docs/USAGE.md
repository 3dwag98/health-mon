# Usage guides

Step by step, for the four ways a worker gets wired up.

- [1. Generic worker](#1-generic-worker)
- [2. Django](#2-django)
- [3. FastAPI](#3-fastapi)
- [4. Custom probes](#4-custom-probes)
- [5. Reading the output](#5-reading-the-output)
- [6. Dashboards](#6-dashboards)
- [7. Troubleshooting](#7-troubleshooting)

---

## 1. Generic worker

### Step 1 — install

```bash
pip install "worker-health[all]"     # or pick the extras you need:
                                     # postgres, redis, rabbitmq, yaml,
                                     # django, fastapi
```

### Step 2 — write the config

`worker-health.yaml`, next to your worker:

```yaml
worker_health:
  service: ${SERVICE:-billing-worker}
  health_port: ${HEALTH_PORT:-8080}
  boot_grace: 30
  default_queue: ${IN_QUEUE:-billing.in}

  probes:
    - type: postgres
      name: postgres
      critical: true
      interval: 15
      timeout: 2
      max_silence: 60          # probe only after 60s with no real traffic
      params:
        engine: "@db_engine"   # a reference into the runtime context

    - type: redis
      name: redis-cache
      critical: false          # degrades, never 503s
      interval: 30
      timeout: 1
      params:
        client: "@redis_client"

    - type: rabbitmq
      name: rabbitmq
      critical: true
      interval: 5
      timeout: 1
      params:
        broker_state: "@broker_state"
        queue: ${IN_QUEUE:-billing.in}
```

No credentials go in this file. `${VAR}` and `${VAR:-default}` are expanded
from the environment before parsing, and `@name` values are resolved from
the runtime context in step 3.

### Step 3 — one call at startup

```python
from worker_health import BrokerState, setup_worker_health

broker_state = BrokerState()

health = setup_worker_health(
    service="billing-worker",
    config_path="worker-health.yaml",
    context={
        "db_engine": engine,          # instrumented automatically
        "redis_client": redis_client, # instrumented automatically
        "amqp_connection": connection,# lifecycle callbacks attached
        "broker_state": broker_state, # referenced by the rabbitmq probe
    },
)
```

That call installs the probes, instruments the clients, registers the
processing check, starts the health server on its own thread, and starts
the scheduler.

### Step 4 — decorate the handler

```python
@health.tracker.handler(queue="billing.in")
def handle(message: dict):
    process_payment(message)      # unchanged
```

This is the only required change to business code. It records received /
succeeded / failed, duration, and last-activity — the whole input to the
processing check and to the worker→health delta.

### Step 5 — consume

```python
from worker_health.instrument import instrument_pika_channel

channel = connection.channel()
instrument_pika_channel(channel, health.monitor, broker_state)
channel.queue_declare(queue="billing.in", durable=True)
channel.basic_qos(prefetch_count=10)

def on_message(ch, method, properties, body):
    try:
        handle(json.loads(body))
    except Exception:
        ch.basic_nack(method.delivery_tag, requeue=False)
        raise
    ch.basic_ack(method.delivery_tag)

channel.basic_consume(queue="billing.in", on_message_callback=on_message)
channel.start_consuming()
```

`instrument_pika_channel` is what removes the hand-written
`last_delivery_at` / `unacked` bookkeeping. Add the passive-declare probe so
the check can see queue depth:

```python
from worker_health import install_broker_probe

install_broker_probe(connection, broker_state, "billing.in", interval=2.0)
```

### Step 6 — shut down cleanly

```python
try:
    channel.start_consuming()
finally:
    health.stop()          # stops the scheduler and the health server
```

That is the whole integration. `workers/` in this repo is three complete
workers built exactly this way.

---

## 2. Django

### Step 1 — install the app

```python
INSTALLED_APPS = [
    # ...
    "worker_health_django.apps.WorkerHealthConfig",
]
```

### Step 2 — configure

```python
WORKER_HEALTH = {
    "ENABLED": True,
    "SERVICE": "billing-worker",
    "HOST": "127.0.0.1",          # loopback unless the port is published
    "PORT": 8080,
    "DEFAULT_QUEUE": "billing.in",
    "BOOT_GRACE": 30,

    # Only these management commands get a monitor. Without it, the
    # default skip-list is used (migrate, collectstatic, shell, test, ...).
    "COMMANDS": ["consume_billing"],

    # {alias: dependency name} — which ORM connections to observe.
    "DATABASES": {"default": "postgres"},
    "CACHE_ALIAS": "default",

    # Objects the probes reference as "@name", as import paths.
    "CONTEXT": {"broker_state": "billing.broker:state"},

    "PROBES": [
        {"type": "django_db", "name": "postgres", "critical": True,
         "interval": 15, "timeout": 2, "max_silence": 60},
        {"type": "redis", "name": "redis-cache", "critical": False,
         "interval": 30, "timeout": 1,
         "params": {"client": "@redis_client"}},
        {"type": "rabbitmq", "name": "rabbitmq", "critical": True,
         "interval": 5, "timeout": 1,
         "params": {"broker_state": "@broker_state", "queue": "billing.in"}},
    ],
}
```

`ready()` wires everything: the ORM is instrumented, the cache client is
found and instrumented when it is Redis-backed, the probes are installed,
and the health server starts.

### Step 3 — the worker command

```python
from django.core.management.base import BaseCommand
from worker_health_django import get_tracker

class Command(BaseCommand):
    help = "Consumes billing events"

    def handle(self, *args, **options):
        tracker = get_tracker()

        @tracker.handler(queue="billing.in")
        def handle_message(body: dict):
            process_payment(body)      # ORM queries observed automatically

        for raw in consume_messages():
            handle_message(json.loads(raw))
```

`get_tracker()` returns a no-op tracker when health is disabled, so the same
command runs unchanged in a test suite or a shell.

### Step 4 — inspect from the same process

```bash
python manage.py worker_health            # human-readable
python manage.py worker_health --json     # the full snapshot
python manage.py worker_health --metrics  # Prometheus exposition
```

### Step 5 — optional: in-app health URLs

A Django worker usually serves no HTTP at all. Where a platform can only
reach one port, mount the views:

```python
urlpatterns = [
    path("internal/health/", include("worker_health_django.urls")),
]
```

That serves `live`, `ready`, `health`, `config`, `events` and `metrics`
under the prefix. They are served by Django's own worker, so they stop
answering under exactly the conditions they exist to report — the SDK's
threaded port stays authoritative for liveness. There is no authentication;
see [OPERATIONS.md](OPERATIONS.md#security).

### Step 6 — optional: keep your existing django-health-check backends

If the project already uses
[django-health-check](https://django-health-check.readthedocs.io/), its
backends run as worker-health checks unchanged:

```python
WORKER_HEALTH = {
    "ADOPT_HEALTH_CHECK_PLUGINS": True,
    # The SDK's own django_db probe covers the database read-only;
    # django-health-check's writes a row on every check.
    "HEALTH_CHECK_SKIP": ["DatabaseBackend"],
}
```

Each backend keeps its own `critical_service` setting and gains scheduling
off the request path, a timeout, thresholds and backoff. A single backend
can also be wired as a probe:

```yaml
- type: django_health_check
  name: vendor-api
  critical: false
  interval: 30
  params:
    backend: "myapp.health:VendorBackend"
```

See [PRIOR-ART.md](PRIOR-ART.md#django) for what differs and why.

A complete example lives in [`examples/django_worker/`](../examples/django_worker).

---

## 3. FastAPI

### Step 1 — the lifespan

```python
from fastapi import FastAPI
from worker_health import BrokerState
from worker_health_fastapi import HealthSettings, health_lifespan

broker_state = BrokerState()

app = FastAPI(lifespan=health_lifespan(
    settings=HealthSettings(),          # reads HEALTH_* env vars
    context=lambda: {                   # a callable: runs at startup
        "db_engine": engine,
        "redis_client": redis_client,
        "broker_state": broker_state,
    },
    consumers=[BillingConsumer],        # started as tasks, cancelled cleanly
))
```

The default runner here is `asyncio`, so the loop-lag probe measures the
thing that actually breaks an async worker: a coroutine blocking the loop.

### Step 2 — the consumer

```python
class BillingConsumer:
    def __init__(self, tracker):
        self.handle_message = tracker.handler(queue="billing.in")(
            self._handle_message
        )

    async def _handle_message(self, body: dict):
        await process_payment(body)

    async def run(self):
        async for raw in consume_from_queue():
            await self.handle_message(json.loads(raw))
```

The decorator detects coroutine functions and adapts; nothing else changes.

### Step 3 — optional in-app routes

```python
from worker_health_fastapi.routes import router
app.include_router(router)      # /internal/live, /ready, /health, /metrics
```

These serve the same data from the event loop. The SDK's own threaded port
remains authoritative for liveness — it keeps answering when the loop is
wedged, which is exactly when the answer matters.

### Step 4 — dependencies

```python
from fastapi import Depends
from worker_health_fastapi import get_monitor

@app.get("/queue-depth")
async def depth(monitor=Depends(get_monitor)):
    return monitor.snapshot_dict()["processing"]
```

A complete example lives in [`examples/fastapi_worker/app.py`](../examples/fastapi_worker/app.py).

---

## 4. Custom probes

### Function-based

```python
from worker_health.probes import default_factory
from worker_health.checks.network import HttpProbe

factory = default_factory()

@factory.probe_type("vendor-api")
def build_vendor_probe(spec, context):
    return HttpProbe(
        url=spec.params["url"],
        expect_status=spec.params.get("expect_status", 200),
        timeout=spec.timeout,
    )

health = setup_worker_health(service="billing", factory=factory, ...)
```

Then in YAML:

```yaml
- type: vendor-api
  name: vendor-api
  critical: false
  interval: 30
  timeout: 2
  params:
    url: https://api.vendor.com/ping
```

### Class-based

```python
import time
from worker_health import CheckResult, Evidence, Status
from worker_health.checks.base import BaseCheck

class S3BucketProbe(BaseCheck):
    def __init__(self, bucket: str, client, timeout: float = 2.0):
        self.name = "s3"
        self.dependency = ""          # set to observe real traffic instead
        self.bucket = bucket
        self.client = client

    def probe(self, ctx):
        started = time.perf_counter()
        self.client.head_bucket(Bucket=self.bucket)   # read-only
        return self.ok(ctx, started, bucket=self.bucket)

    def classify(self, exc):
        from worker_health import ErrorCategory
        return ErrorCategory.CONNECTION_LOST

@factory.probe_type("s3")
def build_s3_probe(spec, context):
    return S3BucketProbe(spec.params["bucket"], context["s3_client"],
                         timeout=spec.timeout)
```

Subclassing `BaseCheck` gets you the whole evidence ladder: set
`dependency` to a name your instrumentation records under and the check
will prefer real traffic, falling back to your `probe()` only after
`max_silence` seconds of silence.

### In-code, no factory

```python
@health.monitor.check("cache-drift", critical=False, interval=5.0)
def cache_drift():
    return Status.OK if drift_ratio() < 0.05 else Status.DEGRADED
```

Returns a `bool`, a `Status`, or a full `CheckResult`. A custom check that
raises is isolated: it reports `unknown` and no other check is affected.

### Shipped as a plugin

Any distribution can advertise probe types:

```toml
[project.entry-points."worker_health.probes"]
s3 = "mycompany.probes.s3:build_s3_probe"
vendor = "mycompany.probes.vendor:build_vendor_probe"
```

```python
factory = default_factory()
factory.load_plugins()          # setup_worker_health() does this for you
```

A plugin that fails to import is skipped rather than fatal — a broken
third-party probe must not stop a worker from reporting health.

---

## 5. Reading the output

```bash
curl -s localhost:8080/ready | jq
```

```json
{
  "status": "degraded",
  "readiness": "degraded",
  "liveness": "alive",
  "service": "billing-worker",
  "instance": "billing-1",
  "uptime_s": 1843.2,
  "reasons": ["check redis-cache is failing (connection_refused)"],
  "checks": {
    "postgres": {
      "status": "pass",
      "internal_status": "ok",
      "evidence": "observed",
      "latency_ms": 3.2,
      "evidence_age_ms": 412.0,
      "critical": true,
      "enabled": true,
      "transitions": 0,
      "next_interval_s": 15.0
    },
    "redis-cache": {
      "status": "fail",
      "internal_status": "failing",
      "evidence": "probed",
      "category": "connection_refused",
      "critical": false,
      "next_interval_s": 20.0
    }
  },
  "processing": {
    "billing.in": {
      "received": 12043, "succeeded": 12040, "failed": 3,
      "in_flight": 1, "queue_lag": 12,
      "last_message_age_s": 0.4, "last_success_age_s": 0.4
    }
  },
  "timing": {
    "loop_lag_ms": 1.8,
    "worker_to_health_delta_ms": 412.0,
    "health_eval_age_ms": 900.2,
    "snapshot_build_ms": 0.21
  }
}
```

The four fields worth learning:

| Field | Question it answers |
|---|---|
| `evidence` | Is this verdict from real traffic, local state, or a synthetic probe? |
| `evidence_age_ms` | How old was the signal when the verdict was formed? |
| `worker_to_health_delta_ms` | How far behind the worker is the health signal? |
| `reasons` | Why is readiness not `ready`? |

Endpoints:

| Path | Purpose |
|---|---|
| `/live` | Loop responsiveness only. Never 503s for a dependency. |
| `/ready` | Full readiness. 503 on `starting` or `unready`. |
| `/health` | Everything, including timing windows, per-check settings and recent events. |
| `/config` | The settings behind the verdicts: intervals, timeouts, thresholds, criticality, and which clients are instrumented. Redacted. |
| `/metrics` | Prometheus exposition. |
| `/events` | The last 50 structured events. |

And from a shell or a PM2 healthcheck:

```bash
worker-health --url http://127.0.0.1:8080          # exit 0 ready, 1 not
worker-health --url http://127.0.0.1:8080 --live
worker-health --url http://127.0.0.1:8080 --json
```

---

## 6. Dashboards

**Prometheus** — [`deploy/prometheus/prometheus.yml`](../deploy/prometheus/prometheus.yml)
and [`alerts.yml`](../deploy/prometheus/alerts.yml):

```yaml
scrape_configs:
  - job_name: worker-health
    metrics_path: /metrics
    static_configs:
      - targets: [billing-worker:8080, notify-worker:8080]
```

**Grafana** — import
[`deploy/grafana/worker-health-overview.json`](../deploy/grafana/worker-health-overview.json).
Six rows, one question each, and only the first three are open by default:
fleet status, dependencies, message flow — then latency detail, evidence
freshness and state changes, collapsed. Every panel has an ⓘ explaining what
it measures and what a bad value looks like. Variables: `service`,
`instance`, `queue`, `check`.

**The bundled live dashboard** — `docker compose up -d` and open
<http://localhost:9000>. It polls `/health` from every worker and streams to
the browser over SSE; no Prometheus required.

It is built to be readable without knowing the SDK: it opens with one plain
sentence about the whole fleet, every tile says what its number means, each
dependency row explains itself in words (*"nothing is listening on that
port"* rather than `connection_refused`), and a "How to read this page" guide
at the bottom covers the four states, the three evidence levels and what each
graph shows.

See [OBSERVABILITY.md](OBSERVABILITY.md) for the full metric reference.

---

## 7. Troubleshooting

### The worker is alive but not processing

Look at, in this order:

```
/live                                    → is the loop turning?
/ready → reasons                         → what does the SDK think is wrong?
processing.<queue>.queue_lag             → is there work waiting?
processing.<queue>.last_message_age_s    → how long has it been silent?
checks.rabbitmq.observed.consumer_state  → is it still subscribed?
checks.rabbitmq.observed.unacked/prefetch→ credit exhausted?
```

Backlog present and nothing received → the check reports `not_consuming`.
Queue empty and nothing received → that is a healthy idle worker, and the
SDK deliberately does not alert on it.

### `/ready` returns 503 but `/live` returns 200

Working as designed: the process is alive but cannot safely process work.
The `reasons` array names the check and its category. Do **not** restart for
a dependency fault — see the failure matrix in
[OPERATIONS.md](OPERATIONS.md#failure-handling-matrix).

### What settings is a check actually running on?

```bash
curl -s localhost:8080/config | jq '.checks.postgres'
```

```json
{
  "critical": true,
  "enabled": true,
  "interval_s": 15.0,
  "timeout_s": 2.0,
  "ttl_s": 32.0,
  "failure_threshold": 3,
  "success_threshold": 2,
  "max_silence_s": 60.0,
  "backoff_initial_s": 5.0,
  "backoff_max_s": 60.0,
  "check_class": "PostgresCheck",
  "dependency": "postgres"
}
```

Read off the running state machine rather than the config file, so it
reflects anything changed at runtime. The same data is on the bundled
dashboard under **Probe configuration**, and per check in `/health` under
`checks.<name>.config`.

### A dependency shows `probed` when you expected `observed`

Either the worker has genuinely been silent for longer than `max_silence`,
or instrumentation did not attach. Check the `worker_health_configured`
event at startup — it lists exactly which clients were instrumented:

```bash
curl -s localhost:8080/events | jq '.events[] | select(.event=="worker_health_configured")'
```

If the client is missing from `instrumented`, the object was not in the
`context` you passed, or its driver version moved the method being patched.
Instrumentation is best-effort; the worker still reports, with weaker
evidence.

### Health says OK but the worker looks silent

Compare:

```
timing.worker_to_health_delta_ms   → age of the freshest observed evidence
timing.health_oldest_eval_age_ms   → age of the stalest check
processing.<queue>.queue_lag
processing.<queue>.last_message_age_s
```

Empty queue → silence is valid. Non-empty queue with a rising
`last_message_age_s` → a stuck consumer, and the processing check will say
so within `max_idle`.

### A check flaps

Raise `failure_threshold` (absorb more blips) and `success_threshold`
(confirm recovery harder). The defaults are 3 and 2. Transition counts are
on every check as `transitions`, and as
`worker_health_check_transitions_total` in the metrics.

### The health server did not start

`setup_worker_health` logs a warning and keeps going if the port is taken —
usually a second copy of the worker on one host, or a Django autoreloader.
Everything except the HTTP endpoints still works. Give each worker its own
port, or set `COMMANDS` in Django so only the real worker wires up.
