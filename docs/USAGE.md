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

A long-running Django worker is a management command that PM2 or systemd
keeps alive. There are two ways to wire it; **the command class is the one
to reach for**, and the settings-driven path exists for projects that
cannot change the command's base class.

### Step 1 — install the app

```python
INSTALLED_APPS = [
    # ...
    "worker_health_django.apps.WorkerHealthConfig",
]
```

### Step 2 — subclass the command

```python
# billing/management/commands/consume_billing.py
from worker_health_django import WorkerHealthCommand

class Command(WorkerHealthCommand):
    health_service = "billing-worker"     # metric label; defaults to the command name
    health_queue = "billing.in"

    def handle(self, *args, **options):
        @self.tracker.handler(queue="billing.in")
        def handle_message(body: dict):
            process_payment(body)         # ORM + cache observed automatically

        for raw in consume(stop=self.stopping):
            handle_message(json.loads(raw))
```

`handle()` is the method you would have written anyway. Four things change:

| | |
|---|---|
| `self.tracker` | already built when `handle()` runs — no module global to reach through |
| `self.stopping` | a `threading.Event`, set on SIGTERM/SIGINT. `self.sleep(n)` returns early on it, `self.should_stop` reads it |
| `--health-port` | plus `--health-host`, `--health-service`, `--health-queue`, `--no-health` |
| the process guard | *this* command is wired; `migrate`, `shell`, `test`, `runserver` are not, without either being listed anywhere |

That last one is the reason to prefer this over settings. `AppConfig.ready()`
runs for **every** `manage.py` entry point, so a settings-driven wiring has
to guess from `sys.argv` which process is a worker. Subclassing is not a
guess, and `should_wire()` checks for it first and steps aside.

### Step 3 — configure the checks

```python
WORKER_HEALTH = {
    "ENABLED": True,
    "HOST": "127.0.0.1",          # loopback unless the port is published
    "PORT": 8080,                 # base port; see "Ports" below
    "BOOT_GRACE": 30,

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

The ORM is instrumented, the cache client is found and instrumented when it
is Redis-backed, the probes are installed, and the health server starts —
in the worker process, on its own thread, never Django's.

### Step 4 — ports, when there is more than one worker

Two `manage.py` workers on one host cannot both have 8080. In precedence
order:

```
--health-port 8091                     explicit, wins over everything
health_port = 8091                     on the command class
"PORTS": {"consume_billing": 8091}     per command, keyed by command name
"PORT": 8080  + instance ordinal       PM2 cluster: 8080, 8081, 8082, ...
```

The ordinal comes from `NODE_APP_INSTANCE` / `PM2_INSTANCE_ID` / `pm_id`, so
`instances: 4` in PM2 lays a cluster out contiguously and gives each copy a
metric label that survives a restart (a pid does not). If the chosen port is
taken anyway, the next 20 are tried and the one actually bound is printed at
startup and published to the run registry — a busy port never stops a worker
from starting.

```
worker-health: billing-worker on http://127.0.0.1:8091
  port 8090 was busy; bound 8091 instead
```

### Step 5 — shutdown

PM2 sends SIGINT, Kubernetes sends SIGTERM, and both then wait before
SIGKILL. Those seconds are for finishing the message in hand, so on the
first signal the command:

- reports **`unready` immediately** — 503 on `/ready`, still **200 on
  `/live`**, so a liveness probe does not escalate an orderly shutdown into
  a kill;
- sets `self.stopping`, which `self.sleep()` and a well-written consume loop
  watch;
- calls `on_shutdown(signum)` — override it to break a blocking consume:

```python
    def on_shutdown(self, signum):
        # signal-handler rules: one non-blocking thing
        self.channel.connection.add_callback_threadsafe(self.channel.stop_consuming)
```

A second signal stops immediately. On the way out the monitor is stopped and
the run-registry entry removed.

### Step 6 — inspect the workers on a host

`manage.py worker_health` is a **new process**, so it cannot read a monitor
that lives in the worker. Every worker that binds a port publishes service,
pid and port to a per-user run directory, and this reads it:

```bash
python manage.py worker_health --list      # every worker on this host
python manage.py worker_health             # status of each, over HTTP
python manage.py worker_health --json
python manage.py worker_health --metrics
python manage.py worker_health --url http://other-host:8080
```

```
billing-worker (http://127.0.0.1:8091, pid 4412): readiness=ready liveness=alive
  postgres           ok        observed      critical
  redis-cache        ok        observed      non-critical
  rabbitmq           ok        introspected  critical
  queue billing.in   received=8134 succeeded=8130 failed=4 depth=12
```

The standalone CLI does the same without Django: `worker-health --list`,
`worker-health` (discovers the only worker on the host), `worker-health
--service billing-worker`.

### Step 7 — under PM2

```js
// ecosystem.config.js
module.exports = {
  apps: [{
    name: "billing-worker",
    script: "manage.py",
    args: "consume_billing",
    interpreter: "python3",
    instances: 2,                 // health ports 8080, 8081
    kill_timeout: 30000,          // give the drain time to finish
    env: { DJANGO_SETTINGS_MODULE: "project.settings" },
  }],
};
```

`kill_timeout` is the setting that matters: it is how long PM2 waits between
the signal and SIGKILL, and therefore how much draining is actually allowed.

### Step 8 — class-based consumers

A consumer that carries state is naturally a class. `@handler` on a class
wraps `__call__` in place:

```python
        @self.tracker.handler(queue="billing.in")
        class BillingConsumer:
            def __call__(self, body): ...

        # or, when the entry point has a name:
        @self.tracker.consumer_class(queue="billing.in", method="handle")
        class BillingConsumer:
            def handle(self, body): ...
```

Coroutines, async generators and sync generators are all detected. An
`async def __call__` is timed **across the await** — the naive
`inspect.iscoroutinefunction(instance)` check returns `False` for a callable
instance, which silently measures how long it took to *create* the coroutine
and reports every message as taking microseconds.

### Alternative — no base class

If the command cannot change its base class, name it in settings and reach
for the tracker through module state:

```python
WORKER_HEALTH = {
    "ENABLED": True,
    # Only these management commands get a monitor.
    "COMMANDS": ["consume_billing"],
    # ...
}
```

```python
from worker_health_django import get_tracker

class Command(BaseCommand):
    def handle(self, *args, **options):
        tracker = get_tracker()

        @tracker.handler(queue="billing.in")
        def handle_message(body): ...
```

`get_tracker()` returns a no-op tracker when health is disabled, so the same
command runs unchanged in a test suite or a shell. `--health-port` cannot
work on this path — the server is already bound by the time the command
parses its arguments — and the port comes from settings alone.

### Optional — in-app health URLs

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

### Optional — keep your existing django-health-check backends

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
app.include_router(router)      # /internal/live, /ready, /health, /config
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
| `/live` | Is this process wedged? Loop lag, or a fault it did to itself. Never 503s for a dependency. |
| `/ready` | Full readiness. 503 on `starting` or `unready`. |
| `/health` | Everything, including timing windows, per-check settings and recent events. |
| `/config` | The settings behind the verdicts: intervals, timeouts, thresholds, criticality, and which clients are instrumented. Redacted. |
| `/events` | The last 50 structured events. |

And from a shell or a PM2 healthcheck:

```bash
worker-health --url http://127.0.0.1:8080          # exit 0 ready, 1 not
worker-health --url http://127.0.0.1:8080 --live
worker-health --url http://127.0.0.1:8080 --json
```

---

## 6. Dashboards

**OTLP push** — set one endpoint and the worker pushes metrics and
transition events to it:

```yaml
worker_health:
  otel_endpoint: http://otel-collector:4318
  otel_interval: 15.0
```

There is no scrape endpoint. A supervised fleet has no stable scrape targets,
so the worker pushes to a URL it is told about instead of waiting to be
found. Export is bounded, off-thread and silent; its counters appear under
`export` on `/health`, which is where you look when a collector goes quiet.
See [OBSERVABILITY.md](OBSERVABILITY.md) for the metric reference and the
alert rules.

The compose stack runs a collector at `docker/otel-collector.yaml` so the
push path is exercised end to end; `docker compose logs otel-collector` shows
what arrives.

**The bundled fleet dashboard** — `docker compose up -d` and open
<http://localhost:9000>. It receives the workers' OTLP push (so workers are
discovered rather than configured), polls `/health` for the addresses it
knows, and streams to the browser over SSE. It groups a shared dependency
failure into one row instead of one row per sick worker, and flags workers
that are alive but not consuming. No backend required.

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
/live                                    → is this process wedged?
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
