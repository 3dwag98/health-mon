# Integrating worker-health

The short version. Full guides are in [USAGE.md](USAGE.md); every setting is
in [CONFIGURATION.md](CONFIGURATION.md).

## The minimum

```python
from worker_health import setup_worker_health

health = setup_worker_health(
    service="billing-worker",
    config_path="worker-health.yaml",
    context={
        "db_engine": engine,
        "redis_client": redis_client,
        "broker_state": broker_state,
    },
)


@health.tracker.handler(queue="billing.in")
def handle(message: dict):
    process_payment(message)      # unchanged
```

That gets you `/live`, `/ready`, `/health`, `/metrics`, `/events`,
structured logs, dependency checks, processing health and dashboard
metrics.

The decorator is the only required change to handler code. It records
received / succeeded / failed, duration and last-activity — the whole input
to the processing check and to the worker→health delta.

## What the context is for

Two things at once:

- **Auto-instrumentation.** The engine, Redis client and broker connection
  are detected by shape and instrumented, so every real query, command and
  broker event becomes `observed` evidence. No per-call wrapping.
- **Probe references.** `"@db_engine"` in the config resolves to the object
  under that key.

```yaml
probes:
  - type: postgres
    name: postgres
    critical: true
    interval: 15
    timeout: 2
    max_silence: 60          # probe only after 60s of real silence
    params:
      engine: "@db_engine"
```

## Criticality is the setting that matters

```
critical: true   → failing makes /ready return 503
critical: false  → failing degrades the worker; /ready stays 200
```

Mark a dependency critical only if the worker genuinely cannot work without
it. Marking a fallback-able cache critical converts a cache outage into a
total outage.

## Per-stage timing

Not automatic, because only your code knows how to attribute it:

```python
with health.tracker.stage("billing.in", "postgres_txn"):
    ...
```

Surfaces as `worker.billing.in.stage.postgres_txn_ms`, so when handler
latency rises you can see which dependency caused it.

## Custom checks

```python
@health.monitor.check("vendor-api", critical=False, interval=30, timeout=2)
def check_vendor():
    return vendor.status() == "ok"     # bool, Status, or a CheckResult
```

A custom check that raises is isolated: it reports `unknown`, and no other
check is affected. For a reusable probe type, register a builder on the
factory — see [USAGE.md §4](USAGE.md#4-custom-probes).

## RabbitMQ: observing the worker's own connection

```python
from worker_health import BrokerState, install_broker_probe
from worker_health.instrument import instrument_pika_channel

broker_state = BrokerState()
install_broker_probe(connection, broker_state, queue="billing.in", interval=2.0)

channel = connection.channel()
instrument_pika_channel(channel, health.monitor, broker_state)
```

`install_broker_probe` drives the passive declare from the connection's
**own** thread via `call_later`: no new connection, no cross-thread access to
a `BlockingConnection`, and if the worker's loop wedges the state goes stale
— which is itself the signal.

`instrument_pika_channel` records deliveries, acks and prefetch, so the
"idle queue vs stuck consumer" distinction works with no bookkeeping in your
callback.

## Framework autowiring

- **Django** — add `worker_health_django.apps.WorkerHealthConfig` to
  `INSTALLED_APPS`, configure `WORKER_HEALTH`, call `get_tracker()` in the
  command. [Guide](USAGE.md#2-django).
- **FastAPI** — `app = FastAPI(lifespan=health_lifespan(...))`.
  [Guide](USAGE.md#3-fastapi).

## Endpoints

| Path | Purpose |
|---|---|
| `/live` | Loop responsiveness only. Never 503s on a dependency failure. |
| `/ready` | Full readiness. 503 on `starting` or `unready`; `degraded` stays 200. |
| `/health` | Full snapshot: checks, processing, timing windows, recent events. |
| `/metrics` | Prometheus exposition, bounded labels. |
| `/events` | The last 50 structured events. |

`worker-health --url http://127.0.0.1:8080` is the CLI equivalent of
`/ready`, for a shell check, a PM2 healthcheck or a container `HEALTHCHECK`.

## Versions

Pinned: **Postgres 16, Redis 7.2, RabbitMQ 3.10**.

Redis 7.2 implements `HELLO`, so RESP2/RESP3 negotiation is a non-issue;
`build_client()` leaves the protocol at the client default and instead
guarantees the setting that actually breaks health checks — socket timeouts,
which redis-py leaves as `None` by default so a black-holed connection hangs
forever.

RabbitMQ's `/api/aliveness-test` is never used: on 3.10 it declares a queue,
publishes and consumes a message, which violates the non-destructive
guardrail. It became a no-op only in 4.0.
