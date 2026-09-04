# Integrating worker-health

## The minimum

```python
from worker_health import HealthMonitor, HealthServer, Tracker, ProcessingState

monitor = HealthMonitor("billing", runner="thread")   # or "asyncio"
state   = ProcessingState()
tracker = Tracker(monitor, state, default_queue="billing.in")

@tracker.handler(queue="billing.in")
def handle(body):
    ...                       # unchanged

HealthServer(monitor, port=8080).start()
monitor.start(boot_grace=20)
```

That decorator is the entire required change to handler code. It records
received / succeeded / failed, duration, and last-activity — the whole input
to the processing check and to the worker→health delta.

## Letting checks stand on real traffic

Wrap real dependency calls so the health check can use them as evidence
instead of issuing a probe:

```python
from worker_health import classify_postgres, classify_redis

with tracker.dependency("postgres", classify=classify_postgres):
    with engine.begin() as conn:
        conn.execute(...)
```

Now `PostgresCheck` reports `evidence: observed` while traffic is flowing, and
falls back to a labelled probe only after `max_silence` seconds of silence.

## Per-stage timing

```python
with tracker.stage("billing.in", "postgres_txn"):
    ...
```

Surfaces as `worker.billing.in.stage.postgres_txn_ms`, so when handler latency
rises you can see which dependency caused it.

## Custom checks

```python
@monitor.check("vendor-api", critical=False, interval=30, timeout=2)
def check_vendor():
    return vendor.status() == "ok"     # bool, Status, or a CheckResult
```

A custom check that raises is isolated: it reports `unknown` and no other
check is affected.

## RabbitMQ: observing the worker's own connection

```python
from worker_health import BrokerState, RabbitMQCheck, install_broker_probe

broker = BrokerState()
install_broker_probe(connection, broker, queue="billing.in", interval=2.0)
monitor.register(RabbitMQCheck(broker, queue="billing.in"), critical=True)
```

`install_broker_probe` drives the passive declare from the connection's **own**
thread via `call_later`. No new connection, no cross-thread access to a
`BlockingConnection`, and if the worker's loop wedges the state goes stale —
which is itself the signal.

Update `last_delivery_at` and `unacked` from your consume callback so the
idle-versus-stuck discrimination has what it needs.

## Redis 5.x

Use `build_client()`, which pins `protocol=2`. redis-py 6+ negotiates RESP3
with `HELLO`, which Redis 5.0.6 does not implement — every command fails,
`PING` included.

## Endpoints

| Path | Purpose |
|---|---|
| `/live` | Loop responsiveness only. Never 503s on a dependency failure. |
| `/ready` | Full readiness. 503 on `starting` or a critical `failing`. |
| `/health` | Full snapshot including timing windows. |
| `/metrics` | Prometheus exposition, bounded labels. |

`worker-health --url http://127.0.0.1:8080` is the CLI equivalent of `/ready`,
for a shell check or a container `HEALTHCHECK`.
