# Operations

What to do when health says something is wrong, and how to run this safely.

- [Failure handling matrix](#failure-handling-matrix)
- [Backoff and recovery](#backoff-and-recovery)
- [Controlled restart](#controlled-restart)
- [PM2 integration](#pm2-integration)
- [Security](#security)
- [Capacity and overhead](#capacity-and-overhead)
- [Fault injection](#fault-injection)

---

## Failure handling matrix

| Failure | Health effect | Action |
|---|---|---|
| Event loop wedged | `liveness: unalive`, `/live` 503 | **Restart.** This is the one a restart genuinely fixes. |
| Consumer cancelled / unsubscribed | `rabbitmq` failing, `not_subscribed` | Recover the consumer; restart if it cannot re-subscribe. |
| Prefetch credit exhausted | `rabbitmq` failing, `credit_exhausted` | Look for unacked messages the handler never settles. |
| Thread pool exhausted | checks age into `unknown`, readiness `degraded` | Restart is reasonable; check for a blocking call in a handler. |
| Database timeout | `postgres` failing, `timeout` | **Do not restart.** Back off and wait; the SDK already is. |
| Database pool exhausted | `postgres` degraded, `pool_exhausted` | The *server* is fine. Look at connection leaks and pool size. |
| Database read-only | `postgres` degraded, `read_only` | A failover promoted a replica. Reads work; writes do not. |
| Redis unavailable | degraded (non-critical) or unready (critical) | Degrade or pause; do not restart. |
| Redis at maxmemory | degraded, `memory_pressure` | Evictions are coming — on idempotency keys or locks this is a correctness problem. |
| Broker unavailable | `rabbitmq` failing, `connection_lost` | Back off and reconnect. Do not restart the fleet. |
| Broker resource alarm | `rabbitmq` failing, `broker_alarm` | The broker is up and refusing publishes. Look at its disk and memory, not the network. |
| Queue backlog high | degraded, `backlog` | Alert and scale. Not an outage. |
| Traffic silent, queue empty | `ok` | **No action.** This is a healthy idle worker. |
| Traffic silent, queue non-empty | failing, `not_consuming` | Investigate the consumer. This is the real one. |
| Handler failing repeatedly | failing, `poison_loop` | A poison message. Restarting re-reads it. |
| DNS failure | `dns` failing | Everything else will fail too, all with connection errors. Check this first. |
| Disk low | degraded, `resource_locked` | The silent killer: broker keeps delivering, every write fails. |

The single most important row is the split between the last two "silent"
cases. Every process-level check gets both wrong.

---

## Backoff and recovery

A failing dependency is probed on a widening schedule so health checks never
become the load that keeps a struggling service down:

```
5s → 10s → 20s → 40s → 60s → 60s …    ×2, capped, ±10% jitter
```

Jitter is not an optimisation: forty workers started by one command have
their ticks aligned to within milliseconds, and un-jittered they would
re-probe in lockstep forever.

Recovery is confirmed, not assumed:

```
failure_threshold: 3 consecutive failures   → status flips to failing
success_threshold: 2 consecutive successes  → status flips back
```

One success resets the backoff ladder immediately; the status still waits
for `success_threshold`. When real traffic resumes, evidence moves back from
`probed` to `observed` on its own — the check prefers traffic whenever it is
fresher than `max_silence`.

Backoff never changes the *reported* status. A breaker that reported
`unknown` while open would hide the outage it exists to survive.

---

## Controlled restart

Off by default. When enabled, the policy never kills anything: it exits its
own process with a chosen code and lets the supervisor apply its restart
rules. That keeps the restart decision where it belongs and means this
package cannot become the thing that takes the fleet down.

```yaml
restart:
  enabled: true
  after_cycles: 5
  min_uptime: 120
  cooldown: 600
  max_per_hour: 3
```

Guards, in order of application:

1. **Trigger categories** — self-inflicted faults only (`stalled`,
   `not_consuming`, `not_subscribed`, `credit_exhausted`, `connection_lost`,
   `poison_loop`). Dependency faults are excluded: restarting a worker
   because Postgres is down converts one database outage into forty
   crash-looping processes hammering a database already in trouble.
2. **`after_cycles`** — the condition must persist across evaluations.
3. **`min_uptime`** — never restart a process that just booted, or a bad
   deploy becomes an infinite loop.
4. **`cooldown`** and **`max_per_hour`** — then it *latches*: stays up,
   keeps reporting failing. A process that never lives long enough to
   inspect is strictly less useful than one that stays up and complains.
5. **`drain_timeout`**, jittered — so forty workers hitting the same
   condition do not exit in the same second.

---

## PM2 integration

`docs/ecosystem.config.js` has the full example. The essentials:

```js
module.exports = {
  apps: [{
    name: "billing-worker",
    script: "worker_billing.py",
    interpreter: "python3",
    env: { HEALTH_PORT: 8080, HEALTH_INSTANCE: "billing-1" },
    // The SDK exits 70 when its restart policy fires; PM2 restarts it.
    stop_exit_codes: [0],
    max_restarts: 5,
    min_uptime: "60s",
    restart_delay: 5000,
    exp_backoff_restart_delay: 200,
  }],
};
```

For an external health gate, use the CLI — it is the same verdict the
dashboard and Prometheus see:

```bash
worker-health --url http://127.0.0.1:8080          # exit 0 ready, 1 not
worker-health --url http://127.0.0.1:8080 --live   # loop responsiveness only
```

Wire a **liveness** gate to `--live` and an alert to `/ready`. Restarting on
`/ready` is what turns a shared-database outage into a fleet-wide crash
loop.

---

## Security

### Secrets never leave the process

Structural, not best-effort:

1. What gets reported is a closed `ErrorCategory` enum, never the driver's
   exception text — psycopg embeds the DSN in several connection errors,
   pika embeds the username in an auth failure.
2. Every free-text field (`detail`, log messages, config echoes) passes
   through `security.redact`, which masks URL credentials, `password=` /
   `token=` style pairs, JWTs and bearer headers.
3. `security.endpoint()` reduces a DSN to `host:port` for the places a
   location is genuinely useful.

```python
>>> redact_dsn("postgres://app:hunter2@db:5432/app")
'postgres://app:***@db:5432/app'
```

The integration tier asserts this with distinctive canary passwords: any
appearance of one in a body, a log line or a metric fails the build.

### Probes are read-only

Enforced, not documented:

- HTTP probes accept **GET, HEAD, OPTIONS only** — a `method: POST` in YAML
  is rejected at wiring time.
- The RabbitMQ adapter uses a **passive** `queue_declare` on its own channel.
  `/api/aliveness-test` is never used: on RabbitMQ 3.10 it declares a queue,
  publishes and consumes a message, which violates the non-destructive rule
  outright. It became a no-op only in 4.0.
- The Postgres probe runs `SELECT 1` on a separate `NullPool` connection, so
  it cannot consume an application pool slot or cause the exhaustion it is
  meant to detect.
- The Django check never calls `connection.is_usable()` — that marks the
  *application's* connection broken on failure, so a health check could
  cause the incident.
- No probe publishes, writes, deletes a key, or creates a queue.

### The health port is not authenticated

Treat it as internal:

```
bind to 127.0.0.1 or a private interface  (health_host)
never publish /health or /metrics publicly
put a network policy in front of the scrape path
rate-limit if it is reachable from anywhere shared
```

The default bind is `0.0.0.0` because the common deployment is a container
whose port is published deliberately. On a shared host, set
`health_host: 127.0.0.1`.

---

## Capacity and overhead

| Path | Cost |
|---|---|
| `@handler` per message | one `perf_counter`, a few increments under a lock, one deque append |
| Instrumented query / command | one `perf_counter`, one dict lookup, one lock-guarded update |
| Check evaluation | one probe *only* when traffic is stale; otherwise a dict read |
| `/live` | one clock read, no lock, no snapshot |
| `/ready`, `/health`, `/metrics` | a cached snapshot; no I/O on the request path |

Under load, the probes essentially stop running: real traffic is always
fresher than `max_silence`, so the evidence ladder never reaches rung 3.
Health load on a dependency is therefore highest when the worker is *idle*,
which is exactly when the dependency has capacity for it.

Memory is bounded: rolling windows are fixed-size deques (512 samples), the
event ring holds 100, and per-check state is a handful of scalars.

---

## Fault injection

The compose stack routes every dependency through Toxiproxy, so faults can
be injected against a running fleet without restarting anything.

```bash
# Port closed → connection_refused
docker compose exec toxiproxy /toxiproxy-cli toggle postgres

# Black hole: packets dropped, socket never closes. The firewall-DROP case,
# and the one that actually breaks naive health checks.
docker compose exec toxiproxy /toxiproxy-cli toxic add postgres \
  -t timeout -a timeout=0 -n blackhole

# 400ms latency, below the check timeout → must stay OK
docker compose exec toxiproxy /toxiproxy-cli toxic add postgres \
  -t latency -a latency=400 -a jitter=0 -n slow

# Non-critical dependency → degrades, must NOT 503
docker compose exec toxiproxy /toxiproxy-cli toggle redis-cache

# Clear everything
curl -X POST http://localhost:8474/reset
```

The scenario worth running once, because it is the one everything else is
built around:

```bash
# 1. Quiet queue must stay OK, forever.
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d '{"rate":0}'
#    watch for a minute — still ok, evidence "observed" ages then falls back
#    to "probed"

# 2. Resume load, then black-hole the database.
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d '{"rate":8}'
docker compose exec toxiproxy /toxiproxy-cli toxic add postgres -t timeout -a timeout=0 -n blackhole
#    evidence flips observed → probed, then postgres → failing (timeout),
#    /ready → 503, /live stays 200

# 3. Recovery.
curl -X POST http://localhost:8474/reset
#    back to ok after success_threshold, evidence back to observed
```
