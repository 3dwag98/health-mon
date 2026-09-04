# worker-health

Operational health for message-driven Python workers. A worker process can be
alive and completely unable to consume; process status alone does not say
whether work is happening. This package reports the difference.

Health is derived from the worker's **own** connections and its real traffic
wherever possible. A synthetic probe is a labelled fallback for when the worker
has been silent, never the primary signal — a probe-first design reports healthy
while a worker sits on a dead pooled connection.

## Run it

    make up          # deps, three workers, load generator, dashboard
    open http://localhost:9000

    make health      # aggregate status of every worker
    make idle        # pause load — a quiet queue must NOT alert
    make burst       # 2000 messages — watch backlog and recovery
    make chaos-db-blackhole ; make restore
    make down

## What it checks

| Check | Observed | Introspected | Probed |
|---|---|---|---|
| postgres | query outcome + latency from real traffic | pool checked-out / size / overflow | `SELECT 1` on an isolated NullPool connection |
| redis | command outcome from real traffic | client pool counters | `PING` + `INFO` |
| rabbitmq | last delivery, last ack | connection + channel state, consumer tags, unacked vs prefetch | passive declare on a dedicated channel of the worker's own connection |
| processing | received / succeeded / failed, idle time vs queue depth | — | — |

Every result carries `evidence` — `observed`, `introspected` or `probed` — so a
probe-backed green never looks like a traffic-backed green.

## Integration

    from worker_health import HealthMonitor, Tracker

    monitor = HealthMonitor("billing", runner="thread")
    tracker = Tracker(monitor, processing_state, default_queue="billing.in")

    @tracker.handler(queue="billing.in")
    def handle(body): ...

See `workers/` for three complete workers and `docs/` for the PM2 example.

## Versions

Pinned deliberately: Postgres 16, RabbitMQ 3.13, **Redis 5.0.6**.

Redis 5.0.6 has no `HELLO`, so redis-py 6+ (which negotiates RESP3 by default)
fails on *every* command. `build_client()` pins `protocol=2`. A caller-supplied
client that gets this wrong is reported as `dependency_version`, not
`connection_refused`.

`/api/aliveness-test` is never used: on 3.13 it declares a queue, publishes and
consumes a message, which violates the non-destructive guardrail.
