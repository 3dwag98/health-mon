# Architecture

How worker-health is put together, and why each seam is where it is.

Diagrams are Mermaid; GitHub, GitLab and most IDE previews render them
inline.

- [1. The problem, drawn](#1-the-problem-drawn)
- [2. Component map](#2-component-map)
- [3. Module layout](#3-module-layout)
- [4. The evidence ladder](#4-the-evidence-ladder)
- [5. Threading model](#5-threading-model)
- [6. One evaluation, end to end](#6-one-evaluation-end-to-end)
- [7. Per-check state machine](#7-per-check-state-machine)
- [8. Backoff while failing](#8-backoff-while-failing)
- [9. Readiness and liveness](#9-readiness-and-liveness)
- [10. Probe factory and configuration](#10-probe-factory-and-configuration)
- [11. Auto-instrumentation](#11-auto-instrumentation)
- [12. Django wiring](#12-django-wiring)
- [13. FastAPI wiring](#13-fastapi-wiring)
- [14. Telemetry pipeline](#14-telemetry-pipeline)
- [15. Deployment topology](#15-deployment-topology)
- [16. Design decisions and their costs](#16-design-decisions-and-their-costs)

---

## 1. The problem, drawn

A PM2-managed worker has one externally visible signal: the process is
running. That signal is true in every one of the states below, and only one
of them is healthy.

```mermaid
flowchart LR
    subgraph Supervisor["What PM2 sees"]
        P["process: online<br/>restarts: 0<br/>cpu: 0.1%"]
    end

    subgraph Reality["What is actually happening"]
        A["Consuming normally"]
        B["Broker connection dead,<br/>consumer never re-subscribed"]
        C["Pooled DB connection stale,<br/>every query times out"]
        D["Handler wedged on one<br/>poison message"]
        E["Queue empty —<br/>genuinely idle, and fine"]
        F["Prefetch credit exhausted:<br/>unacked == prefetch"]
    end

    P --> A
    P --> B
    P --> C
    P --> D
    P --> E
    P --> F

    A -.->|healthy| OK["✅"]
    E -.->|healthy| OK
    B -.->|outage| BAD["🚨"]
    C -.->|outage| BAD
    D -.->|outage| BAD
    F -.->|outage| BAD
```

Two of those six states are fine and four are outages. Telling `E` from `B`
is the hard part, and it is why the SDK takes queue depth as an input
rather than inferring health from timers alone: **idle with an empty queue
is healthy forever; idle with a backlog is a stuck consumer.**

---

## 2. Component map

```mermaid
flowchart TB
    subgraph Worker["Worker process"]
        direction TB

        subgraph Business["Business code — one decorator"]
            H["@tracker.handler(queue=...)<br/>def handle(message)"]
            Q["engine.execute(...)<br/>redis.setex(...)<br/>channel.basic_ack(...)"]
        end

        subgraph SDK["worker-health"]
            direction TB
            TR["Tracker<br/>message counters, timings"]
            IN["instrument/<br/>SQLAlchemy · redis-py · pika · Django ORM"]
            TL["TrafficLog<br/>per-dependency success/failure + age"]
            PS["ProcessingState<br/>received / succeeded / failed, per queue"]

            subgraph Engine["Health engine"]
                RUN["Runner<br/>thread or asyncio"]
                CH["Checks<br/>postgres · redis · rabbitmq · kafka<br/>http · tcp · dns · disk · file_age · custom"]
                SM["StateMachine<br/>thresholds · hysteresis · backoff · TTL"]
                AGG["aggregate()<br/>criticality · boot grace"]
            end

            SNAP["Snapshot<br/>cached, no I/O on read"]
            HTTP["HealthServer thread<br/>/live /ready /health /metrics /events"]
            EV["EventEmitter<br/>structured JSON events"]
        end
    end

    subgraph Deps["Dependencies"]
        PG[("PostgreSQL 16")]
        RD[("Redis 7.2")]
        MQ[("RabbitMQ 3.10")]
        EXT["Vendor APIs, disk, DNS"]
    end

    subgraph Ops["Operations"]
        PM2["PM2 / supervisor"]
        PROM["Prometheus"]
        GRAF["Grafana"]
        LOGS["Log pipeline"]
    end

    H --> TR
    H --> Q
    Q --> IN
    IN --> TL
    TR --> PS
    TR --> TL

    Q -.real traffic.-> PG
    Q -.real traffic.-> RD
    Q -.real traffic.-> MQ

    TL --> CH
    PS --> CH
    RUN --> CH
    CH -.probe, only when silent.-> PG
    CH -.probe, only when silent.-> RD
    CH -.introspect only.-> MQ
    CH -.probe.-> EXT
    CH --> SM --> AGG --> SNAP
    SNAP --> HTTP
    SM --> EV

    HTTP -->|"200/503"| PM2
    HTTP -->|"/metrics"| PROM --> GRAF
    EV --> LOGS
```

The arrow that matters most is the dotted one from business code to the
dependencies: **the worker's own traffic is the primary health signal.**
Probes are the fallback for silence, and they are labelled as such in every
result.

---

## 3. Module layout

```mermaid
flowchart LR
    subgraph core["worker_health.core — pure, no I/O, no drivers"]
        model["model.py<br/>Status · Readiness · Liveness<br/>Evidence · ErrorCategory<br/>CheckResult · Snapshot"]
        machine["machine.py<br/>CheckSpec · StateMachine"]
        aggregate["aggregate.py<br/>aggregate() · readiness() · reasons()"]
        clock["clock.py"]
        timing["timing.py<br/>rolling windows"]
    end

    subgraph checks["worker_health.checks — one file per dependency"]
        base["base.py — the evidence ladder"]
        pg["postgres.py"] ; rd["redis_.py"] ; mq["rabbitmq.py"]
        kf["kafka.py"] ; net["network.py"] ; sys_["system.py"]
        proc["processing.py"] ; dj["django_db.py"] ; cust["custom.py"]
    end

    subgraph probes["worker_health.probes"]
        spec["spec.py — ProbeSpec, @refs"]
        factory["factory.py — registry, plugins"]
        builtin["builtin.py — the 13 built-in types"]
    end

    subgraph instrument["worker_health.instrument"]
        ctx["context.py — probe suppression"]
        rec["recorder.py — the one funnel"]
        sa["sqlalchemy_.py"] ; rdi["redis_.py"] ; pk["pika_.py"] ; dji["django_.py"]
    end

    subgraph runners["worker_health.runners"]
        thr["thread_.py"] ; aio["asyncio_.py"] ; rbase["base.py — shared policy"]
    end

    subgraph telemetry["worker_health.telemetry"]
        prom["prometheus.py"] ; events["events.py"] ; logs["logs.py"]
    end

    monitor["monitor.py — HealthMonitor"]
    setup["setup.py — setup_worker_health()"]
    config["config.py + _yaml.py"]
    security["security.py — redaction"]
    track["track.py — Tracker"]
    transports["transports/ — http.py, cli.py"]
    policy["policy/restart.py"]

    setup --> config & probes & instrument & monitor & track & transports & policy
    monitor --> core & runners & telemetry
    checks --> core
    probes --> checks
    instrument --> rec --> core
    telemetry --> security
    runners --> core

    dj -.lazy import.-> DJANGO(["django"])
    pg -.lazy import.-> SQLA(["sqlalchemy"])
    rd -.lazy import.-> REDIS(["redis"])
    mq -.lazy import.-> PIKA(["pika"])
```

Every driver import is lazy and lives in the adapter that needs it. The
package itself has **no required dependencies**: a worker that uses Redis
but not SQLAlchemy never imports SQLAlchemy, and a config file loads
without PyYAML through the built-in subset parser.

---

## 4. The evidence ladder

The single most important decision in the codebase. Every check answers in
this order, and every result says which rung it came from.

```mermaid
flowchart TD
    START["Check is due"] --> INTRO

    INTRO{"1 · Introspect<br/>local pool / connection state<br/>zero I/O"}
    INTRO -->|"conclusive problem<br/>(pool exhausted,<br/>connection closed)"| RI["Result<br/>evidence: introspected"]
    INTRO -->|"healthy or inconclusive"| TRAF

    TRAF{"2 · Real traffic<br/>TrafficLog age &lt; max_silence?"}
    TRAF -->|"recent success"| RO["Result: ok<br/>evidence: observed<br/>evidence_age_ms = age of that call"]
    TRAF -->|"recent failure"| RF["Result: failing<br/>evidence: observed<br/>category from the real exception"]
    TRAF -->|"nothing recent<br/>(worker has been silent)"| PROBE

    PROBE["3 · Synthetic probe<br/>inside probe_scope()"]
    PROBE --> RP["Result<br/>evidence: probed"]

    RI --> SM["StateMachine.apply()"]
    RO --> SM
    RF --> SM
    RP --> SM

    style RO fill:#d6f5d6,stroke:#2d7a2d
    style RF fill:#f8d7da,stroke:#a52c2c
    style RP fill:#fff3cd,stroke:#8a6000
    style RI fill:#e2e3f5,stroke:#4a4a8a
```

Why this order:

| Rung | Answers | Cost | Blind spot |
|---|---|---|---|
| `introspected` | "Can the app get a connection at all?" | zero | says nothing about the server |
| `observed` | "Is the worker's own connection working?" | zero | needs recent traffic |
| `probed` | "Can this process reach the dependency now?" | a round trip | can succeed on a *fresh* connection while the worker sits on a dead pooled one |

A probe-first design reports green while a worker holds a dead pooled
connection. That is the exact false positive this ordering removes.

`probe_scope()` on rung 3 is not decoration: it sets a `ContextVar` that
the instrumentation checks, so the probe's own `SELECT 1` is **not** written
to the traffic log. Without it, the monitor would observe its own probes,
conclude there had been recent traffic, and report `observed` on a worker
that had processed nothing all day.

---

## 5. Threading model

```mermaid
flowchart TB
    subgraph T1["Thread: worker main (blocking consume loop)"]
        direction TB
        W1["pika start_consuming()"] --> W2["on_message"] --> W3["@handler → ProcessingState"]
        W3 --> W4["engine / redis calls → TrafficLog"]
        W4 --> W5["basic_ack → BrokerState"]
        W6["connection.call_later → passive queue_declare<br/>(broker probe rides the worker's own loop)"]
    end

    subgraph T2["Thread: wh-scheduler"]
        S1["every tick:<br/>update loop beat<br/>collect finished<br/>dispatch due checks<br/>monitor.tick()"]
    end

    subgraph T3["Pool: wh-check-N (bounded)"]
        C1["check.evaluate(ctx)"]
        C2["one black-holed probe<br/>parks exactly one slot"]
    end

    subgraph T4["Thread: wh-http"]
        H1["/live — one clock read"]
        H2["/ready /health /metrics —<br/>cached snapshot, no I/O"]
    end

    T2 -->|submit| T3
    T3 -->|"apply(result)"| SMx["StateMachine + results cache"]
    T1 -->|"lock-free writes"| STATE["TrafficLog · ProcessingState · BrokerState"]
    STATE --> T3
    SMx --> T4
```

Three properties fall out of this shape:

1. **The HTTP endpoints survive a wedged worker.** They are on their own
   thread and serve a cached snapshot, so `/live` still answers when the
   consume loop is stuck — which is the only moment its answer matters.
2. **A hung probe cannot stall the others.** It occupies one pool slot; the
   scheduler records the timeout, reports the check failing, and moves on.
   A blocking driver call cannot be cancelled from outside, so the thread
   stays parked — but nothing waits with it.
3. **The broker probe rides the worker's own connection thread**
   (`call_later`), so if that loop stops turning, the broker state goes
   stale and the check reports it. A dedicated monitor connection would have
   kept reporting a cheerful green.

---

## 6. One evaluation, end to end

```mermaid
sequenceDiagram
    autonumber
    participant App as Business code
    participant Inst as instrument/
    participant TL as TrafficLog
    participant Sched as wh-scheduler
    participant Check as PostgresCheck
    participant SM as StateMachine
    participant Ev as EventEmitter
    participant HTTP as /ready

    App->>Inst: engine.execute(UPDATE ...)
    Inst->>Inst: is_health_probe_active()? → no
    Inst->>TL: success("postgres", 3.2ms)

    Note over Sched: tick — postgres is due
    Sched->>Check: evaluate(ctx: now, max_silence=6s)
    Check->>Check: introspect() → pool 2/5 checked out → OK
    Check->>TL: get("postgres") → 3.2ms, 0.4s old
    Check-->>Sched: OK · observed · evidence_age_ms=400

    Sched->>SM: apply(result)
    SM->>SM: consecutive_ok++, effective stays OK
    SM->>SM: next_due = now + interval ± jitter

    Note over App,TL: ... dependency fails ...
    App->>Inst: engine.execute(...) raises OperationalError
    Inst->>Inst: classify_postgres(exc) → connection_refused
    Inst->>TL: failure("postgres", connection_refused)

    Sched->>Check: evaluate(ctx)
    Check->>TL: newest event is a failure
    Check-->>Sched: FAILING · observed · category=connection_refused
    Sched->>SM: apply(result)
    SM->>SM: consecutive_fail=1 (threshold 2) → still OK
    Sched->>Check: evaluate(ctx)
    Sched->>SM: apply(result) → consecutive_fail=2 → FAILING
    SM->>SM: backoff_step=1 → next probe in 5s
    SM->>Ev: transition ok → failing
    Ev->>Ev: health_transition + readiness_changed

    HTTP->>SM: snapshot (cached)
    HTTP-->>HTTP: 503 · reasons: ["critical check postgres is failing (connection_refused)"]
```

Note what never happens on the `/ready` path: no I/O, no probe, no driver
call. A health endpoint that probes inline is slow exactly when everything
else is already on fire.

---

## 7. Per-check state machine

```mermaid
stateDiagram-v2
    [*] --> UNKNOWN: registered, never evaluated

    UNKNOWN --> OK: first success (no confirmation needed)
    UNKNOWN --> FAILING: failure_threshold consecutive failures

    OK --> FAILING: failure_threshold consecutive failures
    OK --> DEGRADED: measured degradation<br/>(pool pressure, backlog, memory)
    OK --> UNKNOWN: TTL expired — nothing wrote a result

    FAILING --> OK: success_threshold consecutive successes
    FAILING --> DEGRADED: partial recovery
    FAILING --> UNKNOWN: TTL expired

    DEGRADED --> OK: success_threshold consecutive successes
    DEGRADED --> FAILING: failure_threshold consecutive failures

    OK --> DISABLED: set_enabled(name, False)
    DEGRADED --> DISABLED
    FAILING --> DISABLED
    DISABLED --> UNKNOWN: re-enabled, backoff cleared

    note right of UNKNOWN
        UNKNOWN is the absence of a
        measurement, not an outage.
        Severity 2 — below FAILING.
        Treating it as worse produces a
        false outage on every deploy.
    end note

    note right of DEGRADED
        Already thresholded upstream from
        a measured value, so it is entered
        on one sample — but LEAVING it
        still needs success_threshold.
    end note
```

Three asymmetries, each deliberate:

- **Failure needs confirmation, first success does not.** One lost packet is
  not an outage; requiring confirmation of the *first* measurement would
  double every worker's time to ready with nothing to confirm against.
- **Recovery out of a bad state needs confirmation.** Otherwise a flapping
  dependency produces a flapping readiness signal, and the fleet oscillates
  in and out of rotation.
- **TTL is applied at read time, not write time.** This is what makes a
  wedged scheduler visible: nothing is writing results, so every check ages
  into `UNKNOWN` on its own rather than reporting a stale green forever.

---

## 8. Backoff while failing

A failing dependency is asked less often — the guardrail against turning
one outage into a retry storm against a service already in trouble.

```mermaid
gantt
    title Probe schedule after a failure (interval 10s, thresholds 2/2)
    dateFormat X
    axisFormat %Ss

    section Healthy
    every 10s ±20% jitter      :done, h1, 0, 10s
    every 10s                  :done, h2, 10, 10s

    section Confirming
    failure 1 (still OK)       :active, f1, 20, 10s
    failure 2 → FAILING        :crit, f2, 30, 5s

    section Backing off
    retry after 5s             :crit, b1, 35, 10s
    retry after 10s            :crit, b2, 45, 20s
    retry after 20s            :crit, b3, 65, 40s
    retry after 40s            :crit, b4, 105, 60s
    retry after 60s (capped)   :crit, b5, 165, 60s
```

`5 → 10 → 20 → 40 → 60 → 60…`, with 10% jitter so a fleet started by one
command does not re-probe in lockstep. One success resets the ladder
immediately; `success_threshold` still governs when the *status* flips back.

Reported status never changes because of backoff. A breaker that reported
`unknown` while open would hide the outage it exists to survive.

---

## 9. Readiness and liveness

Two questions, two answers, two endpoints — and a dependency failure may
never affect the first one.

```mermaid
flowchart TD
    subgraph live["/live — is this process's loop turning?"]
        L1{"loop_lag_ms > threshold<br/>(default 2000)"}
        L1 -->|no| LA["alive → 200"]
        L1 -->|yes| LU["unalive → 503<br/>a restart genuinely helps here"]
    end

    subgraph ready["/ready — can this worker process work?"]
        R0{"liveness == alive?"}
        R0 -->|no| RU1["unready → 503"]
        R0 -->|yes| R1

        R1{"boot grace active AND<br/>a critical check not yet OK?"}
        R1 -->|yes| RS["starting → 503"]
        R1 -->|no| R2

        R2{"any CRITICAL check failing?"}
        R2 -->|yes| RU2["unready → 503"]
        R2 -->|no| R3

        R3{"any non-critical failing,<br/>degraded, unknown,<br/>or queue lag over threshold?"}
        R3 -->|yes| RD["degraded → 200<br/>still serving"]
        R3 -->|no| RR["ready → 200"]
    end

    style LA fill:#d6f5d6,stroke:#2d7a2d
    style RR fill:#d6f5d6,stroke:#2d7a2d
    style RD fill:#fff3cd,stroke:#8a6000
    style RS fill:#fbeee7,stroke:#9b4a22
    style LU fill:#f8d7da,stroke:#a52c2c
    style RU1 fill:#f8d7da,stroke:#a52c2c
    style RU2 fill:#f8d7da,stroke:#a52c2c
```

The two rules that keep this from causing outages of its own:

- **`/live` never consults a dependency.** If it did, a shared database
  going down would make every worker in the fleet look dead, and the
  supervisor would restart all of them into a database that is already
  struggling.
- **`degraded` returns 200.** A failing *non-critical* cache must not pull a
  worker out of rotation; that is how a cache outage becomes a total outage.

Whenever readiness answers anything but `ready`, the response carries a
`reasons` array naming the check and its error category — closed vocabulary
only, never driver text.

---

## 10. Probe factory and configuration

Configuration names things; the context supplies them; `@name` is where the
two meet.

```mermaid
flowchart TB
    subgraph sources["Configuration sources — precedence low to high"]
        YAML["worker-health.yaml<br/>(or JSON)"]
        DJ["Django settings<br/>WORKER_HEALTH = {...}"]
        ENV["HEALTH_* environment"]
        KW["setup_worker_health(**overrides)"]
    end

    YAML --> EXPAND["expand_env()<br/>\${VAR} and \${VAR:-default}"]
    EXPAND --> HC
    DJ --> HC["HealthConfig"]
    ENV --> HC
    KW --> HC

    HC --> SPECS["list[ProbeSpec]<br/>validated at boot:<br/>intervals > 0, thresholds ≥ 1"]

    subgraph runtime["Runtime context — live objects"]
        OBJ["db_engine · redis_client<br/>broker_state · processing_state<br/>anything you pass"]
    end

    SPECS --> RESOLVE["spec.resolved_params(context)"]
    OBJ --> RESOLVE
    RESOLVE -->|"'@db_engine' → the Engine"| BUILD

    subgraph factory["ProbeFactory"]
        REG["registry: type name → builder"]
        BI["built-ins: postgres · sqlalchemy · django_db · redis<br/>rabbitmq · kafka · http · tcp · dns<br/>disk · file_age · function · processing"]
        PLUG["load_plugins()<br/>entry points: worker_health.probes"]
        CUSTOM["@factory.probe_type('s3')"]
    end

    BI --> REG
    PLUG --> REG
    CUSTOM --> REG
    REG --> BUILD["builder(spec, context) → Check"]
    BUILD --> MON["monitor.register(check, **spec.registration_kwargs())"]

    style RESOLVE fill:#e2e3f5,stroke:#4a4a8a
```

A builder is a plain function of `(spec, context)`. The built-ins use
exactly the same contract as a third-party plugin — nothing in the SDK has
privileges a user's own probe does not.

Failure handling is a deliberate fork: `strict_probes: true` (the default)
stops the worker at boot on a malformed probe, where a human is watching.
Set it false for a fleet rollout, and a bad config line becomes one
permanently-failing named check instead of a hundred workers refusing to
start.

---

## 11. Auto-instrumentation

The reason business code needs only a decorator.

```mermaid
flowchart TB
    subgraph app["Application calls — unchanged"]
        A1["session.execute() / ORM query"]
        A2["redis.get() / setex()"]
        A3["channel.basic_consume / ack"]
    end

    subgraph hooks["Where each is intercepted"]
        H1["SQLAlchemy events:<br/>before/after_cursor_execute,<br/>handle_error"]
        H2["redis-py: execute_command<br/>patched on the class,<br/>routed per instance"]
        H3["pika: connection callbacks +<br/>channel methods wrapped<br/>on the instance"]
        H4["Django: CursorWrapper.execute /<br/>executemany, routed by alias"]
    end

    A1 --> H1
    A2 --> H2
    A3 --> H3
    A1 -.Django ORM.-> H4

    H1 & H2 & H3 & H4 --> GUARD

    GUARD{"is_health_probe_active()?"}
    GUARD -->|"yes — this is our own probe"| DROP["record nothing"]
    GUARD -->|no| REC["TrafficRecorder"]

    REC --> TL["TrafficLog<br/>success/failure + latency + category"]
    REC --> TW["Timings<br/>dependency.NAME.duration_ms"]
    H3 --> BS["BrokerState<br/>last_delivery_at · last_ack_at<br/>unacked · prefetch · consumer_tags"]

    TL --> LADDER["Evidence ladder rung 2"]
    BS --> MQCHECK["RabbitMQCheck — pure introspection"]

    style GUARD fill:#fff3cd,stroke:#8a6000
    style DROP fill:#f1f1f1,stroke:#888
```

Two details worth knowing before you rely on it:

- **redis-py is patched on the class** (there is no other funnel), but the
  target is read off the *instance*. Two clients can report as
  `redis-cache` and `redis-locks`, and an unregistered client is passed
  straight through.
- **pika is wrapped on the channel instance**, so the probe channel on the
  same connection is unaffected — it keeps issuing passive declares without
  being counted as consumption.

Instrumentation is best-effort by design. If a driver version moves the
method being patched, the worker still boots, still probes, and still
reports — with `probed` evidence instead of `observed`. The evidence label
is what tells you which happened.

---

## 12. Django wiring

```mermaid
sequenceDiagram
    autonumber
    participant Mgmt as manage.py consume_billing
    participant Apps as Django app registry
    participant Cfg as WorkerHealthConfig.ready()
    participant Wire as autowire()
    participant SDK as setup_worker_health()
    participant Cmd as Command.handle()

    Mgmt->>Apps: populate INSTALLED_APPS
    Apps->>Cfg: ready()
    Cfg->>Cfg: should_wire(settings.WORKER_HEALTH, sys.argv)

    alt migrate / collectstatic / shell / autoreload parent
        Cfg-->>Apps: return — no server, no scheduler
    else a worker command
        Cfg->>Wire: autowire(config)
        Wire->>Wire: build_config() — UPPER_CASE → HealthConfig
        Wire->>Wire: build_context() — "app.deps:client" → live object
        Wire->>SDK: setup_worker_health(config, context)
        SDK->>SDK: install probes, start HealthServer thread, monitor.start()
        Wire->>Wire: instrument_django_db() per alias
        Wire->>Wire: instrument_django_cache() when Redis-backed
        Wire->>Wire: set_health_state(health)
    end

    Apps->>Cmd: handle()
    Cmd->>Cmd: tracker = get_tracker()
    Cmd->>Cmd: @tracker.handler(queue="billing.in")
    Note over Cmd: every ORM query and cache call<br/>from here on is observed evidence
```

`should_wire` is the part that is easy to get wrong and expensive when you
do. `ready()` fires for *every* Django entry point — `migrate`,
`collectstatic`, `shell`, pytest, and the autoreloader's parent process.
Starting an HTTP server in all of them means port collisions in CI and a
health monitor attached to a migration container. The default skip-list
covers Django's own commands; `COMMANDS: [...]` is the precise answer once
you have more than one worker.

---

## 13. FastAPI wiring

```mermaid
sequenceDiagram
    autonumber
    participant U as uvicorn
    participant LS as health_lifespan()
    participant SDK as setup_worker_health()
    participant C as BillingConsumer
    participant Loop as event loop

    U->>LS: startup
    LS->>LS: context() — engine, redis, broker_state
    LS->>SDK: build monitor (runner="asyncio")
    SDK->>SDK: instrument async SQLAlchemy + redis.asyncio
    SDK->>SDK: install probes whose @refs are present
    SDK->>SDK: HealthServer thread + monitor.start()
    LS->>LS: app.state.monitor / .tracker / .health
    LS->>C: BillingConsumer(tracker)
    LS->>Loop: create_task(consumer.run())
    Note over Loop: the asyncio runner's loop-lag probe now measures<br/>the thing that actually breaks async workers:<br/>a coroutine blocking the loop

    U->>LS: shutdown
    LS->>Loop: task.cancel() for each consumer
    LS->>Loop: await each task
    Note over LS: awaiting the cancelled task is what turns<br/>"Task exception was never retrieved"<br/>into a clean shutdown
    LS->>SDK: health.stop()
```

The optional `/internal/*` routes serve the same data from the event loop.
They are a convenience for platforms that route only one port — and they
stop answering when the loop wedges, which is precisely when the SDK's own
threaded port keeps working. Use the threaded port for liveness wherever
you can.

---

## 14. Telemetry pipeline

```mermaid
flowchart LR
    subgraph inputs["Sources"]
        TR["StateMachine transitions"]
        RE["readiness / liveness changes"]
        BO["boot grace start / complete"]
        PR["probe timeout / error"]
    end

    TR & RE & BO & PR --> EM["EventEmitter"]

    EM --> RED["security.redact / safe_detail<br/>DSNs, tokens, JWTs, bearer headers"]
    RED --> J["JsonFormatter → stdout"]
    RED --> RING["ring buffer (100)<br/>served at /events"]
    RED --> SUB["monitor.on_event(fn)<br/>your own sink"]

    SNAP["Snapshot"] --> PROM["prometheus.render()"]
    PROM --> BIN["binary series<br/>worker_health_ready<br/>worker_health_check_status"]
    PROM --> SEV["severity series<br/>worker_health_status<br/>worker_health_check_severity"]
    PROM --> MSG["processing series<br/>message_*_total · queue_lag<br/>last_message_age_seconds"]
    PROM --> FRESH["freshness series<br/>worker_to_health_delta_ms<br/>evidence_age_ms · loop_lag_ms"]

    BIN --> ALERTS["alerts.yml — every rule uses == 0"]
    SEV --> DASH["Grafana"]
    MSG --> DASH
    FRESH --> DASH
    J --> LOKI["log pipeline → transition log panel"]
```

Two families per verdict is on purpose. Alerts read the binary series
because `== 0` cannot be misread and survives someone adding a status value
later; dashboards read the severity series because they want to colour
`degraded` differently from `failing`. One series doing both jobs is how an
alert ends up saying `< 2` and nobody remembers why.

Label cardinality is bounded by construction: `service`, `instance`,
`check`, `queue`, `critical`, `evidence`, `category`, `state`, `quantile` —
every one of them drawn from a registered name or a closed enum. No error
string ever becomes a label.

---

## 15. Deployment topology

```mermaid
flowchart TB
    subgraph host1["Host / container 1"]
        W1["billing worker<br/>:8080 health"]
        PM21["PM2"]
    end
    subgraph host2["Host / container 2"]
        W2["notify worker<br/>:8080 health"]
        PM22["PM2"]
    end
    subgraph host3["Host / container 3"]
        W3["reconcile worker<br/>:8080 health"]
        PM23["PM2"]
    end

    subgraph deps["Shared dependencies"]
        PG[("PostgreSQL 16")]
        RD[("Redis 7.2")]
        MQ[("RabbitMQ 3.10")]
    end

    subgraph obs["Observability"]
        PROM["Prometheus<br/>scrapes /metrics"]
        GRAF["Grafana<br/>Worker Health Overview"]
        ALERT["Alertmanager"]
        DASH["Fleet dashboard<br/>polls /health, SSE to browsers"]
        LOG["Log pipeline<br/>structured events"]
    end

    W1 & W2 & W3 --> PG & RD & MQ
    PM21 -.->|"worker-health CLI<br/>exit 0/1"| W1
    PM22 -.-> W2
    PM23 -.-> W3

    PROM -->|":8080/metrics"| W1 & W2 & W3
    DASH -->|":8080/health"| W1 & W2 & W3
    PROM --> GRAF
    PROM --> ALERT
    W1 & W2 & W3 -->|stdout JSON| LOG --> GRAF

    style deps fill:#f4f6f7,stroke:#888
```

The health port carries **no authentication**. Bind it to loopback or a
private interface and let the scraper reach it there; never publish it. See
[OPERATIONS.md](OPERATIONS.md#security).

---

## 16. Design decisions and their costs

Every one of these is a trade, and the cost side is real.

| Decision | Bought | Cost |
|---|---|---|
| Traffic-first evidence | Detects a dead pooled connection a fresh probe would miss; near-zero probe load under traffic | Needs instrumentation to be wired; a silent worker falls back to probing, and the evidence label is the only way to tell |
| Probes suppressed from the traffic log | A silent worker can never look busy on the strength of its own health checks | One more moving part (a `ContextVar`) that a custom check bypassing `BaseCheck.probe` would not get |
| RabbitMQ check is introspection-only | A wedged worker shows up as stale state; the check cannot itself break consumption | Requires `install_broker_probe` and an instrumented channel; a broker that is up but unreachable *only* from this worker looks the same as a dead connection — which is arguably correct |
| Restart policy excludes dependency faults | A database outage does not become forty crash-looping workers | A worker genuinely poisoned by a dependency interaction needs a human |
| TTL applied at read time | A wedged scheduler becomes visible instead of freezing a green result | Checks age into `unknown` during long GC pauses on very short TTLs |
| Cached snapshot, no I/O on `/ready` | Sub-millisecond responses under load and during an outage | The answer is up to one check interval old; `health_eval_age_ms` is published so you can see exactly how old |
| Zero required dependencies | Installs anywhere; no version conflict with the worker's own stack | A hand-written YAML subset parser to maintain (PyYAML is used when present) |
| Class-level patch for redis-py | The only funnel that catches every command | Process-global; per-instance routing and an idempotence flag are required to make it safe |
| Binary *and* severity metric families | Unambiguous alerts, expressive dashboards | Two series per verdict to document and keep consistent |

---

## Further reading

- [USAGE.md](USAGE.md) — step-by-step integration guides
- [CONFIGURATION.md](CONFIGURATION.md) — every setting and probe type
- [OBSERVABILITY.md](OBSERVABILITY.md) — metrics, events, dashboards, alerts
- [OPERATIONS.md](OPERATIONS.md) — failure matrix, recovery, security, PM2
