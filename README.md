# worker-health

Operational health for message-driven Python workers. A worker process can be
alive and completely unable to consume; process status alone does not say
whether work is happening. This package reports the difference.

Health is derived from the worker's **own** connections and its real traffic
wherever possible. A synthetic probe is a labelled fallback for when the worker
has been silent, never the primary signal — a probe-first design reports healthy
while a worker sits on a dead pooled connection.

## Run it

Everything runs in Docker — the SDK, the workers, the dashboard and the tests.
The only prerequisite is a working Docker daemon.

### macOS / Linux

    make up          # deps, three workers, load generator, dashboard
    open http://localhost:9000

    make health      # aggregate status of every worker
    make idle        # pause load — a quiet queue must NOT alert
    make burst       # 2000 messages — watch backlog and recovery
    make chaos-db-blackhole ; make restore
    make down

### Windows

Requires **Docker Desktop with the WSL2 backend** (Settings → General → *Use the
WSL 2 based engine*). No `make`, no bash, no curl needed — `run.ps1` covers every
target the Makefile does.

From PowerShell, in the repo root:

    .\run.ps1 up          # builds, starts everything, opens the dashboard
    .\run.ps1 health      # aggregate status of every worker
    .\run.ps1 idle        # pause load — a quiet queue must NOT alert
    .\run.ps1 burst       # 2000 messages — watch backlog and recovery
    .\run.ps1 chaos-db-blackhole
    .\run.ps1 restore
    .\run.ps1 down

    .\run.ps1             # no argument prints every command

From `cmd.exe`, drop the `.\` and the extension — `run up`, `run health`, and so
on. `run.cmd` wraps the PowerShell call and bypasses the execution policy for
that one invocation without changing any machine setting.

If PowerShell refuses to run the script directly (`running scripts is disabled on
this system`), either use `run.cmd`, or allow local scripts for your user once:

    Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

**Port conflicts.** Windows reserves blocks of high ports for Hyper-V and WSL2.
If `up` fails with *"bind: An attempt was made to access a socket in a way
forbidden by its access permissions"*, that port is reserved. Check what is
reserved in an elevated prompt:

    netsh interface ipv4 show excludedportrange protocol=tcp

Then copy `.env.example` to `.env` and change the offending number. Every host
port is configurable there; the containers talk to each other over the compose
network and are unaffected.

**Line endings.** `.gitattributes` forces LF on everything that goes into an
image, so Git for Windows' default `core.autocrlf=true` cannot bake CRLF into
the Linux containers. If you cloned before that file existed, re-normalise once
with `git rm --cached -r . && git reset --hard`.

### Fault injection

Run any of these while watching the dashboard — detection and recovery appear in
the transition log.

| Make | PowerShell | What it does |
|---|---|---|
| `make chaos-db-blackhole` | `.\run.ps1 chaos-db-blackhole` | Packets dropped, socket never closes. The firewall-DROP case, and the hard one. |
| `make chaos-db-down` | `.\run.ps1 chaos-db-down` | Port closed → `connection_refused`. |
| `make chaos-db-slow` | `.\run.ps1 chaos-db-slow` | +400 ms, below the timeout → must stay OK. |
| `make chaos-redis-down` | `.\run.ps1 chaos-redis-down` | Non-critical dependency: degrades, does not 503 readiness. |
| `make chaos-mq-down` | `.\run.ps1 chaos-mq-down` | Broker unreachable. |
| `make restore` | `.\run.ps1 restore` | Clear every fault. |

### Tests

    make test        /  .\run.ps1 test     # full suite, in a container
    make unit        /  .\run.ps1 unit     # unit tier, native Python, no Docker

The unit tier starts no containers and passes with the Docker daemon stopped —
that boundary is enforced, not just intended.

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
