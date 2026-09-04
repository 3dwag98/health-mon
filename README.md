# worker-health

Operational health for message-driven Python workers. A worker process can be
alive and completely unable to consume; process status alone does not say
whether work is happening. This package reports the difference.

Health is derived from the worker's **own** connections and its real traffic
wherever possible. A synthetic probe is a labelled fallback for when the worker
has been silent, never the primary signal — a probe-first design reports healthy
while a worker sits on a dead pooled connection.

## Integration, in full

```python
from worker_health import setup_worker_health

health = setup_worker_health(
    service="billing-worker",
    config_path="worker-health.yaml",
    context={
        "db_engine": engine,           # instrumented automatically
        "redis_client": redis_client,  # instrumented automatically
        "broker_state": broker_state,  # referenced by the rabbitmq probe
    },
)


@health.tracker.handler(queue="billing.in")
def handle(message: dict):
    process_payment(message)           # unchanged
```

That is the whole change to worker code. It gets you `/live`, `/ready`,
`/health`, `/metrics`, `/events`, structured JSON logs, dependency checks
backed by real traffic, processing health, custom probes and dashboard
metrics — with no per-query wrapping anywhere.

Django and FastAPI have dedicated wiring: add one app to `INSTALLED_APPS`, or
one `lifespan=` argument. See [docs/USAGE.md](docs/USAGE.md).

## Documentation

| Document | What is in it |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Component, threading, state-machine, evidence-ladder, wiring and deployment diagrams; the design decisions and what each one costs |
| [docs/USAGE.md](docs/USAGE.md) | Step-by-step guides: generic worker, Django, FastAPI, custom probes, troubleshooting |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Every setting, every probe type and its params |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | Metric reference, structured events, Grafana dashboard, alert rules |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Failure matrix, backoff and recovery, restart policy, PM2, security |
| [docs/INTEGRATION.md](docs/INTEGRATION.md) | The short version of the above |

## Run it

Everything runs in Docker — the SDK, the workers, the dashboard and the tests.
The only prerequisite is a working Docker daemon. On Windows that means **Docker
Desktop with the WSL2 backend** (Settings → General → *Use the WSL 2 based
engine*).

Every command below is plain `docker compose` and works identically in bash,
PowerShell and `cmd.exe`. Nothing here needs `make`, and nothing needs the
helper scripts.

### Start and stop

```
docker compose build
docker compose up -d
```

Then open **http://localhost:9000** for the dashboard.

```
docker compose ps                                  # container status
docker compose logs -f billing notify reconcile    # tail the workers
docker compose down -v --remove-orphans            # stop, remove volumes
```

First start pulls four images and builds two, so give it a couple of minutes.
The workers report `starting` until their dependencies are reachable — that is
the boot grace working, not a failure.

### Look at the health

```
curl http://localhost:8081/health     # billing
curl http://localhost:8082/health     # notify
curl http://localhost:8083/health     # reconcile
curl http://localhost:8081/metrics    # prometheus exposition
curl http://localhost:8081/ready      # 200 or 503, the readiness verdict
curl http://localhost:8081/live       # loop responsiveness only
curl http://localhost:8081/events     # the last 50 structured events
```

`/ready` answers in the operator's vocabulary — `ready`, `degraded`,
`unready`, `starting` — and carries a `reasons` array naming the check and
its error category whenever the answer is not `ready`.

**In PowerShell, write `curl.exe`, not `curl`** — `curl` is an alias for
`Invoke-WebRequest` there and takes different arguments. `curl.exe` ships with
Windows 10 1803 and later.

Or ask a container, which avoids the host entirely:

```
docker compose exec billing worker-health --url http://127.0.0.1:8080 --json
```

That is the package's own CLI probe. It exits 0 when ready and 1 when not, so it
also works as a shell check or a PM2 healthcheck.

### Fault injection

Run any of these while watching the dashboard — detection and recovery appear in
the transition log within a few seconds.

These drive `toxiproxy-cli` **inside** the toxiproxy container, so there is no
JSON quoting to get wrong and the commands are identical in every shell.

```
# port closed -> connection_refused
docker compose exec toxiproxy /toxiproxy-cli toggle postgres

# black hole: packets dropped, socket never closes (the firewall-DROP case,
# and the one that actually breaks health checks)
docker compose exec toxiproxy /toxiproxy-cli toxic add postgres -t timeout -a timeout=0 -n blackhole

# 400ms latency, below the check timeout -> must stay OK
docker compose exec toxiproxy /toxiproxy-cli toxic add postgres -t latency -a latency=400 -a jitter=0 -n slow

# non-critical dependency: degrades, must NOT 503 readiness
docker compose exec toxiproxy /toxiproxy-cli toggle redis-cache

# broker unreachable
docker compose exec toxiproxy /toxiproxy-cli toggle rabbitmq

# inspect what is currently injected
docker compose exec toxiproxy /toxiproxy-cli list
docker compose exec toxiproxy /toxiproxy-cli inspect postgres
```

Clearing a fault:

```
# remove one toxic by the name given when it was added
docker compose exec toxiproxy /toxiproxy-cli toxic remove postgres -n blackhole

# toggle flips, so running it again re-enables the proxy
docker compose exec toxiproxy /toxiproxy-cli toggle postgres
```

To clear **everything** at once — every toxic removed and every proxy re-enabled
— call the reset endpoint:

```
curl -X POST http://localhost:8474/reset
```

PowerShell equivalent:

```
Invoke-RestMethod -Uri http://localhost:8474/reset -Method POST
```

### Load control

The load generator publishes to `billing.in` and takes a JSON body on port 8090.
Rate 0 pauses it, which is how you produce a genuinely idle queue — the case
that must never alert.

bash / `cmd.exe`:

```
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d "{\"rate\":0}"
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d "{\"rate\":8}"
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d "{\"burst\":2000}"
curl http://localhost:8090/
```

PowerShell — use `Invoke-RestMethod`, which avoids the quoting entirely:

```
Invoke-RestMethod -Uri http://localhost:8090/ -Method POST -ContentType application/json -Body '{"rate":0}'
Invoke-RestMethod -Uri http://localhost:8090/ -Method POST -ContentType application/json -Body '{"rate":8}'
Invoke-RestMethod -Uri http://localhost:8090/ -Method POST -ContentType application/json -Body '{"burst":2000}'
```

### A demo worth running

```
docker compose up -d
                                       # wait for 3/3 on the dashboard

# 1. the false-positive test: a quiet queue must stay OK, forever
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d "{\"rate\":0}"
                                       # watch for a minute: still ok

# 2. the hard fault: socket open, nothing comes back
curl -X POST http://localhost:8090/ -H "Content-Type: application/json" -d "{\"rate\":8}"
docker compose exec toxiproxy /toxiproxy-cli toxic add postgres -t timeout -a timeout=0 -n blackhole
                                       # evidence flips observed -> probed,
                                       # then postgres -> failing (timeout)

# 3. recovery
curl -X POST http://localhost:8474/reset
                                       # back to ok, evidence back to observed

# 4. criticality: redis is non-critical, so this degrades but does not fail
docker compose exec toxiproxy /toxiproxy-cli toggle redis-cache
curl -i http://localhost:8081/ready    # still HTTP 200
docker compose exec toxiproxy /toxiproxy-cli toggle redis-cache
```

### Tests

```
docker compose --profile test run --rm tests pytest -q tests
```

Unit tier only — no containers, and it passes with the Docker daemon stopped.
That boundary is enforced by a test, not just intended:

```
docker compose --profile test run --rm tests pytest -q tests/unit
```

Natively, if you have Python 3.11+ and would rather not use Docker:

```
pip install -e ".[dev]"
pytest tests/unit -q          # 104 tests, ~5 seconds, no containers
```

The unit tier covers the state machine, the evidence ladder, the probe
factory, config parsing and precedence, secret redaction, auto-instrumentation
against fake drivers, and the full engine against mocked healthy, slow and
failed dependencies — including detection, recovery, timeout isolation and the
HTTP status codes.

### Port conflicts

Every host port is configurable. Copy `.env.example` to `.env` and change what
collides; the containers reach each other over the compose network and are
unaffected by these values.

On Windows, Hyper-V and WSL2 reserve blocks of high ports. If `up` fails with
*"bind: An attempt was made to access a socket in a way forbidden by its access
permissions"*, the port is reserved rather than in use. Check which ranges are
taken from an elevated prompt:

```
netsh interface ipv4 show excludedportrange protocol=tcp
```

### Line endings on Windows

`.gitattributes` forces LF on everything that goes into an image, so Git for
Windows' default `core.autocrlf=true` cannot bake CRLF into the Linux
containers. If you cloned before that file existed, re-normalise once:

```
git rm --cached -r .
git reset --hard
```

### Optional shortcuts

`make` on macOS and Linux, `.\run.ps1` on Windows, wrap everything above. They
are conveniences — the `docker compose` commands are the real interface, and
every target maps onto one of them.

```
make up          |  .\run.ps1 up
make health      |  .\run.ps1 health
make idle        |  .\run.ps1 idle
make restore     |  .\run.ps1 restore
make down        |  .\run.ps1 down
```

`make help` and `.\run.ps1` with no argument list the rest. From `cmd.exe`, use
`run up` — `run.cmd` wraps the PowerShell call and bypasses the execution policy
for that one invocation without changing any machine setting.

## What it checks

| Check | Observed | Introspected | Probed |
|---|---|---|---|
| postgres | query outcome + latency, from SQLAlchemy events | pool checked-out / size / overflow | `SELECT 1` on an isolated NullPool connection |
| django_db | query outcome, from `CursorWrapper` | connection present, rollback flag, atomic block | `SELECT 1` on its own cursor |
| redis | command outcome, from `execute_command` | client pool counters | `PING` + `INFO` |
| rabbitmq | last delivery, last ack | connection + channel state, consumer tags, unacked vs prefetch, broker alarm | passive declare on a dedicated channel of the worker's own connection |
| kafka | last poll, last delivery | assignment, rebalance state, paused, lag | — |
| processing | received / succeeded / failed, idle time vs queue depth | — | — |
| http · tcp · dns · disk · file_age | — | — | read-only request, connect, resolve, or `statvfs` |

Every result carries `evidence` — `observed`, `introspected` or `probed` — so a
probe-backed green never looks like a traffic-backed green, and
`evidence_age_ms` says how old the signal behind it was.

The observed column is **automatic**: `setup_worker_health()` instruments
SQLAlchemy, the Django ORM, redis-py (sync and async) and pika from the
clients you pass it. Business code wraps nothing. Probes run only after
`max_silence` seconds without real traffic, and never count as traffic
themselves — a `ContextVar` suppresses them, so a silent worker can never look
busy on the strength of its own health checks.

## Extending it

Probes are declarative and pluggable. Thirteen types ship built in —
`postgres`, `django_db`, `redis`, `rabbitmq`, `kafka`, `http`, `tcp`, `dns`,
`disk`, `file_age`, `function`, `processing`, `sqlalchemy` — and your own
register the same way the built-ins do:

```python
@factory.probe_type("vendor-api")
def build_vendor_probe(spec, context):
    return HttpProbe(url=spec.params["url"], timeout=spec.timeout)
```

Another distribution can ship probe types for a whole firm through entry
points under `worker_health.probes`; `factory.load_plugins()` finds them with
no import from this package.

See `workers/` for three complete workers, `examples/` for Django and FastAPI
ones, `deploy/` for Prometheus rules and a Grafana dashboard, and `docs/` for
the PM2 example.

## Versions

Pinned deliberately: **Postgres 16, Redis 7.2, RabbitMQ 3.10**.

Redis 7.2 implements `HELLO`, so RESP2/RESP3 negotiation is a non-issue and
`build_client()` leaves the protocol at the client default. What it does
guarantee is the setting that actually breaks health checks: socket timeouts.
redis-py leaves them as `None`, and a probe on a black-holed connection then
hangs forever — holding a pool slot and reporting nothing, which is strictly
worse than no probe at all. A client pointed at a server too old for `HELLO`
is reported as `dependency_version`, not `connection_refused`, because the fix
is a client setting rather than a network to investigate.

RabbitMQ's `/api/aliveness-test` is never used: on 3.10 it declares a queue,
publishes and consumes a message, which violates the non-destructive
guardrail. It became a no-op only in 4.0.
