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
```

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
pytest tests/unit -q
```

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
