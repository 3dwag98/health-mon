# Configuration reference

Every setting, every probe type, and the precedence rules between them.

- [Sources and precedence](#sources-and-precedence)
- [Top-level settings](#top-level-settings)
- [Per-probe settings](#per-probe-settings)
- [Probe types](#probe-types)
- [Context references](#context-references)
- [Restart policy](#restart-policy)
- [Environment variables](#environment-variables)

---

## Sources and precedence

Lowest to highest:

1. the defaults in `HealthConfig`
2. the config file (`worker-health.yaml`, or JSON) / Django `WORKER_HEALTH`
3. `HEALTH_*` environment variables
4. keyword arguments to `setup_worker_health()`

Environment above file is deliberate: the file is baked into an image, the
environment is what an operator can change without a rebuild.

Config files are expanded for `${VAR}` and `${VAR:-default}` before parsing,
so one file can serve a whole fleet. Probe lists are **not** configurable
from the environment — nested mappings squeezed into environment variables
are unreadable, and every deployment that could set them has a file.

YAML is parsed with PyYAML when installed, and otherwise with a built-in
subset loader (mappings, lists, scalars, quotes, comments, `{}`/`[]`). The
subset loader refuses anchors, merge keys and block scalars rather than
guessing — install `worker-health[yaml]` if you need them.

---

## Top-level settings

| Key | Default | Meaning |
|---|---|---|
| `service` | `"worker"` | Service name. A metric label — keep it stable. |
| `instance` | `$HEALTH_INSTANCE` or `service-pid` | Instance label. |
| `version` | `"0.0.0"` | Reported in `/health`. |
| `health_host` | `"0.0.0.0"` | Bind address. Use `127.0.0.1` unless the port is published deliberately. |
| `health_port` | `8080` | Health server port. `0` picks a free one. |
| `health_port_search` | `0` | Ports to try above `health_port` when it is taken. `0` binds it or nothing, which is what a container with a published port wants; a host running several workers sets it. |
| `serve_http` | `true` | Set false for a worker that only wants metrics and logs. |
| `runner` | `"thread"` | `thread` for a blocking consume loop, `asyncio` for an event loop. |
| `tick` | `0.2` | Scheduler cadence, seconds. |
| `boot_grace` | `30.0` | Seconds `starting` may last before checks are judged normally. |
| `max_workers` | `8` | Check thread-pool size (thread runner). |
| `loop_lag_threshold_ms` | `2000.0` | Above this, liveness reports `unalive`. |
| `default_queue` | `"default"` | Queue label for `@handler` when none is given. |
| `processing_check` | `true` | Register the processing check automatically. |
| `max_idle` | `60.0` | Silence, in seconds, before an idle worker **with a backlog** is `not_consuming`. |
| `max_since_success` | `120.0` | Receiving but not completing for this long is `stalled`. |
| `poison_threshold` | `10` | Consecutive handler failures before `poison_loop`. |
| `log_level` | `"INFO"` | |
| `configure_logging` | `true` | False leaves an app's existing logging alone. |
| `instrument` | `true` | Auto-instrument clients found in the context. |
| `strict_probes` | `true` | A malformed probe stops boot. False registers it as failing instead. |
| `load_plugins` | `true` | Load `worker_health.probes` entry points. |
| `probes` | `[]` | See below. |
| `restart` | `{}` | See below. |

Django spellings (`ENABLED`, `PORT`, `HOST`, `SERVICE`, `DEFAULT_QUEUE`,
`BOOT_GRACE`, `PROBES`, …) map onto the same fields. Django-only keys:
`COMMANDS`, `PORTS`, `DATABASES`, `CACHE_ALIAS`, `CACHE_DEPENDENCY`,
`CONTEXT`, `ADOPT_HEALTH_CHECK_PLUGINS`, `HEALTH_CHECK_SKIP`.

`instance` defaults to the supervisor's own ordinal when there is one
(`NODE_APP_INSTANCE`, `PM2_INSTANCE_ID`, `pm_id`) and to `service-pid`
otherwise — a pid changes on every restart, which turns a metric label into
a new time series each time.

---

## Per-probe settings

```yaml
- type: postgres          # required — which builder to use
  name: postgres          # defaults to `type`; this is the metric label
  critical: true          # true → failing means unready
  enabled: true           # false → registered, reported `disabled`, never run
  interval: 15            # seconds between evaluations, when healthy
  timeout: 2              # seconds before the evaluation is a timeout
  failure_threshold: 3    # consecutive failures before status flips
  success_threshold: 2    # consecutive successes before recovery
  max_silence: 60         # seconds of no real traffic before probing
  ttl: 32                 # optional; defaults to interval*2 + timeout
  params: {}              # type-specific, see below
```

Any key that is not a spec field is treated as a param, so the short form
`url: https://…` works as well as `params: {url: …}`.

**`critical`** is the most consequential setting in the file:

```
critical: true   → failing makes /ready return 503 (out of rotation)
critical: false  → failing degrades the worker; /ready stays 200
```

Make a dependency critical only if the worker genuinely cannot do its job
without it. A cache that the worker can fall back past is not critical, and
marking it so converts a cache outage into a total outage.

**`max_silence`** is what makes traffic-first health work. Under load, real
traffic is fresher than any probe and the probe never runs. Set it to a
couple of multiples of your quiet-period length.

**Backoff** is not per-probe configurable in YAML by design — the defaults
(5s → 60s, ×2, 10% jitter) are what the guardrail against retry storms
requires. They can be overridden per check in code via `monitor.register(...,
backoff_initial=…, backoff_max=…, backoff_multiplier=…)`.

---

## Probe types

| Type | Evidence | Required params | Optional params |
|---|---|---|---|
| `postgres` / `sqlalchemy` | introspect → observe → probe | `engine` **or** `dsn` | `probe_dsn`, `pool_warn_ratio` (0.9), `dependency` |
| `django_db` | introspect → observe → probe | — | `alias` (`default`), `dependency` |
| `django_health_check` | probe | `backend` | `dependency` — adopts an existing `BaseHealthCheckBackend` |
| `redis` | introspect → observe → probe | `client` **or** `url` **or** `host` | `port`, `db`, `password`, `label`, `memory_warn_ratio` (0.9), `dependency` |
| `rabbitmq` | introspect only | `broker_state`, `queue` | `backlog_threshold` (1000), `stale_after` (20) |
| `kafka` | introspect only | — (a `state` is created if absent) | `state`, `group`, `topics`, `max_lag` (10000), `stale_after` (30), `max_rebalance` (60), `lag_fn` |
| `http` | probe | `url` | `expect_status` (200), `method` (GET), `headers`, `slow_ms` |
| `tcp` | probe | `host`, `port` | — |
| `dns` | probe | `host` | `family` (any/ipv4/ipv6), `min_records` (1) |
| `disk` | probe | — | `path` (`/`), `min_free_gb` (5), `min_free_ratio`, `fail_free_gb` |
| `file_age` | probe | `path` | `max_age_s` (300), `min_size_bytes`, `missing_is_failure` (true) |
| `function` | probe | `fn` | — |
| `processing` | observe | — | `state`, `broker_state`, `max_idle`, `max_since_success`, `poison_threshold` |

Notes that matter in practice:

- **`postgres`** — pass `engine` (the application's own) *and* `probe_dsn`.
  The engine is what makes pool exhaustion visible; the DSN builds a
  separate `NullPool` connection for the fallback probe so the probe can
  never compete for application slots or cause the exhaustion it detects.
- **`redis`** — pass the worker's own `client`. A check on a separate client
  proves the server is up, not that the worker's connection is.
- **`rabbitmq`** — has no probe at all. It reads `BrokerState`, written by
  the worker's own connection thread. Pair it with `install_broker_probe`
  (queue depth) and `instrument_pika_channel` (deliveries and acks).
- **`kafka`** — same shape. The client libraries are not thread-safe, so the
  check never touches the consumer; the consumer's loop writes into
  `KafkaConsumerState`.
- **`http`** — GET, HEAD and OPTIONS only. A probe that mutates external
  state is refused at wiring time.
- **`dependency`** — the traffic-log key a check reads observed evidence
  from. Defaults to the check's own name; set `dependency: ""` to opt out of
  observed evidence entirely.

---

## Context references

A YAML file cannot contain a live SQLAlchemy engine. `@name` is the bridge:

```yaml
params:
  engine: "@db_engine"
```

```python
setup_worker_health(context={"db_engine": engine})
```

- `"@name"` → `context["name"]`; a missing key fails at boot with an error
  naming the probe, the param, and everything the context *did* contain.
- `"@@literal"` → the literal string `"@literal"`.
- References resolve inside nested dicts and lists.

Keys the SDK adds to the context for you: `processing_state`, `monitor`,
`tracker`.

Keys auto-instrumentation recognises by shape (the name is a convention,
the detection is duck-typed):

| Context key | Detected by | Instrumented as |
|---|---|---|
| `db_engine`, `engine` | `.pool` and `.dialect` | `postgres` |
| `redis_client`, `redis` | `.execute_command` + `.connection_pool` | `redis` |
| `amqp_connection`, `connection` | `.add_on_connection_closed_callback` | `rabbitmq` |

In Django, `CONTEXT` values are `"module:attribute"` import paths, resolved
at `ready()` time; a callable is called, an already-built client is used
as-is.

---

## Restart policy

Off unless configured. The library never kills anything — it exits its own
process with a chosen code and lets the supervisor apply its restart rules.

```yaml
restart:
  enabled: false
  after_cycles: 5        # consecutive triggering evaluations
  min_uptime: 120        # never restart a process that just booted
  cooldown: 600          # seconds between restarts
  max_per_hour: 3        # then latch: stay up, keep reporting
  drain_timeout: 30      # jittered wait before exiting
  exit_code: 70
  triggers: [stalled, not_consuming, not_subscribed,
             credit_exhausted, connection_lost, poison_loop]
```

The default trigger list contains **only self-inflicted faults**. Restarting
a worker because Postgres is down does not fix Postgres: it converts one
database outage into forty crash-looping processes hammering a database that
is already in trouble, and destroys the in-flight work each was holding. See
[OPERATIONS.md](OPERATIONS.md#failure-handling-matrix).

---

## Environment variables

Any `HealthConfig` field can be set as `HEALTH_<FIELD>`:

| Variable | Field |
|---|---|
| `HEALTH_SERVICE` | `service` |
| `HEALTH_INSTANCE` | `instance` |
| `HEALTH_PORT` | `health_port` |
| `HEALTH_HOST` | `health_host` |
| `HEALTH_BOOT_GRACE` | `boot_grace` |
| `HEALTH_RUNNER` | `runner` |
| `HEALTH_DEFAULT_QUEUE` | `default_queue` |
| `HEALTH_LOG_LEVEL` | `log_level` |
| `HEALTH_MAX_IDLE` | `max_idle` |
| `HEALTH_STRICT_PROBES` | `strict_probes` |
| `WORKER_HEALTH_CONFIG` | path to the config file |

A malformed value keeps the default rather than failing the boot — a typo in
an environment variable must not be able to stop a fleet from starting.
