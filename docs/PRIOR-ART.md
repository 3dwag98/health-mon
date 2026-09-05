# Prior art, and what it changed here

What the existing Python health-check libraries do, where worker-health
differs, and which of those differences came from reading them.

This is not a competitive comparison. Every library below is good at the
job it was built for — which is almost always **a web application behind a
load balancer**. A message-driven worker has no load balancer, no request
to hang a check on, and a failure mode (consuming nothing while looking
perfectly alive) that a web app does not have. The differences follow from
that, not from the libraries being wrong.

- [Django](#django)
- [FastAPI](#fastapi)
- [Celery, and why probes there are hard](#celery-and-why-probes-there-are-hard)
- [What this reading changed](#what-this-reading-changed)

---

## Django

**[django-health-check](https://django-health-check.readthedocs.io/)** is the
de facto standard: a pluggable app with backends for databases, caches,
storage, disk and memory, migrations, Celery, and several brokers. A
project adopts it, subclasses `BaseHealthCheckBackend`, and registers with
`plugin_dir.register(...)`.

It is well built for a web service. Four properties make it a poor fit for
a long-running worker, and each one shaped a decision here:

| django-health-check | worker-health | Why the difference |
|---|---|---|
| Checks run **inside the request**, uncached — every backend, every hit ([source](https://github.com/revsys/django-health-check/blob/master/health_check/views.py)) | Checks run on a scheduler thread; endpoints serve a cached snapshot and do no I/O | A worker's health endpoint is polled by a supervisor every few seconds forever. Running real I/O per poll makes health a load source, and slow exactly when the dependency is already struggling. Its own community notes the endpoint is a DoS vector. |
| The database backend **writes**: creates a row, updates it, deletes it, per check ([backend](https://github.com/revsys/django-health-check/blob/master/health_check/db/backends.py)) | `django_db` runs `SELECT 1` on its own cursor, plus a read-only `SHOW transaction_read_only` | The brief's guardrail is that health checks are non-destructive. A write-per-check also fails on a read-only replica, reporting an outage where there is a failover. |
| One endpoint; **no liveness/readiness split** | `/live` (loop only) and `/ready` (dependencies + processing) are separate, with different codes | A single endpoint wired to a liveness probe restarts the whole fleet when a shared database goes down. The split is what stops a dependency outage becoming a restart storm. |
| Failure returns **HTTP 500**; `critical_service=False` only changes the code | `degraded` returns 200 and stays in rotation; only critical failures 503 | 500 means "this service is broken". A cache being down is not that, and a worker that leaves rotation for it converts a cache outage into a total one. |
| No thresholds, hysteresis, timeouts or backoff | Per-check `failure_threshold`, `success_threshold`, `timeout`, and 5s→60s backoff | One lost packet should not be an outage, and a failing dependency should not be asked every few seconds. |

Also worth naming: **[django-watchman](https://github.com/mwarkentin/django-watchman)**
(similar shape, token-authenticated endpoint — the auth idea is good and
worker-health has no equivalent; it relies on binding to loopback) and
**[django-alive](https://github.com/lincolnloop/django-alive)** (deliberately
minimal, and the closest in spirit to the `/live` endpoint here).

### What was adopted rather than reinvented

Two things django-health-check gets right that are now supported directly:

1. **A registry of pluggable backends.** worker-health's `ProbeFactory` is
   the same idea with entry-point discovery added, so a platform team can
   ship probe types as a distribution.
2. **Existing backends are worth keeping.** `worker_health_django.compat`
   runs any `BaseHealthCheckBackend` subclass as a worker-health check, maps
   its `critical_service` onto criticality, and gives it the scheduling,
   timeout, thresholds and backoff it did not have:

   ```python
   from worker_health_django import install_health_check_plugins

   install_health_check_plugins(
       monitor,
       # The SDK's own django_db probe covers the database read-only.
       skip=("DatabaseBackend",),
   )
   ```

   or, from settings:

   ```python
   WORKER_HEALTH = {
       "ADOPT_HEALTH_CHECK_PLUGINS": True,
       "HEALTH_CHECK_SKIP": ["DatabaseBackend"],
   }
   ```

   A team keeps years of accumulated deployment knowledge and still gets the
   worker-shaped behaviour. Rewriting those backends as a condition of
   adoption is how an adoption stalls.

### The instrumentation hook

The first version of this SDK monkeypatched `CursorWrapper.execute`. That
is what several APM agents historically did, and it is wrong now: Django has
shipped a documented hook since 2.0,
[`connection.execute_wrappers`](https://docs.djangoproject.com/en/5.1/topics/db/instrumentation/),
installed from a `connection_created` receiver.

Switching to it bought four things:

- a supported API, so a Django upgrade that reorganises cursor internals
  cannot silently stop the instrumentation;
- `executemany` coverage through the `many` flag, instead of a second patch;
- composition — Debug Toolbar, django-silk and Scout append to the same
  list, so worker-health sits alongside them rather than fighting for one
  method;
- per-connection routing, so `default` and `replica` report as two
  dependencies naturally.

---

## FastAPI

The ecosystem here is thinner.
**[fastapi-health](https://pypi.org/project/fastapi-health/)** is the
best-known: `health([condition, ...])` builds a route from callables
returning `bool` or `dict`, with configurable success/failure status. Its
genuinely nice property is that conditions are FastAPI dependencies, so
they compose with `Depends`.

What it does not have, and a worker needs: async conditions, per-check
detail in the response, timeouts, criticality, thresholds, any notion of
processing health, or a liveness/readiness distinction. It is a route
builder, which is the right size for what it claims to be.

**[prometheus-fastapi-instrumentator](https://github.com/trallnag/prometheus-fastapi-instrumentator)**
and **[starlette-exporter](https://github.com/stephenhillier/starlette_exporter)**
cover request metrics well but are about HTTP traffic — a worker has none.

Two things this reading changed:

1. **Interop note.** If an app already exposes `/metrics` through one of
   those instrumentators, worker-health's `/metrics` is a *second* endpoint
   on a *different* port. That is fine and deliberate — the worker's health
   port answers when the event loop is wedged and an in-app route does not —
   but it needs saying, and now does, in
   [OBSERVABILITY.md](OBSERVABILITY.md).
2. **Django reached parity with FastAPI.** The FastAPI integration always
   shipped optional in-app routes (`/internal/live` and friends) for
   platforms that route a single port. Django had no equivalent, which was
   an inconsistency rather than a decision. It does now:

   ```python
   urlpatterns = [
       path("internal/health/", include("worker_health_django.urls")),
   ]
   ```

   With the same caveat in both places: these are served by the application's
   own worker, so they stop answering under exactly the conditions they
   exist to report. The SDK's threaded port stays authoritative for liveness.

---

## Celery, and why probes there are hard

Not integrated — but worth recording, because it is the clearest external
evidence for the architecture here.

The standard Celery liveness probe is `celery inspect ping`. It is a
broadcast RPC the worker answers from its main loop, and the documented
behaviour is that **it does not respond while the worker is busy with a
long-running task** — it reports unhealthy precisely when the worker is
healthiest. It also spawns a CPU-heavy subprocess per probe. The workaround
circulated in the Celery issue tracker is `periodSeconds: 300` with
`failureThreshold: 10`, which pushes real detection past fifty minutes
([celery#4079](https://github.com/celery/celery/issues/4079),
[celery discussion #10166](https://github.com/celery/celery/discussions/10166)).

That is the same failure this SDK is built around, in a different codebase:
asking a busy worker a question and mistaking silence for death. The two
design choices that avoid it — answer from a **separate thread** with a
**cached snapshot**, and derive evidence from what the worker already does
rather than from an interrogation — are why `/live` here answers in
microseconds under full load.

django-health-check's Celery backend has the mirror-image problem: it
*publishes a task* and waits three seconds for execution, which both
violates the non-destructive guardrail and fails on any queue that is
legitimately slow. Its own `celery_ping` alternative — check that each
queue has a consumer — is much closer to the introspection approach used
here for RabbitMQ and Kafka.

---

## What this reading changed

Concretely, in this repo:

| Change | Source of the idea |
|---|---|
| Django instrumentation moved from monkeypatching `CursorWrapper` to `connection.execute_wrappers` | Django's documented instrumentation API; the approach Scout and django-silk use |
| `worker_health_django.compat` — adopt existing `BaseHealthCheckBackend` plugins | django-health-check's registry, and the migration cost of ignoring it |
| `django_health_check` probe type, and `ADOPT_HEALTH_CHECK_PLUGINS` | as above |
| Optional in-app Django views at `worker_health_django.urls` | parity with the FastAPI `/internal` routes |
| The `skip=("DatabaseBackend",)` guidance | django-health-check's DB backend writes a row per check |
| Documented `/metrics` coexistence with FastAPI instrumentators | prometheus-fastapi-instrumentator |

And what was *confirmed* rather than changed: the cached-snapshot read path,
the liveness/readiness split, `degraded` staying in rotation, read-only
probes, and thresholds with backoff. Every one of them is a place where the
existing libraries take the other option, and the reasons they can afford
to — a request to hang the check on, a load balancer to be removed from —
do not hold for a worker.

## Sources

- [django-health-check documentation](https://django-health-check.readthedocs.io/en/stable/)
- [django-health-check `views.py`](https://github.com/revsys/django-health-check/blob/master/health_check/views.py)
- [django-health-check `db/backends.py`](https://github.com/KristianOellegaard/django-health-check/blob/master/health_check/db/backends.py)
- [django-health-check `contrib/celery/backends.py`](https://github.com/KristianOellegaard/django-health-check/blob/master/health_check/contrib/celery/backends.py)
- [Django database instrumentation](https://docs.djangoproject.com/en/5.1/topics/db/instrumentation/)
- [Adam Johnson — always-installed Django database instrumentation](https://adamj.eu/tech/2020/07/23/how-to-make-always-installed-django-database-instrumentation/)
- [fastapi-health](https://pypi.org/project/fastapi-health/)
- [Celery signals reference](https://docs.celeryq.dev/en/stable/userguide/signals.html)
- [celery#4079 — liveness and readiness probes for workers](https://github.com/celery/celery/issues/4079)
- [celery discussion #10166](https://github.com/celery/celery/discussions/10166)
