"""A FastAPI worker: async consumer, async SQLAlchemy, async Redis.

The lifespan owns the health monitor and the consumer task.  The consumer
is a plain class with an async ``run()``; the tracker's decorator works on
coroutine functions unchanged, because it detects them and adapts.

Run it with:

    HEALTH_SERVICE=billing-worker uvicorn app:app --port 8000

The health endpoints are on the SDK's own port (8080 by default), served
from a thread that keeps answering even if this event loop wedges -- which
is the whole point of separating liveness from readiness.  The optional
/internal routes below are the same data served from the loop, for
platforms that only route one port.
"""
from __future__ import annotations

import asyncio
import json
import os

from fastapi import Depends, FastAPI, Request
from redis import asyncio as aioredis
from sqlalchemy.ext.asyncio import create_async_engine

from worker_health import BrokerState
from worker_health_fastapi import HealthSettings, get_monitor, health_lifespan
from worker_health_fastapi.routes import router as health_router

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://app:app@postgres/app")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

engine = create_async_engine(DATABASE_URL, pool_size=5, max_overflow=0)
redis_client = aioredis.from_url(REDIS_URL, socket_timeout=1.0,
                                 socket_connect_timeout=1.0)
broker_state = BrokerState()


class BillingConsumer:
    """One decorator, applied to a bound method at construction time."""

    def __init__(self, tracker):
        self.tracker = tracker
        self.handle_message = tracker.handler(queue="billing.in")(self._handle_message)

    async def _handle_message(self, body: dict) -> None:
        await process_payment(body)

    async def run(self) -> None:
        async for raw_message in consume_from_queue():
            await self.handle_message(json.loads(raw_message))


async def process_payment(body: dict) -> None:
    """Business code.  Every query and command here is observed automatically."""
    from sqlalchemy import text

    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE accounts SET balance_cents = balance_cents + :c "
                 "WHERE id = :a"),
            {"c": body["amount_cents"], "a": body["account_id"]},
        )
    await redis_client.setex(f"balance:{body['account_id']}", 300,
                             body["amount_cents"])


async def consume_from_queue():
    """Stand-in for a real async consumer (aio-pika, aiokafka, ...)."""
    while True:
        await asyncio.sleep(1)
        yield json.dumps({"account_id": "acct-001", "amount_cents": 100})


app = FastAPI(
    lifespan=health_lifespan(
        settings=HealthSettings(),
        # A callable, not a dict: it runs at startup, after the engine and
        # client exist and after any fork a process manager performed.
        context=lambda: {
            "db_engine": engine,
            "redis_client": redis_client,
            "broker_state": broker_state,
        },
        consumers=[BillingConsumer],
        # Anything beyond the three standard probes goes here or in YAML.
        probes=[{
            "type": "http", "name": "vendor-api", "critical": False,
            "interval": 30, "timeout": 2,
            "params": {"url": "https://api.vendor.com/ping", "expect_status": 200},
        }],
    )
)

# Optional: the same data, served from the event loop, under /internal.
app.include_router(health_router)


@app.get("/queue-depth")
async def queue_depth(monitor=Depends(get_monitor)):
    return monitor.snapshot_dict(include_timings=False)["processing"]


@app.get("/why-not-ready")
async def why_not_ready(request: Request):
    """The one endpoint on-call actually wants: what is blocking readiness."""
    monitor = request.app.state.monitor
    return {
        "readiness": monitor.readiness().value,
        "reasons": list(monitor.readiness_reasons()),
    }
