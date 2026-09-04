"""The billing pipeline: one message touches Redis, Postgres and RabbitMQ.

Deliberately not a toy.  Each stage can fail independently and each failure
looks different to the health system, which is what makes this worth
demonstrating:

  1. Redis   SETNX idempotency key      -- duplicate suppression
  2. Redis   per-account advisory lock  -- serialises concurrent updates
  3. Postgres  transaction              -- insert invoice, update balance
  4. Redis   write-through cache        -- balance snapshot
  5. RabbitMQ  publish downstream       -- billing.out
  6. ack

Every stage is timed separately, so the dashboard can show which dependency
is responsible when handler latency rises.
"""
from __future__ import annotations

import json
import os
import random
import time
import uuid

from sqlalchemy import text

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    balance_cents BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS invoices (
    id          TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS invoices_account_idx ON invoices (account_id);
"""

ACCOUNTS = [f"acct-{i:03d}" for i in range(1, 25)]


def init_schema(engine) -> None:
    with engine.begin() as conn:
        for stmt in SCHEMA.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
        for acct in ACCOUNTS:
            conn.execute(
                text("INSERT INTO accounts (id) VALUES (:id) "
                     "ON CONFLICT (id) DO NOTHING"),
                {"id": acct},
            )


class LockNotAcquired(RuntimeError):
    pass


class BillingPipeline:
    def __init__(self, engine, redis, publish, tracker, queue: str,
                 logger=None, lock_ttl: int = 10) -> None:
        self.engine = engine
        self.redis = redis
        self.publish = publish
        self.track = tracker
        self.queue = queue
        self.logger = logger
        self.lock_ttl = lock_ttl

    # -- stages ----------------------------------------------------------- #

    def _seen_before(self, message_id: str) -> bool:
        from worker_health import classify_redis
        with self.track.stage(self.queue, "redis_idempotency"):
            with self.track.dependency("redis", classify=classify_redis):
                fresh = self.redis.set(
                    f"billing:seen:{message_id}", "1", nx=True, ex=3600
                )
        return not bool(fresh)

    def _acquire_lock(self, account_id: str) -> str | None:
        from worker_health import classify_redis
        token = uuid.uuid4().hex
        with self.track.stage(self.queue, "redis_lock"):
            with self.track.dependency("redis", classify=classify_redis):
                got = self.redis.set(
                    f"billing:lock:{account_id}", token, nx=True, ex=self.lock_ttl
                )
        return token if got else None

    def _release_lock(self, account_id: str, token: str) -> None:
        from worker_health import classify_redis
        try:
            with self.track.dependency("redis", classify=classify_redis):
                key = f"billing:lock:{account_id}"
                if self.redis.get(key) == token.encode():
                    self.redis.delete(key)
        except Exception:
            pass   # the TTL will clear it; never fail a message on unlock

    def _apply_to_postgres(self, invoice_id, account_id, amount_cents) -> int:
        from worker_health import classify_postgres
        with self.track.stage(self.queue, "postgres_txn"):
            with self.track.dependency("postgres", classify=classify_postgres):
                with self.engine.begin() as conn:
                    conn.execute(
                        text("INSERT INTO invoices (id, account_id, amount_cents) "
                             "VALUES (:i, :a, :c) ON CONFLICT (id) DO NOTHING"),
                        {"i": invoice_id, "a": account_id, "c": amount_cents},
                    )
                    balance = conn.execute(
                        text("UPDATE accounts SET balance_cents = balance_cents + :c, "
                             "updated_at = now() WHERE id = :a "
                             "RETURNING balance_cents"),
                        {"c": amount_cents, "a": account_id},
                    ).scalar()
        return int(balance or 0)

    def _cache_balance(self, account_id: str, balance: int) -> None:
        from worker_health import classify_redis
        with self.track.stage(self.queue, "redis_cache"):
            with self.track.dependency("redis", classify=classify_redis):
                self.redis.setex(f"billing:balance:{account_id}", 300, balance)

    def _emit(self, payload: dict) -> None:
        with self.track.stage(self.queue, "rabbitmq_publish"):
            self.publish(json.dumps(payload).encode())

    # -- entry point ------------------------------------------------------ #

    def handle(self, body: bytes) -> dict:
        msg = json.loads(body)
        message_id = msg["message_id"]
        account_id = msg["account_id"]
        amount = int(msg["amount_cents"])

        if self._seen_before(message_id):
            return {"result": "duplicate", "message_id": message_id}

        token = self._acquire_lock(account_id)
        if token is None:
            # Another worker holds this account.  Not an error -- requeue.
            raise LockNotAcquired(account_id)

        try:
            balance = self._apply_to_postgres(message_id, account_id, amount)
            self._cache_balance(account_id, balance)
            out = {
                "invoice_id": message_id,
                "account_id": account_id,
                "amount_cents": amount,
                "balance_cents": balance,
                "processed_at": time.time(),
            }
            self._emit(out)
            return {"result": "applied", **out}
        finally:
            self._release_lock(account_id, token)


def make_message(account_id: str | None = None) -> dict:
    return {
        "message_id": uuid.uuid4().hex,
        "account_id": account_id or random.choice(ACCOUNTS),
        "amount_cents": random.randint(50, 25_000),
        "created_at": time.time(),
    }
