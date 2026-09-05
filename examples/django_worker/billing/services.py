"""Business code.  Deliberately contains nothing health-related at all."""
from __future__ import annotations

from django.core.cache import cache

from billing.models import Account, Invoice


def process_payment(body: dict) -> None:
    account = Account.objects.get(pk=body["account_id"])
    Invoice.objects.get_or_create(
        pk=body["message_id"],
        defaults={"account": account, "amount_cents": body["amount_cents"]},
    )
    account.balance_cents += int(body["amount_cents"])
    account.save(update_fields=["balance_cents"])
    cache.set(f"balance:{account.pk}", account.balance_cents, 300)
