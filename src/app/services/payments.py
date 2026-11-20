import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Currency, OutboxEvent, Payment
from app.messaging.topology import NEW_QUEUE

PAYMENT_CREATED = "payment.created"


class IdempotencyConflict(Exception):
    """Ключ идемпотентности уже использован с другим телом запроса."""


@dataclass(frozen=True, slots=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    webhook_url: str
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def format_amount(amount: Decimal) -> str:
    """Всегда два знака после точки: "100.5" и "100.50" — одна и та же сумма."""
    return format(amount.quantize(Decimal("0.01")), "f")


def compute_request_hash(data: NewPayment) -> str:
    canonical = {
        "amount": format_amount(data.amount),
        "currency": data.currency.value,
        "description": data.description,
        "metadata": data.metadata,
        "webhook_url": data.webhook_url,
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()


async def create_payment(session: AsyncSession, data: NewPayment, idempotency_key: str) -> Payment:
    """Создаёт платёж и событие outbox одной транзакцией и сам её коммитит.

    Повтор с тем же ключом и телом возвращает уже созданный платёж.
    """
    request_hash = compute_request_hash(data)
    stmt = (
        insert(Payment)
        .values(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=data.webhook_url,
        )
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(Payment)
    )
    payment = (await session.scalars(stmt)).one_or_none()
    if payment is not None:
        session.add(
            OutboxEvent(
                aggregate_id=payment.id,
                event_type=PAYMENT_CREATED,
                routing_key=NEW_QUEUE,
                payload={"payment_id": str(payment.id)},
            )
        )
        await session.commit()
        return payment

    # Отдельный SELECT: параллельная транзакция к этому моменту уже закоммитила строку
    existing = (await session.scalars(sa.select(Payment).where(Payment.idempotency_key == idempotency_key))).one()
    if existing.request_hash != request_hash:
        raise IdempotencyConflict("Idempotency-Key уже использован с другим телом запроса")
    return existing


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
