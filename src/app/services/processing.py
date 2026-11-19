"""Обработка платежа: вызов шлюза, итоговый статус и доставка webhook."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Payment, PaymentStatus
from app.integrations.gateway import PaymentGateway
from app.integrations.webhook import WebhookSender


class PaymentNotFound(Exception):
    """Платежа с таким id нет."""


def build_webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": str(payment.id),
        "status": str(payment.status),
        "amount": format(payment.amount.quantize(Decimal("0.01")), "f"),
        "currency": str(payment.currency),
        "description": payment.description,
        "metadata": payment.metadata_,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class PaymentProcessor:
    """Обрабатывает платёж; блокировка строки сериализует обработку одного платежа."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, payment_id: UUID) -> None:
        # Каждая транзакция — в своей сессии: при expire_on_commit=False повторный
        # SELECT … FOR UPDATE в той же сессии вернул бы объект из кэша со старыми полями.
        # Первая транзакция: итоговый статус сохраняется, даже если webhook не дойдёт.
        async with self._session_factory() as session, session.begin():
            payment = await _lock_payment(session, payment_id)
            if payment.status == PaymentStatus.PENDING:
                succeeded = await self._gateway.charge(payment)
                payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
                payment.processed_at = datetime.now(UTC)

        # Вторая транзакция: webhook, если ещё не доставлен.
        async with self._session_factory() as session, session.begin():
            payment = await _lock_payment(session, payment_id)
            if payment.webhook_delivered_at is None:
                await self._webhook.send(payment.webhook_url, build_webhook_payload(payment))
                payment.webhook_delivered_at = datetime.now(UTC)


async def _lock_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    stmt = sa.select(Payment).where(Payment.id == payment_id).with_for_update()
    payment = (await session.scalars(stmt)).one_or_none()
    if payment is None:
        raise PaymentNotFound(f"платёж {payment_id} не найден")
    return payment
