from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Payment, PaymentStatus
from app.integrations.gateway import PaymentGateway
from app.integrations.webhook import WebhookSender
from app.services.payments import format_amount


class PaymentNotFound(Exception):
    pass


def build_webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": str(payment.id),
        "status": str(payment.status),
        "amount": format_amount(payment.amount),
        "currency": str(payment.currency),
        "description": payment.description,
        "metadata": payment.metadata_,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class PaymentProcessor:
    """Блокировка строки платежа сериализует обработку дублей одного сообщения."""

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
        # Две транзакции: итоговый статус сохраняется, даже если webhook не дойдёт,
        # и повтор после сбоя webhook не вызывает шлюз заново
        await self._charge_if_pending(payment_id)
        await self._send_webhook_if_undelivered(payment_id)

    async def _charge_if_pending(self, payment_id: UUID) -> None:
        async with self._locked_payment(payment_id) as payment:
            if payment.status != PaymentStatus.PENDING:
                return
            succeeded = await self._gateway.charge(payment)
            payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
            payment.processed_at = datetime.now(UTC)

    async def _send_webhook_if_undelivered(self, payment_id: UUID) -> None:
        async with self._locked_payment(payment_id) as payment:
            if payment.webhook_delivered_at is not None:
                return
            await self._webhook.send(payment.webhook_url, build_webhook_payload(payment))
            payment.webhook_delivered_at = datetime.now(UTC)

    @asynccontextmanager
    async def _locked_payment(self, payment_id: UUID) -> AsyncIterator[Payment]:
        # Каждая транзакция — в новой сессии: при expire_on_commit=False повторный
        # SELECT … FOR UPDATE в той же сессии вернул бы объект из кэша со старыми полями
        async with self._session_factory() as session, session.begin():
            stmt = sa.select(Payment).where(Payment.id == payment_id).with_for_update()
            payment = (await session.scalars(stmt)).one_or_none()
            if payment is None:
                raise PaymentNotFound(f"платёж {payment_id} не найден")
            yield payment
