import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import PaymentStatus
from app.integrations.webhook import WebhookDeliveryError
from app.services.processing import PaymentNotFound, PaymentProcessor
from tests.helpers import FakeGateway, FakeWebhook, fetch_payment, make_payment

SessionFactory = async_sessionmaker[AsyncSession]


async def test_success_sets_status_and_delivers_webhook(session_factory: SessionFactory) -> None:
    payment = await make_payment(
        session_factory,
        amount=Decimal("100.5"),
        description="заказ 42",
        metadata_={"order_id": 42},
    )
    gateway, webhook = FakeGateway(result=True), FakeWebhook()
    await PaymentProcessor(session_factory, gateway, webhook).process(payment.id)

    p = await fetch_payment(session_factory, payment.id)
    assert p.status == PaymentStatus.SUCCEEDED
    assert p.processed_at is not None and p.webhook_delivered_at is not None
    url, body = webhook.calls[0]
    assert url == p.webhook_url
    assert body == {
        "payment_id": str(p.id),
        "status": "succeeded",
        "amount": "100.50",
        "currency": p.currency,
        "description": p.description,
        "metadata": p.metadata_,
        "processed_at": p.processed_at.isoformat(),
    }
    # валюта — простая строка, а не член перечисления
    assert type(body["currency"]) is str and body["currency"] == "RUB"


async def test_gateway_decline_is_failed_status_with_webhook(
    session_factory: SessionFactory,
) -> None:
    payment = await make_payment(session_factory)
    gateway, webhook = FakeGateway(result=False), FakeWebhook()
    await PaymentProcessor(session_factory, gateway, webhook).process(payment.id)

    p = await fetch_payment(session_factory, payment.id)
    assert p.status == PaymentStatus.FAILED and len(webhook.calls) == 1
    assert webhook.calls[0][1]["status"] == "failed"
    assert p.processed_at is not None and p.webhook_delivered_at is not None


async def test_webhook_failure_keeps_final_status_and_skips_gateway_on_retry(
    session_factory: SessionFactory,
) -> None:
    payment = await make_payment(session_factory)
    gateway, webhook = FakeGateway(), FakeWebhook(fail_times=1)
    processor = PaymentProcessor(session_factory, gateway, webhook)
    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    p = await fetch_payment(session_factory, payment.id)
    assert p.status == PaymentStatus.SUCCEEDED
    assert p.processed_at is not None and p.webhook_delivered_at is None

    await processor.process(payment.id)
    assert gateway.calls == 1 and len(webhook.calls) == 2
    p = await fetch_payment(session_factory, payment.id)
    assert p.webhook_delivered_at is not None


async def test_processing_twice_changes_nothing(session_factory: SessionFactory) -> None:
    payment = await make_payment(session_factory)
    gateway, webhook = FakeGateway(), FakeWebhook()
    processor = PaymentProcessor(session_factory, gateway, webhook)

    await processor.process(payment.id)
    p = await fetch_payment(session_factory, payment.id)
    first = (p.status, p.processed_at, p.webhook_delivered_at)

    await processor.process(payment.id)
    p = await fetch_payment(session_factory, payment.id)
    assert (p.status, p.processed_at, p.webhook_delivered_at) == first
    assert gateway.calls == 1 and len(webhook.calls) == 1


async def test_concurrent_processing_calls_gateway_and_webhook_once(
    session_factory: SessionFactory,
) -> None:
    payment = await make_payment(session_factory)
    gateway, webhook = FakeGateway(delay=0.2), FakeWebhook()
    processor = PaymentProcessor(session_factory, gateway, webhook)
    await asyncio.gather(*[processor.process(payment.id) for _ in range(5)])
    assert gateway.calls == 1 and len(webhook.calls) == 1


async def test_unknown_payment_raises(session_factory: SessionFactory) -> None:
    processor = PaymentProcessor(session_factory, FakeGateway(), FakeWebhook())
    with pytest.raises(PaymentNotFound):
        await processor.process(uuid.uuid7())
