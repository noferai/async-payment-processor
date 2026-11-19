"""Общие хелперы тестов."""

import asyncio
import inspect
import os
import time
import uuid
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from decimal import Decimal
from typing import Any, Protocol

import aio_pika
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer.handler import PaymentProcessorLike
from app.db.models import Currency, OutboxEvent, Payment
from app.integrations.webhook import WebhookDeliveryError
from app.messaging.publisher import ReliablePublisher
from app.messaging.topology import NEW_QUEUE


async def make_payment(session_factory: async_sessionmaker[AsyncSession], **overrides: Any) -> Payment:
    """Вставляет платёж с валидными значениями по умолчанию."""
    values: dict[str, Any] = {
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "idempotency_key": str(uuid.uuid4()),
        "request_hash": "0" * 64,
        "webhook_url": "http://example.test/webhook",
    }
    values.update(overrides)
    async with session_factory() as session:
        payment = Payment(**values)
        session.add(payment)
        await session.commit()
        await session.refresh(payment)
    return payment


async def fetch_payment(session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    """Читает платёж свежей сессией."""
    async with session_factory() as session:
        return await session.get_one(Payment, payment_id)


async def wait_until(
    predicate: Callable[[], bool | Awaitable[bool]],
    timeout: float = 10.0,
    interval: float = 0.05,
) -> None:
    """Ждёт, пока predicate (обычный или асинхронный) не вернёт True."""
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"condition not met within {timeout} s")
        await asyncio.sleep(interval)


async def get_message(
    broker: RabbitBroker, queue: RabbitQueue, timeout: float = 5.0
) -> aio_pika.abc.AbstractIncomingMessage:
    """Ждёт сообщение в очереди и подтверждает его."""
    robust_queue = await broker.declare_queue(queue)
    received: list[aio_pika.abc.AbstractIncomingMessage] = []

    async def fetched() -> bool:
        msg = await robust_queue.get(no_ack=False, fail=False)
        if msg is not None:
            received.append(msg)
        return bool(received)

    await wait_until(fetched, timeout=timeout)
    await received[0].ack()
    return received[0]


async def queue_size(broker: RabbitBroker, queue: RabbitQueue) -> int:
    """Число готовых к доставке сообщений в очереди."""
    robust_queue = await broker.declare_queue(queue)
    declared = await robust_queue.declare()
    return declared.message_count or 0


async def make_outbox_event(session_factory: async_sessionmaker[AsyncSession], **overrides: Any) -> OutboxEvent:
    """Вставляет неопубликованное событие outbox с валидными значениями по умолчанию."""
    aggregate_id = uuid.uuid4()
    values: dict[str, Any] = {
        "aggregate_id": aggregate_id,
        "event_type": "payment.created",
        "routing_key": NEW_QUEUE,
        "payload": {"payment_id": str(aggregate_id)},
    }
    values.update(overrides)
    async with session_factory() as session:
        event = OutboxEvent(**values)
        session.add(event)
        await session.commit()
        await session.refresh(event)
    return event


class FailingPublisher:
    """Заглушка ReliablePublisher: падает на заданных вызовах, остальные запоминает."""

    def __init__(self, fail_times: int = 0, fail_on_calls: Collection[int] = ()) -> None:
        self._fail_times = fail_times
        self._fail_on_calls = set(fail_on_calls)
        self.calls = 0
        self.published: list[dict[str, Any]] = []

    async def publish(
        self,
        payload: dict[str, Any],
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        self.calls += 1
        if self.calls <= self._fail_times or self.calls in self._fail_on_calls:
            raise RuntimeError(f"publish failure on call {self.calls}")
        self.published.append(
            {
                "payload": payload,
                "routing_key": routing_key,
                "message_id": message_id,
                "headers": headers,
            }
        )


class FakeGateway:
    """Заглушка шлюза: фиксированный результат, необязательная пауза, счётчик вызовов."""

    def __init__(self, result: bool = True, delay: float = 0.0) -> None:
        self._result = result
        self._delay = delay
        self.calls = 0

    async def charge(self, payment: Payment) -> bool:
        self.calls += 1
        await asyncio.sleep(self._delay)
        return self._result


class FailingProcessor:
    """Заглушка обработчика платежа: всегда падает, как при недоступной БД."""

    def __init__(self) -> None:
        self.calls = 0

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls += 1
        raise RuntimeError("db down")


class StartConsumer(Protocol):
    """Тип фикстуры start_consumer."""

    def __call__(
        self,
        processor: PaymentProcessorLike,
        publisher: ReliablePublisher | None = None,
        *,
        delays_ms: Sequence[int] | None = None,
    ) -> Awaitable[None]: ...


class FakeWebhook:
    """Заглушка отправки webhook: первые fail_times вызовов падают, все вызовы запоминаются."""

    def __init__(self, fail_times: int = 0) -> None:
        self._fail_times = fail_times
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        self.calls.append((url, payload))
        if len(self.calls) <= self._fail_times:
            raise WebhookDeliveryError(f"webhook failure on call {len(self.calls)}")


def service_env(settings: Settings) -> dict[str, str]:
    """Окружение для запуска сервиса отдельным процессом с тестовыми контейнерами."""
    return {
        **os.environ,
        "DATABASE_URL": settings.database_url,
        "RABBITMQ_URL": settings.rabbitmq_url,
        "GATEWAY_MIN_DELAY_SECONDS": "0",
        "GATEWAY_MAX_DELAY_SECONDS": "0",
        "RETRY_BASE_DELAY_MS": str(settings.retry_base_delay_ms),
        "OUTBOX_POLL_INTERVAL_SECONDS": str(settings.outbox_poll_interval_seconds),
    }
