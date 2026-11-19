import asyncio
import itertools
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

import aio_pika
import httpx
import pytest
from faststream.rabbit import RabbitBroker
from pytest_httpserver import HTTPServer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from werkzeug.wrappers import Request, Response

from app.config import Settings
from app.db.models import Payment, PaymentStatus
from app.integrations.webhook import HttpxWebhookSender
from app.messaging.publisher import ReliablePublisher
from app.messaging.topology import (
    ATTEMPT_HEADER,
    NEW_QUEUE,
    declare_topology,
    dlq_queue,
    new_queue,
    payments_exchange,
    retry_queue,
)
from app.services.processing import PaymentProcessor
from tests.helpers import (
    FailingProcessor,
    FailingPublisher,
    FakeGateway,
    StartConsumer,
    fetch_payment,
    get_message,
    make_payment,
    queue_size,
    wait_until,
)

SessionFactory = async_sessionmaker[AsyncSession]
Publish = Callable[..., Awaitable[None]]
Delivered = Callable[[uuid.UUID], Awaitable[bool]]


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture
def processor(
    session_factory: SessionFactory, gateway: FakeGateway, http_client: httpx.AsyncClient
) -> PaymentProcessor:
    return PaymentProcessor(session_factory, gateway, HttpxWebhookSender(http_client, 1.0))


@pytest.fixture
async def payment(session_factory: SessionFactory, httpserver: HTTPServer) -> Payment:
    return await make_payment(session_factory, webhook_url=httpserver.url_for("/hook"))


@pytest.fixture
def publish(publisher: ReliablePublisher) -> Publish:
    async def _publish(payment_id: uuid.UUID, message_id: str | None = None) -> None:
        await publisher.publish(
            {"payment_id": str(payment_id)},
            exchange=payments_exchange(),
            routing_key=NEW_QUEUE,
            message_id=message_id or str(uuid.uuid4()),
        )

    return _publish


@pytest.fixture
def delivered(session_factory: SessionFactory) -> Delivered:
    async def _delivered(payment_id: uuid.UUID) -> bool:
        return (await fetch_payment(session_factory, payment_id)).webhook_delivered_at is not None

    return _delivered


@pytest.fixture
def settle_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str | None, str]]:
    """Записывает (message_id, метод) каждого ack/reject/nack сообщений aio-pika."""
    calls: list[tuple[str | None, str]] = []
    for name in ("ack", "reject", "nack"):
        original = getattr(aio_pika.IncomingMessage, name)

        async def spy(
            self: aio_pika.IncomingMessage,
            *args: object,
            _original: Callable[..., Awaitable[None]] = original,
            _name: str = name,
            **kwargs: object,
        ) -> None:
            calls.append((self.message_id, _name))
            await _original(self, *args, **kwargs)

        monkeypatch.setattr(aio_pika.IncomingMessage, name, spy)
    return calls


async def test_success_path_settles_message_exactly_once(
    httpserver: HTTPServer,
    broker: RabbitBroker,
    session_factory: SessionFactory,
    start_consumer: StartConsumer,
    processor: PaymentProcessor,
    payment: Payment,
    publish: Publish,
    delivered: Delivered,
    settle_calls: list[tuple[str | None, str]],
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=200)
    await start_consumer(processor)
    await publish(payment.id, message_id="success")
    await wait_until(lambda: delivered(payment.id))
    p = await fetch_payment(session_factory, payment.id)
    assert p.status == PaymentStatus.SUCCEEDED and p.processed_at is not None
    assert len(httpserver.log) == 1
    # явный ack при политике REJECT_ON_ERROR не приводит ко второму подтверждению
    await asyncio.sleep(0.3)
    assert [method for message_id, method in settle_calls if message_id == "success"] == ["ack"]
    assert await queue_size(broker, new_queue()) == 0
    assert await queue_size(broker, dlq_queue()) == 0


async def test_failing_webhook_gets_three_attempts_then_dlq(
    httpserver: HTTPServer,
    broker: RabbitBroker,
    settings: Settings,
    session_factory: SessionFactory,
    start_consumer: StartConsumer,
    gateway: FakeGateway,
    processor: PaymentProcessor,
    payment: Payment,
    publish: Publish,
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=500)
    await start_consumer(processor)
    started = time.monotonic()
    await publish(payment.id, message_id="m1")
    dead = await get_message(broker, dlq_queue(), timeout=10)
    assert time.monotonic() - started >= sum(settings.retry_delays_ms) / 1000
    assert len(httpserver.log) == 3
    assert dead.headers[ATTEMPT_HEADER] == 3 and dead.message_id == "m1"
    assert json.loads(dead.body) == {"payment_id": str(payment.id)}
    assert gateway.calls == 1
    p = await fetch_payment(session_factory, payment.id)
    assert p.status == PaymentStatus.SUCCEEDED and p.webhook_delivered_at is None


async def test_webhook_recovers_on_second_attempt(
    httpserver: HTTPServer,
    broker: RabbitBroker,
    start_consumer: StartConsumer,
    gateway: FakeGateway,
    processor: PaymentProcessor,
    payment: Payment,
    publish: Publish,
    delivered: Delivered,
) -> None:
    requests = itertools.count(1)

    def first_500_then_200(request: Request) -> Response:
        return Response(status=500 if next(requests) == 1 else 200)

    httpserver.expect_request("/hook").respond_with_handler(first_500_then_200)
    await start_consumer(processor)
    await publish(payment.id)
    await wait_until(lambda: delivered(payment.id))
    assert len(httpserver.log) == 2 and gateway.calls == 1
    assert await queue_size(broker, dlq_queue()) == 0


async def test_db_failure_retries_and_keeps_payment_pending(
    broker: RabbitBroker,
    session_factory: SessionFactory,
    start_consumer: StartConsumer,
    payment: Payment,
    publish: Publish,
) -> None:
    failing = FailingProcessor()
    await start_consumer(failing)
    await publish(payment.id)
    await get_message(broker, dlq_queue(), timeout=10)
    assert failing.calls == 3
    assert (await fetch_payment(session_factory, payment.id)).status == PaymentStatus.PENDING


async def test_retry_publish_failure_sends_original_to_dlq(
    httpserver: HTTPServer,
    broker: RabbitBroker,
    start_consumer: StartConsumer,
    processor: PaymentProcessor,
    payment: Payment,
    publish: Publish,
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=500)
    await start_consumer(processor, publisher=FailingPublisher(fail_times=1))
    await publish(payment.id)
    dead = await get_message(broker, dlq_queue(), timeout=10)
    assert ATTEMPT_HEADER not in dead.headers and len(httpserver.log) == 1


async def test_duplicate_messages_processed_concurrently_act_once(
    httpserver: HTTPServer,
    broker: RabbitBroker,
    session_factory: SessionFactory,
    http_client: httpx.AsyncClient,
    start_consumer: StartConsumer,
    payment: Payment,
    publish: Publish,
    delivered: Delivered,
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=200)
    gateway = FakeGateway(delay=0.3)
    processor = PaymentProcessor(session_factory, gateway, HttpxWebhookSender(http_client, 1.0))
    await start_consumer(processor)
    await asyncio.gather(publish(payment.id), publish(payment.id), publish(payment.id))
    await wait_until(lambda: delivered(payment.id))
    await asyncio.sleep(1.0)
    assert gateway.calls == 1 and len(httpserver.log) == 1
    assert await queue_size(broker, new_queue()) == 0
    assert await queue_size(broker, dlq_queue()) == 0


# Идентификаторы заданы на уровне модуля: параметры вычисляются при сборе тестов,
# до создания платежей.
POISON_ID, UNKNOWN_ID, NEXT_ID = uuid.uuid7(), uuid.uuid7(), uuid.uuid7()
VALID_BODY = json.dumps({"payment_id": str(POISON_ID)}).encode()


@pytest.mark.parametrize(
    "body",
    [b"not json", json.dumps({"payment_id": str(uuid.uuid7())}).encode()],
    ids=["not-json", "unknown-payment"],
)
async def test_poison_message_reaches_dlq_without_retry_delay(
    broker: RabbitBroker,
    start_consumer: StartConsumer,
    processor: PaymentProcessor,
    body: bytes,
) -> None:
    await declare_topology(broker, [30_000])
    await start_consumer(processor, delays_ms=[30_000])
    exchange = await broker.declare_exchange(payments_exchange())
    await exchange.publish(
        aio_pika.Message(body, content_type="application/json"),
        routing_key=NEW_QUEUE,
    )
    assert (await get_message(broker, dlq_queue(), timeout=2)).body == body
    assert await queue_size(broker, retry_queue(30_000)) == 0


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        (b"not json", {}),
        (b"[1, 2]", {}),
        (b'{"payment_id": "not-a-uuid"}', {}),
        (b"{}", {}),
        (json.dumps({"payment_id": str(UNKNOWN_ID)}).encode(), {}),
        (VALID_BODY, {"x-attempt": "abc"}),
        (VALID_BODY, {"x-attempt": 0}),
    ],
    # id по умолчанию содержали бы случайные UUID и менялись бы от запуска к запуску
    ids=[
        "not-json",
        "not-object",
        "bad-uuid",
        "no-payment-id",
        "unknown-payment",
        "attempt-not-int",
        "attempt-zero",
    ],
)
async def test_poison_message_goes_to_dlq_and_consumer_keeps_working(
    httpserver: HTTPServer,
    broker: RabbitBroker,
    session_factory: SessionFactory,
    start_consumer: StartConsumer,
    processor: PaymentProcessor,
    publish: Publish,
    delivered: Delivered,
    body: bytes,
    headers: dict[str, object],
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=200)
    await make_payment(session_factory, id=POISON_ID, webhook_url=httpserver.url_for("/hook"))
    await make_payment(session_factory, id=NEXT_ID, webhook_url=httpserver.url_for("/hook"))
    await start_consumer(processor)
    # сырое тело в обход публикатора, иначе невалидное сообщение не отправить
    exchange = await broker.declare_exchange(payments_exchange())
    await exchange.publish(
        aio_pika.Message(body, content_type="application/json", headers=headers),
        routing_key=NEW_QUEUE,
    )
    await publish(NEXT_ID)
    assert (await get_message(broker, dlq_queue())).body == body
    await wait_until(lambda: delivered(NEXT_ID))  # consumer продолжает работать
    assert (await fetch_payment(session_factory, POISON_ID)).status == PaymentStatus.PENDING
    assert len(httpserver.log) == 1
