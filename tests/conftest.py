import contextlib
import os
from collections.abc import AsyncIterator, Iterator, Sequence
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from app.api.app import create_app
from app.config import Settings, get_settings
from app.consumer.app import register_subscriber
from app.consumer.handler import PaymentMessageHandler, PaymentProcessorLike
from app.db.session import create_engine, create_session_factory
from app.messaging.broker import create_broker
from app.messaging.publisher import RabbitReliablePublisher, ReliablePublisher
from app.messaging.topology import declare_topology, dlq_queue, new_queue, retry_queue
from tests.helpers import StartConsumer

ALEMBIC_INI = Path(__file__).resolve().parent.parent / "alembic.ini"


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:17", driver="asyncpg") as container:
        yield container.get_connection_url()


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    # Не RabbitMqContainer: его проба готовности через pika сыплет DeprecationWarning
    container = (
        DockerContainer("rabbitmq:4.4-management")
        .with_env("RABBITMQ_DEFAULT_USER", "payments")
        .with_env("RABBITMQ_DEFAULT_PASS", "payments")
        .with_exposed_ports(5672)
        .waiting_for(LogMessageWaitStrategy("Server startup complete"))
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://payments:payments@{host}:{port}/"


@pytest.fixture(scope="session")
def settings(postgres_url: str, rabbitmq_url: str) -> Iterator[Settings]:
    # Синхронная фикстура: async env.py сам вызывает asyncio.run.
    # URL передаётся через окружение, т.к. env.py берёт его из get_settings().
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = postgres_url
    get_settings.cache_clear()
    try:
        command.upgrade(Config(str(ALEMBIC_INI)), "head")
        yield Settings(
            database_url=postgres_url,
            rabbitmq_url=rabbitmq_url,
            api_key="test-key",
            gateway_min_delay_seconds=0,
            gateway_max_delay_seconds=0,
            retry_base_delay_ms=50,
            webhook_timeout_seconds=1.0,
            outbox_poll_interval_seconds=0.05,
        )
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        get_settings.cache_clear()


@pytest.fixture(scope="session")
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture(autouse=True)
async def _clean_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(sa.text("TRUNCATE payments, outbox"))


async def _make_client(settings: Settings, headers: dict[str, str]) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    # ASGITransport сам lifespan не запускает.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=headers) as http_client:
            yield http_client


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    async for http_client in _make_client(settings, {"X-API-Key": settings.api_key}):
        yield http_client


@pytest.fixture
async def anon_client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    async for http_client in _make_client(settings, {}):
        yield http_client


@pytest.fixture
async def broker(settings: Settings) -> AsyncIterator[RabbitBroker]:
    """Свежий брокер с объявленной топологией и пустыми очередями."""
    async with create_broker(settings) as rabbit:
        await declare_topology(rabbit, settings.retry_delays_ms)
        queues = [new_queue(), dlq_queue(), *map(retry_queue, settings.retry_delays_ms)]
        for queue in queues:
            await (await rabbit.declare_queue(queue)).purge()
        yield rabbit


@pytest.fixture
def publisher(broker: RabbitBroker, settings: Settings) -> RabbitReliablePublisher:
    return RabbitReliablePublisher(broker, settings.publish_timeout_seconds)


@pytest.fixture
async def start_consumer(broker: RabbitBroker, settings: Settings) -> AsyncIterator[StartConsumer]:
    """Запускает consumer на отдельном брокере; останавливает после теста.

    Зависимость от broker: топология объявлена и очереди очищены до старта consumer.
    """
    async with contextlib.AsyncExitStack() as stack:

        async def start(
            processor: PaymentProcessorLike,
            publisher: ReliablePublisher | None = None,
            *,
            delays_ms: Sequence[int] | None = None,
        ) -> None:
            consumer_broker = create_broker(settings)
            handler = PaymentMessageHandler(
                processor,
                publisher or RabbitReliablePublisher(consumer_broker, settings.publish_timeout_seconds),
                max_attempts=settings.retry_max_attempts,
                delays_ms=settings.retry_delays_ms if delays_ms is None else list(delays_ms),
            )
            # Подписчик регистрируется до старта брокера
            register_subscriber(consumer_broker, handler)
            stack.push_async_callback(consumer_broker.stop)
            await consumer_broker.start()

        yield start
