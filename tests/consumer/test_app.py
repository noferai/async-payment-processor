import asyncio
import contextlib
import signal
import sys

from pytest_httpserver import HTTPServer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.models import PaymentStatus
from app.messaging.publisher import ReliablePublisher
from app.messaging.topology import NEW_QUEUE, payments_exchange
from tests.helpers import fetch_payment, make_payment, service_env, wait_until


async def test_consumer_process_handles_message_and_exits_cleanly_on_sigterm(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    publisher: ReliablePublisher,
    httpserver: HTTPServer,
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=200)
    payment = await make_payment(session_factory, webhook_url=httpserver.url_for("/hook"))

    async def delivered() -> bool:
        return (await fetch_payment(session_factory, payment.id)).webhook_delivered_at is not None

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "faststream",
        "run",
        "app.consumer.app:create_app",
        "--factory",
        env=service_env(settings),
    )
    try:
        await publisher.publish(
            {"payment_id": str(payment.id)},
            exchange=payments_exchange(),
            routing_key=NEW_QUEUE,
            message_id="smoke",
        )
        await wait_until(delivered, timeout=20)
        p = await fetch_payment(session_factory, payment.id)
        assert p.status in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
        assert len(httpserver.log) == 1
    finally:
        # Процесс останавливается в любом случае, даже если проверка выше упала
        with contextlib.suppress(ProcessLookupError):
            proc.send_signal(signal.SIGTERM)
        try:
            returncode = await asyncio.wait_for(proc.wait(), timeout=10)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise
    assert returncode == 0
