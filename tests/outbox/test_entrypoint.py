import asyncio
import contextlib
import signal
import sys

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.messaging.topology import new_queue
from tests.helpers import get_message, make_outbox_event, service_env


async def test_relay_process_publishes_and_exits_cleanly_on_sigterm(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
) -> None:
    event = await make_outbox_event(session_factory)
    proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "app.outbox", env=service_env(settings))
    try:
        msg = await get_message(broker, new_queue(), timeout=20)
        assert msg.message_id == str(event.id)
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
