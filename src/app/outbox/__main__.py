"""Точка входа outbox-релея: `python -m app.outbox`."""

import asyncio
import logging
import signal

from app.config import get_settings
from app.db.session import create_engine, create_session_factory
from app.messaging.broker import create_broker
from app.messaging.publisher import RabbitReliablePublisher
from app.messaging.topology import declare_topology
from app.outbox.relay import OutboxRelay

logger = logging.getLogger("app.outbox")


async def main() -> None:
    settings = get_settings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    # SIGTERM — штатная остановка контейнера (docker compose stop)
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    engine = create_engine(settings)
    try:
        async with create_broker(settings) as broker:
            await declare_topology(broker, settings.retry_delays_ms)
            relay = OutboxRelay(
                create_session_factory(engine),
                RabbitReliablePublisher(broker, settings.publish_timeout_seconds),
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval_seconds,
                max_backoff=settings.outbox_max_backoff_seconds,
            )
            logger.info("outbox relay started")
            await relay.run_forever(stop)
    finally:
        await engine.dispose()
    logger.info("outbox relay stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
