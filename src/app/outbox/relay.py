"""Перенос событий из таблицы outbox в RabbitMQ с доставкой at-least-once."""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent
from app.messaging.publisher import ReliablePublisher
from app.messaging.topology import payments_exchange

logger = logging.getLogger(__name__)


async def claim_batch(session: AsyncSession, limit: int) -> list[OutboxEvent]:
    """Блокирует до limit неопубликованных событий, пропуская занятые другими."""
    stmt = (
        sa.select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at, OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(await session.scalars(stmt))


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: ReliablePublisher,
        *,
        batch_size: int,
        poll_interval: float,
        max_backoff: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_backoff = max_backoff

    async def run_once(self) -> int:
        """Публикует один пакет; возвращает число опубликованных событий."""
        async with self._session_factory() as session:
            events = await claim_batch(session, self._batch_size)
            for published, event in enumerate(events):
                try:
                    await self._publisher.publish(
                        event.payload,
                        exchange=payments_exchange(),
                        routing_key=event.routing_key,
                        message_id=str(event.id),
                    )
                except Exception:
                    # Уже подтверждённые брокером строки пакета сохраняются,
                    # остальные остаются неопубликованными до следующей попытки
                    logger.warning("published %d of %d outbox events", published, len(events))
                    await session.commit()
                    raise
                event.published_at = datetime.now(UTC)
            # Сбой коммита оставляет события неопубликованными: они уйдут повторно
            # с тем же message_id (at-least-once)
            await session.commit()
            return len(events)

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Публикует пакеты до выставления stop; при сбоях ждёт с нарастающей паузой."""
        backoff = self._poll_interval
        while not stop.is_set():
            try:
                published = await self.run_once()
            except Exception:
                logger.exception("outbox publish failed, retrying in %.2f s", backoff)
                await self._wait_for_stop(stop, backoff)
                backoff = min(backoff * 2, self._max_backoff)
                continue
            backoff = self._poll_interval
            if published:
                logger.info("published %d outbox events", published)
            else:
                await self._wait_for_stop(stop, self._poll_interval)

    @staticmethod
    async def _wait_for_stop(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)
