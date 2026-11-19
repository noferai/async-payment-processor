import asyncio
import json

import pytest
import sqlalchemy as sa
from aiormq.exceptions import PublishError
from faststream.rabbit import RabbitBroker
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent
from app.messaging.publisher import ReliablePublisher
from app.messaging.topology import new_queue
from app.outbox.relay import OutboxRelay, claim_batch
from tests.helpers import FailingPublisher, get_message, make_outbox_event, queue_size, wait_until

SessionFactory = async_sessionmaker[AsyncSession]


def make_relay(session_factory: SessionFactory, publisher: ReliablePublisher, batch_size: int = 50) -> OutboxRelay:
    return OutboxRelay(session_factory, publisher, batch_size=batch_size, poll_interval=0.01, max_backoff=0.05)


async def fetch_events(session_factory: SessionFactory) -> list[OutboxEvent]:
    """Читает все строки outbox свежей сессией."""
    async with session_factory() as session:
        return list(await session.scalars(sa.select(OutboxEvent).order_by(OutboxEvent.created_at)))


async def unpublished_count(session_factory: SessionFactory) -> int:
    return sum(e.published_at is None for e in await fetch_events(session_factory))


async def test_publishes_and_marks_event(
    session_factory: SessionFactory, broker: RabbitBroker, publisher: ReliablePublisher
) -> None:
    event = await make_outbox_event(session_factory)
    assert await make_relay(session_factory, publisher).run_once() == 1
    msg = await get_message(broker, new_queue())
    assert msg.message_id == str(event.id) and json.loads(msg.body) == event.payload
    [row] = await fetch_events(session_factory)
    assert row.id == event.id and row.published_at is not None
    assert await make_relay(session_factory, publisher).run_once() == 0


async def test_event_stays_unpublished_while_publisher_fails(
    session_factory: SessionFactory,
) -> None:
    await make_outbox_event(session_factory)
    publisher = FailingPublisher(fail_times=2)
    relay = make_relay(session_factory, publisher)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await relay.run_once()
        assert await unpublished_count(session_factory) == 1
    assert await relay.run_once() == 1 and len(publisher.published) == 1
    assert await unpublished_count(session_factory) == 0


async def test_unroutable_event_stays_unpublished(
    session_factory: SessionFactory, broker: RabbitBroker, publisher: ReliablePublisher
) -> None:
    await make_outbox_event(session_factory, routing_key="no.such.key")
    await make_outbox_event(session_factory)
    with pytest.raises(PublishError):
        await make_relay(session_factory, publisher).run_once()
    assert await unpublished_count(session_factory) == 2
    assert await queue_size(broker, new_queue()) == 0


async def test_partial_batch_keeps_published_rows(session_factory: SessionFactory) -> None:
    for _ in range(3):
        await make_outbox_event(session_factory)
    publisher = FailingPublisher(fail_on_calls=[2])  # второй вызов падает
    with pytest.raises(RuntimeError):
        await make_relay(session_factory, publisher).run_once()
    events = await fetch_events(session_factory)
    assert [e.published_at is not None for e in events] == [True, False, False]
    assert await make_relay(session_factory, publisher).run_once() == 2


async def test_commit_failure_republishes_with_same_message_id(
    session_factory: SessionFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    event = await make_outbox_event(session_factory)
    publisher = FailingPublisher(fail_times=0)

    original_commit = AsyncSession.commit
    failed = False

    async def commit_failing_once(self: AsyncSession) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OperationalError("COMMIT", None, Exception("connection lost"))
        await original_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", commit_failing_once)

    with pytest.raises(OperationalError):
        await make_relay(session_factory, publisher).run_once()
    assert await unpublished_count(session_factory) == 1
    assert await make_relay(session_factory, publisher).run_once() == 1
    assert [m["message_id"] for m in publisher.published] == [str(event.id)] * 2


async def test_claim_batch_skips_rows_locked_by_another_session(
    session_factory: SessionFactory,
) -> None:
    for _ in range(4):
        await make_outbox_event(session_factory)
    async with session_factory() as a, session_factory() as b:
        first = await claim_batch(a, limit=2)  # транзакция a остаётся открытой
        second = await claim_batch(b, limit=10)
        assert len(first) == 2 and len(second) == 2
        assert {e.id for e in first}.isdisjoint({e.id for e in second})


async def test_backlog_larger_than_batch_is_drained_in_order(
    session_factory: SessionFactory,
) -> None:
    events = [await make_outbox_event(session_factory) for _ in range(5)]
    publisher = FailingPublisher(fail_times=0)
    relay = make_relay(session_factory, publisher, batch_size=2)
    assert [await relay.run_once() for _ in range(4)] == [2, 2, 1, 0]
    assert [m["message_id"] for m in publisher.published] == [str(e.id) for e in events]


async def test_run_forever_recovers_after_failures_and_stops(
    session_factory: SessionFactory,
) -> None:
    await make_outbox_event(session_factory)
    publisher = FailingPublisher(fail_times=2)
    stop = asyncio.Event()
    task = asyncio.create_task(make_relay(session_factory, publisher).run_forever(stop))
    try:
        await wait_until(lambda: len(publisher.published) == 1)
    finally:
        stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert publisher.calls == 3
    assert await unpublished_count(session_factory) == 0
