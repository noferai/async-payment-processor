import time

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker, RabbitQueue

from app.config import Settings
from app.messaging.broker import create_broker
from app.messaging.publisher import ReliablePublisher
from app.messaging.topology import (
    ATTEMPT_HEADER,
    NEW_QUEUE,
    declare_topology,
    dlq_queue,
    new_queue,
    payments_exchange,
    retry_exchange,
    retry_queue_name,
    retry_routing_key,
)
from tests.helpers import get_message, wait_until


def test_retry_queue_names_carry_the_delay() -> None:
    assert retry_queue_name(2000) == "payments.retry.2000"
    assert retry_routing_key(1, [2000, 4000]) == "payments.retry.2000"
    assert retry_routing_key(2, [2000, 4000]) == "payments.retry.4000"
    with pytest.raises(ValueError):
        retry_routing_key(3, [2000, 4000])


# FastStream кэширует объявления в пределах одного брокера, поэтому повтор идёт с нового подключения
async def test_declare_topology_is_idempotent_across_connections(broker: RabbitBroker, settings: Settings) -> None:
    async with create_broker(settings) as second:
        await declare_topology(second, settings.retry_delays_ms)  # те же аргументы: без ошибки 406


async def test_new_delays_do_not_break_existing_queues(broker: RabbitBroker, settings: Settings) -> None:
    async with create_broker(settings) as second:
        await declare_topology(second, [70, 140])  # другие задержки поверх уже объявленных 50 и 100
        # Пассивное объявление падает с 404, если очереди нет: старые и новые очереди на месте
        for delay_ms in [*settings.retry_delays_ms, 70, 140]:
            await second.declare_queue(RabbitQueue(retry_queue_name(delay_ms), declare=False))


async def test_retry_queue_returns_message_to_main_queue_with_attempt_header(
    broker: RabbitBroker, publisher: ReliablePublisher, settings: Settings
) -> None:
    started = time.monotonic()
    await publisher.publish(
        {"payment_id": "p"},
        exchange=retry_exchange(),
        routing_key=retry_routing_key(1, settings.retry_delays_ms),
        message_id="m1",
        headers={ATTEMPT_HEADER: 2},
    )
    msg = await get_message(broker, new_queue())
    assert time.monotonic() - started >= settings.retry_delays_ms[0] / 1000
    assert msg.headers[ATTEMPT_HEADER] == 2 and msg.message_id == "m1"


async def test_rejected_message_lands_in_dlq(broker: RabbitBroker, publisher: ReliablePublisher) -> None:
    await publisher.publish({"payment_id": "p"}, exchange=payments_exchange(), routing_key=NEW_QUEUE, message_id="m2")
    # Берём сообщение из payments.new без auto-ack и отклоняем без возврата в очередь
    queue = await broker.declare_queue(new_queue())
    received: list[aio_pika.abc.AbstractIncomingMessage] = []

    async def fetched() -> bool:
        msg = await queue.get(no_ack=False, fail=False)
        if msg is not None:
            received.append(msg)
        return bool(received)

    await wait_until(fetched)
    await received[0].reject(requeue=False)
    assert (await get_message(broker, dlq_queue())).message_id == "m2"
