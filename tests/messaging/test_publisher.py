import asyncio
import json
import time
from typing import cast

import aio_pika
import pytest
from aiormq.exceptions import PublishError
from faststream.rabbit import RabbitBroker

from app.messaging.publisher import RabbitReliablePublisher, ReliablePublisher
from app.messaging.topology import NEW_QUEUE, new_queue, payments_exchange
from tests.helpers import get_message


async def test_published_message_is_persistent_and_routed(broker: RabbitBroker, publisher: ReliablePublisher) -> None:
    await publisher.publish(
        {"payment_id": "p"},
        exchange=payments_exchange(),
        routing_key=NEW_QUEUE,
        message_id="m3",
        headers={"k": "v"},
    )
    msg = await get_message(broker, new_queue())
    assert json.loads(msg.body) == {"payment_id": "p"}
    assert msg.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert msg.message_id == "m3" and msg.headers["k"] == "v"


class _HangingBroker:
    async def publish(self, *args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()


@pytest.mark.timeout(1)
async def test_publish_times_out_when_confirm_never_arrives() -> None:
    publisher = RabbitReliablePublisher(cast(RabbitBroker, _HangingBroker()), timeout=0.05)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await publisher.publish(
            {"payment_id": "p"},
            exchange=payments_exchange(),
            routing_key=NEW_QUEUE,
            message_id="m-timeout",
        )
    assert time.monotonic() - started < 0.5


async def test_broker_channel_has_publisher_confirms(broker: RabbitBroker) -> None:
    channel = broker._channel
    assert channel is not None and channel.publisher_confirms is True


async def test_unroutable_message_raises(publisher: ReliablePublisher) -> None:
    # Брокер вернул mandatory-сообщение без маршрута: aio-pika сообщает об этом PublishError
    with pytest.raises(PublishError):
        await publisher.publish(
            {"payment_id": "p"},
            exchange=payments_exchange(),
            routing_key="no.such.key",
            message_id="m4",
        )
