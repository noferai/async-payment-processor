import json
import uuid
from typing import Any, cast

import pytest
from faststream.rabbit import RabbitMessage

from app.consumer.handler import PaymentMessageHandler
from app.messaging.retry import Outcome
from app.messaging.topology import ATTEMPT_HEADER
from tests.helpers import FailingProcessor, FailingPublisher


class AckFailingMessage:
    """Заглушка RabbitMessage: ack падает, как при обрыве канала; вызовы запоминаются."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.body = json.dumps(payload).encode()
        self.message_id = "m1"
        self.acks = 0
        self.rejects = 0

    async def ack(self) -> None:
        self.acks += 1
        raise ConnectionError("channel closed")

    async def reject(self, requeue: bool = False) -> None:
        self.rejects += 1


async def test_ack_failure_after_retry_publish_neither_republishes_nor_rejects() -> None:
    publisher = FailingPublisher()
    handler = PaymentMessageHandler(FailingProcessor(), publisher, max_attempts=3, delays_ms=[2000, 4000])
    payload = {"payment_id": str(uuid.uuid7())}
    msg = AckFailingMessage(payload)

    with pytest.raises(ConnectionError):
        await handler.settle(cast(RabbitMessage, msg), Outcome.RETRY, 1)

    assert msg.acks == 1 and msg.rejects == 0
    assert len(publisher.published) == 1
    assert publisher.published[0]["payload"] == payload
    assert publisher.published[0]["headers"] == {ATTEMPT_HEADER: 2}
