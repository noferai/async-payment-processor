import json
import logging
import uuid

import pytest

from app.consumer.handler import PaymentMessageHandler
from app.integrations.webhook import WebhookDeliveryError
from app.messaging.retry import Outcome
from tests.helpers import FailingPublisher


class RaisingProcessor:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def process(self, payment_id: uuid.UUID) -> None:
        raise self._exc


async def _process(exc: Exception) -> Outcome:
    handler = PaymentMessageHandler(RaisingProcessor(exc), FailingPublisher(), max_attempts=3, delays_ms=[2000, 4000])
    body = json.dumps({"payment_id": str(uuid.uuid7())}).encode()
    outcome, _ = await handler.process(body, {})
    return outcome


async def test_webhook_failure_is_logged_as_one_line(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="app.consumer.handler"):
        outcome = await _process(WebhookDeliveryError("webhook http://x: ответ 500"))
    assert outcome is Outcome.RETRY
    [record] = caplog.records
    assert record.exc_info is None
    assert "ответ 500" in record.getMessage()


async def test_unexpected_failure_keeps_traceback(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="app.consumer.handler"):
        outcome = await _process(RuntimeError("db down"))
    assert outcome is Outcome.RETRY
    [record] = caplog.records
    assert record.exc_info is not None
