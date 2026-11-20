import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from faststream.rabbit import RabbitMessage

from app.integrations.webhook import WebhookDeliveryError
from app.messaging.publisher import ReliablePublisher
from app.messaging.retry import Outcome, failure_outcome, parse_attempt
from app.messaging.topology import ATTEMPT_HEADER, retry_exchange, retry_routing_key
from app.services.processing import PaymentNotFound

logger = logging.getLogger(__name__)


class PaymentProcessorLike(Protocol):
    async def process(self, payment_id: UUID) -> None: ...


def parse_payment_id(body: bytes) -> UUID:
    """id платежа из тела сообщения; некорректное тело — ValueError."""
    data = json.loads(body)
    if not isinstance(data, dict) or not isinstance(data.get("payment_id"), str):
        raise ValueError("message body is not an object with string payment_id")
    return UUID(data["payment_id"])


class PaymentMessageHandler:
    """Обработка и подтверждение разделены: сбой ack не считается сбоем обработки."""

    def __init__(
        self,
        processor: PaymentProcessorLike,
        publisher: ReliablePublisher,
        *,
        max_attempts: int,
        delays_ms: Sequence[int],
    ) -> None:
        self._processor = processor
        self._publisher = publisher
        self._max_attempts = max_attempts
        self._delays_ms = list(delays_ms)

    async def __call__(self, msg: RabbitMessage) -> None:
        outcome, attempt = await self.process(msg.body, msg.headers)
        await self.settle(msg, outcome, attempt)

    async def process(self, body: bytes, headers: Mapping[str, Any]) -> tuple[Outcome, int]:
        """Исход обработки и номер попытки; исключений не бросает."""
        try:
            attempt = parse_attempt(headers)
        except ValueError:
            logger.exception("invalid attempt header, sending message to DLQ")
            return Outcome.DEAD, 1

        # Повторы не помогут ни нечитаемому телу, ни несуществующему платежу
        try:
            payment_id = parse_payment_id(body)
        except ValueError:
            logger.exception("unparsable message body, sending message to DLQ")
            return Outcome.DEAD, attempt

        try:
            await self._processor.process(payment_id)
        except PaymentNotFound:
            logger.exception("payment %s not found, sending message to DLQ", payment_id)
            return Outcome.DEAD, attempt
        except WebhookDeliveryError as exc:
            # Ожидаемый сбой: текст ошибки полон, трассировка лишь засорит лог
            outcome = failure_outcome(attempt, self._max_attempts)
            logger.warning("payment %s attempt %d failed: %s, outcome %s", payment_id, attempt, exc, outcome)
            return outcome, attempt
        except Exception:
            outcome = failure_outcome(attempt, self._max_attempts)
            logger.exception("payment %s attempt %d failed, outcome %s", payment_id, attempt, outcome)
            return outcome, attempt
        return Outcome.DONE, attempt

    async def settle(self, msg: RabbitMessage, outcome: Outcome, attempt: int) -> None:
        """Подтверждает, отправляет на повтор или отклоняет сообщение в DLQ."""
        if outcome is Outcome.DONE:
            await msg.ack()
            return
        if outcome is Outcome.DEAD:
            # RabbitMQ сам перекладывает отклонённое сообщение в payments.dlq
            await msg.reject(requeue=False)
            return

        try:
            await self._publisher.publish(
                json.loads(msg.body),
                exchange=retry_exchange(),
                routing_key=retry_routing_key(attempt, self._delays_ms),
                message_id=msg.message_id,
                headers={ATTEMPT_HEADER: attempt + 1},
            )
        except Exception:
            # Оригинал уходит в DLQ: сообщение не теряется и не зацикливается
            logger.exception("retry publish failed, sending message to DLQ")
            await msg.reject(requeue=False)
            return
        # Сбой самого ack не перехватывается: копия уже опубликована,
        # второй повтор порождать нельзя
        await msg.ack()
