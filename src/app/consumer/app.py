"""Запуск: `faststream run app.consumer.app:create_app --factory`."""

import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.config import get_settings
from app.consumer.handler import PaymentMessageHandler
from app.db.session import create_engine, create_session_factory
from app.integrations.gateway import GatewayEmulator
from app.integrations.webhook import HttpxWebhookSender
from app.messaging.broker import create_broker
from app.messaging.publisher import RabbitReliablePublisher
from app.messaging.topology import declare_topology, new_queue, payments_exchange
from app.services.processing import PaymentProcessor

logger = logging.getLogger("app.consumer")


def register_subscriber(broker: RabbitBroker, handler: PaymentMessageHandler) -> None:
    # Те же объекты, что в модуле топологии: другие аргументы очереди дали бы ошибку 406.
    # REJECT_ON_ERROR: непредвиденное исключение отправляет сообщение в DLQ,
    # а не оставляет его неподтверждённым.
    # Именованная функция, а не экземпляр: FastStream берёт имя подписчика для логов из неё.
    @broker.subscriber(new_queue(), payments_exchange(), ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def handle_payment_created(msg: RabbitMessage) -> None:
        await handler(msg)


def create_app() -> FastStream:
    # Логгеры FastStream не распространяются в root, поэтому дублей нет;
    # root-обработчик выводит в stderr сообщения приложения (видны в docker logs)
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()

    engine = create_engine(settings)
    broker = create_broker(settings)
    http_client = httpx.AsyncClient()

    processor = PaymentProcessor(
        create_session_factory(engine),
        GatewayEmulator(
            settings.gateway_min_delay_seconds,
            settings.gateway_max_delay_seconds,
            settings.gateway_success_rate,
        ),
        HttpxWebhookSender(http_client, settings.webhook_timeout_seconds),
    )
    handler = PaymentMessageHandler(
        processor,
        RabbitReliablePublisher(broker, settings.publish_timeout_seconds),
        max_attempts=settings.retry_max_attempts,
        delays_ms=settings.retry_delays_ms,
    )
    register_subscriber(broker, handler)

    async def declare() -> None:
        # До старта подписчика: сообщение, упавшее в первые миллисекунды,
        # должно найти и очереди задержки, и payments.dlx
        await broker.connect()
        await declare_topology(broker, settings.retry_delays_ms)
        logger.info("consumer topology declared")

    async def close() -> None:
        # После остановки брокера: обработчики уже не используют клиент и движок
        await http_client.aclose()
        await engine.dispose()
        logger.info("consumer resources closed")

    return FastStream(broker, on_startup=[declare], after_shutdown=[close])
