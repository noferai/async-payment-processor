"""Создание брокера RabbitMQ."""

from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    # Подтверждения публикации включены, возврат немаршрутизируемого сообщения
    # поднимает исключение (по умолчанию в FastStream он молча игнорируется)
    channel = Channel(
        prefetch_count=settings.consumer_prefetch,
        publisher_confirms=True,
        on_return_raises=True,
    )
    return RabbitBroker(settings.rabbitmq_url, default_channel=channel)
