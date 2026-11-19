"""Надёжная публикация: единственный путь отправки сообщений в RabbitMQ."""

import asyncio
from collections.abc import Mapping
from typing import Any, Protocol

from faststream.rabbit import RabbitBroker, RabbitExchange


class ReliablePublisher(Protocol):
    async def publish(
        self,
        payload: dict[str, Any],
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """Публикует сообщение; любой сбой поднимает исключение."""
        ...


class RabbitReliablePublisher:
    """Персистентная публикация с подтверждением брокера и таймаутом."""

    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self._broker = broker
        # Таймаут не даёт заблокированному брокеру подвесить вызывающего
        self._timeout = timeout

    async def publish(
        self,
        payload: dict[str, Any],
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        async with asyncio.timeout(self._timeout):
            await self._broker.publish(
                payload,
                exchange=exchange,
                routing_key=routing_key,
                message_id=message_id,
                headers=dict(headers) if headers is not None else None,
                persist=True,
                mandatory=True,
                timeout=self._timeout,
            )
