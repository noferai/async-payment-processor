"""Отправка webhook о результате обработки платежа."""

import asyncio
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx


def log_url(url: str) -> str:
    """Адрес для текста ошибки: без userinfo, query и fragment."""
    try:
        parts = urlsplit(url)
        if not parts.hostname:
            return "unparsed-webhook-url"
        host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
        netloc = host if parts.port is None else f"{host}:{parts.port}"
        return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
    except ValueError:
        return "unparsed-webhook-url"


class WebhookDeliveryError(Exception):
    """Webhook не доставлен: сетевая ошибка, таймаут или ответ не 2xx."""


class WebhookSender(Protocol):
    async def send(self, url: str, payload: dict[str, Any]) -> None: ...


class HttpxWebhookSender:
    """POST JSON через httpx; редиректы не выполняются, любой ответ не 2xx — сбой."""

    def __init__(self, client: httpx.AsyncClient, timeout: float) -> None:
        self._client = client
        self._timeout = timeout

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        # Таймаут httpx ограничивает каждую фазу отдельно; получатель, который шлёт
        # по байту, держал бы запрос сколь угодно долго. Ограничен весь запрос.
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._client.post(url, json=payload, timeout=self._timeout, follow_redirects=False)
        except TimeoutError as exc:
            logged = log_url(url)
            raise WebhookDeliveryError(f"webhook {logged}: таймаут {self._timeout} с") from exc
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            logged = log_url(url)
            raise WebhookDeliveryError(f"webhook {logged}: {exc!r}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"webhook {log_url(url)}: ответ {response.status_code}")
