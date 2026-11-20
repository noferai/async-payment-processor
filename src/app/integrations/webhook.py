import asyncio
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx


def _redact_url(url: str) -> str:
    """Адрес для текста ошибки: userinfo и query могут содержать секреты клиента."""
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
    """Редиректы не выполняются, любой ответ не 2xx — сбой."""

    def __init__(self, client: httpx.AsyncClient, timeout: float) -> None:
        self._client = client
        self._timeout = timeout

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        target = _redact_url(url)
        # Таймаут httpx ограничивает каждую фазу отдельно: получатель, который отдаёт
        # ответ по байту, держал бы запрос сколь угодно долго. Поэтому ограничен весь запрос.
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._client.post(url, json=payload, timeout=self._timeout, follow_redirects=False)
        except TimeoutError as exc:
            raise WebhookDeliveryError(f"webhook {target}: таймаут {self._timeout} с") from exc
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise WebhookDeliveryError(f"webhook {target}: {exc!r}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"webhook {target}: ответ {response.status_code}")
