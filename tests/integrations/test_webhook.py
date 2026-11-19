import threading
import time
from collections.abc import AsyncIterator, Iterator
from urllib.parse import urlsplit, urlunsplit

import httpx
import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request, Response

from app.integrations.webhook import HttpxWebhookSender, WebhookDeliveryError
from tests.helpers import wait_until


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as http_client:
        yield http_client


@pytest.fixture
def sender(client: httpx.AsyncClient) -> HttpxWebhookSender:
    return HttpxWebhookSender(client, timeout=1.0)


async def test_2xx_is_success_and_body_is_json(httpserver: HTTPServer, sender: HttpxWebhookSender) -> None:
    httpserver.expect_request("/hook", method="POST").respond_with_data(status=204)
    await sender.send(httpserver.url_for("/hook"), {"payment_id": "p"})
    assert httpserver.log[0][0].get_json() == {"payment_id": "p"}


@pytest.mark.parametrize("status", [302, 400, 404, 500, 503])
async def test_non_2xx_is_delivery_error(httpserver: HTTPServer, sender: HttpxWebhookSender, status: int) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=status, headers={"Location": "/x"})
    with pytest.raises(WebhookDeliveryError):
        await sender.send(httpserver.url_for("/hook"), {})
    assert len(httpserver.log) == 1  # редирект не выполнялся


async def test_hanging_receiver_fails_within_timeout(httpserver: HTTPServer, client: httpx.AsyncClient) -> None:
    release = threading.Event()

    def hang(request: Request) -> Response:
        release.wait(5)
        return Response()

    httpserver.expect_request("/hook").respond_with_handler(hang)
    sender = HttpxWebhookSender(client, timeout=0.2)
    started = time.monotonic()
    try:
        with pytest.raises(WebhookDeliveryError):
            await sender.send(httpserver.url_for("/hook"), {})
        assert time.monotonic() - started < 1.0
    finally:
        release.set()
        # сервер общий на сессию и однопоточный: дождаться, пока он освободится для следующих тестов
        await wait_until(lambda: len(httpserver.log) == 1)


async def test_slow_drip_receiver_fails_within_total_timeout(httpserver: HTTPServer, client: httpx.AsyncClient) -> None:
    release = threading.Event()
    finished = threading.Event()

    def drip() -> Iterator[bytes]:
        # По байту каждые 0,1 с: ни одна фаза чтения не превышает таймаут
        try:
            for _ in range(50):
                if release.is_set():
                    return
                yield b"x"
                time.sleep(0.1)
        finally:
            finished.set()

    httpserver.expect_request("/hook").respond_with_handler(lambda request: Response(drip()))
    sender = HttpxWebhookSender(client, timeout=0.5)
    started = time.monotonic()
    try:
        with pytest.raises(WebhookDeliveryError):
            await sender.send(httpserver.url_for("/hook"), {})
        assert time.monotonic() - started < 1.0
    finally:
        release.set()
        # сервер общий на сессию и однопоточный: дождаться конца ответа
        await wait_until(finished.is_set)


async def test_connection_refused_is_delivery_error(sender: HttpxWebhookSender) -> None:
    with pytest.raises(WebhookDeliveryError):
        await sender.send("http://127.0.0.1:1/hook", {})


async def test_invalid_url_is_delivery_error(sender: HttpxWebhookSender) -> None:
    with pytest.raises(WebhookDeliveryError):
        await sender.send("http://[::1/hook", {})


async def test_delivery_error_hides_url_secrets(httpserver: HTTPServer, sender: HttpxWebhookSender) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=500)
    base = urlsplit(httpserver.url_for("/hook"))
    secret_url = urlunsplit((base.scheme, f"user:s3cret@{base.netloc}", base.path, "token=abc", "frag"))
    with pytest.raises(WebhookDeliveryError) as raised:
        await sender.send(secret_url, {})
    text = str(raised.value)
    assert base.hostname in text and base.path in text
    assert "s3cret" not in text and "token=abc" not in text and "frag" not in text
