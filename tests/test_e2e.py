import httpx
from pytest_httpserver import HTTPServer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.integrations.webhook import HttpxWebhookSender
from app.messaging.publisher import ReliablePublisher
from app.outbox.relay import OutboxRelay
from app.services.processing import PaymentProcessor
from tests.api.test_payments_api import BODY, URL
from tests.helpers import FakeGateway, StartConsumer, wait_until


async def test_payment_travels_from_api_to_webhook(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    publisher: ReliablePublisher,
    start_consumer: StartConsumer,
    httpserver: HTTPServer,
) -> None:
    httpserver.expect_request("/hook").respond_with_data(status=200)
    async with httpx.AsyncClient() as http_client:
        sender = HttpxWebhookSender(http_client, 1.0)
        await start_consumer(PaymentProcessor(session_factory, FakeGateway(result=True), sender))
        body = BODY | {"webhook_url": httpserver.url_for("/hook")}
        created = (await client.post(URL, json=body, headers={"Idempotency-Key": "e2e"})).json()
        relay = OutboxRelay(session_factory, publisher, batch_size=10, poll_interval=0.01, max_backoff=0.05)
        assert await relay.run_once() == 1

        async def delivered() -> bool:
            detail = (await client.get(f"{URL}/{created['payment_id']}")).json()
            return detail["webhook_delivered_at"] is not None

        await wait_until(delivered)
    detail = (await client.get(f"{URL}/{created['payment_id']}")).json()
    assert detail["status"] == "succeeded" and detail["processed_at"] is not None
    assert len(httpserver.log) == 1
    assert httpserver.log[0][0].get_json()["payment_id"] == created["payment_id"]
