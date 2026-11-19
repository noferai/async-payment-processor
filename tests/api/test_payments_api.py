import asyncio
import json
import uuid
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent, Payment

URL = "/api/v1/payments"
BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "order 1",
    "metadata": {"order_id": 1},
    "webhook_url": "http://example.test/hook",
}


def idem(key: str = "key-1") -> dict[str, str]:
    return {"Idempotency-Key": key}


def raw_body(**fields: str) -> str:
    """BODY, где поля заменены сырым текстом JSON: json= не отправит NaN и суррогаты."""
    parts = {key: json.dumps(value) for key, value in BODY.items()} | fields
    return "{" + ",".join(f"{json.dumps(key)}:{value}" for key, value in parts.items()) + "}"


async def _rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[list[Payment], list[OutboxEvent]]:
    async with session_factory() as session:
        payments = list((await session.scalars(sa.select(Payment))).all())
        events = list((await session.scalars(sa.select(OutboxEvent))).all())
    return payments, events


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_requires_api_key(anon_client, headers):
    assert (await anon_client.post(URL, json=BODY, headers=headers | idem())).status_code == 401
    assert (await anon_client.get(f"{URL}/{uuid.uuid7()}", headers=headers)).status_code == 401


async def test_create_returns_202_and_writes_payment_with_event(
    client, session_factory, monkeypatch: pytest.MonkeyPatch
):
    commits = 0
    original = AsyncSession.commit

    async def counting(self: AsyncSession) -> None:
        nonlocal commits
        commits += 1
        await original(self)

    monkeypatch.setattr(AsyncSession, "commit", counting)
    r = await client.post(URL, json=BODY, headers=idem())
    assert commits == 1
    assert r.status_code == 202
    assert set(r.json()) == {"payment_id", "status", "created_at"}
    assert r.json()["status"] == "pending"
    payments, events = await _rows(session_factory)
    assert len(payments) == 1 and len(events) == 1
    payment, event = payments[0], events[0]
    assert str(payment.id) == r.json()["payment_id"]
    assert event.aggregate_id == payment.id and event.published_at is None
    assert event.payload == {"payment_id": str(payment.id)}
    assert event.event_type == "payment.created" and event.routing_key == "payments.new"


async def test_replay_returns_same_payment_and_single_event(client, session_factory):
    first = await client.post(URL, json=BODY, headers=idem())
    second = await client.post(URL, json=BODY, headers=idem())
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    payments, events = await _rows(session_factory)
    assert len(payments) == 1 and len(events) == 1


async def test_replay_with_equivalent_body_is_not_a_conflict(client):
    first = await client.post(URL, json=BODY, headers=idem())
    reordered = dict(reversed(list(BODY.items()))) | {"amount": 100.5}
    second = await client.post(URL, json=reordered, headers=idem())
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_replay_of_processed_payment_returns_final_status(client, session_factory):
    await client.post(URL, json=BODY, headers=idem())
    async with session_factory() as session:
        await session.execute(sa.update(Payment).values(status="succeeded"))
        await session.commit()
    payments, _ = await _rows(session_factory)
    assert payments[0].status == "succeeded"
    assert (await client.post(URL, json=BODY, headers=idem())).json()["status"] == "succeeded"


@pytest.mark.parametrize(
    ("body", "expected_hash"),
    [
        (
            {
                "amount": "100.5",
                "currency": "RUB",
                "description": "заказ 1",
                "metadata": {"b": [1, {"x": "й"}], "a": None},
                "webhook_url": "http://example.test/hook",
            },
            "9a7c7bae4878c99b465e9bafc55e975c86d5c5e17af3361519f692266cf31bea",
        ),
        (
            {"amount": "7", "currency": "USD", "webhook_url": "https://Example.TEST"},
            "2a235265c46506fc883b78f618e9bd1ff740f9006430eb6cb02e714f36af0ff4",
        ),
    ],
)
async def test_request_hash_is_stable(client, session_factory, body, expected_hash):
    # Значения посчитаны прежней реализацией: смена хеша сделала бы повторы старых
    # запросов конфликтами 409
    assert (await client.post(URL, json=body, headers=idem())).status_code == 202
    payments, _ = await _rows(session_factory)
    assert payments[0].request_hash == expected_hash


async def test_same_key_different_body_is_409(client, session_factory):
    await client.post(URL, json=BODY, headers=idem())
    r = await client.post(URL, json=BODY | {"amount": "999.00"}, headers=idem())
    assert r.status_code == 409
    payments, events = await _rows(session_factory)
    assert len(payments) == 1 and len(events) == 1
    assert payments[0].amount == Decimal("100.50")


async def test_concurrent_requests_with_same_key_create_one_payment(client, session_factory):
    responses = await asyncio.gather(*[client.post(URL, json=BODY, headers=idem()) for _ in range(10)])
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    payments, events = await _rows(session_factory)
    assert len(payments) == 1 and len(events) == 1


@pytest.mark.parametrize("headers", [{}, {"Idempotency-Key": ""}, {"Idempotency-Key": "k" * 256}])
async def test_invalid_idempotency_key_is_422(client, headers):
    assert (await client.post(URL, json=BODY, headers=headers)).status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        *(
            json.dumps(BODY | patch)
            for patch in [
                {"amount": "0"},
                {"amount": "-1"},
                {"amount": "1.001"},
                {"amount": "1" + "0" * 16},
                {"currency": "GBP"},
                {"description": "x" * 501},
                {"metadata": [1, 2]},
                {"description": "a\u0000b"},
                {"metadata": {"k": {"nested": "a\u0000b"}}},
                {"webhook_url": "ftp://example.test/hook"},
                {"webhook_url": None},
            ]
        ),
        # Разбираются как JSON, но не сохраняются в PostgreSQL
        raw_body(metadata='{"k": NaN}'),
        raw_body(metadata='{"k": [1, Infinity]}'),
        raw_body(metadata='{"k": {"nested": -Infinity}}'),
        raw_body(metadata='{"k": 1e400}'),
        raw_body(description='"a\\ud800b"'),
        raw_body(metadata='{"k": ["a\\udfffb"]}'),
        raw_body(metadata='{"\\ud800": 1}'),
    ],
)
async def test_invalid_body_is_422_and_writes_nothing(client, session_factory, body):
    headers = idem() | {"Content-Type": "application/json"}
    r = await client.post(URL, content=body, headers=headers)
    assert r.status_code == 422 and r.json()["detail"]
    payments, events = await _rows(session_factory)
    assert payments == [] and events == []


async def test_optional_fields_default(client):
    body = {k: v for k, v in BODY.items() if k not in {"description", "metadata"}}
    r = await client.post(URL, json=body, headers=idem())
    detail = (await client.get(f"{URL}/{r.json()['payment_id']}")).json()
    assert detail["description"] is None and detail["metadata"] == {}


async def test_get_returns_payment_details(client):
    created = (await client.post(URL, json=BODY, headers=idem())).json()
    r = await client.get(f"{URL}/{created['payment_id']}")
    assert r.status_code == 200
    assert r.json() == {
        "payment_id": created["payment_id"],
        "amount": "100.50",
        "currency": "RUB",
        "description": "order 1",
        "metadata": {"order_id": 1},
        "status": "pending",
        "idempotency_key": "key-1",
        "webhook_url": "http://example.test/hook",
        "created_at": created["created_at"],
        "processed_at": None,
        "webhook_delivered_at": None,
    }


async def test_get_unknown_is_404_and_bad_id_is_422(client):
    assert (await client.get(f"{URL}/{uuid.uuid7()}")).status_code == 404
    assert (await client.get(f"{URL}/not-a-uuid")).status_code == 422
