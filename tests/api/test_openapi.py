import importlib.metadata
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.api.schemas import WebhookPayload
from app.db.models import Currency, Payment, PaymentStatus
from app.services.processing import build_webhook_payload

PAYMENTS = "/api/v1/payments"
PAYMENT = "/api/v1/payments/{payment_id}"
ERROR_REF = "#/components/schemas/ErrorResponse"


@pytest.fixture
async def spec(anon_client):
    r = await anon_client.get("/openapi.json")
    assert r.status_code == 200
    return r.json()


def _error_ref(operation: dict, code: str) -> str:
    return operation["responses"][code]["content"]["application/json"]["schema"]["$ref"]


async def test_info_has_package_version_and_description(spec):
    assert spec["info"]["version"] == importlib.metadata.version("async-payment-processor")
    assert spec["info"]["description"]


@pytest.mark.parametrize(
    ("path", "method", "codes"),
    [
        (PAYMENTS, "post", {"202", "401", "409", "422"}),
        (PAYMENT, "get", {"200", "401", "404", "422"}),
    ],
)
async def test_documents_every_response_code(spec, path, method, codes):
    operation = spec["paths"][path][method]
    assert set(operation["responses"]) == codes
    assert operation["summary"] and operation["description"]
    for code in codes & {"401", "404", "409"}:
        assert _error_ref(operation, code) == ERROR_REF


async def test_request_fields_and_headers_are_described(spec):
    schemas = spec["components"]["schemas"]
    for name in ("PaymentCreate", "PaymentAccepted", "PaymentDetail", "WebhookPayload"):
        for field, prop in schemas[name]["properties"].items():
            assert prop.get("description"), f"{name}.{field}"
    for field, prop in schemas["PaymentCreate"]["properties"].items():
        assert prop.get("examples"), f"PaymentCreate.{field}"

    header = next(p for p in spec["paths"][PAYMENTS]["post"]["parameters"] if p["name"] == "Idempotency-Key")
    assert header["description"]
    assert spec["components"]["securitySchemes"]["APIKeyHeader"]["description"]


async def test_documents_webhook(spec):
    operation = spec["webhooks"]["payment-processed"]["post"]
    schema = operation["requestBody"]["content"]["application/json"]["schema"]
    assert schema["$ref"] == "#/components/schemas/WebhookPayload"
    # Ответы webhook — это ответы получателя: 422 FastAPI здесь не имеет смысла
    assert set(operation["responses"]) == {"200", "default"}


def test_webhook_model_matches_sent_payload():
    # Модель только документирует webhook: тело собирает build_webhook_payload,
    # и без этой проверки описание в Swagger разошлось бы с отправляемым JSON
    payment = Payment(
        id=uuid.uuid7(),
        amount=Decimal("100.5"),
        currency=Currency.RUB,
        description="order 1",
        metadata_={"order_id": 1},
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )
    payload = build_webhook_payload(payment)
    assert set(payload) == set(WebhookPayload.model_fields)
    WebhookPayload.model_validate(payload)
