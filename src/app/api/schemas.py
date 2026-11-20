import math
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

from app.db.models import Currency, Payment, PaymentStatus


def _unstorable_reason(value: Any) -> str | None:
    """Ищет в произвольном JSON (ключи включительно) значения, которые PostgreSQL не сохранит.

    NUL не хранится ни в text, ни в jsonb; непарный суррогат не кодируется в UTF-8;
    NaN и бесконечность (в том числе 1e400) не входят в JSON для jsonb.
    """
    if isinstance(value, str):
        if "\u0000" in value:
            return "символ NUL (\\u0000) не допускается"
        try:
            value.encode()
        except UnicodeEncodeError:
            return "непарный суррогат UTF-16 не допускается"
    elif isinstance(value, float):
        if not math.isfinite(value):
            return "NaN и бесконечность не допускаются"
    elif isinstance(value, dict):
        for key, item in value.items():
            reason = _unstorable_reason(key) or _unstorable_reason(item)
            if reason:
                return reason
    elif isinstance(value, list):
        for item in value:
            reason = _unstorable_reason(item)
            if reason:
                return reason
    return None


class PaymentCreate(BaseModel):
    amount: Decimal = Field(
        gt=0,
        max_digits=18,
        decimal_places=2,
        description="Сумма: больше нуля, не более двух знаков после точки и 18 цифр всего. "
        "Строка точнее числа: число JSON клиент может округлить как float.",
        examples=["100.50"],
    )
    currency: Currency = Field(description="Валюта платежа.", examples=["RUB"])
    description: str | None = Field(
        default=None, max_length=500, description="Описание платежа, до 500 символов.", examples=["Заказ 1"]
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Произвольный JSON-объект клиента; возвращается в ответах и в webhook.",
        examples=[{"order_id": 1}],
    )
    webhook_url: HttpUrl = Field(
        description="http или https адрес, на который придёт webhook с итогом платежа.",
        examples=["https://shop.example/webhooks/payments"],
    )

    @field_validator("description", "metadata")
    @classmethod
    def _reject_unstorable(cls, value: Any) -> Any:
        reason = _unstorable_reason(value)
        if reason:
            raise ValueError(reason)
        return value


# Описания общих полей: одно поле в ответах API и в webhook должно описываться одинаково
PaymentId = Annotated[UUID, Field(description="Идентификатор платежа.")]
Status = Annotated[PaymentStatus, Field(description="`pending` до обработки, затем `succeeded` или `failed`.")]
CurrencyField = Annotated[Currency, Field(description="Валюта платежа.")]
DescriptionField = Annotated[str | None, Field(description="Описание платежа из запроса.")]
MetadataField = Annotated[dict[str, Any], Field(description="JSON-объект клиента из запроса.")]


class ErrorResponse(BaseModel):
    detail: str = Field(description="Текст ошибки.")


class PaymentAccepted(BaseModel):
    payment_id: PaymentId
    status: Status
    created_at: datetime = Field(description="Время приёма платежа.")

    @classmethod
    def from_payment(cls, payment: Payment) -> PaymentAccepted:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentDetail(BaseModel):
    payment_id: PaymentId
    amount: Decimal = Field(description="Сумма строкой, всегда с двумя знаками после точки.")
    currency: CurrencyField
    description: DescriptionField
    metadata: MetadataField
    status: Status
    idempotency_key: str = Field(description="Значение заголовка Idempotency-Key, с которым создан платёж.")
    webhook_url: str = Field(description="Адрес webhook из запроса.")
    created_at: datetime = Field(description="Время приёма платежа.")
    processed_at: datetime | None = Field(description="Когда сохранён итог платежа; null, пока платёж в обработке.")
    webhook_delivered_at: datetime | None = Field(
        description="Когда webhook принят ответом 2xx; null, пока не доставлен."
    )

    @classmethod
    def from_payment(cls, payment: Payment) -> PaymentDetail:
        # from_attributes не подходит: атрибут metadata занят SQLAlchemy.
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


class WebhookPayload(BaseModel):
    """Тело webhook для документации API; само тело собирает build_webhook_payload."""

    payment_id: PaymentId
    status: Status
    amount: str = Field(description="Сумма строкой, всегда с двумя знаками после точки.", examples=["100.50"])
    currency: CurrencyField
    description: DescriptionField
    metadata: MetadataField
    processed_at: datetime = Field(description="Когда сохранён итог платежа.")
