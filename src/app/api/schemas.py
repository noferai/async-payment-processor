import math
from datetime import datetime
from decimal import Decimal
from typing import Any
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
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str | None = Field(default=None, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("description", "metadata")
    @classmethod
    def _reject_unstorable(cls, value: Any) -> Any:
        reason = _unstorable_reason(value)
        if reason:
            raise ValueError(reason)
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_payment(cls, payment: Payment) -> PaymentAccepted:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentDetail(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

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
