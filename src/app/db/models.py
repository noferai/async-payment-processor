"""ORM-модели: платежи и outbox."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _in_list(column: str, enum: type[StrEnum]) -> str:
    values = ", ".join(f"'{member.value}'" for member in enum)
    return f"{column} IN ({values})"


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.CheckConstraint(_in_list("currency", Currency), name="ck_payments_currency"),
        sa.CheckConstraint(_in_list("status", PaymentStatus), name="ck_payments_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid, primary_key=True, default=uuid.uuid7)
    amount: Mapped[Decimal] = mapped_column(sa.Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(sa.String(3))
    description: Mapped[str | None] = mapped_column(sa.String(500))
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=sa.text("'{}'::jsonb")
    )
    status: Mapped[PaymentStatus] = mapped_column(
        sa.String(16), default=PaymentStatus.PENDING, server_default=PaymentStatus.PENDING.value
    )
    idempotency_key: Mapped[str] = mapped_column(sa.String(255))
    request_hash: Mapped[str] = mapped_column(sa.CHAR(64))
    webhook_url: Mapped[str] = mapped_column(sa.Text)
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), server_default=sa.func.now())
    processed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        sa.Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=sa.text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid, primary_key=True, default=uuid.uuid7)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(sa.Uuid)
    event_type: Mapped[str] = mapped_column(sa.String(64))
    routing_key: Mapped[str] = mapped_column(sa.String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), server_default=sa.func.now())
    published_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
