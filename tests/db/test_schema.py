from decimal import Decimal

import pytest
import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.db.base import Base
from app.db.models import PaymentStatus
from tests.helpers import make_payment


async def test_migration_creates_tables(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        names = await conn.run_sync(lambda c: sa.inspect(c).get_table_names())
    assert {"payments", "outbox"} <= set(names)


async def test_models_match_migration(engine: AsyncEngine) -> None:
    def diff(conn: sa.Connection) -> list[object]:
        context = MigrationContext.configure(conn)
        return list(compare_metadata(context, Base.metadata))

    async with engine.connect() as conn:
        assert await conn.run_sync(diff) == []


async def test_idempotency_key_is_unique(session_factory) -> None:  # type: ignore[no-untyped-def]
    await make_payment(session_factory, idempotency_key="k1")
    with pytest.raises(IntegrityError, match="uq_payments_idempotency_key"):
        await make_payment(session_factory, idempotency_key="k1")


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"amount": Decimal("0")}, "ck_payments_amount_positive"),
        ({"currency": "GBP"}, "ck_payments_currency"),
        ({"status": "refunded"}, "ck_payments_status"),
    ],
)
async def test_check_constraints(session_factory, overrides, constraint) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(IntegrityError, match=constraint):
        await make_payment(session_factory, **overrides)


async def test_new_payment_defaults(session_factory) -> None:  # type: ignore[no-untyped-def]
    p = await make_payment(session_factory)
    assert p.status == PaymentStatus.PENDING
    assert p.metadata_ == {}
    assert p.id.version == 7
    assert p.created_at.tzinfo is not None
    assert p.processed_at is None
    assert p.webhook_delivered_at is None


async def test_outbox_partial_index_exists(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        indexdef = await conn.scalar(
            sa.text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_outbox_unpublished'")
        )
    assert indexdef is not None
    assert "published_at IS NULL" in indexdef
