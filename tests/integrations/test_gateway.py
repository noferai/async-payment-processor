import asyncio
import random

import pytest

from app.db.models import Payment
from app.integrations.gateway import GatewayEmulator

# Эмулятор поля платежа не читает — хватает несохранённого экземпляра.
payment = Payment()


async def test_emulator_outcome_follows_success_rate() -> None:
    always = GatewayEmulator(0, 0, success_rate=1.0)
    never = GatewayEmulator(0, 0, success_rate=0.0)
    assert await always.charge(payment) is True and await never.charge(payment) is False


async def test_emulator_success_share_is_about_90_percent() -> None:
    gateway = GatewayEmulator(0, 0, success_rate=0.9, rng=random.Random(42))
    results = [await gateway.charge(payment) for _ in range(2000)]
    assert 0.87 <= sum(results) / 2000 <= 0.93


async def test_emulator_sleeps_within_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    gateway = GatewayEmulator(2.0, 5.0, success_rate=0.9)
    for _ in range(50):
        await gateway.charge(payment)
    assert all(2.0 <= d <= 5.0 for d in delays) and len(delays) == 50
