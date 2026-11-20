import asyncio
import random
from typing import Protocol

from app.db.models import Payment


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> bool:
        """Списывает платёж; True — платёж прошёл."""
        ...


class GatewayEmulator:
    """Эмулятор шлюза: случайная пауза и успех с заданной вероятностью."""

    def __init__(
        self,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment: Payment) -> bool:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        return self._rng.random() < self._success_rate
