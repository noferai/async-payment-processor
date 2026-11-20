from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from app.messaging.topology import ATTEMPT_HEADER


class Outcome(StrEnum):
    DONE = "done"  # подтвердить
    RETRY = "retry"  # отправить копию в очередь задержки
    DEAD = "dead"  # отклонить в DLQ


def parse_attempt(headers: Mapping[str, Any]) -> int:
    """Номер попытки из заголовка; без заголовка — первая попытка."""
    if ATTEMPT_HEADER not in headers:
        return 1
    value = headers[ATTEMPT_HEADER]
    # bool — подкласс int, но номером попытки не является
    if type(value) is not int or value < 1:
        raise ValueError(f"invalid {ATTEMPT_HEADER} header: {value!r}")
    return value


def failure_outcome(attempt: int, max_attempts: int) -> Outcome:
    # «Больше или равен»: случайное значение заголовка не позволит обойти DLQ
    return Outcome.RETRY if attempt < max_attempts else Outcome.DEAD
