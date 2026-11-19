import pytest

from app.messaging.retry import Outcome, failure_outcome, parse_attempt


def test_parse_attempt() -> None:
    assert parse_attempt({}) == 1 and parse_attempt({"x-attempt": 3}) == 3
    for bad in ("abc", "2", 0, -1, None, 1.5, True):
        with pytest.raises(ValueError):
            parse_attempt({"x-attempt": bad})


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [
        (1, Outcome.RETRY),
        (2, Outcome.RETRY),
        (3, Outcome.DEAD),
        (4, Outcome.DEAD),
        (99, Outcome.DEAD),
    ],
)
def test_failure_outcome(attempt: int, expected: Outcome) -> None:
    assert failure_outcome(attempt, max_attempts=3) is expected
