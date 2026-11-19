import pytest
from pydantic import ValidationError

from app.config import Settings


def test_default_retry_delays():
    # _env_file=None: локальный .env не должен менять значения по умолчанию
    assert Settings(_env_file=None).retry_delays_ms == [2000, 4000]


def test_retry_delays_follow_settings():
    s = Settings(retry_max_attempts=4, retry_base_delay_ms=100, retry_multiplier=3)
    assert s.retry_delays_ms == [100, 300, 900]


def test_settings_read_environment(monkeypatch):
    monkeypatch.setenv("API_KEY", "from-env")
    assert Settings().api_key == "from-env"


@pytest.mark.parametrize(
    "override",
    [
        {"retry_max_attempts": 0},
        {"consumer_prefetch": 0},
        {"outbox_batch_size": 0},
        {"db_pool_size": 0},
        {"retry_base_delay_ms": 0},
        {"retry_multiplier": 0},
        {"gateway_success_rate": -0.1},
        {"gateway_success_rate": 1.1},
        {"publish_timeout_seconds": 0},
        {"publish_timeout_seconds": -1},
    ],
)
def test_out_of_range_setting_is_rejected(override):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **override)
