"""Настройки сервиса, читаются из переменных окружения."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: str = "dev-api-key"
    db_pool_size: int = Field(default=15, ge=1)

    # Эмуляция платёжного шлюза
    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout_seconds: float = 5.0

    # Повторные попытки обработки: всего доставок = retry_max_attempts
    retry_max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_ms: int = Field(default=2000, ge=1)
    retry_multiplier: int = Field(default=2, ge=1)
    consumer_prefetch: int = Field(default=10, ge=1)

    # Outbox-релей
    outbox_batch_size: int = Field(default=50, ge=1)
    outbox_poll_interval_seconds: float = 1.0
    outbox_max_backoff_seconds: float = 30.0
    publish_timeout_seconds: float = Field(default=5.0, gt=0)

    @property
    def retry_delays_ms(self) -> list[int]:
        """Задержки между попытками: base * multiplier**i."""
        return [self.retry_base_delay_ms * self.retry_multiplier**i for i in range(self.retry_max_attempts - 1)]


@lru_cache
def get_settings() -> Settings:
    return Settings()
