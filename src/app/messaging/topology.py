"""Топология RabbitMQ: exchange, основная очередь, очереди задержки и DLQ."""

from collections.abc import Sequence

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = "payments"
RETRY_EXCHANGE = "payments.retry"
DLX_EXCHANGE = "payments.dlx"

NEW_QUEUE = "payments.new"
DLQ_QUEUE = "payments.dlq"

# Номер попытки обработки; RabbitMQ сохраняет его при dead-lettering
ATTEMPT_HEADER = "x-attempt"


def _direct_exchange(name: str) -> RabbitExchange:
    return RabbitExchange(name, type=ExchangeType.DIRECT, durable=True)


def payments_exchange() -> RabbitExchange:
    return _direct_exchange(PAYMENTS_EXCHANGE)


def retry_exchange() -> RabbitExchange:
    return _direct_exchange(RETRY_EXCHANGE)


def dlx_exchange() -> RabbitExchange:
    return _direct_exchange(DLX_EXCHANGE)


def new_queue() -> RabbitQueue:
    # Отклонённые сообщения уходят в DLQ
    return RabbitQueue(
        NEW_QUEUE,
        durable=True,
        routing_key=NEW_QUEUE,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DLQ_QUEUE,
        },
    )


def dlq_queue() -> RabbitQueue:
    return RabbitQueue(DLQ_QUEUE, durable=True, routing_key=DLQ_QUEUE)


def retry_queue_name(delay_ms: int) -> str:
    # TTL очереди нельзя изменить после создания, поэтому задержка входит в имя
    return f"payments.retry.{delay_ms}"


def retry_queue(delay_ms: int) -> RabbitQueue:
    # По истечении TTL сообщение возвращается в основную очередь
    name = retry_queue_name(delay_ms)
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
            "x-dead-letter-routing-key": NEW_QUEUE,
        },
    )


def retry_routing_key(attempt: int, delays_ms: Sequence[int]) -> str:
    """Ключ очереди задержки после неудачной попытки с номером attempt (с 1)."""
    if not 1 <= attempt <= len(delays_ms):
        raise ValueError(f"no retry delay for attempt {attempt}")
    return retry_queue_name(delays_ms[attempt - 1])


async def declare_topology(broker: RabbitBroker, delays_ms: Sequence[int]) -> None:
    """Идемпотентно объявляет exchange, очереди и привязки."""
    bindings = [
        (payments_exchange(), new_queue()),
        (dlx_exchange(), dlq_queue()),
        *((retry_exchange(), retry_queue(delay_ms)) for delay_ms in delays_ms),
    ]
    for exchange, queue in bindings:
        robust_exchange = await broker.declare_exchange(exchange)
        robust_queue = await broker.declare_queue(queue)
        # declare_queue FastStream привязку не создаёт
        await robust_queue.bind(robust_exchange, routing_key=queue.routing())
