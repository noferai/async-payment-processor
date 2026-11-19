# AGENTS.md

Сервис асинхронной обработки платежей. Архитектура, топология очередей и
гарантии описаны в `README.md` (разделы «Схема потока», «Устройство»,
«Гарантии и их границы»). Здесь — правила работы с кодом.

## Команды

Нужны Python 3.14, uv и запущенный Docker.

```bash
uv sync
uv run pytest -q                      # все тесты, около 15 секунд
uv run pytest tests/outbox/test_relay.py::test_publishes_and_marks_event
uv run ruff check .
uv run ruff format --check .
uv run mypy src
docker compose up --build             # сервис целиком
```

Изменение готово, когда проходят `pytest`, обе команды `ruff` и `mypy`.

## Инварианты

В скобках — тесты, относящиеся к правилу.

- Код API (`src/app/api`, `src/app/services/payments.py`) не обращается к
  RabbitMQ. Платёж и событие outbox пишутся одной транзакцией с одним
  `commit`, публикует только `outbox-relay`. Тест проверяет транзакцию, но
  обращение к брокеру не ловит (`tests/api/test_payments_api.py`).
- Сообщения публикуются только через протокол `ReliablePublisher`
  (реализация — `RabbitReliablePublisher`), брокер создаётся только в
  `create_broker`: там включены подтверждения брокера и ошибка на
  немаршрутизируемое сообщение (`tests/messaging/test_publisher.py`).
- Очереди и обменники берутся только из `src/app/messaging/topology.py`.
  Объявление существующей очереди с другими аргументами RabbitMQ отклоняет
  с кодом 406, поэтому задержка повтора входит в имя очереди. Повторное
  объявление и смену задержек проверяет `tests/messaging/test_topology.py`.
- `PaymentProcessor` работает в двух транзакциях, каждая в своей сессии:
  при `expire_on_commit=False` повторный `SELECT … FOR UPDATE` в той же
  сессии вернул бы объект из кэша (`tests/services/test_processing.py`).
- `PaymentMessageHandler.process` не бросает исключений. `settle` не
  перехватывает сбой `ack` после публикации копии на повтор: иначе
  появился бы второй повтор (`tests/consumer/`).
- Каноническая форма `request_hash` закреплена тестом: её изменение ломает
  идемпотентность уже сохранённых ключей (`tests/api/test_payments_api.py`).
- Раздел `README.md` «Гарантии и их границы» — контракт: изменение
  поведения из этого списка правит и `README.md` в том же коммите.

## Схема БД

Изменение `src/app/db/models.py` требует новой миграции; расхождение ловит
`tests/db/test_schema.py`. Миграции пишутся вручную, номер — следующий по
порядку:

```bash
uv run alembic revision --rev-id 0002 -m "<описание>"
```

Этой команде база не нужна. `--autogenerate` требует доступной базы, а
compose порт PostgreSQL наружу не публикует. Шаблон Alembic линтер не
проходит: заполнив `upgrade` и `downgrade`, выполнить
`uv run ruff check --fix migrations` и `uv run ruff format migrations`.
Миграцию `0001` не редактировать.

## Тесты

- Docker нужен для любого теста: фикстура очистки таблиц автоматическая и
  поднимает контейнеры даже там, где база не используется.
- Контейнеры PostgreSQL и RabbitMQ и event loop — по одному на сессию
  тестов.
- Таблицы очищаются перед каждым тестом. Фикстура `broker` очищает только
  очереди из настроек: очередь с другой задержкой тест объявляет сам.
- Событие ждут через `wait_until` из `tests/helpers.py`, а не через
  `sleep` с запасом.
- Заглушки шлюза, webhook, издателя и обработчика уже есть в
  `tests/helpers.py`.
- Подписчик регистрируется до старта брокера. `httpx.ASGITransport` не
  запускает `lifespan` — см. `_make_client` в `tests/conftest.py`.

## Конвенции

- Зависимости добавляются через `uv add`; `uv.lock` коммитится.
- `mypy` запускается только на `src`: тесты под режим strict не
  аннотированы.
- Шлюз, webhook и издатель передаются через конструктор как `Protocol`;
  процессы собираются в `src/app/api/app.py`, `src/app/consumer/app.py` и
  `src/app/outbox/__main__.py`.
- Комментарии и docstring — на русском и объясняют причину, а не действие.
  Сообщения логов — на английском.
- Коммиты — Conventional Commits на английском: `feat: …`, `test: …`.

## Границы

Без прямой просьбы не менять: контракт HTTP API и коды ответов, состав
сервисов в `docker-compose.yml`, список зависимостей. `devtools/` — не
часть сервиса, а приёмник webhook'ов для примеров.
