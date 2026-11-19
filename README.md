# Асинхронный сервис процессинга платежей

## Что это

HTTP API принимает платёж и сразу отвечает `202 Accepted`. Платёж обрабатывается
в фоне: consumer RabbitMQ эмулирует платёжный шлюз (пауза 2–5 с, 90% успеха),
сохраняет итоговый статус `succeeded` или `failed` и отправляет webhook на
`webhook_url` клиента. Сбои обработки повторяются с задержкой, после трёх
неудачных доставок сообщение попадает в Dead Letter Queue.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL 17, RabbitMQ 4.4
через FastStream, Alembic, Docker Compose. События передаются из API в брокер
через паттерн Outbox, поэтому платёж не теряется, если RabbitMQ недоступен в
момент запроса.

## Запуск

Нужны Docker и Docker Compose версии не ниже 2.24 (в ней появился
необязательный `env_file` с `required: false`). Никаких ручных шагов
(миграции, очереди, `.env`) не требуется:

```bash
docker compose up --build
```

Миграции применяет сервис `migrate`, очереди и обменники объявляют сами
`outbox-relay` и `consumer`. Когда в логе появятся строки запуска `api`, сервис
готов. API слушает `http://localhost:8000`, UI RabbitMQ — `http://localhost:15672`
(логин и пароль `payments`).

Остановка с удалением данных: `docker compose down -v`.

### Другой порт

Если порт 8000 занят, задайте `API_PORT` (аналогично `RABBITMQ_UI_PORT` для UI
RabbitMQ):

```bash
API_PORT=18000 docker compose up --build
```

Дальше во всех примерах замените `localhost:8000` на `localhost:18000`. Те же
переменные можно положить в файл `.env` (образец — `.env.example`); без него
используются значения по умолчанию.

Ключ API по умолчанию — `dev-api-key`, меняется переменной `API_KEY`.

## Примеры

Все эндпоинты находятся под `/api/v1` и требуют заголовок `X-API-Key`.
`POST /api/v1/payments` дополнительно требует `Idempotency-Key` (1–255
символов). В примерах `webhook_url` указывает на сервис `webhook-mock` из
compose; адрес `http://webhook-mock:8080/ok` виден только внутри сети compose.

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "100.50", "currency": "RUB", "description": "Заказ 1", "metadata": {"order_id": 1}, "webhook_url": "http://webhook-mock:8080/ok"}'
```

Ответ `202 Accepted`:

```json
{"payment_id": "<uuid>", "status": "pending", "created_at": "<время>"}
```

Правила тела: `amount` больше нуля, не более двух знаков после запятой;
`currency` — `RUB`, `USD` или `EUR`; `description` до 500 символов
(необязательно); `metadata` — JSON-объект (необязательно); `webhook_url` —
http/https URL. Невалидное тело или отсутствующий `Idempotency-Key` дают `422`,
неверный или отсутствующий `X-API-Key` — `401`.

### Повтор с тем же ключом

Тот же запрос с тем же `Idempotency-Key` и тем же телом не создаёт второй
платёж: возвращается `202` с тем же `payment_id` и текущим статусом платежа
(к этому моменту он уже может быть `succeeded` или `failed`).

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "100.50", "currency": "RUB", "description": "Заказ 1", "metadata": {"order_id": 1}, "webhook_url": "http://webhook-mock:8080/ok"}'
```

Сравнение идёт по нормализованному телу: `"100.5"` и `"100.50"`, как и другой
порядок ключей JSON, считаются одним и тем же запросом.

### Конфликт: тот же ключ, другое тело

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "999.00", "currency": "RUB", "webhook_url": "http://webhook-mock:8080/ok"}'
```

Ответ `409 Conflict`.

### Получение платежа

Подставьте `payment_id` из ответа на создание (или возьмите его из ключа
`demo-1` командой ниже):

```bash
PAYMENT_ID=$(curl -s -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "100.50", "currency": "RUB", "description": "Заказ 1", "metadata": {"order_id": 1}, "webhook_url": "http://webhook-mock:8080/ok"}' \
  | sed -E 's/.*"payment_id":"([^"]+)".*/\1/')
echo "$PAYMENT_ID"

curl -i http://localhost:8000/api/v1/payments/$PAYMENT_ID -H 'X-API-Key: dev-api-key'
```

Ответ `200` содержит поля `payment_id`, `amount` (строкой, например `"100.50"`),
`currency`, `description`, `metadata`, `status`, `idempotency_key`,
`webhook_url`, `created_at`, `processed_at`, `webhook_delivered_at`. Через
несколько секунд `status` меняется на `succeeded` (или `failed` — шлюз-эмулятор
отказывает в 10% случаев), а `processed_at` и `webhook_delivered_at`
заполняются. Платежа нет — `404`; `payment_id` не UUID — `422`.

Тело webhook'а, которое получил `webhook-mock`, видно в его логе:

```bash
docker compose logs webhook-mock
```

## Повторы и DLQ

`webhook-mock` отвечает на `POST /ok` кодом 200, а на `POST /fail` — всегда 500.
Чтобы увидеть повторы, создайте платёж с `webhook_url`
`http://webhook-mock:8080/fail`:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: demo-fail-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "10.00", "currency": "USD", "webhook_url": "http://webhook-mock:8080/fail"}'
```

Через ~10–15 секунд (одна пауза шлюза 2–5 с в первой попытке, затем задержки 2 с и 4 с):

- `docker compose logs webhook-mock` показывает три запроса на `/fail`;
- `docker compose logs consumer` показывает записи о сбоях попыток 1 и 2 с
  исходом повтора и попытки 3 с отправкой в DLQ;
- в UI RabbitMQ (`http://localhost:15672`, `payments` / `payments`), вкладка
  Queues, в очереди `payments.dlq` одно сообщение; через `Get messages` видно
  заголовок `x-attempt: 3`. То же из консоли:

  ```bash
  curl -s -u payments:payments -X POST http://localhost:15672/api/queues/%2F/payments.dlq/get \
    -H 'content-type: application/json' \
    -d '{"count": 1, "ackmode": "ack_requeue_true", "encoding": "auto"}'
  ```

Платёж при этом уже имеет итоговый статус (шлюз вызывается один раз), а
`webhook_delivered_at` у него пуст: webhook так и не доставлен.

Чтобы увидеть, что платежи принимаются при недоступном брокере, остановите
RabbitMQ: `docker compose stop rabbitmq`, создайте платёж (ответ `202`), затем
`docker compose start rabbitmq` — `outbox-relay` дождётся брокера, опубликует
событие, и платёж будет обработан.

## Схема потока

```
POST /api/v1/payments
   │  одна транзакция: INSERT payments + INSERT outbox
   ▼
 202 Accepted
                 outbox-relay: SELECT … FOR UPDATE SKIP LOCKED
                    │  publish с подтверждением брокера
                    ▼
        exchange payments ──► queue payments.new ──► consumer
                                   ▲                    │ сбой, попытка < 3
                                   │ TTL истёк          ▼
                       queue payments.retry.<мс> ◄── exchange payments.retry
                                                        │ сбой, попытка = 3
                                                        ▼
                              exchange payments.dlx ──► queue payments.dlq
```

## Устройство

**Outbox.** API записывает платёж и событие `payment.created` в таблицу `outbox`
одной транзакцией и с RabbitMQ не общается вовсе. Отдельный процесс
`outbox-relay` в цикле выбирает до 50 неопубликованных событий в порядке
создания (`SELECT … FOR UPDATE SKIP LOCKED`, поэтому можно запускать несколько
экземпляров), публикует их в exchange `payments` с подтверждением брокера
(персистентное сообщение, `mandatory`, таймаут ожидания) и помечает
`published_at`. При ошибке публикации уже подтверждённые события пакета
фиксируются, остальные остаются в таблице, а relay повторяет попытку с
нарастающей паузой (до 30 с) без ограничения числа попыток. Все публикации в
RabbitMQ (relay и повторы consumer) идут через один класс `ReliablePublisher`.

**Топология RabbitMQ.** Три direct-обменника (`payments`, `payments.retry`,
`payments.dlx`) и durable-очереди: основная `payments.new`; по очереди задержки
на каждый уровень повтора, `payments.retry.2000` и `payments.retry.4000`, с TTL
в имени очереди и возвратом в `payments.new` по истечении TTL; `payments.dlq`
без потребителя. Номер попытки хранится в заголовке `x-attempt`. Сообщение,
отклонённое из `payments.new` (`reject` без requeue), RabbitMQ сам перекладывает
в `payments.dlq`. Топологию объявляют при старте и `outbox-relay`, и `consumer`,
поэтому порядок запуска не важен.

**Идемпотентность.** На уровне API — уникальный индекс по `idempotency_key` и
`INSERT … ON CONFLICT DO NOTHING`: два одновременных запроса с одним ключом
создают один платёж; для сравнения тел хранится SHA-256 нормализованного
запроса (`request_hash`). На уровне consumer — каждая доставка берёт строку
платежа через `SELECT … FOR UPDATE`: шлюз вызывается только для платежа в статусе
`pending`, webhook отправляется только если `webhook_delivered_at` пуст. Дубль
сообщения (в том числе обрабатываемый одновременно) не меняет итоговый статус и
не вызывает шлюз и webhook повторно. Итоговый статус и доставка webhook
сохраняются в двух отдельных транзакциях, поэтому при сбое webhook повтор
шлюз заново не вызывает.

## Зачем в compose сервисы сверх четырёх из задания

Кроме `postgres`, `rabbitmq`, `api` и `consumer` в compose есть:

- `outbox-relay` — публикатор outbox. Он вынесен в отдельный процесс, чтобы
  публикация не зависела от рестартов и числа воркеров API и чтобы паттерн
  Outbox был виден и проверяем отдельно. Это тот же образ с другой командой
  запуска.
- `migrate` — разовый запуск `alembic upgrade head`; остальные прикладные
  сервисы ждут его успешного завершения. Так `docker compose up` работает без
  ручных шагов.
- `webhook-mock` — вспомогательный приёмник webhook'ов (`devtools/webhook_mock.py`):
  `/ok` отвечает 200, `/fail` всегда 500. Нужен, чтобы примеры работали без
  внешних сервисов, а повторы и DLQ можно было воспроизвести. Не часть сервиса.

`api` намеренно не зависит от `rabbitmq`.

## Гарантии и их границы

- **Доставка at-least-once** на всём пути: API → outbox → RabbitMQ → consumer.
  Дубли сообщений возможны (relay упал после публикации до коммита; consumer
  упал после обработки до ack; consumer упал между публикацией копии в очередь
  повтора и ack оригинала). Идемпотентный обработчик их гасит.
- **Webhook тоже at-least-once.** Если consumer упал между отправкой webhook и
  записью `webhook_delivered_at`, клиент получит уведомление дважды. Дубль
  отсеивается по `payment_id` в теле.
- **«3 попытки» — это «не меньше трёх».** Счётчик `x-attempt` растёт только на
  обработанных сбоях. Если consumer падает или рвётся соединение, брокер
  доставляет сообщение заново с тем же номером попытки.
- **Без повторов — сразу в DLQ при первой доставке:** нечитаемое сообщение,
  сообщение о несуществующем платеже и сообщение с некорректным `x-attempt`.
  Повторы им не помогут.
- **Повторяется любой ответ не 2xx, включая 4xx.** Одно правило вместо
  классификации кодов; число попыток всё равно ограничено.
- **Шлюз при повторной доставке может быть вызван ещё раз**, если consumer
  упал после вызова шлюза, но до коммита статуса. Для эмулятора это безвредно;
  настоящему шлюзу передавался бы `payment_id` как ключ идемпотентности.
- **Что остаётся в БД после DLQ.** Если не доставлялся webhook — у платежа
  итоговый статус и пустой `webhook_delivered_at`. Если все попытки упёрлись в
  сбой БД — платёж остаётся `pending`. Разбор DLQ не автоматизирован.
- **Событие, которое нельзя опубликовать, блокирует очередь outbox.**
  Например, событие с routing key без привязки: relay публикует в порядке
  создания и повторяет без ограничения числа попыток, поэтому такое событие
  остаётся первым и задерживает все события за ним.
- **`webhook_url` не проверяется на внутренние адреса (SSRF).** Сервис пошлёт
  запрос на любой http/https адрес, доступный из контейнера.
- **Смена задержек создаёт новые очереди.** Задержка входит в имя очереди
  (`payments.retry.<мс>`), поэтому при изменении настроек старые очереди
  остаются пустыми, а не вызывают ошибку объявления.
- Опубликованные строки outbox не очищаются; `API_KEY` один на весь сервис.

## Тесты и линтеры

Нужен Python 3.14 и [uv](https://docs.astral.sh/uv/). Тесты используют настоящие
PostgreSQL и RabbitMQ в контейнерах (testcontainers), поэтому **Docker должен
быть запущен**.

```bash
uv sync
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Тесты покрывают идемпотентность API, outbox relay (в том числе `SKIP LOCKED` и
сбои публикации), повторы и DLQ, обработку дублей и один сквозной сценарий
API → relay → consumer → webhook.
