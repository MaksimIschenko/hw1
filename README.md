# Homework 1 — Kafka Producer и Consumers

Домашнее задание по курсу Kafka (модуль 1). Асинхронное приложение на Python: один producer отправляет сообщения в топик Kafka в формате Avro, два consumer'а (по одному сообщению и батчами) читают их с разными стратегиями коммита.

## Содержание

- [Описание приложения](#описание-приложения)
- [Классы и модули](#классы-и-модули)
- [Принцип работы](#принцип-работы)
- [Требования](#требования)
- [Запуск](#запуск)
- [Проверка решения](#проверка-решения)

---

## Описание приложения

Приложение демонстрирует:

- **Producer** — непрерывно отправляет в топик сообщения с полем `name` (модель `User`), сериализованные в Avro через Confluent Schema Registry.
- **SingleMessageConsumer** — читает сообщения по одному, логирует каждое, использует автоматический коммит offset'ов.
- **BatchMessageConsumer** — накапливает сообщения в батч заданного размера, обрабатывает батч, затем один раз коммитит offset'ы за весь батч.

Все три компонента запускаются одновременно в одном процессе (asyncio). Остановка по Ctrl+C с корректным завершением через `stop_event`.

---

## Классы и модули

### `src/config.py`

Конфигурация приложения (логирование, адреса Kafka и Schema Registry, имена consumer group, размер батча, имя топика). Значения берутся из переменных окружения с fallback на значения по умолчанию для локального запуска.

- `KAFKA_BOOTSTRAP_CONFIG` — словарь для `bootstrap.servers` (по умолчанию `localhost:9094`).
- `SCHEMA_REGISTRY_URL` — URL Schema Registry (по умолчанию `http://localhost:8082`).
- `CONSUMER_GROUP_ID_1`, `CONSUMER_GROUP_ID_2` — идентификаторы consumer group для одиночного и батч-consumer'а.
- `BATCH_SIZE` — размер батча для `BatchMessageConsumer` (по умолчанию 10).
- `TEST_TOPIC_NAME` — имя топика (`test-topic-homework-1`).

### `src/schemas.py`

- **`User`** — dataclass-модель сообщения с полем `name`. Соответствует Avro-схеме из `schemas/user.avsc`. Метод `to_dict()` возвращает словарь для сериализации в Avro.
- **`load_schema_from_avsc(name)`** — загружает содержимое файла `schemas/<name>.avsc` как строку.
- **`USER_SCHEMA_STR`** — строка схемы User, используется при создании Avro serializer/deserializer.

### `src/avro.py`

- **`AvroContext`** — dataclass, хранит клиент Schema Registry и пару serializer/deserializer для Avro.
  - **`create(schema_registry_url, schema_str)`** — асинхронный фабричный метод: создаёт `AsyncSchemaRegistryClient`, затем `AsyncAvroSerializer` и `AsyncAvroDeserializer`, возвращает готовый `AvroContext`.
- **`build_schema_registry_client(url)`** — создаёт `AsyncSchemaRegistryClient` по URL.

Используется для единообразной сериализации/десериализации сообщений в producer и в обоих consumer'ах.

### `src/producer.py`

- **`Producer`** — асинхронный Kafka producer (обёртка над `confluent_kafka.aio.AIOProducer`). Используется как async context manager (`async with Producer(...) as producer`).

  **Параметры:** `kafka_bootstrap_config`, `topic`, опционально `acks`, `enable_idempotence`, `retries`, `linger_ms`, `stop_event`, `value_serializer`.

  **Поведение:**
  - При входе в контекст producer считается «запущенным».
  - `send(value, key=None)` — отправляет одно сообщение в топик; при наличии `value_serializer` сериализует значение (в нашем случае Avro). Ключ по умолчанию — случайный UUID.
  - `send_messages()` — бесконечный цикл: создаёт объекты `User(name=f"user_{count}")`, сериализует через Avro и отправляет; между сообщениями пауза 0.1 с. Выход по `stop_event` или при отмене задачи.

  При выходе из контекста вызывается `flush()` и `close()` producer'а.

### `src/consumer.py`

- **`SingleMessageConsumer`** — consumer, обрабатывающий сообщения по одному. Использует `confluent_kafka.aio.AIOConsumer`, подписка на топик(и) через async context manager.

  **Параметры:** `kafka_bootstrap_config`, `group_id`, `topics`, опционально `enable_auto_commit`, `auto_offset_reset`, `stop_event`, `handler`, `value_deserializer`.

  **Поведение:**
  - `read_messages()` — цикл `poll()`; при получении сообщения при наличии `value_deserializer` десериализует значение и логирует (или вызывает `handler`). Ошибки партиции обрабатываются (PARTITION_EOF игнорируется, фатальные — исключение). Выход по `stop_event` или при отмене задачи.

- **`BatchMessageConsumer`** — consumer, накапливающий сообщения в батч фиксированного размера и коммитящий offset'ы после обработки батча.

  **Параметры:** `kafka_bootstrap_config`, `group_id`, `topics`, опционально `batch_size`, `fetch_wait_max_ms`, `auto_offset_reset`, `stop_event`, `handler`, `value_deserializer`. Автокоммит отключён (`enable.auto.commit`: False).

  **Поведение:**
  - `read_messages()` — внутренний цикл собирает до `batch_size` сообщений через `poll()`, затем обрабатывает батч (логирование или `handler`) и вызывает `commit(asynchronous=False)`. Так достигается «ровно раз» семантика на уровне батча при падении после коммита.

### `main.py`

Точка входа. В `main()`:

1. Создаётся `asyncio.Event()` для остановки по Ctrl+C.
2. Инициализируется `AvroContext` (Schema Registry + Avro serializer/deserializer) с схемой User.
3. Запускаются три корутины:
   - **producer_runner** — создаёт `Producer` с Avro-сериализатором и вызывает `send_messages()`.
   - **single_consumer_runner** — создаёт `SingleMessageConsumer` (group 1, auto-commit) и вызывает `read_messages()`.
   - **batch_consumer_runner** — создаёт `BatchMessageConsumer` (group 2, batch size из конфига) и вызывает `read_messages()`.
4. По `KeyboardInterrupt` выставляется `stop_event`, задачи отменяются, выполняется `gather(..., return_exceptions=True)` для корректного завершения.

---

## Принцип работы

1. **Схема и формат.** Схема `User` задаётся в `schemas/user.avsc`. Producer создаёт объекты `User`, приводит их к словарю и передаёт в Avro serializer; в топик уходят байты в формате Confluent Avro (с возможной записью id схемы в Schema Registry). Consumer'ы используют один и тот же `AvroContext.deserializer`, чтобы получить из байтов словарь/объект.

2. **Два consumer group.** Оба consumer'а подписаны на один и тот же топик, но с разными `group.id`. Поэтому каждый консумер получает свою копию сообщений (как два независимых подписчика). В рамках одного group при нескольких инстансах партиции бы распределились между ними.

3. **Коммит offset'ов.**  
   - **SingleMessageConsumer:** `enable.auto.commit=True` — брокер периодически фиксирует последний обработанный offset.  
   - **BatchMessageConsumer:** автокоммит выключен; после успешной обработки батча вызывается явный `commit()`. Это даёт контроль: при сбое после чтения батча, но до коммита, при перезапуске сообщения батча будут прочитаны снова.

4. **Остановка.** Общий `stop_event` передаётся producer'у и обоим consumer'ам. По Ctrl+C в `main` вызывается `stop_event.set()`, после чего каждый цикл (`send_messages`, `read_messages`) при следующей итерации видит флаг и выходит. Затем задачи отменяются и ожидаются через `gather`, чтобы соединения и ресурсы закрылись корректно.

---

## Требования

- Python 3.13+
- Зависимости заданы в `pyproject.toml` (в т.ч. `confluent-kafka`, `fastavro`). Рекомендуется использовать `uv` (как в Dockerfile).

Для запуска нужны:

- Kafka (доступна по `KAFKA_BOOTSTRAP_SERVERS`, по умолчанию для локального запуска — `localhost:9094`).
- Confluent Schema Registry (по умолчанию `http://localhost:8082`).

Удобный способ поднять инфраструктуру — Docker Compose из каталога `docker/`.

---

## Запуск

### 1. Инфраструктура (Kafka + Schema Registry) через Docker Compose

Из корня проекта:

```bash
cd docker
docker compose up -d
```

Поднимутся три брокера Kafka (внешние порты 9094, 9095, 9096), Schema Registry (порт 8082) и Kafka UI (порт 8081). Дождитесь готовности Kafka (healthcheck), при необходимости проверьте:

```bash
docker compose ps
```

### 2. Запуск приложения внутри Docker (вместе с инфраструктурой)

Из корня проекта:

```bash
cd docker
docker compose up -d --build
```

Сервис `app` в `docker-compose.yaml` собирает образ по `Dockerfile` и запускает `python main.py`. Переменные окружения для контейнера уже заданы (`KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`). В логах будут строки от producer и обоих consumer'ов.

Просмотр логов:

```bash
docker compose logs -f app
```

### 3. Локальный запуск (без контейнера приложения)

Если Kafka и Schema Registry доступны на `localhost:9094` и `http://localhost:8082` (например, подняты через тот же `docker compose`, но сервис `app` не запускаете или отключён):

```bash
# из корня проекта
uv sync
uv run python main.py
```

Либо без `uv`:

```bash
pip install -e .
python main.py
```

Остановка: **Ctrl+C**.

---

## Проверка решения

1. **Запуск и логи.** После старта приложения в stdout должны чередоваться:
   - сообщения producer'а: `delivered to test-topic-homework-1 (p=..., o=...)`;
   - сообщения SingleMessageConsumer: `received {'name': 'user_N'} (p=..., o=...)`;
   - сообщения BatchMessageConsumer: `received N messages` и построчный вывод сообщений батча с десериализованным значением и (p=..., o=...).

2. **Kafka UI.** Откройте http://localhost:8081 (если сервис `ui` запущен). Выберите кластер, найдите топик `test-topic-homework-1`, убедитесь, что сообщения появляются и что формат/value соответствуют ожидаемому (Confluent Avro).

3. **Schema Registry.** В UI или через API Schema Registry (http://localhost:8082) проверьте, что зарегистрирована схема для топика/субъекта, используемого при записи Avro.

4. **Корректное завершение.** Нажмите Ctrl+C. В логах должны появиться сообщения вроде «Shutting down...», «stop_event set, exiting», «cancellation requested», после чего процесс завершится без трассировок и ошибок закрытия producer/consumer.

5. **Поведение consumer group.** После повторного запуска приложения SingleMessageConsumer с `auto_offset_reset="earliest"` продолжит с последнего закоммиченного offset'а (или с начала топика при первом запуске группы). BatchMessageConsumer ведёт себя аналогично для своей группы; оба консумера снова получают те же сообщения (так как топик один и группы разные).

Если все пункты выполняются — решение считается работающим: producer пишет Avro в топик, оба consumer'а читают и десериализуют сообщения, коммит у одиночного — автоматический, у батч — после обработки батча.
