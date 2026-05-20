# Event-Driven Architecture

---

## Какую проблему я решала

В прошлой работе каждый POST сразу писал данные в MongoDB. Это работало, но мне не нравилось что сервис жёстко зависит от базы: если MongoDB тормозит или временно недоступна, весь запрос падает. Пользователь ждёт, хотя мог бы уже получить ответ.

В этой работе я добавила Kafka как промежуточный слой и разделила запись и чтение через паттерн CQRS. Теперь при POST сервис просто публикует событие в Kafka и сразу возвращает 201. Отдельный Python-скрипт (consumer) читает события из Kafka и пишет в MongoDB. При GET данные читаем из кеша или напрямую из MongoDB.

Это даёт несколько плюсов: сервис не блокируется на записи в MongoDB, если MongoDB временно упала - события копятся в Kafka и consumer обработает их после перезапуска, и в принципе запись и чтение теперь можно масштабировать независимо.

---

## Анализ событий

Три сущности: пользователь, стена, сообщения чата.

Я выделила по одному событию на каждую сущность:

| Событие | Когда происходит |
|---------|-----------------|
| `user.registered` | пользователь зарегистрировался |
| `wall_post.created` | добавил пост на стену |
| `message.sent` | отправил личное сообщение |

Каждое событие инициируется HTTP-запросом:

| HTTP-запрос | Событие | Топик |
|-------------|---------|-------|
| `POST /api/v1/auth/register` | `user.registered` | `users` |
| `POST /api/v1/wall/{user_id}` | `wall_post.created` | `wall_posts` |
| `POST /api/v1/messages` | `message.sent` | `chat_messages` |

О каждом событии уведомляется Python consumer, он один подписан на все три топика. В реальной системе можно было бы добавить ещё email-сервис при регистрации и push-уведомления при входящем сообщении, но в рамках этой работы я ограничилась одним consumer-ом.

---

## Архитектура

Путь записи (POST):
```
клиент -> POST /api/v1/wall/{id}
       -> C++ сервис -> KafkaProducer.Produce("wall_posts", json)
       -> Kafka (топик wall_posts)
       -> Python consumer -> MongoDB.wall_posts.insert_one()
```

Путь чтения (GET):
```
клиент -> GET /api/v1/wall/{id}
       -> C++ сервис -> Cache (если есть) -> MongoDB.wall_posts
```

Важный момент: между POST и появлением данных в GET есть небольшая задержка - доли секунды пока consumer прочитает событие и запишет в MongoDB. Это называется eventual consistency. Для ленты социальной сети это нормально, никто не ожидает что пост появится в ту же миллисекунду.

---

## Producer и consumer

Producer - C++ сервис. Три обработчика публикуют события через класс `KafkaProducer` (`src/KafkaProducer.cpp`), который я написала как обёртку над librdkafka:

| Файл | Топик | Событие |
|------|-------|---------|
| `AuthHandlers.cpp` | `users` | `user.registered` |
| `WallHandlers.cpp` | `wall_posts` | `wall_post.created` |
| `ChatHandlers.cpp` | `chat_messages` | `message.sent` |

Consumer - `kafka_consumer/consumer.py`, запущен в отдельном Docker-контейнере. Подписан на все три топика, по имени топика определяет в какую коллекцию MongoDB писать:

| Топик | Коллекция MongoDB |
|-------|------------------|
| `users` | `users` |
| `wall_posts` | `wall_posts` |
| `chat_messages` | `chat_messages` |

Consumer крутится в бесконечном цикле: читает сообщение, определяет топик, делает `insert_one()` в MongoDB, фиксирует offset. Если пришло неизвестное сообщение или произошла ошибка - логирует и продолжает работу.

---

## Почему Kafka, а не RabbitMQ

Я выбрала Kafka по одной главной причине: она хранит сообщения на диске даже после того как consumer их прочитал. Если consumer упал, то сообщения никуда не денутся, он обработает их после перезапуска. RabbitMQ удаляет сообщение сразу после прочтения, и если consumer упал в момент обработки - данные теряются.

Для CQRS это критично: MongoDB (куда пишет consumer) может временно быть недоступна, а Kafka при этом продолжит накапливать события. После того как MongoDB поднимется, consumer догонит.

| | RabbitMQ | Kafka |
|--|----------|-------|
| Хранение после чтения | удаляет | хранит на диске |
| Consumer упал | сообщение теряется | перечитает после перезапуска |
| Порядок | не гарантирован | гарантирован внутри партиции |

Запускаю Kafka через образ `confluentinc/cp-kafka:7.6.1` в KRaft-режиме, без ZooKeeper, одним контейнером. KRaft-режим появился в новых версиях Kafka и позволяет обойтись без отдельного ZooKeeper-сервиса, что упрощает docker-compose.

---

## Почему Kafka, а не прямая запись

| | Прямая запись | Через Kafka |
|--|--------------|-------------|
| MongoDB упала | POST падает | событие ждёт в Kafka, consumer обработает потом |
| Скорость POST | зависит от MongoDB | быстро, кладём в очередь и сразу отвечаем |
| Масштабирование | вся нагрузка на MongoDB | можно добавить несколько consumer-ов |

---

## Формат сообщений

Все события передаю как JSON в UTF-8. Выбрала JSON потому что он читается при отладке и не требует отдельной схемы.

Producer собирает JSON через userver ValueBuilder и отправляет:
```cpp
json::ValueBuilder event;
event["owner_id"] = owner_id;
event["content"] = content;
event["created_at"] = now;
std::string event_json = json::ToString(event.ExtractValue());
KafkaProducer::Instance().Produce("wall_posts", event_json);
```

Consumer парсит через стандартный json модуль:
```python
data = json.loads(msg.value().decode("utf-8"))
db["wall_posts"].insert_one(data)
```

Структура payload каждого события подробно описана в [`event_catalog.md`](../event_catalog.md).

---

## Гарантии доставки: at-least-once

Сообщение доставится как минимум один раз. Потеря невозможна, но в редких случаях возможен дубликат - например если consumer записал данные в MongoDB, но упал до того как зафиксировал offset, и при перезапуске обработал то же сообщение ещё раз.

Я реализовала это с двух сторон:

**Producer** - вызываю `rd_kafka_flush()` после отправки. Он ждёт подтверждения от Kafka брокера. Если подтверждения нет - librdkafka повторит отправку автоматически.

**Consumer** - отключила автоматическое подтверждение (`enable.auto.commit=False`). Offset фиксирую вручную только после успешного `insert_one()`. Если consumer упадёт между чтением и записью - при перезапуске Kafka отдаст сообщение снова и данные всё равно запишутся.

Почему не exactly-once: нужны транзакции Kafka и idempotent producer. Это заметно усложняет код, а для учебного проекта избыточно. Дубликат поста на стене - редкий и безвредный случай.

---

## Паттерны

### CQRS

Команды и запросы идут разными путями.

**Команды** (POST) публикуют событие в Kafka и сразу возвращают ответ, не ожидая записи в MongoDB:

| Запрос | Что публикует |
|--------|--------------|
| `POST /api/v1/auth/register` | событие `user.registered` в топик `users` |
| `POST /api/v1/wall/{user_id}` | событие `wall_post.created` в топик `wall_posts` |
| `POST /api/v1/messages` | событие `message.sent` в топик `chat_messages` |

**Запросы** (GET) читают из MongoDB, куда consumer уже записал данные:

| Запрос | Откуда читает |
|--------|--------------|
| `GET /api/v1/users` | Cache → MongoDB `users` |
| `GET /api/v1/wall/{user_id}` | Cache → MongoDB `wall_posts` |
| `GET /api/v1/messages` | MongoDB `chat_messages` |

Раньше POST сразу писал в MongoDB:
```
POST /wall -> C++ -> MongoDB (синхронно)
GET  /wall -> C++ -> Cache -> MongoDB
```

Теперь через Kafka:
```
POST /wall -> C++ -> Kafka -> consumer -> MongoDB
GET  /wall -> C++ -> Cache -> MongoDB
```

### Publish-Subscribe

C++ сервис публикует события в топики, consumer подписан на все три и по `msg.topic()` определяет куда писать.

### Eventual Consistency

После POST данные появляются в MongoDB не сразу, а через долю секунды пока consumer прочитает и запишет. Для ленты социальной сети это нормально.

---

## Технические решения

- **librdkafka** (C API) - для producer в C++. Взяла C API вместо C++, потому что он проще подключается через pkg-config в Dockerfile.
- **confluent-kafka** (Python) - для consumer. Python удобнее для простого скрипта чем писать ещё один C++ бинарник.
- **KRaft-режим** - без отдельного ZooKeeper, один контейнер вместо двух.
- **auto.create.topics.enable=true** - топики создаются автоматически при первой публикации, не нужно создавать руками.
