# ДЗ №6 - Event-Driven Architecture и CQRS через Apache Kafka
## Вариант 1: Социальная сеть Facebook

**Автор:** Гуреева Алина, М8О-102СВ-25

---

## Что сделала в этом задании

В предыдущем задании я добавила кеширование и rate limiting. Здесь задача была другая - научиться строить событийно-ориентированную архитектуру и применить паттерн CQRS.

Основная идея: раньше когда пользователь добавлял пост, C++ сервис сразу писал его в MongoDB. Это работает, но сервис напрямую зависит от базы - если MongoDB тормозит, тормозит и весь запрос. Я переделала это через Kafka: сервис публикует событие в брокер и сразу отвечает клиенту, а отдельный Python-скрипт читает события из Kafka и записывает данные в MongoDB. Это и есть CQRS - запись и чтение идут разными путями.

Что добавила:
- Apache Kafka как брокер сообщений (запускается в Docker, KRaft-режим - без ZooKeeper)
- CQRS для всех трёх сущностей: регистрация пользователя, пост на стене, отправка сообщения - всё теперь идёт через Kafka
- Python consumer - отдельный Docker-контейнер, читает события из трёх топиков и сохраняет данные в MongoDB
- Документацию: `event_driven_design.md` (архитектура) и `event_catalog.md` (каталог событий)

Кеширование и rate limiting из ДЗ №5 остались без изменений.

---

## 1. Анализ событий в системе

Три сущности: Пользователь, Стена, Сообщения чата (PtP).

Я определила три события - по одному на каждую сущность:

| Событие | Когда происходит |
|---------|-----------------|
| `user.registered` | Пользователь зарегистрировался |
| `wall_post.created` | Пользователь добавил пост на стену |
| `message.sent` | Пользователь отправил личное сообщение |

Каждое событие инициируется HTTP-командой:

| HTTP-запрос | Событие |
|------------|---------|
| `POST /api/v1/auth/register` | `user.registered` |
| `POST /api/v1/wall/{user_id}` | `wall_post.created` |
| `POST /api/v1/messages` | `message.sent` |

О каждом событии уведомляется Python consumer - он подписан на все три топика и записывает данные в MongoDB. В реальной системе можно было бы добавить ещё email-сервис (для приветственного письма при регистрации) и сервис push-уведомлений (для входящего сообщения), но в рамках этой работы я ограничилась одним consumer-ом.

---

## 2. Проектирование Event-Driven архитектуры

### Производители событий

Единственный producer - C++ сервис. Три обработчика публикуют события через класс `KafkaProducer`:

| Файл | Топик | Событие |
|------|-------|---------|
| `src/Auth/AuthHandlers.cpp` | `users` | `user.registered` |
| `src/Wall/WallHandlers.cpp` | `wall_posts` | `wall_post.created` |
| `src/Chat/ChatHandlers.cpp` | `chat_messages` | `message.sent` |

### Потребители событий

Единственный consumer - Python-скрипт `kafka_consumer/consumer.py` в отдельном Docker-контейнере. Он подписан на все три топика и по имени топика понимает что делать:

| Топик | Что делает |
|-------|-----------|
| `users` | Сохраняет профиль пользователя в MongoDB |
| `wall_posts` | Сохраняет пост на стене в MongoDB |
| `chat_messages` | Сохраняет личное сообщение в MongoDB |

### Поток событий

```
POST /register → C++ → Kafka "users" → consumer → MongoDB users
POST /wall → C++ → Kafka "wall_posts" → consumer → MongoDB wall_posts
POST /messages → C++ → Kafka "chat_messages" → consumer → MongoDB chat_messages

GET /users → C++ → Cache → MongoDB users
GET /wall → C++ → Cache → MongoDB wall_posts
GET /messages → C++ → MongoDB chat_messages
```

Подробная архитектурная схема - в [`event_driven_design.md`](event_driven_design.md).

---

## 3. Взаимодействие через брокер сообщений

### Почему Kafka, а не RabbitMQ

Я выбрала Apache Kafka. Главная причина: Kafka хранит сообщения на диске даже после того как consumer их прочитал. Если consumer временно упал - сообщения никуда не денутся, он прочитает их после перезапуска. RabbitMQ удаляет сообщение сразу после прочтения, поэтому если consumer упал в момент обработки - данные теряются.

Для CQRS это важно: MongoDB (куда пишет consumer) может временно быть недоступна, а Kafka при этом продолжит накапливать события.

Запускаю Kafka в Docker через образ `confluentinc/cp-kafka:7.6.1` в KRaft-режиме - это значит без отдельного ZooKeeper, один контейнер.

Подробное сравнение RabbitMQ и Kafka - в [`event_driven_design.md`](event_driven_design.md).

### Формат сообщений

Все события передаются как JSON в кодировке UTF-8. Выбрала JSON потому что он читается человеком при отладке и не требует отдельной схемы.

| Топик | Пример |
|-------|--------|
| `users` | `{"user_id":17,"login":"ivan6","first_name":"Ivan","last_name":"Ivanov","registered_at":"2026-05-19 17:49:08"}` |
| `wall_posts` | `{"owner_id":17,"author_id":17,"content":"post via Kafka","created_at":"2026-05-19 17:51:55"}` |
| `chat_messages` | `{"from_user_id":17,"to_user_id":17,"content":"hello via Kafka","sent_at":"2026-05-19 17:56:14"}` |

### Гарантии доставки: at-least-once

Сообщение доставится как минимум один раз - потеря невозможна, но в редких случаях возможен дубликат.

Я реализовала это с двух сторон:

**Producer (C++)** - после отправки вызывается `rd_kafka_flush()`, который ждёт подтверждения от Kafka. Если подтверждения нет - librdkafka повторит отправку автоматически.

**Consumer (Python)** - отключила автоматическое подтверждение (`enable.auto.commit=False`). Offset фиксируется вручную только после успешной записи в MongoDB. Если consumer упадёт между чтением и записью - при перезапуске Kafka отдаст сообщение снова.

Почему не exactly-once: для этого нужны транзакции Kafka и idempotent producer - это усложняет код, а для учебного проекта избыточно. Дубликат поста на стене - редкий и безвредный случай.

---

## 4. Применение паттерна CQRS

### Зачем CQRS в социальной сети

Мне кажется, социальная сеть - удачный вариант для применения CQRS. Стену открывают постоянно, а новые посты пишут редко. Я разделила эти два пути: запись идёт асинхронно через Kafka, чтение - из кеша или MongoDB.

### Разделение команд и запросов

**Команды** (изменяют состояние) - публикуют событие в Kafka и сразу возвращают ответ, не ожидая записи в MongoDB:

| Команда | Что публикует |
|---------|--------------|
| `POST /api/v1/auth/register` | `user.registered` → топик `users` |
| `POST /api/v1/wall/{user_id}` | `wall_post.created` → топик `wall_posts` |
| `POST /api/v1/messages` | `message.sent` → топик `chat_messages` |

**Запросы** (только читают) - читают из MongoDB, куда consumer уже записал данные:

| Запрос | Источник данных |
|--------|-----------------|
| `GET /api/v1/users` | Cache → MongoDB `users` |
| `GET /api/v1/wall/{user_id}` | Cache → MongoDB `wall_posts` |
| `GET /api/v1/messages` | MongoDB `chat_messages` |

### Как события синхронизируют модели

Раньше POST сразу писал в MongoDB:
```
POST /wall → C++ → MongoDB (синхронно)
GET  /wall → C++ → Cache → MongoDB
```

Теперь синхронизация через Kafka:
```
POST /wall → C++ → Kafka → consumer → MongoDB
GET  /wall → C++ → Cache → MongoDB
```

Между POST и появлением данных в GET есть небольшая задержка (доли секунды) - пока consumer прочитает и запишет. Это называется eventual consistency - данные в конечном счёте согласуются. Для ленты социальной сети это приемлемо.

---

## 5. Реализация

### Kafka в Docker

Все сервисы запускаются одной командой `docker-compose up`. В `docker-compose.yml` описано 6 контейнеров: postgres, mongodb, kafka, service_template (C++ сервис), kafka-consumer (Python), prometheus + grafana.

### Producer - C++ сервис

Класс `KafkaProducer` (`src/KafkaProducer.cpp`) - обёртка над библиотекой librdkafka. Три обработчика вызывают `KafkaProducer::Produce("топик", json_строка)` после каждого успешного POST-запроса.

### Consumer - Python скрипт

`kafka_consumer/consumer.py` - бесконечный цикл: читает сообщения из трёх топиков и для каждого вызывает `insert_one()` в нужную коллекцию MongoDB. Запущен в отдельном Docker-контейнере.

### Результаты тестирования

Проверила все три цепочки. Все команды выполнялись в Git Bash.

#### Шаг 1 - Регистрация и вход

```bash
curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"login":"ivan6","password":"pass123","first_name":"Ivan","last_name":"Ivanov"}'
```

Ответ:
```json
{"id":17,"login":"ivan6"}
```

```bash
curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"login":"ivan6","password":"pass123"}'
```

Ответ:
```json
{"token":"c9347ee5c96cec78e04a119c62f4c625","user_id":17}
```

#### Шаг 2 - Проверяю событие `user.registered`

Регистрация пишет в PostgreSQL синхронно, а в MongoDB - через Kafka. Через секунду пользователь появляется в поиске:

```bash
curl -s "http://localhost:8080/api/v1/users?login=ivan6" \
  -H "Authorization: Bearer c9347ee5c96cec78e04a119c62f4c625"
```

Ответ (данные пришли через Kafka → consumer → MongoDB):
```json
{"login":"ivan6","first_name":"Ivan","last_name":"Ivanov"}
```

#### Шаг 3 - Проверяю событие `wall_post.created`

POST публикует событие в Kafka и сразу возвращает 201, не ожидая записи в MongoDB:

```bash
curl -s -X POST "http://localhost:8080/api/v1/wall/17" \
  -H "Authorization: Bearer c9347ee5c96cec78e04a119c62f4c625" \
  -H "Content-Type: application/json" \
  -d '{"content":"post via Kafka"}'
```

Ответ:
```json
{"owner_id":17,"author_id":17,"content":"post via Kafka","created_at":"2026-05-19 17:51:55"}
```

Через секунду читаем стену - пост уже есть:

```bash
curl -s "http://localhost:8080/api/v1/wall/17" \
  -H "Authorization: Bearer c9347ee5c96cec78e04a119c62f4c625"
```

Ответ:
```json
[{"owner_id":17,"author_id":17,"content":"post via Kafka","created_at":"2026-05-19 17:51:55"}]
```

#### Шаг 4 - Проверяю событие `message.sent`

```bash
curl -s -X POST "http://localhost:8080/api/v1/messages" \
  -H "Authorization: Bearer c9347ee5c96cec78e04a119c62f4c625" \
  -H "Content-Type: application/json" \
  -d '{"receiver_id":17,"text":"hello via Kafka"}'
```

Ответ:
```json
{"from_user_id":17,"to_user_id":17,"content":"hello via Kafka","sent_at":"2026-05-19 17:56:14"}
```

Через секунду читаем сообщения:

```bash
curl -s "http://localhost:8080/api/v1/messages?user_id=17" \
  -H "Authorization: Bearer c9347ee5c96cec78e04a119c62f4c625"
```

Ответ:
```json
[{"from_user_id":17,"to_user_id":17,"content":"hello via Kafka","sent_at":"2026-05-19 17:53:10"},{"from_user_id":17,"to_user_id":17,"content":"hello via Kafka","sent_at":"2026-05-19 17:54:17"},{"from_user_id":17,"to_user_id":17,"content":"hello via Kafka","sent_at":"2026-05-19 17:56:14"}]
```

#### Шаг 5 - Логи C++ сервиса (producer)

```bash
docker-compose logs service_template 2>&1 | Select-String -Pattern "kafka|Kafka"
```

Вывод - видна инициализация producer-а и все запросы:
```
service_template-1  | [KafkaProducer] Инициализирован, брокер: kafka:9092

service_template-1  | ... method=POST  uri=/api/v1/wall/17        meta_code=201
service_template-1  |     body={"owner_id":17,"author_id":17,"content":"post via Kafka","created_at":"2026-05-19 17:51:55"}

service_template-1  | ... method=GET   uri=/api/v1/wall/17        meta_code=200
service_template-1  |     body=[{"owner_id":17,"author_id":17,"content":"post via Kafka","created_at":"2026-05-19 17:51:55"}]

service_template-1  | ... method=POST  uri=/api/v1/messages       meta_code=201
service_template-1  |     body={"from_user_id":17,"to_user_id":17,"content":"hello via Kafka","sent_at":"2026-05-19 17:53:10"}

service_template-1  | ... method=GET   uri=/api/v1/messages?user_id=17  meta_code=200
service_template-1  |     body=[{"from_user_id":17,...},{"from_user_id":17,...},{"from_user_id":17,...}]
```

#### Шаг 6 - Логи consumer-а (все три события)

```bash
docker-compose logs kafka-consumer --tail=20
```

Вывод:
```
kafka-consumer-1  | [consumer] Старт
kafka-consumer-1  | [consumer] Подключилась к MongoDB: mongodb://mongodb:27017/social_network
kafka-consumer-1  | [consumer] Подписалась на топики ['wall_posts', 'chat_messages', 'users'], брокер: kafka:9092
kafka-consumer-1  | [consumer] Жду сообщений...
kafka-consumer-1  | [consumer] Ошибка Kafka: KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3,str="Subscribed topic not available: chat_messages: Broker: Unknown topic or partition"}
kafka-consumer-1  | [consumer] Ошибка Kafka: KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3,str="Subscribed topic not available: users: Broker: Unknown topic or partition"}
kafka-consumer-1  | [consumer] Ошибка Kafka: KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3,str="Subscribed topic not available: wall_posts: Broker: Unknown topic or partition"}
kafka-consumer-1  | [consumer] user.registered: user_id=17 login=ivan6
kafka-consumer-1  | [consumer] wall_post: owner_id=17, author_id=17
kafka-consumer-1  | [consumer] message.sent: from=17 to=17
kafka-consumer-1  | [consumer] message.sent: from=17 to=17
kafka-consumer-1  | [consumer] message.sent: from=17 to=17
```

---

## 6. Документация событий

Создала файл [`event_catalog.md`](event_catalog.md) - каталог всех событий с полным описанием.

Для каждого события в каталоге указано: название, структура payload с типами полей, производитель, потребитель, гарантии доставки и жизненный цикл от HTTP-запроса до записи в MongoDB.

| | `user.registered` | `wall_post.created` | `message.sent` |
|---|---|---|---|
| **Топик** | `users` | `wall_posts` | `chat_messages` |
| **Producer** | `AuthHandlers.cpp` | `WallHandlers.cpp` | `ChatHandlers.cpp` |
| **Consumer** | `consumer.py` | `consumer.py` | `consumer.py` |
| **Гарантии** | at-least-once | at-least-once | at-least-once |

---

## Запуск

```bash
cd service_template
docker-compose up --build
```

Остановить:
```bash
docker-compose down
```

---

## Проверка работы 

Все команды выполнялись в Git Bash.

### Шаг 1 - Регистрация и вход

```bash
curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"login":"ivan2","password":"pass123","first_name":"Ivan","last_name":"Ivanov"}'
```

Ответ:
```json
{"id":16,"login":"ivan2"}
```

```bash
curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"login":"ivan2","password":"pass123"}'
```

Ответ:
```json
{"token":"4053890b3016014db3994469adaa688a","user_id":16}
```

---

### Шаг 2 - Проверка кеша (поиск пользователя по логину)

Первый запрос идёт в MongoDB - cache miss:

```bash
curl -s -w "\nВремя: %{time_total}s\n" \
  "http://localhost:8080/api/v1/users?login=ivan2" \
  -H "Authorization: Bearer 4053890b3016014db3994469adaa688a"
```

Ответ:
```json
{"login":"ivan2","first_name":"Ivan","last_name":"Ivanov"}
Время: 0.034794s
```

Второй запрос - cache hit, данные из памяти:

```bash
curl -s -w "\nВремя: %{time_total}s\n" \
  "http://localhost:8080/api/v1/users?login=ivan2" \
  -H "Authorization: Bearer 4053890b3016014db3994469adaa688a"
```

Ответ:
```json
{"login":"ivan2","first_name":"Ivan","last_name":"Ivanov"}
Время: 0.014436s
```

Второй запрос быстрее в 2.4 раза (34ms → 14ms) за счёт кеша. В реальном окружении разница была бы ещё больше - сетевая задержка до MongoDB добавляет десятки миллисекунд.

---

### Шаг 3 - Проверка инвалидации кеша стены

Смотрим стену - пусто, результат закешировался:

```bash
curl -s "http://localhost:8080/api/v1/wall/16" \
  -H "Authorization: Bearer 4053890b3016014db3994469adaa688a"
```

Ответ:
```json
[]
```

Добавляем пост - кеш стены при этом сбрасывается:

```bash
curl -s -X POST "http://localhost:8080/api/v1/wall/16" \
  -H "Authorization: Bearer 4053890b3016014db3994469adaa688a" \
  -H "Content-Type: application/json" \
  -d "{\"content\":\"hello world\"}"
```

Ответ:
```json
{"owner_id":16,"author_id":16,"content":"hello world","created_at":"2026-05-04 12:59:18"}
```

Смотрим стену снова - новый пост виден сразу:

```bash
curl -s "http://localhost:8080/api/v1/wall/16" \
  -H "Authorization: Bearer 4053890b3016014db3994469adaa688a"
```

Ответ:
```json
[{"owner_id":16,"author_id":16,"content":"hello world","created_at":"2026-05-04 12:59:18"}]
```

Без инвалидации новый пост не был бы виден ещё 60 секунд (TTL стены = 1 минута).

---

### Шаг 4 - Проверка rate limiting

Отправляем 35 запросов подряд к поиску по имени:

```bash
for i in $(seq 1 35); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://localhost:8080/api/v1/users?name=Ivan" \
    -H "Authorization: Bearer 4053890b3016014db3994469adaa688a")
  echo "Запрос $i: HTTP $CODE"
done
```

Ответ:
```
Запрос 1: HTTP 200
...
Запрос 30: HTTP 200
Запрос 31: HTTP 429
Запрос 32: HTTP 429
Запрос 33: HTTP 429
Запрос 34: HTTP 429
Запрос 35: HTTP 429
```

Ровно с 31-го запроса сервер возвращает `429 Too Many Requests`. Через 60 секунд счётчик сбрасывается.

Заголовки при запросе:

```bash
curl -si "http://localhost:8080/api/v1/users?name=Ivan" \
  -H "Authorization: Bearer 4053890b3016014db3994469adaa688a" | grep -i "ratelimit"
```

Ответ:
```
X-RateLimit-Limit: 30
X-RateLimit-Remaining: 29
X-RateLimit-Reset: 1746356400
```

---

### Шаг 5 - Метрики через /metrics до и после нагрузки

Смотрю метрики сразу после запуска - всё на нуле:

```bash
curl -s http://localhost:8080/metrics
```

Ответ:
```
cache_hits_total 0
cache_misses_total 0
cache_requests_total 0
cache_hit_rate 0
rate_limit_blocked_total 0
```

Запускаю нагрузочный тест - 20 запросов на кеш и 35 на rate limit:

```bash
for i in $(seq 1 20); do
  curl -s "http://localhost:8080/api/v1/users?login=ivan2" -H "Authorization: Bearer a50d99e7c49ed7f048a6106a0963d0fb" > /dev/null
done

for i in $(seq 1 35); do
  curl -s "http://localhost:8080/api/v1/users?name=Ivan" -H "Authorization: Bearer a50d99e7c49ed7f048a6106a0963d0fb" > /dev/null
done
```

Смотрю метрики после:

```bash
curl -s http://localhost:8080/metrics
```

Ответ:
```
cache_hits_total 19
cache_misses_total 1
cache_requests_total 20
cache_hit_rate 0.95
rate_limit_blocked_total 5
```

Из 20 запросов 19 попали в кеш (только первый пошёл в MongoDB), hit rate 0.95. Из 35 запросов к поиску по имени 5 заблокировал rate limiter.

---

### Шаг 6 - Grafana

Сделала дашборд в Grafana с четырьмя панелями:
- эффективность кеша - gauge, показывает hit rate от 0 до 1
- запросы из кеша - график по времени
- промахи кеша - сколько раз пришлось идти в MongoDB
- заблокировано rate limiter - счётчик запросов с HTTP 429

До тестов - всё на нуле:

![Grafana до теста](service_template/screenshots/before_grafana_screenshot.jpg)

После тестов - видно скачки:

![Grafana после теста](service_template/screenshots/after_grafana_screenshot.jpg)

На скрине:
- эффективность кеша 0.971 - 97% запросов отдаётся из памяти
- резкий скачок на графике запросов в момент нагрузочного теста
- заблокировано rate limiter: 15 запросов за оба теста

---

## API endpoints

Все запросы, кроме `/register` и `/login`, требуют заголовок `Authorization: Bearer <token>`.

| Метод | URL | Описание |
|-------|-----|----------|
| POST | `/api/v1/auth/register` | Создание пользователя |
| POST | `/api/v1/auth/login` | Вход, возвращает токен |
| POST | `/api/v1/auth/logout` | Выход |
| GET  | `/api/v1/users?login=` | Поиск по логину (кеш 5 минут) |
| GET  | `/api/v1/users?name=` | Поиск по имени/фамилии (rate limit 30/мин) |
| POST | `/api/v1/wall/{user_id}` | Добавить пост на стену |
| GET  | `/api/v1/wall/{user_id}` | Загрузить стену (кеш 1 минута) |
| POST | `/api/v1/messages` | Отправить сообщение |
| GET  | `/api/v1/messages?user_id=` | Получить сообщения |

---

## Структура файлов

```
lab2/
├── event_driven_design.md   # описание EDA-архитектуры и CQRS
├── event_catalog.md         # каталог событий Kafka
└── service_template/
    ├── docker-compose.yml       # все контейнеры: postgres, mongo, kafka, consumer, сервис
    ├── Dockerfile               # сборка C++ сервиса
    ├── schema.sql               # схема PostgreSQL
    ├── data.sql                 # тестовые данные
    ├── kafka_consumer/          # Python consumer
    │   ├── consumer.py          # читает из Kafka, пишет в MongoDB
    │   ├── Dockerfile
    │   └── requirements.txt
    └── src/
        ├── KafkaProducer.hpp/.cpp  # Kafka producer (librdkafka)
        ├── Cache.hpp/.cpp          # in-memory кеш с TTL
        ├── RateLimiter.hpp/.cpp    # rate limiter (Fixed Window Counter)
        ├── Storage.hpp/.cpp        # хранилище сессий
        ├── MongoHelper.hpp/.cpp    # обёртка над libmongoc
        ├── Auth/                   # регистрация, вход, выход
        ├── User/                   # поиск пользователей
        ├── Wall/                   # стена (CQRS через Kafka + кеш)
        └── Chat/                   # личные сообщения
```
