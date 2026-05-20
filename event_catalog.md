# Каталог событий

**Автор:** Гуреева Алина, М8О-102СВ-25

Описываю все события, которые я публикую в Kafka. Их три - по одному на каждую сущность из варианта задания: пользователь, стена, чат.

Каждое событие - это JSON-сообщение, которое C++ сервис отправляет в Kafka после успешного POST-запроса. Отдельный Python-скрипт (consumer) читает эти сообщения и сохраняет данные в MongoDB. Так реализуется CQRS: сервис не пишет в MongoDB напрямую, а только публикует событие.

---

## `user.registered`

Это событие я публикую когда пользователь успешно регистрируется.

Почему через Kafka, а не напрямую в MongoDB: PostgreSQL нужен для авторизации - туда пишу синхронно, потому что без этого логин не заработает. MongoDB нужна для поиска пользователей по логину и имени - это некритично, небольшая задержка допустима, поэтому пишу туда через событие асинхронно.

| | |
|--|--|
| Топик | `users` |
| Кто публикует | C++ сервис, `AuthHandlers.cpp`, `POST /api/v1/auth/register` |
| Кто читает | Python consumer, `kafka_consumer/consumer.py` |
| Что делает consumer | `db["users"].insert_one(data)` |
| Гарантии | at-least-once |

Структура сообщения:
```json
{
  "user_id": 17,
  "login": "ivan6",
  "first_name": "Ivan",
  "last_name": "Ivanov",
  "registered_at": "2026-05-19 17:49:08"
}
```

Жизненный цикл:
```
POST /api/v1/auth/register
  -> пишу в PostgreSQL (логин + пароль) синхронно - нужно для авторизации
  -> публикую событие user.registered в топик "users"
  -> возвращаю клиенту {"id": 17, "login": "ivan6"}, не жду MongoDB
  -> consumer читает событие и делает insert_one в MongoDB коллекцию users
  -> GET /api/v1/users?login=ivan6 находит пользователя через MongoDB
```

---

## `wall_post.created`

Это событие я публикую когда пользователь добавляет пост на стену.

Это главный пример CQRS в работе. Раньше POST сразу писал в MongoDB и сервис ждал ответа от базы. Теперь я только публикую событие и сразу возвращаю 201. Consumer запишет данные чуть позже, через долю секунды.

| | |
|--|--|
| Топик | `wall_posts` |
| Кто публикует | C++ сервис, `WallHandlers.cpp`, `POST /api/v1/wall/{user_id}` |
| Кто читает | Python consumer, `kafka_consumer/consumer.py` |
| Что делает consumer | `db["wall_posts"].insert_one(data)` |
| Гарантии | at-least-once |

Структура сообщения:
```json
{
  "owner_id": 17,
  "author_id": 17,
  "content": "post via Kafka",
  "created_at": "2026-05-19 17:51:55"
}
```

Жизненный цикл:
```
POST /api/v1/wall/17
  -> публикую событие wall_post.created в топик "wall_posts"
  -> сразу возвращаю 201 клиенту - MongoDB ещё не обновлена
  -> consumer читает событие и делает insert_one в MongoDB коллекцию wall_posts
  -> GET /api/v1/wall/17 возвращает пост (данные уже есть в MongoDB)
```

После POST до появления поста в GET есть задержка - доли секунды пока consumer обработает. Это eventual consistency, для ленты социальной сети приемлемо.

---

## `message.sent`

Это событие я публикую когда пользователь отправляет личное сообщение другому пользователю.

Работает по той же схеме что и `wall_post.created`: сообщение сначала летит в Kafka, consumer сохраняет его в MongoDB, только после этого оно появляется в GET /messages. Получатель проверяется через PostgreSQL синхронно - чтобы не отправлять сообщение несуществующему пользователю.

| | |
|--|--|
| Топик | `chat_messages` |
| Кто публикует | C++ сервис, `ChatHandlers.cpp`, `POST /api/v1/messages` |
| Кто читает | Python consumer, `kafka_consumer/consumer.py` |
| Что делает consumer | `db["chat_messages"].insert_one(data)` |
| Гарантии | at-least-once |

Структура сообщения:
```json
{
  "from_user_id": 17,
  "to_user_id": 18,
  "content": "hello via Kafka",
  "sent_at": "2026-05-19 17:56:14"
}
```

Жизненный цикл:
```
POST /api/v1/messages
  -> проверяю что получатель существует (SELECT из PostgreSQL)
  -> публикую событие message.sent в топик "chat_messages"
  -> сразу возвращаю 201 клиенту
  -> consumer читает событие и делает insert_one в MongoDB коллекцию chat_messages
  -> GET /api/v1/messages?user_id=17 возвращает сообщения
```

---

## Про гарантии доставки

Все три события работают по принципу at-least-once: сообщение точно дойдёт, но теоретически может прийти дважды.

В C++ я вызываю `rd_kafka_flush()` после отправки. Он ждёт подтверждения от брокера и если что-то пошло не так librdkafka сама повторит отправку.

В Python-consumer я отключила автокоммит и фиксирую offset только после того как данные записались в MongoDB. Если consumer упадёт при перезапуске Kafka отдаст то же сообщение снова.

Exactly-once я не делала, там нужны транзакции Kafka и idempotent producer. Для лабы это лишнее, дубликат поста не катастрофа.
