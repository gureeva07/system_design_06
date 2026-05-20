# читаю события из Kafka и сохраняю в MongoDB (CQRS)

import json
import os
import time

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
MONGO_DSN = os.environ.get("MONGO_DSN", "mongodb://localhost:27017/social_network")
TOPICS = ["wall_posts", "chat_messages", "users"]


def connect_mongo(dsn: str):
    """Подключаюсь к MongoDB, повторяю попытки пока не подключусь."""
    while True:
        try:
            client = MongoClient(dsn, serverSelectionTimeoutMS=3000)
            # Проверяю, что соединение живое
            client.admin.command("ping")
            print(f"[consumer] Подключилась к MongoDB: {dsn}")
            return client
        except Exception as e:
            print(f"[consumer] MongoDB недоступна: {e} — повтор через 3 сек")
            time.sleep(3)


def connect_kafka(brokers: str):
    """Создаю Kafka consumer, повторяю если Kafka ещё не стартовала."""
    config = {
        "bootstrap.servers": brokers,
        "group.id": "wall-consumer-group",
        "auto.offset.reset": "earliest",
        # отключаю авто-коммит, буду коммитить вручную после записи в MongoDB
        "enable.auto.commit": False,
    }
    # confluent-kafka не кидает ошибку сразу при создании, просто создаю
    consumer = Consumer(config)
    consumer.subscribe(TOPICS)
    print(f"[consumer] Подписалась на топики {TOPICS}, брокер: {brokers}")
    return consumer


def main():
    print("[consumer] Старт")

    mongo_client = connect_mongo(MONGO_DSN)
    # Имя БД беру из DSN (последняя часть после /)
    db_name = MONGO_DSN.rstrip("/").split("/")[-1]
    db = mongo_client[db_name]

    # Даю Kafka время подняться перед первым подключением
    time.sleep(5)
    consumer = connect_kafka(KAFKA_BROKERS)

    print("[consumer] Жду сообщений...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Нет новых сообщений, просто продолжаю ждать
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Дошла до конца партиции — это нормально
                    continue
                print(f"[consumer] Ошибка Kafka: {msg.error()}")
                continue

            # получила сообщение — определяю топик и сохраняю в нужную коллекцию
            try:
                data = json.loads(msg.value().decode("utf-8"))
                topic = msg.topic()

                if topic == "wall_posts":
                    db["wall_posts"].insert_one(data)
                    print(f"[consumer] wall_post: owner_id={data.get('owner_id')}, "
                          f"author_id={data.get('author_id')}")

                elif topic == "chat_messages":
                    db["chat_messages"].insert_one(data)
                    print(f"[consumer] message.sent: from={data.get('from_user_id')} "
                          f"to={data.get('to_user_id')}")

                elif topic == "users":
                    db["users"].insert_one(data)
                    print(f"[consumer] user.registered: user_id={data.get('user_id')} "
                          f"login={data.get('login')}")

                # коммичу offset только после успешной записи
                consumer.commit(message=msg)
            except Exception as e:
                print(f"[consumer] Ошибка обработки сообщения: {e}")
                # offset не коммичу — при перезапуске сообщение перечитается

    except KeyboardInterrupt:
        print("[consumer] Остановка")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()
