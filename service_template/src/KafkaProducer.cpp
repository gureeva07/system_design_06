#include "KafkaProducer.hpp"

#include <iostream>
#include <cstring>

namespace social {

KafkaProducer& KafkaProducer::Instance() {
    static KafkaProducer instance;
    return instance;
}

void KafkaProducer::Init(const std::string& brokers) {
    char errstr[512];

    // Создаю конфигурацию продюсера
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    // Указываю адрес брокера Kafka
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(),
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "[KafkaProducer] Ошибка конфига: " << errstr << std::endl;
        rd_kafka_conf_destroy(conf);
        return;
    }

    // Создаю продюсера с этой конфигурацией
    rk_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk_) {
        std::cerr << "[KafkaProducer] Не удалось создать продюсера: " << errstr << std::endl;
        return;
    }

    initialized_ = true;
    std::cout << "[KafkaProducer] Инициализирован, брокер: " << brokers << std::endl;
}

bool KafkaProducer::Produce(const std::string& topic, const std::string& message) {
    if (!initialized_) {
        std::cerr << "[KafkaProducer] Продюсер не инициализирован" << std::endl;
        return false;
    }

    // отправляю сообщение в Kafka, F_COPY — librdkafka сама копирует буфер
    rd_kafka_resp_err_t err = rd_kafka_producev(
        rk_,
        RD_KAFKA_V_TOPIC(topic.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void*)message.c_str(), message.size()),
        RD_KAFKA_V_END
    );

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "[KafkaProducer] Ошибка отправки: "
                  << rd_kafka_err2str(err) << std::endl;
        return false;
    }

    // Жду завершения отправки (до 5 секунд), чтобы сообщение точно ушло
    rd_kafka_flush(rk_, 5 * 1000);
    return true;
}

KafkaProducer::~KafkaProducer() {
    if (rk_) {
        // даю время на отправку накопленных сообщений перед уничтожением
        rd_kafka_flush(rk_, 3 * 1000);
        rd_kafka_destroy(rk_);
    }
}

}  // namespace social
