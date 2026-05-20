#pragma once

#include <string>
#include <librdkafka/rdkafka.h>

namespace social {

// singleton-обёртка над librdkafka, использую C API 
class KafkaProducer {
public:
    // возвращаю единственный экземпляр продюсера
    static KafkaProducer& Instance();

    // инициализирую соединение с брокером, вызываю один раз при старте
    void Init(const std::string& brokers);

    // отправляю сообщение в топик, возвращаю true если успешно
    bool Produce(const std::string& topic, const std::string& message);

    // Проверяю, был ли вызван Init
    bool IsInitialized() const { return initialized_; }

    ~KafkaProducer();

private:
    KafkaProducer() = default;

    rd_kafka_t* rk_ = nullptr;   // handle продюсера librdkafka
    bool initialized_ = false;
};

}  // namespace social
