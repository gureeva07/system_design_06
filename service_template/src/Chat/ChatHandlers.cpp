#include "ChatHandlers.hpp"
#include "../AuthHelper.hpp"
#include "../MongoHelper.hpp"
#include "../KafkaProducer.hpp"

#include <mongoc/mongoc.h>
#include <userver/formats/json/value_builder.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/server/http/http_status.hpp>
#include <userver/storages/postgres/cluster.hpp>

namespace social {

namespace json = userver::formats::json;

std::string ChatHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& req,
    userver::server::request::RequestContext&) const {

  int sender_id = RequireAuth(req);

  // отправляю сообщение
  if (req.GetMethod() == userver::server::http::HttpMethod::kPost) {
    auto body = json::FromString(req.RequestBody());
    int receiver_id = body["receiver_id"].As<int>(0);
    std::string text = body["text"].As<std::string>("");

    if (receiver_id == 0 || text.empty()) {
      req.SetResponseStatus(userver::server::http::HttpStatus::kBadRequest);
      return R"({"error":"receiver_id and text are required"})";
    }

    // проверяю, что получатель существует в PostgreSQL
    auto check = pg_->Execute(
        userver::storages::postgres::ClusterHostType::kSlave,
        "SELECT id FROM users WHERE id = $1", receiver_id);

    if (check.IsEmpty()) {
      req.SetResponseStatus(userver::server::http::HttpStatus::kNotFound);
      return R"({"error":"Receiver not found"})";
    }

    std::string now = MongoHelper::NowString();

    // публикую событие message.sent в Kafka, consumer сохранит в MongoDB
    json::ValueBuilder event;
    event["from_user_id"] = sender_id;
    event["to_user_id"] = receiver_id;
    event["content"] = text;
    event["sent_at"] = now;
    std::string event_json = json::ToString(event.ExtractValue());

    bool ok = KafkaProducer::Instance().Produce("chat_messages", event_json);
    if (!ok) {
      req.SetResponseStatus(userver::server::http::HttpStatus::kInternalServerError);
      return R"({"error":"Failed to publish event"})";
    }

    json::ValueBuilder resp;
    resp["from_user_id"] = sender_id;
    resp["to_user_id"] = receiver_id;
    resp["content"] = text;
    resp["sent_at"] = now;

    req.SetResponseStatus(userver::server::http::HttpStatus::kCreated);
    return json::ToString(resp.ExtractValue());
  }

  // возвращаю сообщения
  if (req.HasArg("user_id")) {
    int uid = std::stoi(req.GetArg("user_id"));

    // Сообщения где юзер отправитель или получатель
    bson_t* filter = BCON_NEW(
        "$or", "[",
        "{", "from_user_id", BCON_INT32(uid), "}",
        "{", "to_user_id", BCON_INT32(uid), "}",
        "]");

    auto docs = MongoHelper::Instance().Find("chat_messages", filter);
    bson_destroy(filter);

    json::ValueBuilder arr(json::Type::kArray);
    for (const auto& item : docs) {
      bson_error_t err;
      bson_t* doc = bson_new_from_json(
          reinterpret_cast<const uint8_t*>(item.c_str()), item.size(), &err);

      json::ValueBuilder msg;
      msg["from_user_id"] = MongoHelper::GetInt(doc, "from_user_id");
      msg["to_user_id"] = MongoHelper::GetInt(doc, "to_user_id");
      msg["content"] = MongoHelper::GetStr(doc, "content");
      msg["sent_at"] = MongoHelper::GetStr(doc, "sent_at");
      arr.PushBack(msg.ExtractValue());
      bson_destroy(doc);
    }

    return json::ToString(arr.ExtractValue());
  }

  req.SetResponseStatus(userver::server::http::HttpStatus::kBadRequest);
  return R"({"error":"Provide 'user_id' query parameter or POST to send a message"})";
}

}  // namespace social
