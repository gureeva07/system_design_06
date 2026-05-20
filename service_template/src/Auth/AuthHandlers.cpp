#include "AuthHandlers.hpp"
#include "../Storage.hpp"
#include "../MongoHelper.hpp"
#include "../KafkaProducer.hpp"

#include <userver/formats/json/serialize.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/server/http/http_status.hpp>
#include <userver/storages/postgres/cluster.hpp>
#include <userver/storages/postgres/component.hpp>

namespace social {

// регистрирую нового пользователя
std::string RegisterHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& req,
    userver::server::request::RequestContext&) const {

  auto body = userver::formats::json::FromString(req.RequestBody());
  std::string login = body["login"].As<std::string>("");
  std::string password = body["password"].As<std::string>("");
  std::string first_name = body["first_name"].As<std::string>("");
  std::string last_name = body["last_name"].As<std::string>("");

  if (login.empty() || password.empty()) {
    req.SetResponseStatus(userver::server::http::HttpStatus::kBadRequest);
    return R"({"error":"login and password are required"})";
  }

  // не занят ли логин
  auto check = pg_->Execute(
      userver::storages::postgres::ClusterHostType::kMaster,
      "SELECT id FROM users WHERE login = $1",
      login);

  if (!check.IsEmpty()) {
    req.SetResponseStatus(userver::server::http::HttpStatus::kConflict);
    return R"({"error":"User with this login already exists"})";
  }

  // вставляю пользователя и получаю его id
  auto result = pg_->Execute(
      userver::storages::postgres::ClusterHostType::kMaster,
      "INSERT INTO users (login, password_hash, first_name, last_name) "
      "VALUES ($1, $2, $3, $4) "
      "RETURNING id",
      login, password, first_name, last_name);

  int id = result[0][0].As<int>();

  // публикую событие user.registered в Kafka, consumer запишет пользователя в MongoDB
  userver::formats::json::ValueBuilder event;
  event["user_id"] = id;
  event["login"] = login;
  event["first_name"] = first_name;
  event["last_name"] = last_name;
  event["registered_at"] = MongoHelper::NowString();
  KafkaProducer::Instance().Produce(
      "users",
      userver::formats::json::ToString(event.ExtractValue()));

  req.SetResponseStatus(userver::server::http::HttpStatus::kCreated);
  userver::formats::json::ValueBuilder resp;
  resp["id"] = id;
  resp["login"] = login;
  return userver::formats::json::ToString(resp.ExtractValue());
}

// проверяю логин/пароль, создаю сессию и возвращаю токен
std::string LoginHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& req,
    userver::server::request::RequestContext&) const {

  auto body = userver::formats::json::FromString(req.RequestBody());
  std::string login = body["login"].As<std::string>("");
  std::string password = body["password"].As<std::string>("");

  // ищу пользователя по логину
  auto result = pg_->Execute(
      userver::storages::postgres::ClusterHostType::kMaster,
      "SELECT id, password_hash FROM users WHERE login = $1",
      login);

  if (result.IsEmpty()) {
    req.SetResponseStatus(userver::server::http::HttpStatus::kUnauthorized);
    return R"({"error":"Invalid login or password"})";
  }

  int user_id = result[0][0].As<int>();
  std::string password_hash = result[0][1].As<std::string>();

  if (password_hash != password) {
    req.SetResponseStatus(userver::server::http::HttpStatus::kUnauthorized);
    return R"({"error":"Invalid login or password"})";
  }

  // создаю сессию в памяти и возвращаю токен
  std::string token = Storage::Instance().CreateSession(user_id);

  userver::formats::json::ValueBuilder resp;
  resp["token"] = token;
  resp["user_id"] = user_id;
  return userver::formats::json::ToString(resp.ExtractValue());
}

// удаляю сессию из памяти
std::string LogoutHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& req,
    userver::server::request::RequestContext&) const {

  const std::string& auth = req.GetHeader("Authorization");
  if (auth.size() > 7) {
    Storage::Instance().DeleteSession(auth.substr(7));
  }
  return R"({"message":"Logged out"})";
}

}  // namespace social
