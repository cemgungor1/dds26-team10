import logging
import os
import atexit
import json
import threading
import time
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except Exception:  # kafka-python not installed
    class KafkaProducer:  # type: ignore[dead-code]
        pass

    class KafkaConsumer:  # type: ignore[dead-code]
        pass

    KafkaError = Exception  # type: ignore[assignment]
    NoBrokersAvailable = Exception  # type: ignore[assignment]
    KAFKA_AVAILABLE = False

DB_ERROR_STR = "DB error"
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
KAFKA_COMMAND_TOPIC = os.environ.get("KAFKA_COMMAND_TOPIC", "saga-commands")
KAFKA_EVENT_TOPIC = os.environ.get("KAFKA_EVENT_TOPIC", "saga-events")
PROCESSED_MESSAGES_SET = "payment:processed_messages"
PAYMENT_CHARGED_PREFIX = "payment:charged:"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


def _get_user_entry(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return None
    if not entry:
        return None
    try:
        return msgpack.decode(entry, type=UserValue)
    except Exception:
        return None


def _message_processed(message_id: str) -> bool:
    try:
        return db.sismember(PROCESSED_MESSAGES_SET, message_id)
    except redis.exceptions.RedisError:
        return False


def _mark_message_processed(message_id: str) -> None:
    try:
        db.sadd(PROCESSED_MESSAGES_SET, message_id)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to mark message processed: %s", message_id)


_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer | None:
    if not KAFKA_AVAILABLE:
        return None
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if v else None,
                retries=3,
            )
        except NoBrokersAvailable:
            app.logger.warning("Kafka broker unavailable for producer")
            return None
    return _producer


def send_event(event: dict) -> bool:
    producer = get_producer()
    if producer is None:
        return False
    key = event.get("order_id")
    try:
        producer.send(KAFKA_EVENT_TOPIC, value=event, key=key)
        producer.flush()
    except KafkaError:
        app.logger.exception("Failed to send saga event")
        return False
    return True


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


def _charge_payment(user_id: str, amount: int) -> tuple[bool, str]:
    user_entry = _get_user_entry(user_id)
    if user_entry is None:
        return False, "User not found"
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        return False, "User out of credit"
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return False, "DB error"
    return True, ""


def _handle_command(command: dict) -> None:
    message_id = command.get("message_id")
    if not message_id or _message_processed(message_id):
        return
    _mark_message_processed(message_id)

    if command.get("type") != "ChargePayment":
        return

    order_id = command.get("order_id")
    user_id = command.get("user_id")
    amount = command.get("amount")
    if not order_id or user_id is None or amount is None:
        return
    app.logger.info("ChargePayment received order_id=%s message_id=%s", order_id, message_id)

    charged_key = f"{PAYMENT_CHARGED_PREFIX}{order_id}"
    try:
        if db.exists(charged_key):
            event = {
                "message_id": str(uuid.uuid4()),
                "type": "PaymentCharged",
                "order_id": order_id,
                "success": True,
                "reason": "Already charged",
                "timestamp": time.time(),
            }
            send_event(event)
            return
    except redis.exceptions.RedisError:
        app.logger.error("Failed to check payment idempotency for %s", order_id)

    ok, reason = _charge_payment(user_id, int(amount))
    if ok:
        try:
            db.set(charged_key, str(amount))
        except redis.exceptions.RedisError:
            app.logger.error("Failed to mark payment charged for %s", order_id)
    app.logger.info("ChargePayment result order_id=%s success=%s", order_id, ok)
    event = {
        "message_id": str(uuid.uuid4()),
        "type": "PaymentCharged",
        "order_id": order_id,
        "success": ok,
        "reason": reason,
        "timestamp": time.time(),
    }
    send_event(event)


def _command_consumer_loop() -> None:
    if not KAFKA_AVAILABLE:
        return
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_COMMAND_TOPIC,
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda v: v.decode("utf-8") if v else None,
                group_id="payment-saga-commands",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            for message in consumer:
                try:
                    _handle_command(message.value)
                except Exception:
                    app.logger.exception("Failed to handle saga command")
        except NoBrokersAvailable:
            app.logger.warning("Kafka broker unavailable for command consumer; retrying")
            time.sleep(1.0)
        except Exception:
            app.logger.exception("Command consumer failed; retrying")
            time.sleep(1.0)


def _start_command_consumer() -> None:
    if not KAFKA_AVAILABLE:
        return
    thread = threading.Thread(target=_command_consumer_loop, daemon=True)
    thread.start()


if __name__ == '__main__':
    _start_command_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
