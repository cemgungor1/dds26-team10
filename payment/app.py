import logging
import os
import atexit
import json
import threading
import time
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from sharded_redis import create_sharded_redis
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
KAFKA_COMMAND_TOPIC = os.environ.get("KAFKA_COMMAND_TOPIC", "payment-commands")
KAFKA_EVENT_TOPIC = os.environ.get("KAFKA_EVENT_TOPIC", "payment-events")
PROCESSED_MSG_PREFIX = "payment:processed:"
PAYMENT_CHARGED_PREFIX = "payment:charged:"
PAYMENT_ROLLEDBACK_PREFIX = "payment:rolledback:"
IDEMPOTENCY_TTL = int(os.environ.get("IDEMPOTENCY_TTL", "3600"))


app = Flask("payment-service")


def service_route(prefix: str, rule: str, **options):
    """Register both direct service routes and gateway-style prefixed aliases."""
    def decorator(func):
        app.route(rule, **options)(func)
        app.route(f"/{prefix}{rule}", **options)(func)
        return func
    return decorator

db = create_sharded_redis()


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
        return db.exists(f"{PROCESSED_MSG_PREFIX}{message_id}") > 0
    except redis.exceptions.RedisError:
        return False


def _mark_message_processed(message_id: str) -> None:
    try:
        db.set(f"{PROCESSED_MSG_PREFIX}{message_id}", "1", ex=IDEMPOTENCY_TTL)
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


@service_route("payment", "/create_user", methods=["POST"])
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@service_route("payment", "/batch_init/<n>/<starting_money>", methods=["POST"])
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    start_id = request.args.get("start_id", default=0, type=int)
    kv_pairs: dict[str, bytes] = {f"{start_id + i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({
        "msg": "Batch init for users successful",
        "start_id": start_id,
        "count": n,
    })


@service_route("payment", "/find_user/<user_id>", methods=["GET"])
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@service_route("payment", "/health", methods=["GET"])
def health():
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return Response("Redis unavailable", status=503)
    return Response("OK", status=200)


@service_route("payment", "/add_funds/<user_id>/<amount>", methods=["POST"])
def add_credit(user_id: str, amount: int):
    amount = int(amount)
    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                raw = pipe.get(user_id)
                if not raw:
                    abort(400, f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                user_entry.credit += amount
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
                return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
        except redis.exceptions.WatchError:
            continue
    return abort(400, DB_ERROR_STR)


@service_route("payment", "/pay/<user_id>/<amount>", methods=["POST"])
def remove_credit(user_id: str, amount: int):
    amount = int(amount)
    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                raw = pipe.get(user_id)
                if not raw:
                    abort(400, f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                user_entry.credit -= amount
                if user_entry.credit < 0:
                    pipe.unwatch()
                    abort(400, f"User: {user_id} credit cannot get reduced below zero!")
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
                return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
        except redis.exceptions.WatchError:
            continue
    return abort(400, DB_ERROR_STR)


def _charge_payment(user_id: str, amount: int, charged_key: str) -> tuple[bool, str]:
    """Deduct credit atomically on the user's shard, then set idempotency marker."""
    amount = int(amount)
    retries = 3
    while retries > 0:
        retries -= 1
        try:
            with db.pipeline(shard_hint=user_id) as pipe:
                pipe.watch(user_id)
                raw = pipe.get(user_id)
                if not raw:
                    pipe.unwatch()
                    return False, "User not found"
                user_entry = msgpack.decode(raw, type=UserValue)
                user_entry.credit -= amount
                if user_entry.credit < 0:
                    pipe.unwatch()
                    return False, "User out of credit"
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
            # idempotency marker (may be on a different shard)
            try:
                db.set(charged_key, json.dumps({"user_id": user_id, "amount": amount}), ex=IDEMPOTENCY_TTL)
            except redis.exceptions.RedisError:
                pass
            return True, ""
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, "DB error"
    return False, "Concurrent modification"


def _rollback_payment(user_id: str, amount: int, rolled_key: str, charged_key: str) -> tuple[bool, str]:
    """Refund credit atomically on the user's shard, then update idempotency markers."""
    amount = int(amount)
    retries = 3
    while retries > 0:
        retries -= 1
        try:
            with db.pipeline(shard_hint=user_id) as pipe:
                pipe.watch(user_id)
                raw = pipe.get(user_id)
                if not raw:
                    pipe.unwatch()
                    return False, "User not found"
                user_entry = msgpack.decode(raw, type=UserValue)
                user_entry.credit += amount
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
            # idempotency markers (may be on different shards)
            try:
                db.set(rolled_key, "1", ex=IDEMPOTENCY_TTL)
                db.delete(charged_key)
            except redis.exceptions.RedisError:
                pass
            return True, ""
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, "DB error"
    return False, "Concurrent modification"


def _handle_command(command: dict) -> None:
    message_id = command.get("message_id")
    if not message_id:
        return

    command_type = command.get("type")
    order_id = command.get("order_id")
    saga_id = command.get("saga_id") or order_id
    user_id = command.get("user_id")
    amount = command.get("amount")

    # ── ChargePayment ────────────────────────────────────────────────────
    if command_type == "ChargePayment":
        if _message_processed(message_id):
            return
        if not order_id or user_id is None or amount is None:
            return
        app.logger.info("ChargePayment received order_id=%s message_id=%s", order_id, message_id)

        idem_key = command.get("idempotency_key", order_id)
        attempt_id = command.get("attempt_id", "")
        charged_key = f"{PAYMENT_CHARGED_PREFIX}{idem_key}"
        try:
            if db.exists(charged_key):
                event = {
                    "message_id": str(uuid.uuid4()),
                    "type": "PaymentCharged",
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "attempt_id": attempt_id,
                    "success": True,
                    "reason": "Already charged",
                    "timestamp": time.time(),
                }
                if send_event(event):
                    _mark_message_processed(message_id)
                return
        except redis.exceptions.RedisError:
            app.logger.error("Failed to check payment idempotency for %s", order_id)

        # Atomic: deduct credit + set charged_key in one transaction
        ok, reason = _charge_payment(user_id, int(amount), charged_key)
        app.logger.info("ChargePayment result order_id=%s success=%s", order_id, ok)
        event = {
            "message_id": str(uuid.uuid4()),
            "type": "PaymentCharged",
            "saga_id": saga_id,
            "order_id": order_id,
            "attempt_id": attempt_id,
            "success": ok,
            "reason": reason,
            "timestamp": time.time(),
        }
        if send_event(event):
            _mark_message_processed(message_id)
        return

    # ── RollbackPayment ──────────────────────────────────────────────────
    if command_type == "RollbackPayment":
        if _message_processed(message_id):
            return
        if not order_id or user_id is None or amount is None:
            return
        app.logger.info("RollbackPayment received order_id=%s message_id=%s", order_id, message_id)

        idem_key = command.get("idempotency_key", order_id)
        attempt_id = command.get("attempt_id", "")
        rolled_key = f"{PAYMENT_ROLLEDBACK_PREFIX}{idem_key}"
        charged_key = f"{PAYMENT_CHARGED_PREFIX}{idem_key}"
        try:
            if db.exists(rolled_key):
                event = {
                    "message_id": str(uuid.uuid4()),
                    "type": "PaymentRolledBack",
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "attempt_id": attempt_id,
                    "success": True,
                    "reason": "Already rolled back",
                    "timestamp": time.time(),
                }
                if send_event(event):
                    _mark_message_processed(message_id)
                return
        except redis.exceptions.RedisError:
            app.logger.error("Failed to check rollback idempotency for %s", order_id)

        # Atomic: refund credit + set rolled_key + delete charged_key in one transaction
        ok, reason = _rollback_payment(user_id, int(amount), rolled_key, charged_key)
        app.logger.info("RollbackPayment result order_id=%s success=%s", order_id, ok)
        event = {
            "message_id": str(uuid.uuid4()),
            "type": "PaymentRolledBack",
            "saga_id": saga_id,
            "order_id": order_id,
            "attempt_id": attempt_id,
            "success": ok,
            "reason": reason,
            "timestamp": time.time(),
        }
        if send_event(event):
            _mark_message_processed(message_id)
        return


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
                enable_auto_commit=False,
            )
            for message in consumer:
                try:
                    _handle_command(message.value)
                    consumer.commit()
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


_background_services_started = False
_background_services_lock = threading.Lock()


def start_background_services() -> None:
    global _background_services_started
    if not KAFKA_AVAILABLE:
        return
    with _background_services_lock:
        if _background_services_started:
            return
        _start_command_consumer()
        _background_services_started = True


if __name__ == '__main__':
    start_background_services()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
