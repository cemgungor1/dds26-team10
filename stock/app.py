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
KAFKA_COMMAND_TOPIC = os.environ.get("KAFKA_COMMAND_TOPIC", "stock-commands")
KAFKA_EVENT_TOPIC = os.environ.get("KAFKA_EVENT_TOPIC", "stock-events")
PROCESSED_MSG_PREFIX = "stock:processed:"
STOCK_RESERVED_PREFIX = "stock:reserved:"
STOCK_ROLLEDBACK_PREFIX = "stock:rolledback:"
IDEMPOTENCY_TTL = int(os.environ.get("IDEMPOTENCY_TTL", "3600"))

app = Flask("stock-service")


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


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


def _get_item_entry(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return None
    if not entry:
        return None
    try:
        return msgpack.decode(entry, type=StockValue)
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


@service_route("stock", "/item/create/<price>", methods=["POST"])
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@service_route("stock", "/batch_init/<n>/<starting_stock>/<item_price>", methods=["POST"])
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    start_id = request.args.get("start_id", default=0, type=int)
    kv_pairs: dict[str, bytes] = {f"{start_id + i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({
        "msg": "Batch init for stock successful",
        "start_id": start_id,
        "count": n,
    })


@service_route("stock", "/find/<item_id>", methods=["GET"])
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@service_route("stock", "/health", methods=["GET"])
def health():
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return Response("Redis unavailable", status=503)
    return Response("OK", status=200)


@service_route("stock", "/add/<item_id>/<amount>", methods=["POST"])
def add_stock(item_id: str, amount: int):
    amount = int(amount)
    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(item_id)
                raw = pipe.get(item_id)
                if not raw:
                    abort(400, f"Item: {item_id} not found!")
                item_entry = msgpack.decode(raw, type=StockValue)
                item_entry.stock += amount
                pipe.multi()
                pipe.set(item_id, msgpack.encode(item_entry))
                pipe.execute()
                return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
        except redis.exceptions.WatchError:
            continue
    return abort(400, DB_ERROR_STR)


@service_route("stock", "/subtract/<item_id>/<amount>", methods=["POST"])
def remove_stock(item_id: str, amount: int):
    amount = int(amount)
    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(item_id)
                raw = pipe.get(item_id)
                if not raw:
                    abort(400, f"Item: {item_id} not found!")
                item_entry = msgpack.decode(raw, type=StockValue)
                item_entry.stock -= amount
                if item_entry.stock < 0:
                    pipe.unwatch()
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
                pipe.multi()
                pipe.set(item_id, msgpack.encode(item_entry))
                pipe.execute()
                return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
        except redis.exceptions.WatchError:
            continue
    return abort(400, DB_ERROR_STR)


def _reserve_single_item(item_id: str, quantity: int) -> tuple[bool, str]:
    """Atomically subtract stock for a single item on its shard."""
    for _retry in range(3):
        try:
            with db.pipeline(shard_hint=item_id) as pipe:
                pipe.watch(item_id)
                raw = pipe.get(item_id)
                if not raw:
                    pipe.unwatch()
                    return False, f"Item {item_id} not found"
                entry = msgpack.decode(raw, type=StockValue)
                if entry.stock - quantity < 0:
                    pipe.unwatch()
                    return False, f"Insufficient stock for {item_id}"
                entry.stock -= quantity
                pipe.multi()
                pipe.set(item_id, msgpack.encode(entry))
                pipe.execute()
                return True, ""
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, "DB error"
    return False, "Concurrent modification"


def _rollback_single_item(item_id: str, quantity: int) -> None:
    """Best-effort restore stock for a single item."""
    for _retry in range(3):
        try:
            with db.pipeline(shard_hint=item_id) as pipe:
                pipe.watch(item_id)
                raw = pipe.get(item_id)
                if not raw:
                    return
                entry = msgpack.decode(raw, type=StockValue)
                entry.stock += quantity
                pipe.multi()
                pipe.set(item_id, msgpack.encode(entry))
                pipe.execute()
                return
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return


def _reserve_stock(items: list[dict], reserved_key: str = None) -> tuple[bool, str]:
    """Subtract stock for all items, one per shard-safe transaction."""
    reserved: list[dict] = []
    for item in items:
        item_id = item["item_id"]
        quantity = int(item["quantity"])
        ok, reason = _reserve_single_item(item_id, quantity)
        if not ok:
            # rollback already-reserved items
            for prev in reserved:
                _rollback_single_item(prev["item_id"], int(prev["quantity"]))
            return False, reason
        reserved.append(item)
    if reserved_key:
        try:
            db.set(reserved_key, json.dumps(items), ex=IDEMPOTENCY_TTL)
        except redis.exceptions.RedisError:
            pass
    return True, ""


def _rollback_stock(items: list[dict], rolled_key: str = None, reserved_key_to_delete: str = None) -> tuple[bool, str]:
    """Restore stock for all items, one per shard-safe transaction."""
    for item in items:
        item_id = item["item_id"]
        quantity = int(item["quantity"])
        _rollback_single_item(item_id, quantity)
    try:
        if rolled_key:
            db.set(rolled_key, "1", ex=IDEMPOTENCY_TTL)
        if reserved_key_to_delete:
            db.delete(reserved_key_to_delete)
    except redis.exceptions.RedisError:
        pass
    return True, ""


def _handle_command(command: dict) -> None:
    message_id = command.get("message_id")
    if not message_id:
        return

    command_type = command.get("type")
    order_id = command.get("order_id")
    saga_id = command.get("saga_id") or order_id
    attempt_id = command.get("attempt_id", "")
    idem_key = command.get("idempotency_key", order_id)
    items = command.get("items", [])
    if not order_id:
        return

    # ── ReserveStock ──────────────────────────────────────────────────────
    if command_type == "ReserveStock":
        if _message_processed(message_id):
            return
        app.logger.info("ReserveStock received order_id=%s message_id=%s", order_id, message_id)
        reserved_key = f"{STOCK_RESERVED_PREFIX}{idem_key}"
        try:
            if db.exists(reserved_key):
                event = {
                    "message_id": str(uuid.uuid4()),
                    "type": "StockReserved",
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "attempt_id": attempt_id,
                    "success": True,
                    "reason": "Already reserved",
                    "timestamp": time.time(),
                }
                if send_event(event):
                    _mark_message_processed(message_id)
                return
        except redis.exceptions.RedisError:
            app.logger.error("Failed to check stock idempotency for %s", order_id)
        # Atomic: subtract stock + set reserved_key in one transaction
        ok, reason = _reserve_stock(items, reserved_key=reserved_key)
        app.logger.info("ReserveStock result order_id=%s success=%s", order_id, ok)
        event = {
            "message_id": str(uuid.uuid4()),
            "type": "StockReserved",
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

    # ── RollbackStock ─────────────────────────────────────────────────────
    if command_type == "RollbackStock":
        if _message_processed(message_id):
            return
        app.logger.info("RollbackStock received order_id=%s message_id=%s", order_id, message_id)
        rolled_key = f"{STOCK_ROLLEDBACK_PREFIX}{idem_key}"
        reserved_key = f"{STOCK_RESERVED_PREFIX}{idem_key}"
        try:
            if db.exists(rolled_key):
                event = {
                    "message_id": str(uuid.uuid4()),
                    "type": "StockRolledBack",
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
        # Atomic: restore stock + set rolled_key + delete reserved_key in one transaction
        ok, reason = _rollback_stock(items, rolled_key=rolled_key, reserved_key_to_delete=reserved_key)
        app.logger.info("RollbackStock result order_id=%s success=%s", order_id, ok)
        event = {
            "message_id": str(uuid.uuid4()),
            "type": "StockRolledBack",
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
                group_id="stock-saga-commands",
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
