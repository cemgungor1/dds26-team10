import logging
import os
import atexit
import json
import random
import threading
import time
import uuid
from collections import defaultdict

import redis
import grpc
import services_pb2
from grpc_clients import stock_stub, payment_stub

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except Exception as e:
    class KafkaProducer:
        pass
    class KafkaConsumer:
        pass
    KafkaError = Exception
    NoBrokersAvailable = Exception
    KAFKA_AVAILABLE = False


DB_ERROR_STR = "DB error"

# Kafka configuration - dedicated in/out queues per service
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
STOCK_COMMAND_TOPIC = os.environ.get("STOCK_COMMAND_TOPIC", "stock-commands")    # out-queue → stock in-queue
STOCK_EVENT_TOPIC = os.environ.get("STOCK_EVENT_TOPIC", "stock-events")          # stock out-queue → in-queue
PAYMENT_COMMAND_TOPIC = os.environ.get("PAYMENT_COMMAND_TOPIC", "payment-commands")  # out-queue → payment in-queue
PAYMENT_EVENT_TOPIC = os.environ.get("PAYMENT_EVENT_TOPIC", "payment-events")        # payment out-queue → in-queue

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "saga").lower()
if TRANSACTION_MODE == "saga" and not KAFKA_AVAILABLE:
    TRANSACTION_MODE = "sync"

SAGA_TIMEOUT_SECONDS = float(os.environ.get("SAGA_TIMEOUT_SECONDS", "10"))

# Redis key prefixes for saga logging
SAGA_LOG_PREFIX = "saga:log:"
SAGA_STATE_PREFIX = "saga:state:"
SAGA_LOCK_PREFIX = "saga:lock:"
SAGA_LOCK_TTL = max(int(SAGA_TIMEOUT_SECONDS) * 5, 60)
SAGA_COMPLETED_TTL = int(os.environ.get("SAGA_COMPLETED_TTL", "3600"))
SAGA_NOTIFY_TTL = 60

app = Flask("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    '''Retrieve an order from the database by its ID. This is used in request contexts where we can abort on failure.'''
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


def _get_order_entry(order_id: str) -> OrderValue | None:
    """Read order directly from Redis (safe for non-request contexts like Kafka consumer threads)."""
    try:
        raw = db.get(order_id)
    except redis.exceptions.RedisError:
        return None
    if not raw:
        return None
    try:
        return msgpack.decode(raw, type=OrderValue)
    except Exception:
        return None


# ─── Saga Log Helpers (log-first approach) ───────────────────────────────────

def _saga_log_key(saga_id: str) -> str:
    return f"{SAGA_LOG_PREFIX}{saga_id}"


def _saga_state_key(saga_id: str) -> str:
    return f"{SAGA_STATE_PREFIX}{saga_id}"


def _saga_lock_key(order_id: str) -> str:
    return f"{SAGA_LOCK_PREFIX}{order_id}"


def _saga_in_progress(state: dict | None) -> bool:
    return bool(state) and state.get("status") not in ("completed", "failed")


def _decode_json_value(raw: bytes | None) -> dict | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None


def _decode_lock_value(raw: bytes | None) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return str(raw)


def _encode_saga_log_entry(saga_id: str, entry: dict) -> str:
    payload = dict(entry)
    payload["saga_id"] = saga_id
    payload["timestamp"] = time.time()
    return json.dumps(payload)


def _refresh_saga_lock(order_id: str, lock_token: str | None) -> None:
    '''Refresh the saga lock TTL if the token matches (to prevent premature expiration during long processing).'''
    if not lock_token:
        return
    lock_key = _saga_lock_key(order_id)
    for _attempt in range(3):
        try:
            with db.pipeline() as pipe:
                pipe.watch(lock_key)
                current_token = _decode_lock_value(pipe.get(lock_key))
                if current_token and current_token != lock_token:
                    pipe.unwatch()
                    return
                pipe.multi()
                pipe.set(lock_key, lock_token, ex=SAGA_LOCK_TTL)
                pipe.execute()
                return
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            app.logger.warning("Failed to refresh saga lock for %s", order_id)
            return


def _release_saga_lock(order_id: str, lock_token: str | None) -> None:
    '''Release the saga lock if the token matches (to allow new checkouts after completion).'''
    if not lock_token:
        return
    lock_key = _saga_lock_key(order_id)
    for _attempt in range(3):
        try:
            with db.pipeline() as pipe:
                pipe.watch(lock_key)
                current_token = _decode_lock_value(pipe.get(lock_key))
                if current_token != lock_token:
                    pipe.unwatch()
                    return
                pipe.multi()
                pipe.delete(lock_key)
                pipe.execute()
                return
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            app.logger.warning("Failed to release saga lock for %s", order_id)
            return


def append_saga_log(saga_id: str, entry: dict) -> None:
    """Append a log entry to this saga's Redis log list. Called BEFORE the action (log-first)."""
    try:
        db.rpush(_saga_log_key(saga_id), _encode_saga_log_entry(saga_id, entry))
    except redis.exceptions.RedisError:
        app.logger.error("Failed to append saga log for %s: %s", saga_id, entry)


def get_saga_log(saga_id: str) -> list[dict]:
    """Retrieve all log entries for a saga."""
    try:
        raw_entries = db.lrange(_saga_log_key(saga_id), 0, -1)
    except redis.exceptions.RedisError:
        return []
    entries = []
    for raw in raw_entries:
        try:
            entries.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return entries


def get_saga_state(saga_id: str) -> dict | None:
    '''Retrieve the current state of a saga. Returns None if not found or on error.'''
    try:
        raw = db.get(_saga_state_key(saga_id))
    except redis.exceptions.RedisError:
        return None
    return _decode_json_value(raw)


def set_saga_state(saga_id: str, state: dict) -> bool:
    """Persist the current state of a saga. Returns True on success."""
    try:
        db.set(_saga_state_key(saga_id), json.dumps(state))
        return True
    except redis.exceptions.RedisError:
        app.logger.error("Failed to persist saga state for %s", saga_id)
        return False


def _notify_saga_done(saga_id: str, result: str) -> None:
    """Push notification for the waiting checkout request (cross-process via Redis)."""
    notify_key = f"saga:notify:{saga_id}"
    try:
        db.rpush(notify_key, result)
        db.expire(notify_key, SAGA_NOTIFY_TTL)
    except redis.exceptions.RedisError:
        app.logger.warning("Failed to notify saga completion for %s", saga_id)


def _expire_saga_keys(saga_id: str) -> None:
    """Set TTL on saga state and log keys after reaching terminal state."""
    try:
        db.expire(_saga_state_key(saga_id), SAGA_COMPLETED_TTL)
        db.expire(_saga_log_key(saga_id), SAGA_COMPLETED_TTL)
    except redis.exceptions.RedisError:
        pass


# ─── Kafka Producer ──────────────────────────────────────────────────────────

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
                retries=3, #Possible tuneable parameter for better resilience in case of transient Kafka issues??
            )
        except NoBrokersAvailable:
            app.logger.warning("Kafka broker unavailable for producer")
            return None
    return _producer


def send_to_topic(topic: str, message: dict) -> bool:
    """Send a message to a specific Kafka topic (service out-queue)."""
    producer = get_producer()
    if producer is None:
        return False
    key = message.get("saga_id") or message.get("order_id")
    try:
        producer.send(topic, value=message, key=key)
        producer.flush()
    except KafkaError:
        app.logger.exception("Failed to send to topic %s", topic)
        return False
    return True


# ─── REST Endpoints ──────────────────────────────────────────────────────────

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


# This endpoint is for testing/demo purposes: it creates n orders with random items and users.
@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs = {f"{i}": msgpack.encode(generate_entry()) for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


@app.get('/health')
def health():
    '''Health check endpoint to verify Redis connectivity.'''
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return Response("Redis unavailable", status=503)
    return Response("OK", status=200)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    quantity = int(quantity)
    try:
        item_reply = stock_stub.FindItem(
            services_pb2.FindItemRequest(item_id=item_id)
        )
    except grpc.RpcError as e:
        abort(400, f"Item: {item_id} does not exist! [{e.details()}]")

    # Atomic read-modify-write with guards for paid/in-flight checkout
    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id, f"{SAGA_LOCK_PREFIX}{order_id}")
                raw = pipe.get(order_id)
                if not raw:
                    abort(400, f"Order: {order_id} not found!")
                order_entry = msgpack.decode(raw, type=OrderValue)
                if order_entry.paid:
                    pipe.unwatch()
                    abort(400, f"Order: {order_id} is already paid!")
                lock_val = pipe.get(f"{SAGA_LOCK_PREFIX}{order_id}")
                if lock_val:
                    pipe.unwatch()
                    abort(400, f"Order: {order_id} checkout is in progress!")
                order_entry.items.append((item_id, quantity))
                order_entry.total_cost += quantity * item_reply.price
                pipe.multi()
                pipe.set(order_id, msgpack.encode(order_entry))
                pipe.execute()
                return Response(
                    f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200,
                )
        except redis.exceptions.WatchError:
            continue
    return abort(400, DB_ERROR_STR)


# ─── Sync Checkout (fallback when Kafka is unavailable) ─────────────────────

def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        try:
            stock_stub.AddStock(
                services_pb2.AddStockRequest(item_id=item_id, quantity=quantity)
            )
        except grpc.RpcError:
            app.logger.error(f"Rollback failed for item {item_id}")


# This is not the main async checkout flow, but a fallback for when TRANSACTION_MODE is set to "sync" (e.g. if Kafka is unavailable). It directly executes the checkout logic in a blocking way without going through the saga orchestration.
def checkout_sync(order_id: str):
    app.logger.debug(f"Checking out {order_id} (sync mode)")
    order_entry: OrderValue = get_order_from_db(order_id)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        try:
            stock_reply = stock_stub.SubtractStock(
                services_pb2.SubtractStockRequest(item_id=item_id, quantity=quantity)
            )
        except grpc.RpcError as e:
            rollback_stock(removed_items)
            abort(400, f"Stock error: {e.details()}")

        if not stock_reply.success:
            rollback_stock(removed_items)
            abort(400, f"Insufficient stock for {item_id}")
        removed_items.append((item_id, quantity))

    try:
        pay_reply = payment_stub.Pay(
            services_pb2.PayRequest(user_id=order_entry.user_id, amount=order_entry.total_cost)
        )
    except grpc.RpcError as e:
        rollback_stock(removed_items)
        abort(400, f"Payment service error: {e.details()}")

    if not pay_reply.success:
        rollback_stock(removed_items)
        abort(400, "User out of credit")

    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        # Rollback payment and stock on final DB write failure
        try:
            payment_stub.Refund(
                services_pb2.PayRequest(user_id=order_entry.user_id, amount=order_entry.total_cost)
            )
        except grpc.RpcError:
            app.logger.error("Failed to rollback payment during sync checkout for %s", order_id)
        rollback_stock(removed_items)
        return abort(400, DB_ERROR_STR)
    return Response("Checkout successful", status=200)


# ─── Saga Orchestrator ──────────────────────────────────────────────────────
# The order service is the orchestrator: it drives the saga by sending commands
# to stock and payment in-queues and reacting to events from their out-queues.
#
# Saga steps:
#   1. ReserveStock  → stock-commands (in-queue)  → StockReserved  (stock-events out-queue)
#   2. ChargePayment → payment-commands (in-queue) → PaymentCharged (payment-events out-queue)
#   3. If any step fails → compensate from the beginning:
#        RollbackPayment (if charged) → RollbackStock → saga failed
#
# Every action is logged to Redis BEFORE execution (log-first approach).
# Each log entry carries the saga_id (= order_id).

def _build_saga_start(order_id: str, order_entry: OrderValue) -> tuple[dict, dict, list[str]]:
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items = [{"item_id": item_id, "quantity": quantity} for item_id, quantity in items_quantities.items()]

    attempt_id = str(uuid.uuid4())
    stock_idem_key = str(uuid.uuid4())
    payment_idem_key = str(uuid.uuid4())
    lock_token = str(uuid.uuid4())

    state = {
        "saga_id": order_id,
        "order_id": order_id,
        "attempt_id": attempt_id,
        "lock_token": lock_token,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": items,
        "status": "started",
        "step": "reserve_stock",
        "stock_idem_key": stock_idem_key,
        "payment_idem_key": payment_idem_key,
        "reason": "",
        "last_updated": time.time(),
    }

    command = {
        "message_id": str(uuid.uuid4()),
        "type": "ReserveStock",
        "saga_id": order_id,
        "order_id": order_id,
        "attempt_id": attempt_id,
        "items": items,
        "idempotency_key": stock_idem_key,
    }

    log_entries = [
        _encode_saga_log_entry(order_id, {
            "action": "START_SAGA",
            "order_id": order_id,
            "attempt_id": attempt_id,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
            "items": items,
        }),
        _encode_saga_log_entry(order_id, {
            "action": "SEND_RESERVE_STOCK",
            "items": items,
            "idempotency_key": stock_idem_key,
        }),
        _encode_saga_log_entry(order_id, {
            "action": "COMPENSATION_PLAN_STOCK",
            "items": items,
            "idempotency_key": stock_idem_key,
            "description": "Rollback stock reservation for all items",
        }),
    ]
    return state, command, log_entries


def _start_saga(order_id: str) -> tuple[bool, str]:
    """Atomically create the initial saga state and lock before sending the first command."""
    saga_id = order_id
    lock_key = _saga_lock_key(order_id)
    state_key = _saga_state_key(saga_id)
    log_key = _saga_log_key(saga_id)

    state: dict | None = None
    command: dict | None = None

    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id, lock_key, state_key)
                raw_order = pipe.get(order_id)
                if not raw_order:
                    pipe.unwatch()
                    return False, f"Order: {order_id} not found!"
                try:
                    order_entry = msgpack.decode(raw_order, type=OrderValue)
                except Exception:
                    pipe.unwatch()
                    return False, DB_ERROR_STR
                if order_entry.paid:
                    pipe.unwatch()
                    return False, "Order already paid"

                existing_state = _decode_json_value(pipe.get(state_key))
                if _saga_in_progress(existing_state):
                    pipe.unwatch()
                    _refresh_saga_lock(order_id, existing_state.get("lock_token"))
                    return False, "Checkout already in progress for this order"

                raw_lock = pipe.get(lock_key)
                state, command, log_entries = _build_saga_start(order_id, order_entry)

                pipe.multi()
                if raw_lock:
                    pipe.delete(lock_key)
                pipe.set(lock_key, state["lock_token"], ex=SAGA_LOCK_TTL)
                pipe.set(state_key, json.dumps(state))
                pipe.delete(log_key)
                if log_entries:
                    pipe.rpush(log_key, *log_entries)
                pipe.execute()
                break
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
    else:
        return False, DB_ERROR_STR

    if not state or not command:
        return False, DB_ERROR_STR

    if not send_to_topic(STOCK_COMMAND_TOPIC, command):
        append_saga_log(saga_id, {"action": "FAIL_SAGA", "reason": "Kafka unavailable"})
        state["status"] = "failed"
        state["reason"] = "Kafka unavailable"
        set_saga_state(saga_id, state)
        _release_saga_lock(order_id, state.get("lock_token"))
        return False, "Failed to start saga"

    app.logger.info("Saga %s started – ReserveStock sent to stock in-queue", saga_id)
    return True, ""


def _wait_for_saga(saga_id: str) -> tuple[bool, str]:
    """Block until the saga reaches a terminal state or times out."""
    notify_key = f"saga:notify:{saga_id}"

    # Quick check if already completed before blocking
    state = get_saga_state(saga_id)
    if state:
        status = state.get("status")
        if status == "completed":
            return True, ""
        if status == "failed":
            return False, state.get("reason", "Saga failed")

    # Efficient block-wait via Redis BLPOP (no busy-polling)
    try:
        result = db.blpop(notify_key, timeout=int(SAGA_TIMEOUT_SECONDS))
    except redis.exceptions.RedisError:
        result = None

    if result is not None:
        _, value = result
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if value == "completed":
            return True, ""
        if value.startswith("failed:"):
            return False, value[7:]
        return False, value

    # Timeout — final state check (in case notification was missed)
    state = get_saga_state(saga_id)
    if state:
        if state.get("status") == "completed":
            return True, ""
        if state.get("status") == "failed":
            return False, state.get("reason", "Saga failed")
    return False, "Checkout timed out"


def _send_with_retry(topic: str, command: dict, max_retries: int = 3) -> bool:
    """Send a Kafka message with retries. Returns True on success."""
    for attempt in range(max_retries):
        if send_to_topic(topic, command):
            return True
        time.sleep(0.2 * (attempt + 1))
    return False

def _should_process_event(event: dict, state: dict | None, saga_id: str) -> bool:
    if not state:
        return False

    # Ignore terminal sagas
    if state.get("status") in ("completed", "failed"):
        return False

    # Ignore stale events from an older attempt
    event_attempt_id = event.get("attempt_id")
    state_attempt_id = state.get("attempt_id")
    if event_attempt_id and state_attempt_id and event_attempt_id != state_attempt_id:
        app.logger.warning(
            "Ignoring stale event from attempt %s (current: %s) for saga %s",
            event_attempt_id,
            state_attempt_id,
            saga_id,
        )
        return False

    return True


def _build_charge_payment_command(saga_id: str, state: dict) -> dict:
    return {
        "message_id": str(uuid.uuid4()),
        "type": "ChargePayment",
        "saga_id": saga_id,
        "order_id": state["order_id"],
        "attempt_id": state.get("attempt_id", ""),
        "user_id": state["user_id"],
        "amount": state["total_cost"],
        "idempotency_key": state["payment_idem_key"],
    }


def _build_rollback_payment_command(saga_id: str, state: dict) -> dict:
    return {
        "message_id": str(uuid.uuid4()),
        "type": "RollbackPayment",
        "saga_id": saga_id,
        "order_id": state["order_id"],
        "attempt_id": state.get("attempt_id", ""),
        "user_id": state["user_id"],
        "amount": state["total_cost"],
        "idempotency_key": state["payment_idem_key"],
    }


def _build_rollback_stock_command(saga_id: str, state: dict) -> dict:
    return {
        "message_id": str(uuid.uuid4()),
        "type": "RollbackStock",
        "saga_id": saga_id,
        "order_id": state["order_id"],
        "attempt_id": state.get("attempt_id", ""),
        "items": state["items"],
        "idempotency_key": state["stock_idem_key"],
    }


def _finish_saga_failed(saga_id: str, state: dict, reason: str) -> None:
    append_saga_log(saga_id, {
        "action": "FAIL_SAGA",
        "reason": reason,
    })
    state["status"] = "failed"
    state["step"] = "done"
    state["reason"] = reason
    state["last_updated"] = time.time()
    set_saga_state(saga_id, state)
    _release_saga_lock(state["order_id"], state.get("lock_token"))
    _notify_saga_done(saga_id, f"failed:{reason}")
    _expire_saga_keys(saga_id)


def _finish_saga_completed(saga_id: str, state: dict) -> None:
    append_saga_log(saga_id, {"action": "COMPLETE_SAGA"})
    state["status"] = "completed"
    state["step"] = "done"
    state["last_updated"] = time.time()
    set_saga_state(saga_id, state)
    _release_saga_lock(state["order_id"], state.get("lock_token"))
    _notify_saga_done(saga_id, "completed")
    _expire_saga_keys(saga_id)


def _transition_to_charge_payment(saga_id: str, state: dict) -> None:
    app.logger.info("Saga %s stock reserved – proceeding to payment", saga_id)
    state["step"] = "charge_payment"
    state["last_updated"] = time.time()
    set_saga_state(saga_id, state)

    append_saga_log(saga_id, {
        "action": "SEND_CHARGE_PAYMENT",
        "user_id": state["user_id"],
        "amount": state["total_cost"],
        "idempotency_key": state["payment_idem_key"],
    })

    append_saga_log(saga_id, {
        "action": "COMPENSATION_PLAN_PAYMENT",
        "user_id": state["user_id"],
        "amount": state["total_cost"],
        "idempotency_key": state["payment_idem_key"],
        "description": "Rollback payment charge for user",
    })

    command = _build_charge_payment_command(saga_id, state)
    if not _send_with_retry(PAYMENT_COMMAND_TOPIC, command):
        app.logger.error("Saga %s failed to send ChargePayment – will retry via recovery", saga_id)


def _transition_to_rollback_stock(saga_id: str, state: dict, reason: str) -> None:
    app.logger.info("Saga %s entering rollback_stock: %s", saga_id, reason)
    state["status"] = "compensating"
    state["step"] = "rollback_stock"
    state["reason"] = reason
    state["last_updated"] = time.time()
    set_saga_state(saga_id, state)

    append_saga_log(saga_id, {
        "action": "SEND_ROLLBACK_STOCK",
        "items": state["items"],
        "idempotency_key": state["stock_idem_key"],
    })

    command = _build_rollback_stock_command(saga_id, state)
    if not _send_with_retry(STOCK_COMMAND_TOPIC, command):
        app.logger.error("Saga %s failed to send RollbackStock – will retry via recovery", saga_id)


def _transition_to_rollback_payment(saga_id: str, state: dict, reason: str) -> None:
    app.logger.info("Saga %s entering rollback_payment: %s", saga_id, reason)
    state["status"] = "compensating"
    state["step"] = "rollback_payment"
    state["reason"] = reason
    state["last_updated"] = time.time()
    set_saga_state(saga_id, state)

    append_saga_log(saga_id, {
        "action": "SEND_ROLLBACK_PAYMENT",
        "user_id": state["user_id"],
        "amount": state["total_cost"],
        "idempotency_key": state["payment_idem_key"],
    })

    command = _build_rollback_payment_command(saga_id, state)
    if not _send_with_retry(PAYMENT_COMMAND_TOPIC, command):
        app.logger.error("Saga %s failed to send RollbackPayment – will retry via recovery", saga_id)


def _handle_stock_reserved(saga_id: str, state: dict, event: dict) -> None:
    if state.get("step") != "reserve_stock":
        app.logger.warning(
            "Ignoring StockReserved for saga %s in step %s",
            saga_id,
            state.get("step"),
        )
        return

    success = event.get("success", False)
    reason = event.get("reason", "")

    append_saga_log(saga_id, {
        "action": "RECV_STOCK_RESERVED",
        "success": success,
        "reason": reason,
    })

    if not success:
        app.logger.info("Saga %s stock reserve failed: %s", saga_id, reason)
        _finish_saga_failed(saga_id, state, reason or "Stock reservation failed")
        return

    _transition_to_charge_payment(saga_id, state)


def _handle_payment_charged(saga_id: str, state: dict, event: dict) -> None:
    if state.get("step") != "charge_payment":
        app.logger.warning(
            "Ignoring PaymentCharged for saga %s in step %s",
            saga_id,
            state.get("step"),
        )
        return

    success = event.get("success", False)
    reason = event.get("reason", "")

    append_saga_log(saga_id, {
        "action": "RECV_PAYMENT_CHARGED",
        "success": success,
        "reason": reason,
    })

    if not success:
        app.logger.info("Saga %s payment failed: %s – rolling back stock", saga_id, reason)
        _transition_to_rollback_stock(saga_id, state, reason or "Payment failed")
        return

    app.logger.info("Saga %s payment succeeded – completing saga", saga_id)

    order_entry = _get_order_entry(state["order_id"])
    if order_entry is None:
        app.logger.error("Saga %s order not found – compensating", saga_id)
        _transition_to_rollback_payment(saga_id, state, "Order not found after payment")
        return

    order_entry.paid = True
    try:
        db.set(state["order_id"], msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        app.logger.error("Saga %s order update failed – compensating", saga_id)
        _transition_to_rollback_payment(saga_id, state, "Order update failed")
        return

    _finish_saga_completed(saga_id, state)


def _handle_payment_rolled_back(saga_id: str, state: dict, event: dict) -> None:
    if state.get("step") != "rollback_payment":
        app.logger.warning(
            "Ignoring PaymentRolledBack for saga %s in step %s",
            saga_id,
            state.get("step"),
        )
        return

    success = event.get("success", False)
    reason = event.get("reason", "")

    append_saga_log(saga_id, {
        "action": "RECV_PAYMENT_ROLLED_BACK",
        "success": success,
        "reason": reason,
    })

    if not success:
        app.logger.error("Saga %s payment rollback failed: %s – retrying", saga_id, reason)
        command = _build_rollback_payment_command(saga_id, state)
        if not _send_with_retry(PAYMENT_COMMAND_TOPIC, command):
            app.logger.error("Saga %s failed to re-send RollbackPayment", saga_id)
        return

    app.logger.info("Saga %s payment rolled back – rolling back stock", saga_id)
    _transition_to_rollback_stock(
        saga_id,
        state,
        state.get("reason", "Payment rolled back, proceeding to stock rollback"),
    )


def _handle_stock_rolled_back(saga_id: str, state: dict, event: dict) -> None:
    if state.get("step") != "rollback_stock":
        app.logger.warning(
            "Ignoring StockRolledBack for saga %s in step %s",
            saga_id,
            state.get("step"),
        )
        return

    success = event.get("success", False)
    reason = event.get("reason", "")

    append_saga_log(saga_id, {
        "action": "RECV_STOCK_ROLLED_BACK",
        "success": success,
        "reason": reason,
    })

    if not success:
        app.logger.error("Saga %s stock rollback failed: %s – retrying", saga_id, reason)
        command = _build_rollback_stock_command(saga_id, state)
        if not _send_with_retry(STOCK_COMMAND_TOPIC, command):
            app.logger.error("Saga %s failed to re-send RollbackStock", saga_id)
        return

    app.logger.info("Saga %s fully compensated – marking failed", saga_id)
    _finish_saga_failed(
        saga_id,
        state,
        state.get("reason", "Compensated and failed"),
    )

def _handle_event(event: dict) -> None:
    event_type = event.get("type")
    saga_id = event.get("saga_id") or event.get("order_id")
    if not saga_id:
        return

    state = get_saga_state(saga_id)
    if not _should_process_event(event, state, saga_id):
        return

    handlers = {
        "StockReserved": _handle_stock_reserved,
        "PaymentCharged": _handle_payment_charged,
        "PaymentRolledBack": _handle_payment_rolled_back,
        "StockRolledBack": _handle_stock_rolled_back,
    }

    handler = handlers.get(event_type)
    if handler is None:
        return

    handler(saga_id, state, event)

# ─── Kafka Event Consumer (reads from stock & payment out-queues) ────────────

def _event_consumer_loop() -> None:
    if not KAFKA_AVAILABLE:
        return
    while True:
        try:
            consumer = KafkaConsumer(
                STOCK_EVENT_TOPIC,
                PAYMENT_EVENT_TOPIC,
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda v: v.decode("utf-8") if v else None,
                group_id="order-saga-events",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            for message in consumer:
                try:
                    _handle_event(message.value)
                    consumer.commit()
                except Exception:
                    app.logger.exception("Failed to handle saga event")
        except NoBrokersAvailable:
            app.logger.warning("Kafka unavailable for event consumer; retrying in 1s")
            time.sleep(1.0)
        except Exception:
            app.logger.exception("Event consumer failed; retrying in 1s")
            time.sleep(1.0)


def _start_event_consumer() -> None:
    if not KAFKA_AVAILABLE:
        return
    thread = threading.Thread(target=_event_consumer_loop, daemon=True)
    thread.start()


# ─── Saga Recovery Thread ────────────────────────────────────────────────────
# Periodically scans for sagas stuck in non-terminal states and re-sends the
# appropriate command. This handles cases where a Kafka send failed silently,
# a consumer crashed after processing but before sending a response, etc.

SAGA_RECOVERY_INTERVAL = float(os.environ.get("SAGA_RECOVERY_INTERVAL", "5"))
SAGA_STALE_THRESHOLD = float(os.environ.get("SAGA_STALE_THRESHOLD", "8"))


def _recover_saga(saga_id: str, state: dict) -> None:
    """Re-send the appropriate command based on current saga step."""
    step = state.get("step")
    order_id = state.get("order_id")
    attempt_id = state.get("attempt_id", "")
    original_last_updated = state.get("last_updated", 0)
    app.logger.info("Recovering saga %s stuck at step=%s, with state=%s", saga_id, step, state)

    if order_id:
        _refresh_saga_lock(order_id, state.get("lock_token"))

    if step == "reserve_stock":
        command = {
            "message_id": str(uuid.uuid4()),
            "type": "ReserveStock",
            "saga_id": saga_id,
            "order_id": order_id,
            "attempt_id": attempt_id,
            "items": state["items"],
            "idempotency_key": state["stock_idem_key"],
        }
        _send_with_retry(STOCK_COMMAND_TOPIC, command)

    elif step == "charge_payment":
        command = {
            "message_id": str(uuid.uuid4()),
            "type": "ChargePayment",
            "saga_id": saga_id,
            "order_id": order_id,
            "attempt_id": attempt_id,
            "user_id": state["user_id"],
            "amount": state["total_cost"],
            "idempotency_key": state["payment_idem_key"],
        }
        _send_with_retry(PAYMENT_COMMAND_TOPIC, command)

    elif step == "rollback_payment":
        command = {
            "message_id": str(uuid.uuid4()),
            "type": "RollbackPayment",
            "saga_id": saga_id,
            "order_id": order_id,
            "attempt_id": attempt_id,
            "user_id": state["user_id"],
            "amount": state["total_cost"],
            "idempotency_key": state["payment_idem_key"],
        }
        _send_with_retry(PAYMENT_COMMAND_TOPIC, command)

    elif step == "rollback_stock":
        command = {
            "message_id": str(uuid.uuid4()),
            "type": "RollbackStock",
            "saga_id": saga_id,
            "order_id": order_id,
            "attempt_id": attempt_id,
            "items": state["items"],
            "idempotency_key": state["stock_idem_key"],
        }
        _send_with_retry(STOCK_COMMAND_TOPIC, command)

    # Atomically update last_updated only if state hasn't been modified concurrently
    state_key = _saga_state_key(saga_id)
    for _cas_attempt in range(3):
        try:
            with db.pipeline() as pipe:
                pipe.watch(state_key)
                raw = pipe.get(state_key)
                current = _decode_json_value(raw) if raw else None
                if not current or current.get("status") in ("completed", "failed"):
                    pipe.unwatch()
                    return
                if current.get("last_updated", 0) != original_last_updated:
                    pipe.unwatch()
                    return  # State was updated concurrently, skip
                current["last_updated"] = time.time()
                pipe.multi()
                pipe.set(state_key, json.dumps(current))
                pipe.execute()
                return
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return


def _saga_recovery_loop() -> None:
    """Background loop that scans for stuck sagas and recovers them."""
    if not KAFKA_AVAILABLE:
        return
    while True:
        try:
            time.sleep(SAGA_RECOVERY_INTERVAL)
            now = time.time()
            # Scan all saga state keys
            for key in db.scan_iter(match=f"{SAGA_STATE_PREFIX}*", count=100):
                try:
                    raw = db.get(key)
                    if not raw:
                        continue
                    state = json.loads(raw)
                    status = state.get("status")
                    if status in ("completed", "failed"):
                        continue
                    last_updated = state.get("last_updated", 0)
                    if now - last_updated > SAGA_STALE_THRESHOLD:
                        saga_id = state.get("saga_id")
                        if saga_id:
                            _recover_saga(saga_id, state)
                except Exception:
                    app.logger.exception("Error recovering saga from key %s", key)
        except Exception:
            app.logger.exception("Saga recovery loop error")
            time.sleep(SAGA_RECOVERY_INTERVAL)


def _start_saga_recovery() -> None:
    if not KAFKA_AVAILABLE:
        return
    thread = threading.Thread(target=_saga_recovery_loop, daemon=True)
    thread.start()


# ─── Checkout Endpoint ───────────────────────────────────────────────────────

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    # Sync fallback when Kafka is unavailable
    if TRANSACTION_MODE == "sync":
        return checkout_sync(order_id)
    
    # Handle empty items list in checkout
    order_entry = get_order_from_db(order_id)
    if not order_entry.items:
        if order_entry.paid:
            return Response("Order already paid", status=200)
        order_entry.paid = True
        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        return Response("Checkout successful", status=200)

    started, reason = _start_saga(order_id)
    if not started:
        if reason == "Order already paid":
            return Response("Order already paid", status=200)
        return abort(400, reason)

    ok, reason = _wait_for_saga(order_id)

    # Only release lock if saga reached terminal state
    state = get_saga_state(order_id)
    if state and state.get("status") in ("completed", "failed"):
        _release_saga_lock(order_id, state.get("lock_token"))
    
    if ok:
        return Response("Checkout successful", status=200)

    if state and state.get("status") not in ("completed", "failed"):
        step = state.get("step")

        if step == "reserve_stock":
            _transition_to_rollback_stock(
                order_id,
                state,
                reason or "Checkout timed out",
            )

        elif step == "charge_payment":
            # We have reserved stock, payment may be in air or suc without notification, regardless rollback
            _transition_to_rollback_payment(
                order_id,
                state,
                reason or "Checkout timed out",
            )

        elif step == "rollback_payment":
            # Already compensating
            pass

        elif step == "rollback_stock":
            # Already compensating
            pass

        else:
            _finish_saga_failed(
                order_id,
                state,
                reason or "Checkout timed out",
            )
                
    return abort(400, reason)


_background_services_started = False
_background_services_lock = threading.Lock()


def start_background_services() -> None:
    global _background_services_started
    if not KAFKA_AVAILABLE:
        return
    with _background_services_lock:
        if _background_services_started:
            return
        _start_event_consumer()
        _start_saga_recovery()
        _background_services_started = True

if __name__ == '__main__':
    start_background_services()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
