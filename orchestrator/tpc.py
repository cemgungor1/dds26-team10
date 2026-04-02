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
from redis.exceptions import RedisError
import grpc
import services_pb2
from grpc_clients import stock_stub, payment_stub

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from typing import Any

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


logger = logging.getLogger(__name__)

# Variable for 2PC
CHECKOUT_METHOD = os.getenv("CHECKOUT_METHOD", "2PC").upper()

DB_ERROR_STR = "DB error"

# Kafka configuration - dedicated in/out queues per service
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
STOCK_COMMAND_TOPIC = os.environ.get("STOCK_COMMAND_TOPIC", "stock-commands")    # out-queue → stock in-queue
STOCK_EVENT_TOPIC = os.environ.get("STOCK_EVENT_TOPIC", "stock-events")          # stock out-queue → in-queue
PAYMENT_COMMAND_TOPIC = os.environ.get("PAYMENT_COMMAND_TOPIC", "payment-commands")  # out-queue → payment in-queue
PAYMENT_EVENT_TOPIC = os.environ.get("PAYMENT_EVENT_TOPIC", "payment-events")        # payment out-queue → in-queue

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "saga").lower()
if (TRANSACTION_MODE == "saga" or TRANSACTION_MODE == "2pc") and not KAFKA_AVAILABLE :
    TRANSACTION_MODE = "sync"

TPC_TIMEOUT_SECONDS = float(os.environ.get("SAGA_TIMEOUT_SECONDS", "10"))
TPC_NOTIFY_TTL = 60
TPC_LOCK_TTL = max(int(TPC_TIMEOUT_SECONDS) * 5, 60)
TPC_COMPLETED_TTL = 1000
# Redis key prefixes for saga logging
TPC_STATE_PREFIX = "tpc:state:"
TPC_LOCK_PREFIX = "tpc:lock:"
TPC_NOTIFY_PREFIX = "tpc:notify:"

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


def _release_tpc_lock(order_id: str, lock_token: str | None) -> None:
    if not lock_token:
        return
    lock_key = _tpc_lock_key(order_id)
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
            return



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
            logger.warning("Kafka broker unavailable for producer")
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
        logger.exception("Failed to send to topic %s", topic)
        return False
    return True


# 2PC Checkout (Kafka-based)

def checkout_2pc(order_id: str) -> Response:
    return checkout_kafka_2pc(order_id)

# ─── Sync Checkout (fallback when Kafka is unavailable) ─────────────────────

def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        try:
            stock_stub.AddStock(
                services_pb2.AddStockRequest(item_id=item_id, quantity=quantity)
            )
        except grpc.RpcError:
            logger.error(f"Rollback failed for item {item_id}")


# This is not the main async checkout flow, but a fallback for when TRANSACTION_MODE is set to "sync" (e.g. if Kafka is unavailable). It directly executes the checkout logic in a blocking way without going through the saga orchestration.
def checkout_sync(order_id: str):
    logger.debug(f"Checking out {order_id} (sync mode)")
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
            logger.error("Failed to rollback payment during sync checkout for %s", order_id)
        rollback_stock(removed_items)
        return abort(400, DB_ERROR_STR)
    return Response("Checkout successful", status=200)


def _send_with_retry(topic: str, command: dict, max_retries: int = 3) -> bool:
    """Send a Kafka message with retries. Returns True on success."""
    for attempt in range(max_retries):
        if send_to_topic(topic, command):
            return True
        time.sleep(0.2 * (attempt + 1))
    return False


# # ─── Kafka Event Consumer (reads from stock & payment out-queues) ────────────

# def _event_consumer_loop() -> None:
#     if not KAFKA_AVAILABLE:
#         return
#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 STOCK_EVENT_TOPIC,
#                 PAYMENT_EVENT_TOPIC,
#                 bootstrap_servers=KAFKA_BROKERS.split(","),
#                 value_deserializer=lambda v: json.loads(v.decode("utf-8")),
#                 key_deserializer=lambda v: v.decode("utf-8") if v else None,
#                 group_id="order-saga-events",
#                 auto_offset_reset="earliest",
#                 enable_auto_commit=False,
#             )
#             for message in consumer:
#                 try:
#                     _handle_tpc_event(message.value)
#                     consumer.commit()
#                 except Exception:
#                     logger.exception("Failed to handle tpc event")
#         except NoBrokersAvailable:
#             logger.warning("Kafka unavailable for event consumer; retrying in 1s")
#             time.sleep(1.0)
#         except Exception:
#             logger.exception("Event consumer failed; retrying in 1s")
#             time.sleep(1.0)


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


# ─── Kafka 2PC Coordinator ──────────────────────────────────────────────────

def _tpc_state_key(order_id: str) -> str:
    return f"{TPC_STATE_PREFIX}{order_id}"


def _tpc_lock_key(order_id: str) -> str:
    return f"{TPC_LOCK_PREFIX}{order_id}"


def _tpc_notify_key(order_id: str) -> str:
    return f"{TPC_NOTIFY_PREFIX}{order_id}"


def _build_tpc_start(order_id: str, order_entry: OrderValue) -> tuple[dict, dict, dict]:
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items = [{"item_id": item_id, "quantity": quantity} for item_id, quantity in items_quantities.items()]

    transaction_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    state = {
        "order_id": order_id,
        "transaction_id": transaction_id,
        "attempt_id": attempt_id,
        "lock_token": str(uuid.uuid4()),
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": items,
        "status": "preparing",
        "step": "wait_prepare",
        "decision": "",
        "reason": "",
        "votes": {
            "stock": None,
            "payment": None,
            "stock_reason": "",
            "payment_reason": "",
        },
        "acks": {
            "stock": False,
            "payment": False,
        },
        "stock_prepare_idem": str(uuid.uuid4()),
        "payment_prepare_idem": str(uuid.uuid4()),
        "stock_decision_idem": str(uuid.uuid4()),
        "payment_decision_idem": str(uuid.uuid4()),
        "last_updated": time.time(),
    }

    stock_prepare = {
        "message_id": str(uuid.uuid4()),
        "type": "PrepareStock",
        "order_id": order_id,
        "transaction_id": transaction_id,
        "attempt_id": attempt_id,
        "items": items,
        "idempotency_key": state["stock_prepare_idem"],
    }
    payment_prepare = {
        "message_id": str(uuid.uuid4()),
        "type": "PreparePayment",
        "order_id": order_id,
        "transaction_id": transaction_id,
        "attempt_id": attempt_id,
        "user_id": order_entry.user_id,
        "amount": order_entry.total_cost,
        "idempotency_key": state["payment_prepare_idem"],
    }
    return state, stock_prepare, payment_prepare


def _persist_tpc_state(order_id: str, state: dict) -> bool:
    state["last_updated"] = time.time()
    try:
        db.set(_tpc_state_key(order_id), json.dumps(state))
        return True
    except redis.exceptions.RedisError:
        return False


def _tpc_send_decision_commands(order_id: str, state: dict) -> None:
    decision = state.get("decision")
    transaction_id = state["transaction_id"]
    attempt_id = state.get("attempt_id", "")

    if decision == "commit":
        stock_cmd = {
            "message_id": str(uuid.uuid4()),
            "type": "CommitStock",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "items": state["items"],
            "idempotency_key": state["stock_decision_idem"],
        }
        payment_cmd = {
            "message_id": str(uuid.uuid4()),
            "type": "CommitPayment",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "user_id": state["user_id"],
            "amount": state["total_cost"],
            "idempotency_key": state["payment_decision_idem"],
        }
    else:
        stock_cmd = {
            "message_id": str(uuid.uuid4()),
            "type": "AbortStock",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "items": state["items"],
            "idempotency_key": state["stock_decision_idem"],
        }
        payment_cmd = {
            "message_id": str(uuid.uuid4()),
            "type": "AbortPayment",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "user_id": state["user_id"],
            "amount": state["total_cost"],
            "idempotency_key": state["payment_decision_idem"],
        }

    _send_with_retry(STOCK_COMMAND_TOPIC, stock_cmd)
    _send_with_retry(PAYMENT_COMMAND_TOPIC, payment_cmd)


def _start_tpc(order_id: str) -> tuple[bool, str]:
    lock_key = _tpc_lock_key(order_id)
    state_key = _tpc_state_key(order_id)
    state: dict | None = None
    stock_prepare: dict | None = None
    payment_prepare: dict | None = None

    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id, lock_key, state_key)
                raw_order = pipe.get(order_id)
                if not raw_order:
                    pipe.unwatch()
                    return False, f"Order: {order_id} not found!"

                order_entry = msgpack.decode(raw_order, type=OrderValue)
                if order_entry.paid:
                    pipe.unwatch()
                    return False, "Order already paid"

                existing_state = _decode_json_value(pipe.get(state_key))
                if existing_state and existing_state.get("status") not in ("completed", "failed"):
                    pipe.unwatch()
                    return False, "Checkout already in progress for this order"

                state, stock_prepare, payment_prepare = _build_tpc_start(order_id, order_entry)

                pipe.multi()
                pipe.set(lock_key, state["lock_token"], ex=TPC_LOCK_TTL)
                pipe.set(state_key, json.dumps(state))
                pipe.execute()
                break
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
    else:
        return False, DB_ERROR_STR

    if not state or not stock_prepare or not payment_prepare:
        return False, DB_ERROR_STR

    stock_sent = _send_with_retry(STOCK_COMMAND_TOPIC, stock_prepare)
    payment_sent = _send_with_retry(PAYMENT_COMMAND_TOPIC, payment_prepare)
    if stock_sent and payment_sent:
        return True, ""

    state["status"] = "failed"
    state["step"] = "done"
    state["reason"] = "Failed to send prepare commands"
    _persist_tpc_state(order_id, state)
    _release_tpc_lock(order_id, state.get("lock_token"))
    try:
        db.rpush(_tpc_notify_key(order_id), f"failed:{state['reason']}")
        db.expire(_tpc_notify_key(order_id), TPC_NOTIFY_TTL)
    except redis.exceptions.RedisError:
        pass
    return False, state["reason"]


def _finish_tpc(order_id: str, state: dict, success: bool, reason: str = "") -> None:
    state["status"] = "completed" if success else "failed"
    state["step"] = "done"
    state["reason"] = reason
    _persist_tpc_state(order_id, state)
    _release_tpc_lock(order_id, state.get("lock_token"))

    notify_value = "completed" if success else f"failed:{reason or '2PC failed'}"
    try:
        db.rpush(_tpc_notify_key(order_id), notify_value)
        db.expire(_tpc_notify_key(order_id), TPC_NOTIFY_TTL)
        db.expire(_tpc_state_key(order_id), TPC_COMPLETED_TTL)
    except redis.exceptions.RedisError:
        pass


def _handle_tpc_prepare_event(state: dict, event: dict) -> None:
    order_id = state["order_id"]
    event_type = event.get("type")
    success = bool(event.get("success", False))
    reason = event.get("reason", "")

    if event_type == "PrepareStockResult":
        state["votes"]["stock"] = success
        state["votes"]["stock_reason"] = reason
    elif event_type == "PreparePaymentResult":
        state["votes"]["payment"] = success
        state["votes"]["payment_reason"] = reason
    else:
        return

    stock_vote = state["votes"].get("stock")
    payment_vote = state["votes"].get("payment")
    if stock_vote is None or payment_vote is None:
        _persist_tpc_state(order_id, state)
        return

    if stock_vote and payment_vote:
        state["decision"] = "commit"
        state["step"] = "wait_commit_ack"
        state["status"] = "committing"
    else:
        state["decision"] = "abort"
        state["step"] = "wait_abort_ack"
        state["status"] = "aborting"
        reason = state["votes"].get("stock_reason") or state["votes"].get("payment_reason") or "Participant voted NO"
        state["reason"] = reason

    _persist_tpc_state(order_id, state)
    _tpc_send_decision_commands(order_id, state)


def _handle_tpc_decision_ack(state: dict, event: dict) -> None:
    order_id = state["order_id"]
    event_type = event.get("type")
    success = bool(event.get("success", False))
    reason = event.get("reason", "")
    decision = state.get("decision")

    if decision == "commit":
        if event_type == "CommitStockResult":
            state["acks"]["stock"] = success
            if not success and reason:
                state["reason"] = reason
        elif event_type == "CommitPaymentResult":
            state["acks"]["payment"] = success
            if not success and reason:
                state["reason"] = reason
        else:
            return

        _persist_tpc_state(order_id, state)
        if state["acks"]["stock"] and state["acks"]["payment"]:
            order_entry = _get_order_entry(order_id)
            if order_entry is None:
                _finish_tpc(order_id, state, False, "Order not found after commit")
                return
            order_entry.paid = True
            try:
                db.set(order_id, msgpack.encode(order_entry))
            except redis.exceptions.RedisError:
                _finish_tpc(order_id, state, False, "Order update failed")
                return
            _finish_tpc(order_id, state, True)
        return

    if decision == "abort":
        if event_type == "AbortStockResult":
            state["acks"]["stock"] = success
        elif event_type == "AbortPaymentResult":
            state["acks"]["payment"] = success
        else:
            return

        _persist_tpc_state(order_id, state)
        if state["acks"]["stock"] and state["acks"]["payment"]:
            _finish_tpc(order_id, state, False, state.get("reason", "2PC aborted"))


def _handle_tpc_event(event: dict) -> bool:
    order_id = event.get("order_id")
    if not order_id:
        return False

    state = _decode_json_value(db.get(_tpc_state_key(order_id)))
    if not state:
        return False
    if state.get("status") in ("completed", "failed"):
        return True

    event_attempt_id = event.get("attempt_id")
    state_attempt_id = state.get("attempt_id")
    if event_attempt_id and state_attempt_id and event_attempt_id != state_attempt_id:
        return True

    event_type = event.get("type")
    if event_type in ("PrepareStockResult", "PreparePaymentResult"):
        _handle_tpc_prepare_event(state, event)
        return True
    if event_type in ("CommitStockResult", "CommitPaymentResult", "AbortStockResult", "AbortPaymentResult"):
        _handle_tpc_decision_ack(state, event)
        return True
    return False


def _wait_for_tpc(order_id: str) -> tuple[bool, str]:
    notify_key = _tpc_notify_key(order_id)

    state = _decode_json_value(db.get(_tpc_state_key(order_id)))
    if state:
        status = state.get("status")
        if status == "completed":
            return True, ""
        if status == "failed":
            return False, state.get("reason", "2PC failed")

    try:
        result = db.blpop(notify_key, timeout=int(TPC_TIMEOUT_SECONDS))
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

    state = _decode_json_value(db.get(_tpc_state_key(order_id)))
    if state and state.get("status") == "completed":
        return True, ""
    if state and state.get("status") == "failed":
        return False, state.get("reason", "2PC failed")
    return False, "Checkout timed out"


def _recover_tpc(order_id: str, state: dict) -> None:
    step = state.get("step")
    if step == "wait_prepare":
        stock_prepare = {
            "message_id": str(uuid.uuid4()),
            "type": "PrepareStock",
            "order_id": order_id,
            "transaction_id": state["transaction_id"],
            "attempt_id": state.get("attempt_id", ""),
            "items": state["items"],
            "idempotency_key": state["stock_prepare_idem"],
        }
        payment_prepare = {
            "message_id": str(uuid.uuid4()),
            "type": "PreparePayment",
            "order_id": order_id,
            "transaction_id": state["transaction_id"],
            "attempt_id": state.get("attempt_id", ""),
            "user_id": state["user_id"],
            "amount": state["total_cost"],
            "idempotency_key": state["payment_prepare_idem"],
        }
        _send_with_retry(STOCK_COMMAND_TOPIC, stock_prepare)
        _send_with_retry(PAYMENT_COMMAND_TOPIC, payment_prepare)
    elif step in ("wait_commit_ack", "wait_abort_ack"):
        _tpc_send_decision_commands(order_id, state)

    _persist_tpc_state(order_id, state)


def checkout_kafka_2pc(order_id: str) -> Response:
    # Handle empty cart same as saga path
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

    started, reason = _start_tpc(order_id)
    if not started:
        if reason == "Order already paid":
            return Response("Order already paid", status=200)
        return abort(400, reason)

    ok, reason = _wait_for_tpc(order_id)
    if ok:
        return Response("Checkout successful", status=200)
    return abort(400, reason)


def handle_event(event: dict) -> bool:
    return _handle_tpc_event(event)


# ─── Checkout Endpoint ───────────────────────────────────────────────────────

def checkout(order_id: str):
    return checkout_2pc(order_id)

def start_background_services() -> None:
    _start_event_consumer()

def start_transaction(body):
    order_id = body.get("order_id")
    if not order_id:
        abort(400, "Missing order_id")
    return checkout(order_id)
