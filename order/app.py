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
#import requests deprecated, instead use grpc
import grpc
import services_pb2
from grpc_clients import stock_stub, payment_stub

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
#REQ_ERROR_STR = "Requests error"
# ^ and v not needed due to gRPC
#GATEWAY_URL = os.environ['GATEWAY_URL']
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
KAFKA_COMMAND_TOPIC = os.environ.get("KAFKA_COMMAND_TOPIC", "saga-commands")
KAFKA_EVENT_TOPIC = os.environ.get("KAFKA_EVENT_TOPIC", "saga-events")
TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "saga").lower()
if TRANSACTION_MODE == "saga" and not KAFKA_AVAILABLE:
    TRANSACTION_MODE = "sync"
SAGA_TIMEOUT_SECONDS = float(os.environ.get("SAGA_TIMEOUT_SECONDS", "10"))
SAGA_RECOVERY_INTERVAL_SECONDS = float(os.environ.get("SAGA_RECOVERY_INTERVAL_SECONDS", "5"))
OUTBOX_RETRY_INTERVAL_SECONDS = float(os.environ.get("OUTBOX_RETRY_INTERVAL_SECONDS", "1"))

SAGA_STATE_PREFIX = "saga:"
SAGA_PROCESSED_EVENTS_SET = "saga:processed_events"
SAGA_OUTBOX_HASH = "saga:outbox"
SAGA_IDEMPOTENCY_PREFIX = "saga:idem:"

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
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
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


def _saga_state_key(order_id: str) -> str:
    return f"{SAGA_STATE_PREFIX}{order_id}"


def get_saga_state(order_id: str) -> dict | None:
    try:
        raw = db.get(_saga_state_key(order_id))
    except redis.exceptions.RedisError:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def set_saga_state(order_id: str, state: dict) -> None:
    try:
        db.set(_saga_state_key(order_id), json.dumps(state))
    except redis.exceptions.RedisError:
        app.logger.error("Failed to persist saga state for %s", order_id)


def _event_already_processed(message_id: str) -> bool:
    try:
        return db.sismember(SAGA_PROCESSED_EVENTS_SET, message_id)
    except redis.exceptions.RedisError:
        return False


def _mark_event_processed(message_id: str) -> None:
    try:
        db.sadd(SAGA_PROCESSED_EVENTS_SET, message_id)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to mark event processed: %s", message_id)


def _outbox_add(command: dict) -> None:
    message_id = command.get("message_id")
    if not message_id:
        return
    try:
        db.hset(SAGA_OUTBOX_HASH, message_id, json.dumps(command))
    except redis.exceptions.RedisError:
        app.logger.error("Failed to enqueue outbox command %s", message_id)


def _outbox_publish_loop() -> None:
    if not KAFKA_AVAILABLE:
        return
    while True:
        try:
            entries = db.hgetall(SAGA_OUTBOX_HASH)
        except redis.exceptions.RedisError:
            time.sleep(OUTBOX_RETRY_INTERVAL_SECONDS)
            continue
        for message_id, raw in entries.items():
            try:
                command = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    db.hdel(SAGA_OUTBOX_HASH, message_id)
                except redis.exceptions.RedisError:
                    pass
                continue
            if _send_command_now(command):
                try:
                    db.hdel(SAGA_OUTBOX_HASH, message_id)
                except redis.exceptions.RedisError:
                    app.logger.error("Failed to delete outbox command %s", message_id)
        time.sleep(OUTBOX_RETRY_INTERVAL_SECONDS)


def _start_outbox_publisher() -> None:
    if not KAFKA_AVAILABLE:
        return
    thread = threading.Thread(target=_outbox_publish_loop, daemon=True)
    thread.start()


def _idem_key(order_id: str, idem_key: str) -> str:
    return f"{SAGA_IDEMPOTENCY_PREFIX}{order_id}:{idem_key}"


def _get_idempotency_result(order_id: str, idem_key: str) -> dict | None:
    try:
        raw = db.get(_idem_key(order_id, idem_key))
    except redis.exceptions.RedisError:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _set_idempotency_result(order_id: str, idem_key: str, status_code: int, body: str, status: str) -> None:
    payload = {"status": status, "status_code": status_code, "body": body}
    try:
        db.set(_idem_key(order_id, idem_key), json.dumps(payload))
    except redis.exceptions.RedisError:
        app.logger.error("Failed to persist idempotency result for %s", order_id)


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


def _send_command_now(command: dict) -> bool:
    producer = get_producer()
    if producer is None:
        return False
    key = command.get("order_id")
    try:
        producer.send(KAFKA_COMMAND_TOPIC, value=command, key=key)
        producer.flush()
    except KafkaError:
        app.logger.exception("Failed to send saga command")
        return False
    return True


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


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
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
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
            "total_cost": order_entry.total_cost
        }
    )

# Not needed when using gRPC
#def send_post_request(url: str):
#    try:
#        response = requests.post(url)
#    except requests.exceptions.RequestException:
#        abort(400, REQ_ERROR_STR)
#    else:
#        return response
#
#
#def send_get_request(url: str):
#    try:
#        response = requests.get(url)
#    except requests.exceptions.RequestException:
#        abort(400, REQ_ERROR_STR)
#    else:
#        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
   
    # Deprecated in gRPC
    #item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    #if item_reply.status_code != 200:
    #    # Request failed because item does not exist
    #    abort(400, f"Item: {item_id} does not exist!")
    #item_json: dict = item_reply.json()

    try:
        item_reply = stock_stub.FindItem(
            services_pb2.FindItemRequest(item_id=item_id)
        )
    except grpc.RpcError as e:
        abort(400, f"Item: {item_id} does not exist! [{e.details()}]")

    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_reply.price
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        # Deprecated in gRPC
        #send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")
        
        try:
            stock_stub.AddStock(
                services_pb2.AddStockRequest(item_id=item_id, quantity=quantity)
            )
        except grpc.RpcError:
            app.logger.error(f"Rollback failed for item {item_id}")


def checkout_sync(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        # Deprecated in gRPC
        #stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        try:
            stock_reply = stock_stub.SubtractStock(
                services_pb2.SubtractStockRequest(item_id=item_id, quantity=quantity)
            )
        except grpc.RpcError as e:
            rollback_stock(removed_items)
            abort(400, "User out of credit")
        
        if not stock_reply.success:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, "User out of credit")
        removed_items.append((item_id, quantity))

    # Deprecated in gRPC
    #user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")

    try:
        pay_reply = payment_stub.Pay(
            services_pb2.PayRequest(
                user_id = order_entry.user_id,
                amount = order_entry.total_cost,
            )
        )
    except grpc.RpcError as e:
        rollback_stock(removed_items)
        abort(400, f"Payment service error: {e.details()}")

    if not pay_reply.success:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


def _start_saga(order_id: str, order_entry: OrderValue, items_quantities: dict[str, int]) -> bool:
    state = {
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": [{"item_id": k, "quantity": v} for k, v in items_quantities.items()],
        "status": "pending",
        "step": "reserve_stock",
        "compensations": {"stock": "not_started"},
        "reason": "",
        "last_update": time.time(),
    }
    set_saga_state(order_id, state)
    command = {
        "message_id": str(uuid.uuid4()),
        "type": "ReserveStock",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "items": state["items"],
        "timestamp": time.time(),
    }
    _outbox_add(command)
    app.logger.info("Saga %s started, command enqueued %s", order_id, command["message_id"])
    return True


def _wait_for_saga(order_id: str) -> tuple[bool, str]:
    deadline = time.time() + SAGA_TIMEOUT_SECONDS
    while time.time() < deadline:
        state = get_saga_state(order_id)
        if not state:
            time.sleep(0.1)
            continue
        status = state.get("status")
        if status == "completed":
            return True, ""
        if status == "failed":
            return False, state.get("reason", "Saga failed")
        time.sleep(0.1)
    return False, "Checkout timed out"


def _handle_event(event: dict) -> None:
    message_id = event.get("message_id")
    if not message_id or _event_already_processed(message_id):
        return
    _mark_event_processed(message_id)

    event_type = event.get("type")
    order_id = event.get("order_id")
    if not order_id:
        return

    state = get_saga_state(order_id)
    if not state:
        return

    if event_type == "StockReserved":
        if not event.get("success", False):
            app.logger.info("Saga %s stock reserve failed: %s", order_id, event.get("reason"))
            state["status"] = "failed"
            state["reason"] = event.get("reason", "Stock reservation failed")
            state["last_update"] = time.time()
            set_saga_state(order_id, state)
            return
        app.logger.info("Saga %s stock reserved", order_id)
        state["step"] = "charge_payment"
        state["last_update"] = time.time()
        set_saga_state(order_id, state)
        command = {
            "message_id": str(uuid.uuid4()),
            "type": "ChargePayment",
            "order_id": order_id,
            "user_id": state["user_id"],
            "amount": state["total_cost"],
            "timestamp": time.time(),
        }
        _outbox_add(command)
        app.logger.info("Saga %s charge_payment enqueued %s", order_id, command["message_id"])
        return

    if event_type == "PaymentCharged":
        if not event.get("success", False):
            app.logger.info("Saga %s payment failed: %s", order_id, event.get("reason"))
            state["status"] = "compensating"
            state["reason"] = event.get("reason", "Payment failed")
            state["compensations"]["stock"] = "pending"
            state["last_update"] = time.time()
            set_saga_state(order_id, state)
            command = {
                "message_id": str(uuid.uuid4()),
                "type": "RollbackStock",
                "order_id": order_id,
                "items": state["items"],
                "timestamp": time.time(),
            }
            _outbox_add(command)
            app.logger.info("Saga %s rollback_stock enqueued %s", order_id, command["message_id"])
            return
        app.logger.info("Saga %s payment succeeded", order_id)
        try:
            order_entry: OrderValue = get_order_from_db(order_id)
            order_entry.paid = True
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            state["status"] = "failed"
            state["reason"] = "Order update failed"
            state["last_update"] = time.time()
            set_saga_state(order_id, state)
            return
        state["status"] = "completed"
        state["step"] = "done"
        state["last_update"] = time.time()
        set_saga_state(order_id, state)
        return

    if event_type == "StockRolledBack":
        app.logger.info("Saga %s stock rollback confirmed", order_id)
        state["status"] = "failed"
        state["reason"] = state.get("reason", "Rollback completed")
        state["compensations"]["stock"] = "done"
        state["step"] = "done"
        state["last_update"] = time.time()
        set_saga_state(order_id, state)


def _event_consumer_loop() -> None:
    if not KAFKA_AVAILABLE:
        return
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_EVENT_TOPIC,
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda v: v.decode("utf-8") if v else None,
                group_id="order-saga-events",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            for message in consumer:
                try:
                    _handle_event(message.value)
                except Exception:
                    app.logger.exception("Failed to handle saga event")
        except NoBrokersAvailable:
            app.logger.warning("Kafka broker unavailable for event consumer; retrying")
            time.sleep(1.0)
        except Exception:
            app.logger.exception("Event consumer failed; retrying")
            time.sleep(1.0)


def _start_event_consumer() -> None:
    if not KAFKA_AVAILABLE:
        return
    thread = threading.Thread(target=_event_consumer_loop, daemon=True)
    thread.start()


_start_event_consumer()
_start_outbox_publisher()


def _recover_sagas_loop() -> None:
    while True:
        try:
            cursor = 0
            pattern = f"{SAGA_STATE_PREFIX}*"
            while True:
                cursor, keys = db.scan(cursor=cursor, match=pattern, count=100)
                for key in keys:
                    try:
                        raw = db.get(key)
                    except redis.exceptions.RedisError:
                        continue
                    if not raw:
                        continue
                    try:
                        state = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    status = state.get("status")
                    order_id = state.get("order_id")
                    if not order_id:
                        continue
                    last_update = float(state.get("last_update", 0))
                    if time.time() - last_update < SAGA_RECOVERY_INTERVAL_SECONDS:
                        continue
                    if status == "pending" and state.get("step") == "reserve_stock":
                        command = {
                            "message_id": str(uuid.uuid4()),
                            "type": "ReserveStock",
                            "order_id": order_id,
                            "user_id": state.get("user_id"),
                            "items": state.get("items", []),
                            "timestamp": time.time(),
                        }
                        _outbox_add(command)
                        state["last_update"] = time.time()
                        set_saga_state(order_id, state)
                    if status == "pending" and state.get("step") == "charge_payment":
                        command = {
                            "message_id": str(uuid.uuid4()),
                            "type": "ChargePayment",
                            "order_id": order_id,
                            "user_id": state.get("user_id"),
                            "amount": state.get("total_cost"),
                            "timestamp": time.time(),
                        }
                        _outbox_add(command)
                        state["last_update"] = time.time()
                        set_saga_state(order_id, state)
                    if status == "compensating" and state.get("compensations", {}).get("stock") != "done":
                        command = {
                            "message_id": str(uuid.uuid4()),
                            "type": "RollbackStock",
                            "order_id": order_id,
                            "items": state.get("items", []),
                            "timestamp": time.time(),
                        }
                        _outbox_add(command)
                        state["last_update"] = time.time()
                        set_saga_state(order_id, state)
                if cursor == 0:
                    break
        except redis.exceptions.RedisError:
            pass
        time.sleep(SAGA_RECOVERY_INTERVAL_SECONDS)


def _start_recovery_loop() -> None:
    thread = threading.Thread(target=_recover_sagas_loop, daemon=True)
    thread.start()


_start_recovery_loop()


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    if TRANSACTION_MODE == "sync":
        return checkout_sync(order_id)
    order_entry: OrderValue = get_order_from_db(order_id)
    if order_entry.paid:
        return Response("Order already paid", status=200)
    idem_key = None
    try:
        from flask import request
        idem_key = request.headers.get("Idempotency-Key")
    except Exception:
        idem_key = None
    if idem_key:
        existing = _get_idempotency_result(order_id, idem_key)
        if existing:
            status = existing.get("status")
            if status == "completed" or status == "failed":
                return Response(existing.get("body", ""), status=int(existing.get("status_code", 400)))
            if status == "in_progress":
                ok, reason = _wait_for_saga(order_id)
                if ok:
                    result = _get_idempotency_result(order_id, idem_key)
                    if result:
                        return Response(result.get("body", ""), status=int(result.get("status_code", 400)))
                return abort(400, "Checkout in progress")
        _set_idempotency_result(order_id, idem_key, 202, "Checkout in progress", "in_progress")
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    if not _start_saga(order_id, order_entry, items_quantities):
        if idem_key:
            _set_idempotency_result(order_id, idem_key, 400, "Kafka unavailable", "failed")
        return abort(400, "Kafka unavailable")
    ok, reason = _wait_for_saga(order_id)
    if not ok:
        if idem_key:
            _set_idempotency_result(order_id, idem_key, 400, reason, "failed")
        return abort(400, reason)
    if idem_key:
        _set_idempotency_result(order_id, idem_key, 200, "Checkout successful", "completed")
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
