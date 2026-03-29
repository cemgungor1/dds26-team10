import logging
import os
import atexit
import json
import threading
import time
import uuid

import redis
from redis.exceptions import RedisError

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
KAFKA_COMMAND_TOPIC = os.environ.get("KAFKA_COMMAND_TOPIC", "payment-commands")
KAFKA_EVENT_TOPIC = os.environ.get("KAFKA_EVENT_TOPIC", "payment-events")
PROCESSED_MSG_PREFIX = "payment:processed:"
PAYMENT_CHARGED_PREFIX = "payment:charged:"
PAYMENT_ROLLEDBACK_PREFIX = "payment:rolledback:"
PAYMENT_2PC_PREPARED_PREFIX = "payment:2pc:prepared:"
PAYMENT_2PC_COMMITTED_PREFIX = "payment:2pc:committed:"
PAYMENT_2PC_ABORTED_PREFIX = "payment:2pc:aborted:"
IDEMPOTENCY_TTL = int(os.environ.get("IDEMPOTENCY_TTL", "3600"))


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


@app.get('/health')
def health():
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return Response("Redis unavailable", status=503)
    return Response("OK", status=200)


@app.post('/add_funds/<user_id>/<amount>')
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


@app.post('/pay/<user_id>/<amount>')
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
    """Atomically deduct credit and set the idempotency marker in one Redis transaction."""
    amount = int(amount)
    retries = 3
    while retries > 0:
        retries -= 1
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                user_entry = _get_user_entry(user_id)
                if user_entry is None:
                    pipe.unwatch()
                    return False, "User not found"
                user_entry.credit -= amount
                if user_entry.credit < 0:
                    pipe.unwatch()
                    return False, "User out of credit"
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.set(charged_key, json.dumps({"user_id": user_id, "amount": amount}), ex=IDEMPOTENCY_TTL)
                pipe.execute()
                return True, ""
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, "DB error"
    return False, "Concurrent modification"


def _rollback_payment(user_id: str, amount: int, rolled_key: str, charged_key: str) -> tuple[bool, str]:
    """Atomically refund credit and set rollback marker in one Redis transaction."""
    amount = int(amount)
    retries = 3
    while retries > 0:
        retries -= 1
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                user_entry = _get_user_entry(user_id)
                if user_entry is None:
                    pipe.unwatch()
                    return False, "User not found"
                user_entry.credit += amount
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.set(rolled_key, "1", ex=IDEMPOTENCY_TTL)
                pipe.delete(charged_key)
                pipe.execute()
                return True, ""
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, "DB error"
    return False, "Concurrent modification"


def _prepare_payment_2pc(user_id: str, amount: int, transaction_id: str, prepared_key: str) -> tuple[bool, str]:
    lock_key = f"lock:user:{user_id}"
    try:
        lock_owner = db.get(lock_key)
        if lock_owner and lock_owner.decode("utf-8") != transaction_id:
            return False, "User account locked by another transaction"
    except redis.exceptions.RedisError:
        return False, "DB error"

    try:
        if not db.set(lock_key, transaction_id, nx=True, ex=300):
            lock_owner = db.get(lock_key)
            if not lock_owner or lock_owner.decode("utf-8") != transaction_id:
                return False, "User account locked by another transaction"
    except redis.exceptions.RedisError:
        return False, "DB error"

    user_entry = _get_user_entry(user_id)
    if user_entry is None:
        try:
            db.delete(lock_key)
        except redis.exceptions.RedisError:
            pass
        return False, "User not found"

    if user_entry.credit < int(amount):
        try:
            owner = db.get(lock_key)
            if owner and owner.decode("utf-8") == transaction_id:
                db.delete(lock_key)
        except redis.exceptions.RedisError:
            pass
        return False, "User out of credit"

    payload = json.dumps({"user_id": user_id, "amount": int(amount), "transaction_id": transaction_id})
    try:
        db.set(prepared_key, payload, ex=IDEMPOTENCY_TTL)
    except redis.exceptions.RedisError:
        try:
            owner = db.get(lock_key)
            if owner and owner.decode("utf-8") == transaction_id:
                db.delete(lock_key)
        except redis.exceptions.RedisError:
            pass
        return False, "DB error"
    return True, ""


def _commit_payment_2pc(user_id: str, amount: int, transaction_id: str, prepared_key: str, committed_key: str) -> tuple[bool, str]:
    retries = 3
    while retries > 0:
        retries -= 1
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                user_entry = _get_user_entry(user_id)
                if user_entry is None:
                    pipe.unwatch()
                    return False, "User not found"
                if user_entry.credit < int(amount):
                    pipe.unwatch()
                    return False, "User out of credit"
                user_entry.credit -= int(amount)
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.set(committed_key, "1", ex=IDEMPOTENCY_TTL)
                pipe.delete(prepared_key)
                pipe.execute()
                break
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, "DB error"
    else:
        return False, "Concurrent modification"

    lock_key = f"lock:user:{user_id}"
    try:
        lock_owner = db.get(lock_key)
        if lock_owner and lock_owner.decode("utf-8") == transaction_id:
            db.delete(lock_key)
    except redis.exceptions.RedisError:
        return False, "DB error"
    return True, ""


def _abort_payment_2pc(user_id: str, transaction_id: str, prepared_key: str, aborted_key: str) -> tuple[bool, str]:
    lock_key = f"lock:user:{user_id}"
    try:
        with db.pipeline() as pipe:
            pipe.multi()
            pipe.set(aborted_key, "1", ex=IDEMPOTENCY_TTL)
            pipe.delete(prepared_key)
            pipe.execute()
    except redis.exceptions.RedisError:
        return False, "DB error"

    try:
        lock_owner = db.get(lock_key)
        if lock_owner and lock_owner.decode("utf-8") == transaction_id:
            db.delete(lock_key)
    except redis.exceptions.RedisError:
        return False, "DB error"
    return True, ""


def _handle_command(command: dict) -> None:
    message_id = command.get("message_id")
    if not message_id:
        return

    command_type = command.get("type")
    order_id = command.get("order_id")
    saga_id = command.get("saga_id") or order_id
    user_id = command.get("user_id")
    amount = command.get("amount")
    transaction_id = command.get("transaction_id")
    attempt_id = command.get("attempt_id", "")

    # ── PreparePayment (2PC) ─────────────────────────────────────────────
    if command_type == "PreparePayment":
        if _message_processed(message_id):
            return
        if not order_id or not transaction_id or user_id is None or amount is None:
            return

        prepared_key = f"{PAYMENT_2PC_PREPARED_PREFIX}{transaction_id}"
        committed_key = f"{PAYMENT_2PC_COMMITTED_PREFIX}{transaction_id}"
        try:
            if db.exists(committed_key):
                ok, reason = True, "Already committed"
            elif db.exists(prepared_key):
                ok, reason = True, "Already prepared"
            else:
                ok, reason = _prepare_payment_2pc(user_id, int(amount), transaction_id, prepared_key)
        except redis.exceptions.RedisError:
            ok, reason = False, "DB error"

        event = {
            "message_id": str(uuid.uuid4()),
            "type": "PreparePaymentResult",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "success": ok,
            "reason": reason,
            "timestamp": time.time(),
        }
        if send_event(event):
            _mark_message_processed(message_id)
        return

    # ── CommitPayment (2PC) ──────────────────────────────────────────────
    if command_type == "CommitPayment":
        if _message_processed(message_id):
            return
        if not order_id or not transaction_id or user_id is None or amount is None:
            return

        prepared_key = f"{PAYMENT_2PC_PREPARED_PREFIX}{transaction_id}"
        committed_key = f"{PAYMENT_2PC_COMMITTED_PREFIX}{transaction_id}"
        try:
            if db.exists(committed_key):
                ok, reason = True, "Already committed"
            elif not db.exists(prepared_key):
                ok, reason = True, "No reservation"
            else:
                ok, reason = _commit_payment_2pc(user_id, int(amount), transaction_id, prepared_key, committed_key)
        except redis.exceptions.RedisError:
            ok, reason = False, "DB error"

        event = {
            "message_id": str(uuid.uuid4()),
            "type": "CommitPaymentResult",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "success": ok,
            "reason": reason,
            "timestamp": time.time(),
        }
        if send_event(event):
            _mark_message_processed(message_id)
        return

    # ── AbortPayment (2PC) ───────────────────────────────────────────────
    if command_type == "AbortPayment":
        if _message_processed(message_id):
            return
        if not order_id or not transaction_id or user_id is None:
            return

        prepared_key = f"{PAYMENT_2PC_PREPARED_PREFIX}{transaction_id}"
        aborted_key = f"{PAYMENT_2PC_ABORTED_PREFIX}{transaction_id}"
        try:
            if db.exists(aborted_key):
                ok, reason = True, "Already aborted"
            elif not db.exists(prepared_key):
                ok, reason = True, "No reservation"
            else:
                ok, reason = _abort_payment_2pc(user_id, transaction_id, prepared_key, aborted_key)
        except redis.exceptions.RedisError:
            ok, reason = False, "DB error"

        event = {
            "message_id": str(uuid.uuid4()),
            "type": "AbortPaymentResult",
            "order_id": order_id,
            "transaction_id": transaction_id,
            "attempt_id": attempt_id,
            "success": ok,
            "reason": reason,
            "timestamp": time.time(),
        }
        if send_event(event):
            _mark_message_processed(message_id)
        return

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

# 2PC functions for Payment App

# Prepare phase - Acquire lock and verify credit
@app.post('/prepare/pay/<user_id>/<amount>/<transaction_id>')
def prepare_pay(user_id: str, amount: int, transaction_id: str):
    app.logger(f"Prepare phase for Payment Service {user_id}")

    lock_key = f"lock:user:{user_id}"
    lock_value = transaction_id

    # Try to acquire lock
    locked = db.set(lock_key, lock_value, nx=True, ex=300) # Expire after 5 mins
    if not locked:
        return Response(f"User account locked by another transaction", status=409)

    # Now that we have the lock, we check for credit
    try:
        user_entry: UserValue = get_user_from_db(user_id)
    except:
        db.delete(lock_key)
        raise

    # check if sufficient amount is present
    if user_entry.credit < amount:
        return Response(f"Insufficient credit for user {user_id}")
    
    # Store the payment reservation with lock held
    reservation_key = f"payment_reservation:{transaction_id}"
    reservation_data = msgpack.encode({"user_id": user_id, "amount": int(amount)})
    try:
        db.setex(reservation_key, 300, reservation_data)  # 5 min TTL
    except redis.exceptions.RedisError:
        db.delete(lock_key)  # Release lock on error
        return abort(400, DB_ERROR_STR)
    
    app.logger.debug(f"Payment prepared for user {user_id}, transaction {transaction_id}")
    return Response("PREPARED", status=200)

# COMMIT phase - Deduct credit and release lock
@app.post('/commit/pay/<transaction_id>')
def commit_pay(transaction_id: str):
    reservation_key = f"payment_reservation:{transaction_id}"
    reservation_data = db.get(reservation_key)
    
    if not reservation_data:
        return abort(400, "Transaction not found")
    
    data = msgpack.decode(reservation_data, type=dict)
    user_id = data["user_id"]
    amount = data["amount"]
    
    # Perform the actual credit deduction
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= amount
    
    try:
        db.set(user_id, msgpack.encode(user_entry))
        db.delete(reservation_key)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    # Release the lock
    lock_key = f"lock:user:{user_id}"
    db.delete(lock_key)
    
    app.logger.debug(f"Payment committed for user {user_id}, transaction {transaction_id}")
    return Response("COMMITTED", status=200)

# ABORT phase - Release reservation and lock
@app.post('/abort/pay/<transaction_id>')
def abort_pay(transaction_id: str):
    reservation_key = f"payment_reservation:{transaction_id}"
    reservation_data = db.get(reservation_key)
    
    if reservation_data:
        data = msgpack.decode(reservation_data, type=dict)
        user_id = data["user_id"]
        
        # Release lock
        lock_key = f"lock:user:{user_id}"
        # Only delete if this transaction owns the lock
        lock_owner = db.get(lock_key)
        if lock_owner and lock_owner.decode() == transaction_id:
            db.delete(lock_key)
        
        db.delete(reservation_key)
    
    app.logger.debug(f"Payment aborted for transaction {transaction_id}")
    return Response("ABORTED", status=200)



if __name__ == '__main__':
    start_background_services()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
