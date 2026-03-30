import atexit
import json
import logging
import os
import threading
import time

import redis
from flask import Flask, abort, jsonify, request

try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import KafkaError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except Exception:
    KafkaConsumer = None
    KafkaProducer = None
    KafkaError = Exception
    NoBrokersAvailable = Exception
    KAFKA_AVAILABLE = False

from storage import Storage
from workflow import CheckoutWorkflow
from saga import SagaEngine


KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
STOCK_EVENT_TOPIC = os.environ.get("STOCK_EVENT_TOPIC", "stock-events")
PAYMENT_EVENT_TOPIC = os.environ.get("PAYMENT_EVENT_TOPIC", "payment-events")
TX_TIMEOUT_SECONDS = float(os.environ.get("TX_TIMEOUT_SECONDS", "10"))
RECOVERY_INTERVAL = float(os.environ.get("RECOVERY_INTERVAL", "5"))
STALE_THRESHOLD = float(os.environ.get("STALE_THRESHOLD", "8"))

app = Flask("orchestrator")


redis_client = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db() -> None:
    redis_client.close()


atexit.register(close_db)


class Dispatcher:
    def __init__(self) -> None:
        self._producer = None

    def _get_producer(self):
        if not KAFKA_AVAILABLE:
            return None
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if v else None,
            )
        return self._producer

    def send(self, topic: str, message: dict) -> bool:
        producer = self._get_producer()
        if producer is None:
            return False
        key = message.get("transaction_id") or message.get("order_id")
        try:
            producer.send(topic, value=message, key=key)
            producer.flush()
            return True
        except KafkaError:
            app.logger.exception("Failed to send to topic %s", topic)
            return False

    def send_with_retry(self, topic: str, message: dict, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            if self.send(topic, message):
                return True
            time.sleep(0.2 * (attempt + 1))
        return False


storage = Storage(redis_client, lock_ttl=max(int(TX_TIMEOUT_SECONDS) * 5, 60))
workflow = CheckoutWorkflow()
dispatcher = Dispatcher()
saga = SagaEngine(storage, dispatcher, workflow, timeout_s=TX_TIMEOUT_SECONDS)


@app.get("/health")
def health():
    try:
        redis_client.ping()
    except redis.exceptions.RedisError:
        return "Redis unavailable", 503
    return "OK", 200


@app.post("/transactions")
def start_transaction():
    body = request.get_json(force=True)

    protocol = body.get("protocol", "saga")
    if protocol != "saga":
        abort(400, "Unsupported protocol")

    workflow_name = body.get("workflow")
    if workflow_name != "checkout":
        abort(400, "Unsupported workflow")

    tx_id = body["transaction_id"]
    payload = body["payload"]

    try:
        tx = saga.start_transaction(tx_id, payload)
    except ValueError as e:
        abort(400, str(e))

    return jsonify({
        "transaction_id": tx["transaction_id"],
        "status": tx["status"],
        "step": tx["step"],
    }), 201


@app.get("/transactions/<tx_id>")
def get_transaction(tx_id: str):
    tx = storage.get_tx(tx_id)
    if not tx:
        abort(404, "Transaction not found")
    return jsonify(tx)


@app.post("/transactions/<tx_id>/wait")
def wait_transaction(tx_id: str):
    ok, reason = storage.wait_for_done(tx_id, TX_TIMEOUT_SECONDS)
    tx = storage.get_tx(tx_id)

    if not ok and reason == "Transaction timed out" and tx and tx.get("status") not in ("completed", "failed"):
        saga.cancel_transaction(tx_id, reason)
        tx = storage.get_tx(tx_id)

    return jsonify({
        "transaction_id": tx_id,
        "ok": ok,
        "status": tx["status"] if tx else "unknown",
        "step": tx["step"] if tx else "unknown",
        "reason": reason,
    })


@app.post("/events")
def ingest_event():
    event = request.get_json(force=True)
    saga.handle_event(event)
    return jsonify({"accepted": True}), 202


def event_consumer_loop() -> None:
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
                group_id="orchestrator-events",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            for message in consumer:
                try:
                    saga.handle_event(message.value)
                    consumer.commit()
                except Exception:
                    app.logger.exception("Failed to handle event")
        except NoBrokersAvailable:
            app.logger.warning("Kafka unavailable for event consumer; retrying")
            time.sleep(1.0)
        except Exception:
            app.logger.exception("Event consumer loop failed; retrying")
            time.sleep(1.0)


def recovery_loop() -> None:
    while True:
        time.sleep(RECOVERY_INTERVAL)
        for tx in storage.list_stale_txs(STALE_THRESHOLD):
            try:
                saga.recover_transaction(tx)
            except Exception:
                app.logger.exception("Failed recovering tx %s", tx.get("transaction_id"))


_started = False
_started_lock = threading.Lock()


def start_background_threads() -> None:
    global _started
    with _started_lock:
        if _started:
            return
        threading.Thread(target=event_consumer_loop, daemon=True).start()
        threading.Thread(target=recovery_loop, daemon=True).start()
        _started = True


if __name__ == "__main__":
    start_background_threads()
    app.run(host="0.0.0.0", port=8001, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    start_background_threads()