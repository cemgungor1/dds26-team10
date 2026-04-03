import logging
import os
import time
import json
import threading
import saga
import redis
import tpc

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request

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

app = Flask("orchestrator")

TPC_PROTOCOL = os.getenv("TPC_PROTOCOL", "false").lower() == "true"

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']))

DB_ERROR_STR = "DB error"

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int



@app.post("/transactions")
def start_transaction():
    body = request.get_json(force=True)
    protocol = body.get("protocol", "saga")

    requested_protocol = body.get("protocol", "saga")
    protocol = "2pc" if TPC_PROTOCOL else requested_protocol

    if protocol == "saga":
        app.logger.info("Using SAGA protocol")
        return saga.start_transaction(body)
    elif protocol == "2pc":
        app.logger.info("Using TPC protocol")
        return tpc.start_transaction(body)
    else:
        abort(400, "Unsupported protocol")

@app.get('/health')
def health():
    '''Health check endpoint to verify Redis connectivity.'''
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return Response("Redis unavailable", status=503)
    return Response("OK", status=200)


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
        saga.start_recovery_worker()
        _background_services_started = True


def _handle_event(event: dict) -> None:
    if tpc.handle_event(event):
        return
    if saga.handle_event(event):
        return

def _event_consumer_loop() -> None:
    if not KAFKA_AVAILABLE:
        return

    while True:
        try:
            consumer = KafkaConsumer(
                saga.STOCK_EVENT_TOPIC,
                saga.PAYMENT_EVENT_TOPIC,
                bootstrap_servers=saga.KAFKA_BROKERS.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda v: v.decode("utf-8") if v else None,
                group_id="orchestrator-events",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            for message in consumer:
                try:
                    _handle_event(message.value)
                    consumer.commit()
                except Exception:
                    app.logger.exception("Failed to handle orchestrator event")
        except NoBrokersAvailable:
            app.logger.warning("Kafka unavailable for orchestrator event consumer; retrying in 1s")
            time.sleep(1.0)
        except Exception:
            app.logger.exception("Orchestrator event consumer failed; retrying in 1s")
            time.sleep(1.0)

def _start_event_consumer() -> None:
    if not KAFKA_AVAILABLE:
        return
    thread = threading.Thread(target=_event_consumer_loop, daemon=True)
    thread.start()


if __name__ == '__main__':
    start_background_services()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

