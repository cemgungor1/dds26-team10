import logging
import os
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

    if protocol == "saga":
        return saga.start_transaction(body)
    elif protocol == "2pc":
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

        saga.start_background_workers()
        _background_services_started = True


if __name__ == '__main__':
    start_background_services()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_background_services()

    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

