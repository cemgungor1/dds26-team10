import logging
import os
import atexit
import random
import requests
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


DB_ERROR_STR = "DB error"

ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://orchestrator:8000")

TRANSACTION_MODE = os.environ.get("TRANSACTION_MODE", "saga").lower()

SAGA_TIMEOUT_SECONDS = float(os.environ.get("SAGA_TIMEOUT_SECONDS", "10"))

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

    # Atomic read-modify-write with guard only for already-paid orders
    for _attempt in range(5):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id)

                raw = pipe.get(order_id)
                if not raw:
                    pipe.unwatch()
                    abort(400, f"Order: {order_id} not found!")

                order_entry = msgpack.decode(raw, type=OrderValue)

                if order_entry.paid:
                    pipe.unwatch()
                    abort(400, f"Order: {order_id} is already paid!")

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


# ─── Checkout Endpoint ───────────────────────────────────────────────────────

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    # Handle empty cart locally
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

    # Decide protocol
    if TRANSACTION_MODE == "2pc":
        protocol = "2pc"
    else:
        protocol = "saga"

    try:
        orch_resp = requests.post(
            f"{ORCHESTRATOR_URL}/transactions",
            json={
                "order_id": order_id,
                "protocol": protocol,
            },
            timeout=30,
        )
    except requests.RequestException as e:
        return abort(400, f"Orchestrator unavailable: {e}")

    if orch_resp.status_code == 200:
        return Response(orch_resp.text, status=200)

    if orch_resp.status_code == 400 and "Order already paid" in orch_resp.text:
        return Response("Order already paid", status=200)

    return abort(400, orch_resp.text)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
