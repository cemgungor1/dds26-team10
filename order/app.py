import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
#import requests deprecated, instead use grpc
import grpc
import services_pb2
from grpc_clients import stock_stub, payment_stub

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
#REQ_ERROR_STR = "Requests error"
# ^ and v not needed due to gRPC
#GATEWAY_URL = os.environ['GATEWAY_URL']

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


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
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


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
