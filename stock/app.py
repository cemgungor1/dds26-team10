import logging
import os
import atexit
import uuid

import redis
from redis.exceptions import RedisError
from redis.sentinel import Sentinel


from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

app = Flask("stock-service")


def _make_redis_client(
    host_var: str = "REDIS_HOST",
    port_var: str = "REDIS_PORT",
    password_var: str = "REDIS_PASSWORD",
    db_var: str = "REDIS_DB",
    sentinels_var: str = "REDIS_SENTINELS",
    sentinel_master_var: str = "REDIS_MASTER_NAME",
) -> redis.Redis:
    sentinels_raw = os.environ.get(sentinels_var)
    if sentinels_raw:
        sentinel_addrs: list[tuple[str, int]] = []
        for part in sentinels_raw.split(","):
            host, port = part.strip().split(":")
            sentinel_addrs.append((host, int(port)))

        master_name = os.environ[sentinel_master_var]
        password = os.environ.get(password_var)
        db_index = int(os.environ.get(db_var, "0"))

        sentinel = Sentinel(
            sentinel_addrs,
            socket_timeout=1.0,
            sentinel_kwargs={},
        )
        return sentinel.master_for(
            service_name=master_name,
            password=password,
            db=db_index,
            socket_timeout=1.0,
            retry_on_timeout=True,
            decode_responses=False,
        )

    host = os.environ[host_var]
    port = int(os.environ.get(port_var, "6379"))
    password = os.environ.get(password_var)
    db_index = int(os.environ.get(db_var, "0"))
    return redis.Redis(
        host=host,
        port=port,
        password=password,
        db=db_index,
        socket_timeout=1.0,
        retry_on_timeout=True,
    )


db: redis.Redis = _make_redis_client()



def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue:
    # get serialized data
    try:
        entry: bytes | None = db.get(item_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
