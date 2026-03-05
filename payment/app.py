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

app = Flask("payment-service")

def make_redis_client() -> redis.Redis:
    # REDIS_SENTINELS="host1:26379,host2:26379"
    sentinels_raw = os.environ["REDIS_SENTINELS"]
    sentinel_addrs = []
    for part in sentinels_raw.split(","):
        host, port = part.strip().split(":")
        sentinel_addrs.append((host, int(port)))

    master_name = os.environ["REDIS_MASTER_NAME"]
    password = os.environ.get("REDIS_PASSWORD")  # password for Redis master/replicas
    db_index = int(os.environ.get("REDIS_DB", "0"))

    # sentinel_kwargs: auth to talk to Sentinel itself (only needed if you set requirepass for sentinel)
    # If your sentinel does NOT require auth, leave it empty.
    sentinel = Sentinel(
        sentinel_addrs,
        socket_timeout=1.0,
        sentinel_kwargs={},  # e.g. {"password": os.environ["SENTINEL_PASSWORD"]}
    )

    # This returns a Redis client that always targets the CURRENT master.
    return sentinel.master_for(
        service_name=master_name,
        password=password,
        db=db_index,
        socket_timeout=1.0,
        retry_on_timeout=True,
        decode_responses=False,  # keep bytes because you msgpack encode/decode
    )

db: redis.Redis = make_redis_client()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes | None = db.get(user_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except RedisError:
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
    except RedisError:
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


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
