import os
import time
from typing import Any

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel


SOCKET_CONNECT_TIMEOUT = 0.5
SOCKET_TIMEOUT = 1.0
HEALTH_CHECK_INTERVAL = 5
FAILOVER_TIMEOUT_SECONDS = 4.0
WAIT_TIMEOUT_MS = 2000
MAX_REQUIRED_REPLICAS = 2
HEALTHY_REPLICA_LAG = 1
RETRY_ATTEMPTS = 3
BACKOFF_BASE = 0.1
BACKOFF_CAP = 1.0


def _retryable_errors() -> tuple[type[BaseException], ...]:
    errors: list[type[BaseException] | None] = [
        redis.exceptions.ConnectionError,
        redis.exceptions.TimeoutError,
        redis.exceptions.ReadOnlyError,
        getattr(redis.exceptions, "MasterDownError", None),
        getattr(redis.exceptions, "MasterNotFoundError", None),
        getattr(redis.exceptions, "SlaveNotFoundError", None),
    ]
    return tuple(error for error in errors if error is not None)


RETRYABLE_ERRORS = _retryable_errors()


def _build_retry() -> Retry:
    return Retry(ExponentialBackoff(base=BACKOFF_BASE, cap=BACKOFF_CAP), RETRY_ATTEMPTS)


class RedisHAClient:
    def __init__(self, client: redis.Redis):
        self._client = client

    def close(self) -> None:
        self._client.close()

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_with_failover(self._client.get, *args, **kwargs)

    def set(self, *args: Any, **kwargs: Any) -> Any:
        return self._write_with_wait("set", *args, **kwargs)

    def mset(self, *args: Any, **kwargs: Any) -> Any:
        return self._write_with_wait("mset", *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return self._call_with_failover(attr, *args, **kwargs)

        return wrapped

    def _call_with_failover(self, operation, *args: Any, **kwargs: Any) -> Any:
        last_error: BaseException | None = None
        deadline = time.monotonic() + FAILOVER_TIMEOUT_SECONDS

        for attempt in range(RETRY_ATTEMPTS + 1):
            try:
                return operation(*args, **kwargs)
            except RETRYABLE_ERRORS as exc:
                last_error = exc
                self._disconnect_pool()
                if attempt >= RETRY_ATTEMPTS:
                    break

                delay = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_CAP)
                if time.monotonic() + delay > deadline:
                    break
                time.sleep(delay)

        if last_error is not None:
            raise last_error
        raise redis.exceptions.RedisError("Redis operation failed without an exception")

    def _disconnect_pool(self) -> None:
        try:
            self._client.connection_pool.disconnect()
        except Exception:
            pass

    def _write_with_wait(self, command: str, *args: Any, **kwargs: Any) -> Any:
        def execute_write() -> Any:
            replicas_to_wait_for = min(MAX_REQUIRED_REPLICAS, self._healthy_replica_count())
            pipe = self._client.pipeline(transaction=False)
            try:
                getattr(pipe, command)(*args, **kwargs)
                if replicas_to_wait_for > 0:
                    pipe.wait(replicas_to_wait_for, WAIT_TIMEOUT_MS)
                results = pipe.execute()
            finally:
                pipe.reset()

            if replicas_to_wait_for > 0:
                acknowledgements = int(results[-1])
                if acknowledgements < replicas_to_wait_for:
                    raise redis.exceptions.TimeoutError(
                        "WAIT only acknowledged "
                        f"{acknowledgements}/{replicas_to_wait_for} healthy replicas"
                    )
            return results[0]

        return self._call_with_failover(execute_write)

    def _healthy_replica_count(self) -> int:
        replication_info = self._client.info("replication")
        connected_slaves = int(replication_info.get("connected_slaves", 0))
        healthy_replicas = 0

        for index in range(connected_slaves):
            replica = replication_info.get(f"slave{index}")
            parsed = self._parse_replica(replica)
            if parsed.get("state") != "online":
                continue

            try:
                lag = int(parsed.get("lag", HEALTHY_REPLICA_LAG + 1))
            except (TypeError, ValueError):
                continue

            if lag <= HEALTHY_REPLICA_LAG:
                healthy_replicas += 1

        return healthy_replicas

    @staticmethod
    def _parse_replica(replica: Any) -> dict[str, str]:
        if replica is None:
            return {}
        if isinstance(replica, dict):
            return {str(key): str(value) for key, value in replica.items()}
        if isinstance(replica, bytes):
            replica = replica.decode("utf-8")

        parsed: dict[str, str] = {}
        for part in str(replica).split(","):
            key, _, value = part.partition("=")
            if key:
                parsed[key] = value
        return parsed


def make_redis_client(
    host_var: str = "REDIS_HOST",
    port_var: str = "REDIS_PORT",
    password_var: str = "REDIS_PASSWORD",
    db_var: str = "REDIS_DB",
    sentinels_var: str = "REDIS_SENTINELS",
    sentinel_master_var: str = "REDIS_MASTER_NAME",
) -> RedisHAClient:
    password = os.environ.get(password_var)
    db_index = int(os.environ.get(db_var, "0"))
    retry_on_error = list(RETRYABLE_ERRORS)

    sentinels_raw = os.environ.get(sentinels_var)
    if sentinels_raw:
        sentinel_addrs: list[tuple[str, int]] = []
        for part in sentinels_raw.split(","):
            host, port = part.strip().split(":")
            sentinel_addrs.append((host, int(port)))

        master_name = os.environ[sentinel_master_var]
        sentinel = Sentinel(
            sentinel_addrs,
            socket_timeout=SOCKET_TIMEOUT,
            socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
            sentinel_kwargs={
                "socket_timeout": SOCKET_TIMEOUT,
                "socket_connect_timeout": SOCKET_CONNECT_TIMEOUT,
                "retry": _build_retry(),
                "retry_on_timeout": True,
                "retry_on_error": retry_on_error,
            },
        )
        client = sentinel.master_for(
            service_name=master_name,
            password=password,
            db=db_index,
            socket_timeout=SOCKET_TIMEOUT,
            socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
            health_check_interval=HEALTH_CHECK_INTERVAL,
            retry=_build_retry(),
            retry_on_timeout=True,
            retry_on_error=retry_on_error,
            decode_responses=False,
        )
        return RedisHAClient(client)

    host = os.environ[host_var]
    port = int(os.environ.get(port_var, "6379"))
    client = redis.Redis(
        host=host,
        port=port,
        password=password,
        db=db_index,
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
        health_check_interval=HEALTH_CHECK_INTERVAL,
        retry=_build_retry(),
        retry_on_timeout=True,
        retry_on_error=retry_on_error,
        decode_responses=False,
    )
    return RedisHAClient(client)
