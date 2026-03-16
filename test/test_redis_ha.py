import importlib.util
import pathlib
import sys
import types
import unittest
from unittest.mock import patch


def install_fake_redis_modules() -> None:
    redis_module = types.ModuleType("redis")
    backoff_module = types.ModuleType("redis.backoff")
    retry_module = types.ModuleType("redis.retry")
    sentinel_module = types.ModuleType("redis.sentinel")

    class RedisError(Exception):
        pass

    class ConnectionError(RedisError):
        pass

    class TimeoutError(RedisError):
        pass

    class ReadOnlyError(RedisError):
        pass

    class MasterDownError(RedisError):
        pass

    class MasterNotFoundError(RedisError):
        pass

    class SlaveNotFoundError(RedisError):
        pass

    class Retry:
        def __init__(self, backoff, retries):
            self.backoff = backoff
            self.retries = retries

    class ExponentialBackoff:
        def __init__(self, base, cap):
            self.base = base
            self.cap = cap

    class Sentinel:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def master_for(self, *args, **kwargs):
            return types.SimpleNamespace()

    redis_module.Redis = object
    redis_module.exceptions = types.SimpleNamespace(
        RedisError=RedisError,
        ConnectionError=ConnectionError,
        TimeoutError=TimeoutError,
        ReadOnlyError=ReadOnlyError,
        MasterDownError=MasterDownError,
        MasterNotFoundError=MasterNotFoundError,
        SlaveNotFoundError=SlaveNotFoundError,
    )
    backoff_module.ExponentialBackoff = ExponentialBackoff
    retry_module.Retry = Retry
    sentinel_module.Sentinel = Sentinel

    sys.modules["redis"] = redis_module
    sys.modules["redis.backoff"] = backoff_module
    sys.modules["redis.retry"] = retry_module
    sys.modules["redis.sentinel"] = sentinel_module


install_fake_redis_modules()


ORDER_REDIS_HA_PATH = (
    pathlib.Path(__file__).resolve().parents[1] / "order" / "redis_ha.py"
)
SPEC = importlib.util.spec_from_file_location("order_redis_ha", ORDER_REDIS_HA_PATH)
order_redis_ha = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(order_redis_ha)


class FakePool:
    def __init__(self):
        self.disconnected = 0

    def disconnect(self):
        self.disconnected += 1


class FakePipeline:
    def __init__(self, execute_result):
        self.execute_result = execute_result
        self.commands = []
        self.reset_called = False

    def set(self, *args, **kwargs):
        self.commands.append(("set", args, kwargs))

    def mset(self, *args, **kwargs):
        self.commands.append(("mset", args, kwargs))

    def wait(self, replicas, timeout_ms):
        self.commands.append(("wait", (replicas, timeout_ms), {}))

    def execute(self):
        return list(self.execute_result)

    def reset(self):
        self.reset_called = True


class FakeRedis:
    def __init__(self, info_response, execute_result):
        self.info_response = info_response
        self.execute_result = execute_result
        self.connection_pool = FakePool()
        self.closed = False

    def info(self, section):
        return self.info_response

    def pipeline(self, transaction=False):
        self.transaction = transaction
        self.pipeline_instance = FakePipeline(self.execute_result)
        return self.pipeline_instance

    def close(self):
        self.closed = True


class RedisHAClientTests(unittest.TestCase):
    def test_set_waits_for_two_healthy_replicas(self):
        client = FakeRedis(
            {
                "connected_slaves": 2,
                "slave0": "state=online,lag=0",
                "slave1": "state=online,lag=1",
            },
            [True, 2],
        )
        ha_client = order_redis_ha.RedisHAClient(client)

        result = ha_client.set("key", b"value")

        self.assertTrue(result)
        self.assertEqual(
            client.pipeline_instance.commands,
            [
                ("set", ("key", b"value"), {}),
                ("wait", (2, order_redis_ha.WAIT_TIMEOUT_MS), {}),
            ],
        )
        self.assertTrue(client.pipeline_instance.reset_called)

    def test_set_waits_for_one_healthy_replica(self):
        client = FakeRedis(
            {
                "connected_slaves": 2,
                "slave0": "state=online,lag=1",
                "slave1": "state=online,lag=5",
            },
            [True, 1],
        )
        ha_client = order_redis_ha.RedisHAClient(client)

        result = ha_client.set("key", b"value")

        self.assertTrue(result)
        self.assertEqual(client.pipeline_instance.commands[-1], ("wait", (1, order_redis_ha.WAIT_TIMEOUT_MS), {}))

    def test_set_skips_wait_without_healthy_replicas(self):
        client = FakeRedis(
            {
                "connected_slaves": 2,
                "slave0": "state=online,lag=3",
                "slave1": "state=offline,lag=0",
            },
            [True],
        )
        ha_client = order_redis_ha.RedisHAClient(client)

        result = ha_client.set("key", b"value")

        self.assertTrue(result)
        self.assertEqual(client.pipeline_instance.commands, [("set", ("key", b"value"), {})])

    def test_set_raises_when_wait_falls_short(self):
        client = FakeRedis(
            {
                "connected_slaves": 2,
                "slave0": "state=online,lag=0",
                "slave1": "state=online,lag=0",
            },
            [True, 1],
        )
        ha_client = order_redis_ha.RedisHAClient(client)

        with self.assertRaises(order_redis_ha.redis.exceptions.TimeoutError):
            ha_client.set("key", b"value")

    def test_retry_disconnects_stale_connections(self):
        attempts = {"count": 0}
        client = FakeRedis({"connected_slaves": 0}, [True])
        ha_client = order_redis_ha.RedisHAClient(client)

        def flaky():
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise order_redis_ha.redis.exceptions.ReadOnlyError("stale master")
            return "ok"

        with patch.object(order_redis_ha.time, "sleep", return_value=None):
            result = ha_client._call_with_failover(flaky)

        self.assertEqual(result, "ok")
        self.assertEqual(client.connection_pool.disconnected, 1)


if __name__ == "__main__":
    unittest.main()
