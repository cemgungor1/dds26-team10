"""
Client-side sharded Redis wrapper.

Distributes keys across multiple standalone Redis instances using CRC32 hashing.
The routing key is extracted as the segment after the last colon in the key name,
so that related keys (e.g. "order_id" and "saga:state:order_id") always land on
the same shard.  Each shard is a plain redis.Redis instance, so WATCH/MULTI/EXEC,
BLPOP, and every other command work without limitations.
"""

import os
import zlib

import redis


def _routing_key(key) -> bytes:
    """Extract the routing portion of a Redis key.

    For prefixed keys like "saga:state:<id>", returns "<id>" so that all
    keys related to the same entity hash to the same shard.  For plain
    keys (no colon) returns the key itself.
    """
    if isinstance(key, memoryview):
        key = bytes(key)
    if isinstance(key, bytes):
        idx = key.rfind(b":")
        return key[idx + 1:] if idx >= 0 else key
    key_str = str(key)
    idx = key_str.rfind(":")
    segment = key_str[idx + 1:] if idx >= 0 else key_str
    return segment.encode("utf-8")


class ShardedRedis:
    """Drop-in replacement for ``redis.Redis`` that shards across *N* nodes."""

    def __init__(self, nodes: list[dict]):
        """*nodes* is a list of connection-kwarg dicts, one per shard."""
        if not nodes:
            raise ValueError("At least one Redis node is required")
        self.shards: list[redis.Redis] = [redis.Redis(**kw) for kw in nodes]
        self.num_shards: int = len(self.shards)

    # ── shard selection ───────────────────────────────────────────────────
    def _shard_for(self, key) -> redis.Redis:
        return self.shards[zlib.crc32(_routing_key(key)) % self.num_shards]

    def get_shard(self, key) -> redis.Redis:
        """Public accessor – useful when callers need a raw connection."""
        return self._shard_for(key)

    # ── single-key commands ───────────────────────────────────────────────
    def get(self, name):
        return self._shard_for(name).get(name)

    def set(self, name, value, **kw):
        return self._shard_for(name).set(name, value, **kw)

    def delete(self, *names):
        ret = 0
        for n in names:
            ret += self._shard_for(n).delete(n)
        return ret

    def exists(self, *names):
        if len(names) == 1:
            return self._shard_for(names[0]).exists(names[0])
        return sum(self._shard_for(n).exists(n) for n in names)

    def expire(self, name, time):
        return self._shard_for(name).expire(name, time)

    def rpush(self, name, *values):
        return self._shard_for(name).rpush(name, *values)

    def lrange(self, name, start, end):
        return self._shard_for(name).lrange(name, start, end)

    def blpop(self, keys, timeout=0):
        if isinstance(keys, (str, bytes)):
            return self._shard_for(keys).blpop(keys, timeout=timeout)
        # Multiple keys – only first key is used for routing (matches usage)
        return self._shard_for(keys[0]).blpop(keys, timeout=timeout)

    def ping(self):
        """Ping all shards; raises on the first failure."""
        for s in self.shards:
            s.ping()
        return True

    # ── multi-key helpers ─────────────────────────────────────────────────
    def mset(self, mapping: dict):
        """Split the mapping by shard and issue one ``mset`` per shard."""
        buckets: dict[int, dict] = {}
        for k, v in mapping.items():
            idx = zlib.crc32(_routing_key(k)) % self.num_shards
            buckets.setdefault(idx, {})[k] = v
        for idx, sub in buckets.items():
            self.shards[idx].mset(sub)

    def scan_iter(self, match=None, count=None, **kw):
        """Iterate over keys across all shards."""
        for shard in self.shards:
            yield from shard.scan_iter(match=match, count=count, **kw)

    # ── pipeline (routes by shard_hint) ───────────────────────────────────
    def pipeline(self, transaction=True, shard_hint=None):
        """Return a pipeline bound to a specific shard.

        *shard_hint* must be a key (or the routing portion of a key) that
        determines which shard the pipeline targets.  When ``None``, a
        ``_LazyShardPipeline`` is returned that picks its shard on the
        first ``watch()`` call – this covers every existing usage pattern.
        """
        if shard_hint is not None:
            return self._shard_for(shard_hint).pipeline(transaction=transaction)
        return _LazyShardPipeline(self, transaction=transaction)

    # ── lifecycle ─────────────────────────────────────────────────────────
    def close(self):
        for s in self.shards:
            s.close()


class _LazyShardPipeline:
    """Pipeline proxy that lazily binds to a shard on the first ``watch()``."""

    def __init__(self, sharded: ShardedRedis, transaction: bool = True):
        self._sharded = sharded
        self._transaction = transaction
        self._pipe: redis.client.Pipeline | None = None

    # -- context manager --------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if self._pipe is not None:
            return self._pipe.__exit__(*exc)

    # -- shard binding ----------------------------------------------------
    def _bind(self, key):
        if self._pipe is None:
            shard = self._sharded._shard_for(key)
            self._pipe = shard.pipeline(transaction=self._transaction)
        return self._pipe

    def watch(self, *names):
        self._bind(names[0])
        return self._pipe.watch(*names)

    # -- delegate everything else -----------------------------------------
    def __getattr__(self, name):
        if self._pipe is not None:
            return getattr(self._pipe, name)
        raise AttributeError(
            f"Pipeline not bound to a shard yet – call watch() first "
            f"or pass shard_hint to pipeline(). (attr={name!r})"
        )


# ── factory ──────────────────────────────────────────────────────────────────

def create_sharded_redis() -> ShardedRedis:
    """Build a ``ShardedRedis`` from environment variables.

    Supports two modes:
      * ``REDIS_HOSTS=host1:port1,host2:port2,...`` – multi-shard
      * ``REDIS_HOST`` / ``REDIS_PORT`` – single-node fallback
    """
    password = os.environ.get("REDIS_PASSWORD", "redis")
    db_num = int(os.environ.get("REDIS_DB", "0"))

    hosts_csv = os.environ.get("REDIS_HOSTS", "")
    if hosts_csv:
        nodes = []
        for entry in hosts_csv.split(","):
            entry = entry.strip()
            if ":" in entry:
                h, p = entry.rsplit(":", 1)
                nodes.append(dict(host=h, port=int(p), password=password, db=db_num))
            else:
                nodes.append(dict(host=entry, port=6379, password=password, db=db_num))
        return ShardedRedis(nodes)

    # Fallback: single node
    return ShardedRedis([dict(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        password=password,
        db=db_num,
    )])
