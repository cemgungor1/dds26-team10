import json
import time
from typing import Any

import redis


class Storage:
    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        tx_prefix: str = "orch:tx:",
        log_prefix: str = "orch:log:",
        notify_prefix: str = "orch:notify:",
        lock_prefix: str = "orch:lock:",
        notify_ttl: int = 60,
        completed_ttl: int = 3600,
        lock_ttl: int = 60,
    ) -> None:
        self.db = redis_client
        self.tx_prefix = tx_prefix
        self.log_prefix = log_prefix
        self.notify_prefix = notify_prefix
        self.lock_prefix = lock_prefix
        self.notify_ttl = notify_ttl
        self.completed_ttl = completed_ttl
        self.lock_ttl = lock_ttl

    def _tx_key(self, tx_id: str) -> str:
        return f"{self.tx_prefix}{tx_id}"

    def _log_key(self, tx_id: str) -> str:
        return f"{self.log_prefix}{tx_id}"

    def _notify_key(self, tx_id: str) -> str:
        return f"{self.notify_prefix}{tx_id}"

    def _lock_key(self, tx_id: str) -> str:
        return f"{self.lock_prefix}{tx_id}"

    def get_tx(self, tx_id: str) -> dict[str, Any] | None:
        try:
            raw = self.db.get(self._tx_key(tx_id))
        except redis.exceptions.RedisError:
            return None
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def set_tx(self, tx: dict[str, Any]) -> bool:
        tx["last_updated"] = time.time()
        try:
            self.db.set(self._tx_key(tx["transaction_id"]), json.dumps(tx))
            return True
        except redis.exceptions.RedisError:
            return False

    def append_log(self, tx_id: str, entry: dict[str, Any]) -> None:
        payload = dict(entry)
        payload["transaction_id"] = tx_id
        payload["timestamp"] = time.time()
        try:
            self.db.rpush(self._log_key(tx_id), json.dumps(payload))
        except redis.exceptions.RedisError:
            pass

    def get_logs(self, tx_id: str) -> list[dict[str, Any]]:
        try:
            raw_entries = self.db.lrange(self._log_key(tx_id), 0, -1)
        except redis.exceptions.RedisError:
            return []
        out: list[dict[str, Any]] = []
        for raw in raw_entries:
            try:
                out.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        return out

    def notify_done(self, tx_id: str, result: str) -> None:
        try:
            self.db.rpush(self._notify_key(tx_id), result)
            self.db.expire(self._notify_key(tx_id), self.notify_ttl)
        except redis.exceptions.RedisError:
            pass

    def wait_for_done(self, tx_id: str, timeout_s: float) -> tuple[bool, str]:
        tx = self.get_tx(tx_id)
        if tx:
            if tx.get("status") == "completed":
                return True, ""
            if tx.get("status") == "failed":
                return False, tx.get("reason", "Transaction failed")

        try:
            result = self.db.blpop(self._notify_key(tx_id), timeout=int(timeout_s))
        except redis.exceptions.RedisError:
            result = None

        if result is not None:
            _, value = result
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            if value == "completed":
                return True, ""
            if value.startswith("failed:"):
                return False, value[7:]
            return False, value

        tx = self.get_tx(tx_id)
        if tx:
            if tx.get("status") == "completed":
                return True, ""
            if tx.get("status") == "failed":
                return False, tx.get("reason", "Transaction failed")
        return False, "Transaction timed out"

    def acquire_lock(self, tx_id: str, token: str) -> bool:
        try:
            return bool(self.db.set(self._lock_key(tx_id), token, ex=self.lock_ttl, nx=True))
        except redis.exceptions.RedisError:
            return False

    def refresh_lock(self, tx_id: str, token: str) -> None:
        lock_key = self._lock_key(tx_id)
        for _ in range(3):
            try:
                with self.db.pipeline() as pipe:
                    pipe.watch(lock_key)
                    current = pipe.get(lock_key)
                    if current and current.decode("utf-8") != token:
                        pipe.unwatch()
                        return
                    pipe.multi()
                    pipe.set(lock_key, token, ex=self.lock_ttl)
                    pipe.execute()
                    return
            except redis.exceptions.WatchError:
                continue
            except redis.exceptions.RedisError:
                return

    def release_lock(self, tx_id: str, token: str | None) -> None:
        if not token:
            return
        lock_key = self._lock_key(tx_id)
        for _ in range(3):
            try:
                with self.db.pipeline() as pipe:
                    pipe.watch(lock_key)
                    current = pipe.get(lock_key)
                    if not current or current.decode("utf-8") != token:
                        pipe.unwatch()
                        return
                    pipe.multi()
                    pipe.delete(lock_key)
                    pipe.execute()
                    return
            except redis.exceptions.WatchError:
                continue
            except redis.exceptions.RedisError:
                return

    def expire_tx(self, tx_id: str) -> None:
        try:
            self.db.expire(self._tx_key(tx_id), self.completed_ttl)
            self.db.expire(self._log_key(tx_id), self.completed_ttl)
        except redis.exceptions.RedisError:
            pass

    def list_stale_txs(self, older_than_s: float) -> list[dict[str, Any]]:
        now = time.time()
        out: list[dict[str, Any]] = []
        try:
            for key in self.db.scan_iter(match=f"{self.tx_prefix}*"):
                raw = self.db.get(key)
                if not raw:
                    continue
                tx = json.loads(raw)
                if tx.get("status") in ("completed", "failed"):
                    continue
                if now - tx.get("last_updated", 0) > older_than_s:
                    out.append(tx)
        except Exception:
            return out
        return out