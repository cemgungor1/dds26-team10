#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys

import redis
from msgspec import Struct, msgpack


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump all order DB keys from Redis and decode msgpack payloads."
    )
    parser.add_argument("--host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--password", default=os.getenv("REDIS_PASSWORD", "redis"))
    parser.add_argument("--db", type=int, default=int(os.getenv("REDIS_DB", "0")))
    parser.add_argument("--pattern", default="*")
    parser.add_argument(
        "--container",
        default=os.getenv("ORDER_SERVICE_CONTAINER", ""),
        help="Order service container name for docker fallback (optional).",
    )
    parser.add_argument(
        "--output",
        default="dump_orderredis.txt",
        help="Output txt file path for decoded records.",
    )
    return parser.parse_args()


def decode_with_redis_client(args: argparse.Namespace) -> tuple[list[str], int]:
    client = redis.Redis(
        host=args.host,
        port=args.port,
        password=args.password,
        db=args.db,
        decode_responses=False,
    )
    found = 0
    lines: list[str] = []
    keys = sorted(client.scan_iter(match=args.pattern))
    for raw_key in keys:
        found += 1
        raw_value = client.get(raw_key)
        key = raw_key.decode("utf-8", errors="replace")
        if raw_value is None:
            lines.append(f"{key}: <missing>")
            continue
        try:
            decoded = msgpack.decode(raw_value, type=OrderValue)
            printable = {
                "paid": decoded.paid,
                "items": decoded.items,
                "user_id": decoded.user_id,
                "total_cost": decoded.total_cost,
            }
            lines.append(f"{key}: {json.dumps(printable, ensure_ascii=True)}")
        except Exception as exc:
            lines.append(f"{key}: <decode_error: {exc}>")
    return lines, found


def resolve_order_service_container(explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    out = subprocess.check_output(["docker", "ps", "--format", "{{.Names}}"], text=True)
    candidates = [name.strip() for name in out.splitlines() if "order-service" in name]
    if len(candidates) != 1:
        raise RuntimeError(
            f"Could not uniquely resolve order-service container from running containers: {candidates}"
        )
    return candidates[0]


def decode_via_docker(args: argparse.Namespace) -> tuple[list[str], int]:
    container = resolve_order_service_container(args.container)
    password = args.password
    py_code = f"""
import json
import redis
from msgspec import msgpack
from app import OrderValue

r = redis.Redis(host="order-db", port=6379, password={password!r}, db={args.db})
keys = sorted(r.scan_iter(match={args.pattern!r}))
count = 0

for raw_key in keys:
    count += 1
    key = raw_key.decode("utf-8", "replace")
    raw_value = r.get(raw_key)
    if raw_value is None:
        print(f"{{key}}: <missing>")
        continue
    try:
        decoded = msgpack.decode(raw_value, type=OrderValue)
        printable = {{
            "paid": decoded.paid,
            "items": decoded.items,
            "user_id": decoded.user_id,
            "total_cost": decoded.total_cost,
        }}
        print(f"{{key}}: {{json.dumps(printable, ensure_ascii=True)}}")
    except Exception as exc:
        print(f"{{key}}: <decode_error: {{exc}}>")

print(f"__TOTAL__:{{count}}")
""".strip()
    out = subprocess.check_output(["docker", "exec", container, "python", "-c", py_code], text=True)
    lines = [line for line in out.splitlines() if line and not line.startswith("__TOTAL__:")]
    totals = [line for line in out.splitlines() if line.startswith("__TOTAL__:")]
    found = int(totals[-1].split(":", 1)[1]) if totals else len(lines)
    return lines, found


def main() -> None:
    args = parse_args()
    try:
        lines, found = decode_with_redis_client(args)
    except redis.exceptions.ConnectionError:
        print(
            "Direct Redis connection failed; trying docker exec fallback via order-service container...",
            file=sys.stderr,
        )
        try:
            lines, found = decode_via_docker(args)
        except Exception as exc:
            print(
                "Redis connection failed and docker fallback also failed.\n"
                f"Direct attempt: host={args.host} port={args.port} db={args.db}\n"
                f"Docker fallback error: {exc}",
                file=sys.stderr,
            )
            raise SystemExit(2) from exc

    with open(args.output, "w", encoding="utf-8") as out:
        out.write("\n".join(lines))
        out.write(f"\nTotal keys scanned: {found}\n")
    print(f"Wrote decoded dump to: {args.output} ({found} keys)")


if __name__ == "__main__":
    main()
