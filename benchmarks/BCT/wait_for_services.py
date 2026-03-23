import sys
import time
import requests

URLS = [
    "http://127.0.0.1:8000/orders/health",
    "http://127.0.0.1:8000/payment/health",
    "http://127.0.0.1:8000/stock/health",
]

TIMEOUT_SECONDS = 60


def wait_for(url: str, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(1)
    return False


def main() -> int:
    for url in URLS:
        print(f"Waiting for {url} ...")
        if not wait_for(url, TIMEOUT_SECONDS):
            print(f"Timed out waiting for {url}")
            return 1
        print(f"Ready: {url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())