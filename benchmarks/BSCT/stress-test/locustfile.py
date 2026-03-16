import os.path
import random
import json
import subprocess
import time
import logging

from locust import HttpUser, SequentialTaskSet, constant, task, between

from init_orders import NUMBER_OF_ORDERS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def stop_service(name: str):
    try:
        subprocess.run(
            ["docker", "compose", "stop", name],
            cwd=COMPOSE_DIR, check=True, capture_output=True
        )
        time.sleep(2)
        logger.info(f"Stopped {name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to stop {name}: {e.stderr.decode()}")


def start_service(name: str):
    try:
        subprocess.run(
            ["docker", "compose", "start", name],
            cwd=COMPOSE_DIR, check=True, capture_output=True
        )
        time.sleep(4)
        logger.info(f"Started {name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start {name}: {e.stderr.decode()}")


class CreateAndCheckoutOrder(SequentialTaskSet):
    @task
    def user_checks_out_order(self):
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"/orders/checkout/{order_id}",
            name="/orders/checkout/[order_id]",
            catch_response=True
        ) as response:
            if 400 <= response.status_code < 500:
                response.failure(response.text)
            else:
                response.success()


class CheckoutWithStockDown(SequentialTaskSet):
    def on_start(self):
        r = self.client.post(
            "/payment/create_user",
            name="/payment/create_user/[failure-test]",
        )
        if r.status_code == 200:
            self.user_id = r.json().get("user_id")
            self.client.post(
                f"/payment/add_funds/{self.user_id}/99999",
                name="/payment/add_funds/[failure-test]",
            )
        else:
            self.user_id = None

    @task
    def checkout_stock_down(self):
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        stop_service("stock-service")
        try:
            with self.client.post(
                f"/orders/checkout/{order_id}",
                name="/orders/checkout/[stock-down]",
                catch_response=True,
            ) as response:
                if response.status_code == 500:
                    response.failure("Ungraceful crash during stock outage: 500")
                else:
                    response.success()

            if self.user_id:
                r = self.client.get(
                    f"/payment/find_user/{self.user_id}",
                    name="/payment/find_user/[failure-verify]",
                )
                if r.status_code == 200:
                    credit = r.json().get("credit", 0)
                    if credit != 99999:
                        logger.error(
                            f"INCONSISTENCY: credit changed during stock outage: {credit}"
                        )
        finally:
            start_service("stock-service")


class CheckoutWithPaymentDown(SequentialTaskSet):
    def on_start(self):
        r = self.client.post(
            "/stock/item/create/5",
            name="/stock/item/create/[failure-test]",
        )
        if r.status_code == 200:
            self.item_id = r.json().get("item_id")
            self.client.post(
                f"/stock/add/{self.item_id}/99999",
                name="/stock/add/[failure-test]",
            )
        else:
            self.item_id = None

    @task
    def checkout_payment_down(self):
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)

        stock_before = None
        if self.item_id:
            r = self.client.get(
                f"/stock/find/{self.item_id}",
                name="/stock/find/[failure-verify]",
            )
            if r.status_code == 200:
                stock_before = r.json().get("stock")

        stop_service("payment-service")
        try:
            with self.client.post(
                f"/orders/checkout/{order_id}",
                name="/orders/checkout/[payment-down]",
                catch_response=True,
            ) as response:
                if response.status_code == 500:
                    response.failure("Ungraceful crash during payment outage: 500")
                else:
                    response.success()

            if self.item_id and stock_before is not None:
                r = self.client.get(
                    f"/stock/find/{self.item_id}",
                    name="/stock/find/[failure-verify]",
                )
                if r.status_code == 200:
                    stock_after = r.json().get("stock")
                    if stock_after != stock_before:
                        logger.error(
                            f"INCONSISTENCY: stock not rolled back during payment outage: "
                            f"before={stock_before}, after={stock_after}"
                        )
        finally:
            start_service("payment-service")


class CheckoutAfterRecovery(SequentialTaskSet):
    @task
    def stock_recovery_checkout(self):
        stop_service("stock-service")
        start_service("stock-service")
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"/orders/checkout/{order_id}",
            name="/orders/checkout/[post-stock-recovery]",
            catch_response=True,
        ) as response:
            if response.status_code == 500:
                response.failure("System did not recover from stock outage: 500")
            else:
                response.success()

    @task
    def payment_recovery_checkout(self):
        stop_service("payment-service")
        start_service("payment-service")
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"/orders/checkout/{order_id}",
            name="/orders/checkout/[post-payment-recovery]",
            catch_response=True,
        ) as response:
            if response.status_code == 500:
                response.failure("System did not recover from payment outage: 500")
            else:
                response.success()


class MicroservicesUser(HttpUser):
    wait_time = constant(1)
    tasks = {CreateAndCheckoutOrder: 100}


class FailureResilienceUser(HttpUser):
    wait_time = between(5, 10)
    tasks = {
        CheckoutWithStockDown:   1,
        CheckoutWithPaymentDown: 1,
        CheckoutAfterRecovery:   1,
    }
