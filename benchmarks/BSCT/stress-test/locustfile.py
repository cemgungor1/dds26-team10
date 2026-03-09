import os.path
import random
import json
import subprocess
import time
import logging

from locust import HttpUser, SequentialTaskSet, constant, task, between

from init_orders import NUMBER_OF_ORDERS


# replace the example urls and ports with the appropriate ones
with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']

# Failure tests
COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

### Utility functions ###

def get_compose_services() -> set[str]:
    out = subprocess.check_output(
        ["docker", "ps", "--format", "{{.Names}}"],
        text=True,
    )
    return {s.strip() for s in out.splitlines() if s.strip()}

def resolve_service(service_name: str) -> str:
    services = get_compose_services()
    matches = [s for s in services if service_name in s]
    if len(matches) != 1:
        raise RuntimeError(f"Could not uniquely resolve '{service_name}' from {services}")
    return matches[0]

def stop_service(service: str):
    subprocess.run(
        ["docker", "stop", service], 
        cwd=COMPOSE_DIR,
        check=True
    )
    time.sleep(2)

def start_service(service: str):
    subprocess.run(
        ["docker", "start", service], 
        cwd=COMPOSE_DIR,
        check=True
    )
    time.sleep(3)


COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

STOCK_SERVICE = resolve_service("stock-service")
PAYMENT_SERVICE = resolve_service("payment-service")
ORDER_SERVICE = resolve_service("order-service")

### Basic Tasks ###
class CreateAndCheckoutOrder(SequentialTaskSet):
    @task
    def user_checks_out_order(self):
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}", name="/orders/checkout/[order_id]",
                              catch_response=True) as response:
            if 400 <= response.status_code < 500:
                response.failure(response.text)
            else:
                response.success()

### Failure Tasks ###
class CheckoutWithStockDown(SequentialTaskSet):
    # Test to try to checkout while the stock service is down
    def on_start(self):
        r = self.client.post(
            f"{PAYMENT_URL}/payment/create_user",
            name="/payment/create_user/[failure-test]",
        )
        if r.status_code == 200:
            self.user_id = r.json().get("user_id")
            self.client.post(
                f"{PAYMENT_URL}/payment/add_funds/{self.user_id}/99999",
                name="/payment/add_funds/[failure-test]",
            )
        else:
            self.user_id = None

    @task
    def checkout_stock_down(self):
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)

        # Stop the service stock-service
        stop_service(STOCK_SERVICE)
        try:
            with self.client.post(
                f"{ORDER_URL}/orders/checkout/{order_id}",
                name="/orders/checkout/[stock-down]",
                catch_response=True,
            ) as response:
                if response.status_code == 500:
                    response.failure("Ungraceful crash during stock outage: 500")
                else:
                    response.success()

            # Verify that credit is the same
            if self.user_id:
                r = self.client.get(
                    f"{PAYMENT_URL}/payment/find_user/{self.user_id}",
                    name="/payment/find_user/[failure-verify]",
                )
                if r.status_code == 200:
                    credit = r.json().get("credit", 0)
                    if credit != 99999:
                        logger.error(
                            f"INCONSISTENCY: credit changed during stock outage: {credit}"
                        )
        finally:
            start_service(STOCK_SERVICE)

class CheckoutWithPaymentDown(SequentialTaskSet):
    # Test to try to checkout while the payment service is down
    def on_start(self):
        r = self.client.post(
            f"{STOCK_URL}/stock/item/create/5",
            name="/stock/item/create/[failure-test]",
        )
        if r.status_code == 200:
            self.item_id = r.json().get("item_id")
            self.client.post(
                f"{STOCK_URL}/stock/add/{self.item_id}/99999",
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
                f"{STOCK_URL}/stock/find/{self.item_id}",
                name="/stock/find/[failure-verify]",
            )
            if r.status_code == 200:
                stock_before = r.json().get("stock")

        # Stop the payment service
        stop_service(PAYMENT_SERVICE)
        try:
            with self.client.post(
                f"{ORDER_URL}/orders/checkout/{order_id}",
                name="/orders/checkout/[payment-down]",
                catch_response=True
            ) as response:
                if response.status_code == 500:
                    response.failure("Ungraceful crash during payment outage: 500")
                else:
                    response.success()

            # Verify stock rolled back
            if self.item_id and stock_before is not None:
                r = self.client.get(
                    f"{STOCK_URL}/stock/find/{self.item_id}",
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
            start_service(PAYMENT_SERVICE)

class CheckoutAfterRecovery(SequentialTaskSet):
    # Test to see if checkout works the same after stock recovery
    @task
    def stock_recovery_checkout(self):
        stop_service(STOCK_SERVICE)
        start_service(STOCK_SERVICE)

        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/checkout/{order_id}",
            name="/orders/checkout/[post-stock-recovery]",
            catch_response=True,
        ) as response:
            if response.status_code == 500:
                response.failure("System did not recover from stock outage: 500")
            else:
                response.success()

    @task
    def payment_recovery_checkout(self):
        stop_service(PAYMENT_SERVICE)
        start_service(PAYMENT_SERVICE)

        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/checkout/{order_id}",
            name="/orders/checkout/[post-payment-recovery]",
            catch_response=True,
        ) as response:
            if response.status_code == 500:
                response.failure("System did not recover from payment outage: 500")
            else:
                response.success()

### Basic User ###
class MicroservicesUser(HttpUser):
    # how much time a user waits (seconds) to run another TaskSequence (you could also use between (start, end))
    wait_time = constant(1)
    # [SequentialTaskSet]: [weight of the SequentialTaskSet]
    tasks = {
        CreateAndCheckoutOrder: 100
    }

### User to create Failures ###
class FailureResilienceUser(HttpUser):
    wait_time = between(5, 10)
    tasks = {
        CheckoutWithStockDown:   1,
        CheckoutWithPaymentDown: 1,
        CheckoutAfterRecovery:   1,
    }
