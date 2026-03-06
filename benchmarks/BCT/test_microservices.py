import unittest
import subprocess
import time
import requests
import os.path

import utils as tu

# Path to docker compose
# Assume you run it in the same directory as the test_microservices.py
COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..",))

class TestFailureResilience(unittest.TestCase):
    def _stop_service(self, service: str):
        try:
            subprocess.run(
                ["docker", "compose", "stop", service],
                cwd=COMPOSE_DIR,
                check=True,
                capture_output=True
            )
            time.sleep(3) # Give time to stop full\y
            print(f"Stopped {service}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to stop {service}: {e.stderr.decode()}")
        time.sleep(3) # Give time to stop full\y

    def _start_service(self, service: str):
        try:
            subprocess.run(
                ["docker", "compose", "start", service],
                cwd=COMPOSE_DIR,
                check=True,
                capture_output=True
            )
            time.sleep(3) # Give time to stop full\y
            print(f"Started {service}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to start {service}: {e.stderr.decode()}")

    def test_stock_service_down(self):
        # Checkout should fail cleanly when stock service is down
        user = tu.create_user()
        tu.add_credit_to_user(user['user_id'], 100)
        order = tu.create_order(user['user_id'])

        self._stop_service("stock-service")
        try:
            response = tu.checkout_order(order['order_id'])
            # Should be 400
            self.assertNotEqual(response.status_code, 500)
            self.assertTrue(tu.status_code_is_failure(response.status_code))

            # Credit must be untouched
            credit = tu.find_user(user['user_id'])['credit']
            self.assertEqual(credit, 100)
        finally:
            self._start_service("stock-service")

    def test_payment_service_down(self):
        # Checkout should rollback stock when payment service is down
        item = tu.create_item(5)
        tu.add_stock(item['item_id'], 50)

        user = tu.create_user()
        order = tu.create_order(user['user_id'])
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        stock_before = tu.find_item(item['item_id'])['stock']

        self._stop_service("payment-service")
        try:
            response = tu.checkout_order(order['order_id'])
            self.assertTrue(tu.status_code_is_failure(response.status_code))

            # Stock must be rolled rolledback
            stock_after = tu.find_item(item['item_id'])['stock']
            self.assertEqual(stock_after, stock_before)
        finally:
            self._start_service("payment-service")

    def test_service_recovers(self):
        # After service restarts, normal operations must resume correctly
        self._stop_service("stock-service")
        self._start_service("stock-service")

        # Full flow work after recovery
        item = tu.create_item(5)
        tu.add_stock(item['item_id'], 10)
        stock = tu.find_item(item['item_id'])['stock']
        self.assertEqual(stock, 10)

class TestMicroservices(unittest.TestCase):

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        # add item to the stock service
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        item_id1: str = item1['item_id']
        add_stock_response = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # add item to the stock service
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2)
        item_id2: str = item2['item_id']
        add_stock_response = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        subtract_stock_response = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 15)

        add_stock_response = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 0)

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(add_credit_response)))

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15)

        stock: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock, 15)

        checkout_response = tu.checkout_order(order_id)
        print(checkout_response.text)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 14)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)


if __name__ == '__main__':
    unittest.main()
