import unittest
import subprocess
import requests
import time
import os
import threading

import utils as tu

COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

PAYMENT_URL = "http://127.0.0.1:8000"

ROLLBACK_TIMEOUT = 15 # seconds to wait for async
class TestFailureResilience(unittest.TestCase):

    def _stop_service(self, service: str):
        subprocess.run(
            ["docker", "compose", "stop", service], 
            cwd=COMPOSE_DIR,
            check=True
        )
        time.sleep(5)

    def _start_service(self, service: str):
        subprocess.run(
            ["docker", "compose", "start", service], 
            cwd=COMPOSE_DIR,
            check=True
        )
        time.sleep(5)

    def test_stock_service_down(self):
        # Create user
        user = tu.create_user()

        # Add credit to user
        tu.add_credit_to_user(user['user_id'], 100)

        # Create order for the user
        order = tu.create_order(user['user_id'])

        # Try to checkout while service is down
        self._stop_service("stock-service")
        try:
            response = tu.checkout_order(order['order_id'])
            self.assertTrue(response.status_code, 503)

            # Credit must be the same
            credit = tu.find_user(user['user_id'])['credit']
            self.assertEqual(credit, 100)
        finally:
            # Start back the stock service
            self._start_service("stock-service")

    def test_payment_service_down(self):
        # Create the item
        item = tu.create_item(5)
        # Add stocks to the item
        tu.add_stock(item['item_id'], 50)
        # Create a user
        user = tu.create_user()
        # Create order for the user
        order = tu.create_order(user['user_id'])
        # Add the item to the order
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        stock_before = tu.find_item(item['item_id'])['stock']

        self._stop_service("payment-service")
        try:
            response = tu.checkout_order(order['order_id'])
            self.assertTrue(tu.status_code_is_failure(response.status_code))

        finally:
            # Start back the payment service
            self._start_service("payment-service")
        
        time.sleep(15) # wait for rollback
        stock_after = tu.find_item(item['item_id'])['stock']
        self.assertEqual(stock_after, stock_before)
    def test_stock_service_recovers(self):
        # Restart stock service
        self._stop_service("stock-service")
        self._start_service("stock-service")

        # Should work after recovery
        item = tu.create_item(5)
        tu.add_stock(item['item_id'], 10)
        stock = tu.find_item(item['item_id'])['stock']
        self.assertEqual(stock, 10)


