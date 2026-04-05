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

class TestCoordinatorFailure(unittest.TestCase):

    def _kill_order_service(self):
        subprocess.run(
            ["docker", "compose", "stop", "order-service"],
            cwd=COMPOSE_DIR, 
            check=True
        )
        time.sleep(20)

    def _start_order_service(self):
        subprocess.run(
            ["docker", "compose", "start", "order-service"],
            cwd=COMPOSE_DIR, 
            check=True
        )
        time.sleep(20)  

    def test_crash_before_commit_point(self):
        # Create item
        item = tu.create_item(5)
        # Add stocks to item
        tu.add_stock(item['item_id'], 50)
        # Create user
        user = tu.create_user()
        # Add credit to user
        tu.add_credit_to_user(user['user_id'], 100)
        # Create order for user
        order = tu.create_order(user['user_id'])
        # Add item to order
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        stock_before = tu.find_item(item['item_id'])['stock']
        credit_before = tu.find_user(user['user_id'])['credit']

        # Simulate crash by killing order service
        self._kill_order_service()
        self._start_order_service()

        # After recovery state must be unchanged
        stock_after = tu.find_item(item['item_id'])['stock']
        credit_after = tu.find_user(user['user_id'])['credit']

        self.assertEqual(stock_after, stock_before,
            "Stock changed after coordinator crash before commit point")
        self.assertEqual(credit_after, credit_before,
            "Credit changed after coordinator crash before commit point")

    def test_system_works_after_coordinator_recovery(self):
        self._kill_order_service()
        self._start_order_service()

        # Create item
        item = tu.create_item(5)
        # Add stocks to item
        tu.add_stock(item['item_id'], 20)
        # Create user
        user = tu.create_user()
        # Add credit to user
        tu.add_credit_to_user(user['user_id'], 100)
        # Create an order
        order = tu.create_order(user['user_id'])
        # Add an item to the order
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        # Checkout the order
        response = tu.checkout_order(order['order_id'])
        self.assertTrue(
            tu.status_code_is_success(response.status_code),
            f"Checkout failed after coordinator recovery: {response.text}"
        )

        self.assertEqual(tu.find_item(item['item_id'])['stock'], 19)
        self.assertEqual(tu.find_user(user['user_id'])['credit'], 95)


