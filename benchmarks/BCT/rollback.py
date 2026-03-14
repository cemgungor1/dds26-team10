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
class TestRollback(unittest.TestCase):
    
    def _wait_for_rollback(self, seconds: int = ROLLBACK_TIMEOUT):
        time.sleep(seconds)
 
    def _stop_service(self, service: str):
        subprocess.run(
            ["docker", "compose", "stop", service],
            cwd=COMPOSE_DIR,
            check=True,
        )
        time.sleep(5)
 
    def _start_service(self, service: str):
        subprocess.run(
            ["docker", "compose", "start", service],
            cwd=COMPOSE_DIR,
            check=True,
        )
        time.sleep(5)
 
    def test_success_commits_all_resources(self):
        # Basic SAGA checkout (baseline)
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)
 
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 20)
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 3)  # cost = 30
 
        response = tu.checkout_order(order_id)
        self.assertTrue(
            tu.status_code_is_success(response.status_code),
            f"Expected successful checkout, got {response.status_code}: {response.text}",
        )
 
        self.assertEqual(tu.find_item(item_id)["stock"], 17)   # 20 - 3
        self.assertEqual(tu.find_user(user_id)["credit"], 70)  # 100 - 30
        self.assertTrue(tu.find_order(order_id)["paid"])
 
    def test_rolls_back_stock_when_payment_fails(self):
        # SAGA doomed to fail, user does not have enough credits
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)  # not enough
 
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 50)
 
        initial_stock = tu.find_item(item_id)["stock"]
        initial_credit = tu.find_user(user_id)["credit"]
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)  # cost = 20 > 5
 
        response = tu.checkout_order(order_id)
        self.assertTrue(
            tu.status_code_is_failure(response.status_code),
            f"Expected failure, got {response.status_code}",
        )
 
        self._wait_for_rollback()
 
        self.assertEqual(
            tu.find_item(item_id)["stock"],
            initial_stock,
            "Stock was not restored after payment failure",
        )
        self.assertEqual(
            tu.find_user(user_id)["credit"],
            initial_credit,
            "Credit must not change when checkout fails",
        )
        self.assertFalse(tu.find_order(order_id)["paid"])
 
    def test_does_not_charge_payment_when_stock_fails(self):
        # SAGA doomed to fail, not enough stocks
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 1000)
 
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 1)  # only 1 in stock
 
        initial_credit = tu.find_user(user_id)["credit"]
        initial_stock = tu.find_item(item_id)["stock"]
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)  # requests 5, only 1 available
 
        response = tu.checkout_order(order_id)
        self.assertTrue(
            tu.status_code_is_failure(response.status_code),
            f"Expected failure, got {response.status_code}",
        )
 
        self._wait_for_rollback()
 
        self.assertEqual(
            tu.find_user(user_id)["credit"],
            initial_credit,
            "Credit must not be deducted when stock step fails",
        )
        self.assertEqual(
            tu.find_item(item_id)["stock"],
            initial_stock,
            "Stock must be restored after failed subtraction",
        )
        self.assertFalse(tu.find_order(order_id)["paid"])
 
    def test_rolls_back_all_items_on_payment_failure(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 1)  # nearly nothing
 
        items = []
        for price in [10, 20, 30]:
            item = tu.create_item(price)
            tu.add_stock(item["item_id"], 100)
            items.append(item["item_id"])
 
        initial_stocks = {iid: tu.find_item(iid)["stock"] for iid in items}
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        for iid in items:
            tu.add_item_to_order(order_id, iid, 1)  # total = 60 > 1
 
        response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(response.status_code))
 
        self._wait_for_rollback()
 
        for iid in items:
            self.assertEqual(
                tu.find_item(iid)["stock"],
                initial_stocks[iid],
                f"Stock for item {iid} was not fully restored",
            )
        self.assertEqual(tu.find_user(user_id)["credit"], 1)
        self.assertFalse(tu.find_order(order_id)["paid"])
 
    def test_exact_credit_succeeds_second_checkout_rolls_back(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 50)
 
        item = tu.create_item(50)
        item_id = item["item_id"]
        tu.add_stock(item_id, 10)
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)
 
        # First checkout should succeed
        r = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(r.status_code))
        self.assertEqual(tu.find_user(user_id)["credit"], 0)
 
        # Second checkout on a new order must fail (credit = 0) and not go negative
        order2 = tu.create_order(user_id)
        order_id2 = order2["order_id"]
        tu.add_item_to_order(order_id2, item_id, 1)
 
        r2 = tu.checkout_order(order_id2)
        self.assertTrue(tu.status_code_is_failure(r2.status_code))
 
        self._wait_for_rollback()
 
        self.assertEqual(
            tu.find_user(user_id)["credit"],
            0,
            "Credit must stay 0 — must not go negative after failed second checkout",
        )
 
    def test_rolls_back_stock_when_payment_service_down(self):
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 30)
 
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 200)
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)
 
        initial_stock = tu.find_item(item_id)["stock"]
        initial_credit = tu.find_user(user_id)["credit"]
 
        # STOP the payment service
        self._stop_service("payment-service")
        try:
            response = tu.checkout_order(order_id)
            self.assertTrue(
                tu.status_code_is_failure(response.status_code),
                f"Expected failure with payment service down, got {response.status_code}",
            )
        finally:
            self._start_service("payment-service")
 
        self._wait_for_rollback()
 
        self.assertEqual(
            tu.find_item(item_id)["stock"],
            initial_stock,
            "Stock must be restored when payment service was down",
        )
        self.assertEqual(
            tu.find_user(user_id)["credit"],
            initial_credit,
            "Credit must be unchanged when payment service was down",
        )
        self.assertFalse(tu.find_order(order_id)["paid"])
 
    def test_does_not_charge_when_stock_service_down(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
 
        initial_credit = tu.find_user(user_id)["credit"]
 
        # STOP the stock service
        self._stop_service("stock-service")
        try:
            response = tu.checkout_order(order_id)
            self.assertTrue(
                tu.status_code_is_failure(response.status_code),
                f"Expected failure with stock service down, got {response.status_code}",
            )
        finally:
            self._start_service("stock-service")
 
        self._wait_for_rollback()
 
        self.assertEqual(
            tu.find_user(user_id)["credit"],
            initial_credit,
            "Credit must not be charged when stock service was down",
        )
        self.assertFalse(tu.find_order(order_id)["paid"])
 
    def test_duplicate_checkout_does_not_double_charge(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)
 
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 20)
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)  # cost = 10
 
        # First checkout succeeds
        r1 = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(r1.status_code))
 
        stock_after_first = tu.find_item(item_id)["stock"]   # 19
        credit_after_first = tu.find_user(user_id)["credit"]  # 90
 
        # Second checkout on the same order
        r2 = tu.checkout_order(order_id)
 
        self._wait_for_rollback()
 
        # Resources must be identical to after the first checkout
        self.assertEqual(
            tu.find_item(item_id)["stock"],
            stock_after_first,
            "Stock was deducted a second time on duplicate checkout",
        )
        self.assertEqual(
            tu.find_user(user_id)["credit"],
            credit_after_first,
            "Credit was deducted a second time on duplicate checkout",
        )
 
    def test_order_remains_unpaid_after_rollback(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 1)  # too little
 
        item = tu.create_item(50)
        item_id = item["item_id"]
        tu.add_stock(item_id, 10)
 
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)
 
        # Checkout fails
        r = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(r.status_code))
        self._wait_for_rollback()
        self.assertFalse(tu.find_order(order_id)["paid"])
 
        # Add more credit and retry, must now succeed
        tu.add_credit_to_user(user_id, 100)
        r2 = tu.checkout_order(order_id)
        self.assertTrue(
            tu.status_code_is_success(r2.status_code),
            f"Retry after top-up should succeed: {r2.text}",
        )
        self.assertTrue(tu.find_order(order_id)["paid"])
        self.assertEqual(tu.find_item(item_id)["stock"], 9)
 
    def test_system_healthy_after_rollback(self):
        # After a rollback, make sure the system is okay to use

        # First: trigger a rollback
        bad_user = tu.create_user()
        bad_item = tu.create_item(100)
        tu.add_stock(bad_item["item_id"], 5)
        bad_order = tu.create_order(bad_user["user_id"])
        tu.add_item_to_order(bad_order["order_id"], bad_item["item_id"], 1)
        r = tu.checkout_order(bad_order["order_id"])
        self.assertTrue(tu.status_code_is_failure(r.status_code))
        self._wait_for_rollback()
 
        # Now: a checkout must succeed
        good_user = tu.create_user()
        good_user_id = good_user["user_id"]
        tu.add_credit_to_user(good_user_id, 200)
 
        good_item = tu.create_item(20)
        good_item_id = good_item["item_id"]
        tu.add_stock(good_item_id, 10)
 
        good_order = tu.create_order(good_user_id)
        good_order_id = good_order["order_id"]
        tu.add_item_to_order(good_order_id, good_item_id, 2)  # cost = 40
 
        r2 = tu.checkout_order(good_order_id)
        self.assertTrue(
            tu.status_code_is_success(r2.status_code),
            f"System should be healthy after a prior rollback: {r2.text}",
        )
        self.assertEqual(tu.find_item(good_item_id)["stock"], 8)
        self.assertEqual(tu.find_user(good_user_id)["credit"], 160)
        self.assertTrue(tu.find_order(good_order_id)["paid"])


