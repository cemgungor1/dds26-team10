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

class TestCredits(unittest.TestCase):
    # Basic HTTP Credits Testing

    def test_new_user_starts_with_zero_credit(self):
        user: dict = tu.create_user()
        self.assertIn('user_id', user)
        self.assertEqual(tu.find_user(user['user_id'])['credit'], 0)

    def test_add_funds_increases_credit(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        r = tu.add_credit_to_user(user_id, 100)
        self.assertTrue(tu.status_code_is_success(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_add_funds_multiple_times_accumulates(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        tu.add_credit_to_user(user_id, 30)
        tu.add_credit_to_user(user_id, 20)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_pay_deducts_credit_correctly(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        r = tu.payment_pay(user_id, 40)
        self.assertTrue(tu.status_code_is_success(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 60)

    def test_pay_exact_amount_leaves_zero(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        r = tu.payment_pay(user_id, 50)
        self.assertTrue(tu.status_code_is_success(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 0)

    def test_pay_more_than_credit_returns_failure(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        r = tu.payment_pay(user_id, 100)
        self.assertTrue(tu.status_code_is_failure(r))

    def test_credit_unchanged_after_failed_payment(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        tu.payment_pay(user_id, 100)
        self.assertEqual(tu.find_user(user_id)['credit'], 50)

    def test_pay_with_zero_credit_returns_failure(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        r = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_failure(r))

    def test_pay_zero_amount_does_not_change_credit(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        tu.payment_pay(user_id, 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_add_zero_funds_does_not_change_credit(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        tu.add_credit_to_user(user_id, 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_large_credit_stored_and_deducted_correctly(self):
        # Does it work with large values?
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 10_000_000)
        self.assertEqual(tu.find_user(user_id)['credit'], 10_000_000)
        tu.payment_pay(user_id, 9_999_999)
        self.assertEqual(tu.find_user(user_id)['credit'], 1)

    def test_find_nonexistent_user_returns_failure(self):
        r = requests.get(f"{PAYMENT_URL}/payment/find_user/nonexistent-xyz")
        self.assertEqual(r.status_code, 400)

    def test_add_funds_nonexistent_user_returns_failure(self):
        r = tu.add_credit_to_user("nonexistent-xyz", 10)
        self.assertTrue(tu.status_code_is_failure(r))

    def test_pay_nonexistent_user_returns_failure(self):
        r = tu.payment_pay("nonexistent-xyz", 10)
        self.assertTrue(tu.status_code_is_failure(r))


